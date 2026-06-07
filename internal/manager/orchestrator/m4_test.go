package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	rworktree "github.com/agent-parley/parley/internal/runner/worktree"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestIdeaIntakeFreezesVerbatimIdeaIntoContractAndSnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "add a thing\n<script>alert(1)</script>", RefinementLevel: contract.RefinementLevelDeep})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	engine := NewEngineWithOptions(st, nil, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{DataRoot: t.TempDir(), ProjectID: "p1"})
	rep, err := engine.runIdeaIntake(ctx, wr)
	if err != nil {
		t.Fatalf("runIdeaIntake() error = %v", err)
	}
	if rep.Status != "completed" {
		t.Fatalf("status = %s", rep.Status)
	}
	contractID, _ := rep.Payload["task_contract_artifact_id"].(string)
	if contractID == "" {
		t.Fatalf("missing task contract artifact id: %+v", rep.Payload)
	}
	planID, _ := rep.Payload["task_plan_artifact_id"].(string)
	if planID == "" {
		t.Fatalf("missing task plan artifact id: %+v", rep.Payload)
	}
	if rep.Payload["refinement_level"] != contract.RefinementLevelDeep {
		t.Fatalf("refinement level = %v, want %s", rep.Payload["refinement_level"], contract.RefinementLevelDeep)
	}
	_, contractContent, err := st.GetArtifact(ctx, contractID)
	if err != nil {
		t.Fatalf("read task contract: %v", err)
	}
	if !strings.Contains(string(contractContent), wr.Run.Idea) {
		t.Fatalf("task contract did not preserve idea verbatim:\n%s", contractContent)
	}
	_, planContent, err := st.GetArtifact(ctx, planID)
	if err != nil {
		t.Fatalf("read task plan: %v", err)
	}
	planText := string(planContent)
	for _, want := range []string{"# Task Plan", "Refinement level: `deep`", "## Deep Plan", "This artifact is a task plan, not a workflow definition."} {
		if !strings.Contains(planText, want) {
			t.Fatalf("task plan missing %q:\n%s", want, planText)
		}
	}
	loaded, err := st.GetWorkflowRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("get workflow run: %v", err)
	}
	if loaded.IdeaIntakeStage.TaskPlanArtifactID != planID {
		t.Fatalf("stage task plan ref = %s, want %s", loaded.IdeaIntakeStage.TaskPlanArtifactID, planID)
	}

	var rawSnapshot string
	if err := st.DB().QueryRowContext(ctx, `SELECT snapshot_json FROM workflow_snapshots WHERE run_id = ? ORDER BY id DESC LIMIT 1`, wr.Run.ID).Scan(&rawSnapshot); err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	var snapshot map[string]any
	if err := json.Unmarshal([]byte(rawSnapshot), &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snapshot["idea_verbatim"] != wr.Run.Idea || snapshot["frozen"] != true {
		t.Fatalf("snapshot did not freeze idea: %+v", snapshot)
	}
	if snapshot["task_contract_artifact_id"] != contractID {
		t.Fatalf("snapshot task contract id = %v, want %s", snapshot["task_contract_artifact_id"], contractID)
	}
	if snapshot["task_plan_artifact_id"] != planID {
		t.Fatalf("snapshot task plan id = %v, want %s", snapshot["task_plan_artifact_id"], planID)
	}
	if snapshot["refinement_level"] != contract.RefinementLevelDeep {
		t.Fatalf("snapshot refinement level = %v, want %s", snapshot["refinement_level"], contract.RefinementLevelDeep)
	}
	if snapshot["workflow_template_id"] != workflow.DefaultTemplateID {
		t.Fatalf("snapshot workflow template id = %v, want %s", snapshot["workflow_template_id"], workflow.DefaultTemplateID)
	}
	if snapshot["workflow_template_frozen"] != true {
		t.Fatalf("workflow_template_frozen = %v, want true", snapshot["workflow_template_frozen"])
	}
	templateSnapshot, ok := snapshot["workflow_template_snapshot"].(map[string]any)
	if !ok {
		t.Fatalf("snapshot workflow template missing or wrong type: %+v", snapshot["workflow_template_snapshot"])
	}
	if templateSnapshot["id"] != workflow.DefaultTemplateID || templateSnapshot["name"] != "Balanced PR Delivery" {
		t.Fatalf("workflow template snapshot = %+v", templateSnapshot)
	}
}

func TestTaskPlanMarkdownSupportsThreeRefinementLevels(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	cases := []struct {
		level string
		want  string
	}{
		{contract.RefinementLevelDirect, "## Direct Plan"},
		{contract.RefinementLevelStandard, "## Standard Plan"},
		{contract.RefinementLevelDeep, "## Deep Plan"},
	}
	for _, tc := range cases {
		t.Run(tc.level, func(t *testing.T) {
			wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "ship a change", RefinementLevel: tc.level})
			if err != nil {
				t.Fatalf("create run: %v", err)
			}
			plan := taskPlanMarkdown(wr)
			if !strings.Contains(plan, "Refinement level: `"+tc.level+"`") || !strings.Contains(plan, tc.want) {
				t.Fatalf("plan for %s missing expected content:\n%s", tc.level, plan)
			}
			if strings.Contains(plan, "workflow template") || strings.Contains(plan, "add workflow stage") {
				t.Fatalf("plan appears to define workflow policy:\n%s", plan)
			}
		})
	}
}

func TestCommitWorktreeUsesWorkerSnapshotAndExcludesValidatorTrackedEdits(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	source := initCommitSourceRepo(t, ctx, map[string]string{
		"go.sum":  "base sum\n",
		"main.go": "package main\n\nfunc main() {}\n",
	})
	worktreePath := addDetachedWorktree(t, ctx, source)
	workerMain := "package main\n\nfunc main() { println(\"worker\") }\n"
	if err := os.WriteFile(filepath.Join(worktreePath, "main.go"), []byte(workerMain), 0o600); err != nil {
		t.Fatalf("write worker main.go: %v", err)
	}
	opts := snapshotCommitOptions(t, ctx, worktreePath, "run_test", "task_test", "artifact_impl")
	if err := os.WriteFile(filepath.Join(worktreePath, "go.sum"), []byte("validation dirtied sum\n"), 0o600); err != nil {
		t.Fatalf("dirty go.sum: %v", err)
	}

	result, err := commitWorktree(ctx, opts)
	if err != nil {
		t.Fatalf("commitWorktree() error = %v", err)
	}
	commitTree := strings.TrimSpace(string(runCommitGitOutput(t, ctx, worktreePath, "show", "-s", "--format=%T", result.CommitSHA)))
	if commitTree != opts.WorkerTreeSHA {
		t.Fatalf("commit tree = %s, want worker tree %s", commitTree, opts.WorkerTreeSHA)
	}
	goSum := string(runCommitGitOutput(t, ctx, worktreePath, "show", result.CommitSHA+":go.sum"))
	if goSum != "base sum\n" {
		t.Fatalf("committed go.sum = %q, want base content", goSum)
	}
	mainGo := string(runCommitGitOutput(t, ctx, worktreePath, "show", result.CommitSHA+":main.go"))
	if mainGo != workerMain {
		t.Fatalf("committed main.go = %q, want worker content", mainGo)
	}
	branchSHA := strings.TrimSpace(string(runCommitGitOutput(t, ctx, worktreePath, "rev-parse", "refs/heads/"+result.Branch)))
	if branchSHA != result.CommitSHA {
		t.Fatalf("branch ref = %s, want %s", branchSHA, result.CommitSHA)
	}
}

func TestCommitWorktreeNoChangesUsesSnapshotNotValidationDirtyWorktree(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	source := initCommitSourceRepo(t, ctx, map[string]string{
		"go.sum":  "base sum\n",
		"main.go": "package main\n\nfunc main() {}\n",
	})
	worktreePath := addDetachedWorktree(t, ctx, source)
	opts := snapshotCommitOptions(t, ctx, worktreePath, "run_no_changes", "task_test", "artifact_impl")
	if opts.WorkerTreeSHA != opts.BaseTreeSHA {
		t.Fatalf("snapshot should have no worker changes: worker=%s base=%s", opts.WorkerTreeSHA, opts.BaseTreeSHA)
	}
	if err := os.WriteFile(filepath.Join(worktreePath, "go.sum"), []byte("validation dirtied sum\n"), 0o600); err != nil {
		t.Fatalf("dirty go.sum: %v", err)
	}

	_, err := commitWorktree(ctx, opts)
	if err == nil || !strings.Contains(err.Error(), "no changes to commit") {
		t.Fatalf("commitWorktree() error = %v, want no changes to commit", err)
	}
}

func TestCommitWorktreeCommitsWorkerDeletion(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	source := initCommitSourceRepo(t, ctx, map[string]string{
		"keep.txt":   "keep\n",
		"delete.txt": "delete\n",
	})
	worktreePath := addDetachedWorktree(t, ctx, source)
	if err := os.Remove(filepath.Join(worktreePath, "delete.txt")); err != nil {
		t.Fatalf("delete worker file: %v", err)
	}
	opts := snapshotCommitOptions(t, ctx, worktreePath, "run_delete", "task_test", "artifact_impl")

	result, err := commitWorktree(ctx, opts)
	if err != nil {
		t.Fatalf("commitWorktree() error = %v", err)
	}
	names := string(runCommitGitOutput(t, ctx, worktreePath, "ls-tree", "-r", "--name-only", result.CommitSHA))
	if strings.Contains(names, "delete.txt") {
		t.Fatalf("deleted file still present in commit tree:\n%s", names)
	}
	if !strings.Contains(names, "keep.txt") {
		t.Fatalf("kept file missing from commit tree:\n%s", names)
	}
}

func TestCommitWorktreeCreatesAgentBranchWithIdentityAndNoHooks(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	source := initCommitSourceRepo(t, ctx, map[string]string{"README.md": "hello\n"})
	worktreePath := addDetachedWorktree(t, ctx, source)
	if err := os.WriteFile(filepath.Join(worktreePath, "feature.txt"), []byte("feature\n"), 0o600); err != nil {
		t.Fatalf("write feature: %v", err)
	}

	sentinel := filepath.Join(t.TempDir(), "hook-ran")
	installFailingHook(t, filepath.Join(source, ".git", "hooks", "pre-commit"), sentinel)
	installFailingHook(t, filepath.Join(source, ".git", "hooks", "commit-msg"), sentinel)
	installFailingHook(t, filepath.Join(source, ".git", "hooks", "prepare-commit-msg"), sentinel)

	opts := snapshotCommitOptions(t, ctx, worktreePath, "run_test", "task_test", "artifact_diff")
	opts.Idea = "Add file\nsecond line"
	opts.ReportSummary = "validation passed"
	opts.AuthorName = "Harness Bot"
	opts.AuthorEmail = "bot@example.invalid"
	result, err := commitWorktree(ctx, opts)
	if err != nil {
		t.Fatalf("commitWorktree() error = %v", err)
	}
	if result.Branch != "agent/run_test/task_test" {
		t.Fatalf("branch = %s", result.Branch)
	}
	branchSHA := strings.TrimSpace(string(runCommitGitOutput(t, ctx, worktreePath, "rev-parse", "refs/heads/"+result.Branch)))
	if branchSHA != result.CommitSHA {
		t.Fatalf("branch ref = %s, want %s", branchSHA, result.CommitSHA)
	}
	identity := strings.TrimSpace(string(runCommitGitOutput(t, ctx, worktreePath, "show", "-s", "--format=%an <%ae>|%cn <%ce>", result.CommitSHA)))
	if identity != "Harness Bot <bot@example.invalid>|Harness Bot <bot@example.invalid>" {
		t.Fatalf("identity = %s", identity)
	}
	message := string(runCommitGitOutput(t, ctx, worktreePath, "show", "-s", "--format=%B", result.CommitSHA))
	for _, want := range []string{"parley: Add file", "validation passed", "Run ID: run_test", "Task ID: task_test", "Diff artifact ID: artifact_diff"} {
		if !strings.Contains(message, want) {
			t.Fatalf("commit message missing %q:\n%s", want, message)
		}
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("repo hook ran or sentinel stat failed: %v", err)
	}
}

func TestRunCommitUsesImplementationDiffArtifact(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	dataRoot := t.TempDir()
	projectID := "p1"
	if _, err := st.EnsureProject(ctx, store.ProjectSpec{ID: projectID, Name: "p1", QueueAutoWhenReady: true, QueueMaxConcurrent: 1, QueueBacklogCap: 100}); err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	wr, err := st.CreateWorkflowRunForProject(ctx, projectID, "record the clean diff")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	source := initCommitSourceRepo(t, ctx, map[string]string{"main.go": "package main\n\nfunc main() {}\n"})
	wt, err := rworktree.Create(ctx, rworktree.CreateOptions{
		DataRoot:   dataRoot,
		ProjectID:  projectID,
		RunID:      wr.Run.ID,
		TaskID:     wr.Task.ID,
		AttemptID:  wr.Attempt.ID,
		SourceRepo: source,
	})
	if err != nil {
		t.Fatalf("create worktree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wt.Path, "main.go"), []byte("package main\n\nfunc main() { println(\"worker\") }\n"), 0o600); err != nil {
		t.Fatalf("write main.go: %v", err)
	}
	engine := NewEngineWithOptions(st, nil, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{
		DataRoot:       dataRoot,
		ProjectID:      projectID,
		GitAuthorName:  "Harness Bot",
		GitAuthorEmail: "bot@example.invalid",
	})
	snapshot, err := engine.snapshotWorktree(ctx, wr, report.Report{Payload: map[string]any{"diff_artifact_id": "implementation_diff"}})
	if err != nil {
		t.Fatalf("snapshotWorktree() error = %v", err)
	}
	validationReport := report.Report{Summary: "validation passed", Payload: map[string]any{"diff_artifact_id": "validation_diff"}}

	rep, err := engine.runCommit(ctx, wr, validationReport, snapshot, nil)
	if err != nil {
		t.Fatalf("runCommit() error = %v", err)
	}
	if got := payloadString(rep.Payload, "diff_artifact_id"); got != "implementation_diff" {
		t.Fatalf("commit diff_artifact_id = %s, want implementation_diff", got)
	}
	if len(rep.EvidenceRefs) != 1 || rep.EvidenceRefs[0] != "implementation_diff" {
		t.Fatalf("evidence refs = %#v, want implementation_diff", rep.EvidenceRefs)
	}
	if rep.Payload["no_verify"] != true || rep.Payload["hooks_disabled"] != true {
		t.Fatalf("commit flags missing: %+v", rep.Payload)
	}
	commitSHA := payloadString(rep.Payload, "commit_sha")
	message := string(runCommitGitOutput(t, ctx, wt.Path, "show", "-s", "--format=%B", commitSHA))
	if !strings.Contains(message, "Diff artifact ID: implementation_diff") || strings.Contains(message, "validation_diff") {
		t.Fatalf("commit message references wrong diff artifact:\n%s", message)
	}
}

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
}

type fakeFragmentRenderer struct{}

func (fakeFragmentRenderer) RenderRunFragments(store.RunBundle) (string, error) { return "", nil }

type fakeBroadcaster struct{}

func (fakeBroadcaster) Broadcast(string, event.Event, string) {}

func initCommitSourceRepo(t *testing.T, ctx context.Context, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	runCommitGit(t, ctx, dir, "init")
	runCommitGit(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runCommitGit(t, ctx, dir, "config", "user.name", "Parley Test")
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create parent for %s: %v", name, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	runCommitGit(t, ctx, dir, "add", "-A")
	runCommitGit(t, ctx, dir, "commit", "-m", "initial")
	return dir
}

func addDetachedWorktree(t *testing.T, ctx context.Context, source string) string {
	t.Helper()
	worktreePath := filepath.Join(t.TempDir(), "wt")
	runCommitGit(t, ctx, source, "worktree", "add", "--detach", worktreePath, "HEAD")
	return worktreePath
}

func snapshotCommitOptions(t *testing.T, ctx context.Context, worktreePath, runID, taskID, diffArtifactID string) commitOptions {
	t.Helper()
	baseSHA, baseTreeSHA, workerTreeSHA, err := snapshotGitWorktree(ctx, "git", worktreePath)
	if err != nil {
		t.Fatalf("snapshotGitWorktree() error = %v", err)
	}
	return commitOptions{
		WorktreePath:   worktreePath,
		BaseSHA:        baseSHA,
		BaseTreeSHA:    baseTreeSHA,
		WorkerTreeSHA:  workerTreeSHA,
		RunID:          runID,
		TaskID:         taskID,
		Idea:           "Change worktree",
		ReportSummary:  "validation passed",
		DiffArtifactID: diffArtifactID,
		AuthorName:     "Harness Bot",
		AuthorEmail:    "bot@example.invalid",
	}
}

func installFailingHook(t *testing.T, path, sentinel string) {
	t.Helper()
	content := "#!/bin/sh\necho hook >> " + shellQuote(sentinel) + "\nexit 1\n"
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatalf("write hook %s: %v", path, err)
	}
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func runCommitGit(t *testing.T, ctx context.Context, dir string, args ...string) {
	t.Helper()
	_ = runCommitGitOutput(t, ctx, dir, args...)
}

func runCommitGitOutput(t *testing.T, ctx context.Context, dir string, args ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
	return out
}
