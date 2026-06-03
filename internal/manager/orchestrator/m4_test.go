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
	"github.com/agent-parley/parley/internal/shared/event"
)

func TestIdeaIntakeFreezesVerbatimIdeaIntoContractAndSnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "add a thing\n<script>alert(1)</script>")
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
	_, contractContent, err := st.GetArtifact(ctx, contractID)
	if err != nil {
		t.Fatalf("read task contract: %v", err)
	}
	if !strings.Contains(string(contractContent), wr.Run.Idea) {
		t.Fatalf("task contract did not preserve idea verbatim:\n%s", contractContent)
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
}

func TestCommitWorktreeCreatesAgentBranchWithIdentityAndNoHooks(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	ctx := context.Background()
	source := initCommitSourceRepo(t, ctx)
	worktreePath := filepath.Join(t.TempDir(), "wt")
	runCommitGit(t, ctx, source, "worktree", "add", "--detach", worktreePath, "HEAD")
	if err := os.WriteFile(filepath.Join(worktreePath, "feature.txt"), []byte("feature\n"), 0o600); err != nil {
		t.Fatalf("write feature: %v", err)
	}

	sentinel := filepath.Join(t.TempDir(), "hook-ran")
	installFailingHook(t, filepath.Join(source, ".git", "hooks", "pre-commit"), sentinel)
	installFailingHook(t, filepath.Join(source, ".git", "hooks", "commit-msg"), sentinel)
	installFailingHook(t, filepath.Join(source, ".git", "hooks", "prepare-commit-msg"), sentinel)

	result, err := commitWorktree(ctx, commitOptions{
		WorktreePath:   worktreePath,
		RunID:          "run_test",
		TaskID:         "task_test",
		Idea:           "Add file\nsecond line",
		ReportSummary:  "validation passed",
		DiffArtifactID: "artifact_diff",
		AuthorName:     "Harness Bot",
		AuthorEmail:    "bot@example.invalid",
	})
	if err != nil {
		t.Fatalf("commitWorktree() error = %v", err)
	}
	if result.Branch != "agent/run_test/task_test" {
		t.Fatalf("branch = %s", result.Branch)
	}
	branch := strings.TrimSpace(string(runCommitGitOutput(t, ctx, worktreePath, "rev-parse", "--abbrev-ref", "HEAD")))
	if branch != result.Branch {
		t.Fatalf("git branch = %s, want %s", branch, result.Branch)
	}
	author := strings.TrimSpace(string(runCommitGitOutput(t, ctx, worktreePath, "log", "-1", "--format=%an <%ae>")))
	if author != "Harness Bot <bot@example.invalid>" {
		t.Fatalf("author = %s", author)
	}
	message := string(runCommitGitOutput(t, ctx, worktreePath, "log", "-1", "--format=%B"))
	for _, want := range []string{"parley: Add file", "validation passed", "Run ID: run_test", "Task ID: task_test", "Diff artifact ID: artifact_diff"} {
		if !strings.Contains(message, want) {
			t.Fatalf("commit message missing %q:\n%s", want, message)
		}
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("repo hook ran or sentinel stat failed: %v", err)
	}
	args := gitCommitArgs(gitIdentity{Name: "n", Email: "e"}, "/tmp/no-hooks", "s", "b")
	if !containsArg(args, "--no-verify") || !containsArg(args, "core.hooksPath=/tmp/no-hooks") {
		t.Fatalf("commit args do not disable verification/hooks: %#v", args)
	}
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

type fakeFragmentRenderer struct{}

func (fakeFragmentRenderer) RenderRunFragments(store.RunBundle) (string, error) { return "", nil }

type fakeBroadcaster struct{}

func (fakeBroadcaster) Broadcast(string, event.Event, string) {}

func initCommitSourceRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	runCommitGit(t, ctx, dir, "init")
	runCommitGit(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runCommitGit(t, ctx, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runCommitGit(t, ctx, dir, "add", "README.md")
	runCommitGit(t, ctx, dir, "commit", "-m", "initial")
	return dir
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
