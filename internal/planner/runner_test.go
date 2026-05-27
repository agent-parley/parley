package planner_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/adapters/pi"
	"github.com/agent-parley/parley/internal/containers"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/planner"
	"github.com/agent-parley/parley/internal/profiles"
	"github.com/agent-parley/parley/internal/worktrees"
)

type scriptedRuntime struct {
	outputs        []string
	invocations    []containers.Invocation
	sawPrivateDocs bool
}

func (r *scriptedRuntime) Run(ctx context.Context, invocation containers.Invocation) (containers.Result, error) {
	select {
	case <-ctx.Done():
		return containers.Result{}, ctx.Err()
	default:
	}
	r.invocations = append(r.invocations, invocation)
	for _, mount := range invocation.Mounts {
		if mount.ContainerPath == "/workspace" {
			if _, err := os.Stat(filepath.Join(mount.HostPath, "docs", "private.md")); err == nil {
				r.sawPrivateDocs = true
			}
		}
	}
	if err := os.MkdirAll(invocation.OutputDir, 0o700); err != nil {
		return containers.Result{}, err
	}
	stdoutPath := filepath.Join(invocation.OutputDir, "stdout.txt")
	stderrPath := filepath.Join(invocation.OutputDir, "stderr.txt")
	body := "{}"
	if len(r.outputs) > 0 {
		body = r.outputs[0]
		r.outputs = r.outputs[1:]
	}
	if err := os.WriteFile(stdoutPath, []byte(body), 0o600); err != nil {
		return containers.Result{}, err
	}
	if err := os.WriteFile(stderrPath, nil, 0o600); err != nil {
		return containers.Result{}, err
	}
	return containers.Result{ExitCode: 0, StdoutPath: stdoutPath, StderrPath: stderrPath}, nil
}

func TestDryRunPlannerProducesApprovalGatedDraft(t *testing.T) {
	result, err := planner.NewDryRunRunner().Run(context.Background(), planner.Input{
		Project: models.Project{Name: "Demo", AgentContext: "important context"},
		Session: models.PlannerSession{Prompt: "Add real planning", DraftTitle: "Existing draft"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Mode != planner.ModeDryRun || result.PlannerProfile != profiles.ProfilePlanner || result.CriticProfile != profiles.ProfileCritic {
		t.Fatalf("unexpected dry-run metadata: %+v", result)
	}
	if result.Draft.Title != "Existing draft" || len(result.Draft.GraphPreview) == 0 {
		t.Fatalf("unexpected dry-run draft: %+v", result.Draft)
	}
}

func TestDryRunPlannerEmitsProgressEvents(t *testing.T) {
	var events []string
	_, err := planner.NewDryRunRunner().Run(context.Background(), planner.Input{
		Project: models.Project{Name: "Demo"},
		Session: models.PlannerSession{Prompt: "Add progress"},
		Progress: func(eventType, summary string, data map[string]any) {
			events = append(events, eventType)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{models.PlannerGenerationEventPlannerStarted, models.PlannerGenerationEventPlannerFinished, models.PlannerGenerationEventCriticStarted, models.PlannerGenerationEventCriticFinished}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected progress events: got %v want %v", events, want)
	}
}

func TestLocalPiPreflightRejectsMissingWorktreeManager(t *testing.T) {
	runner := planner.NewLocalPiRunner(nil, &scriptedRuntime{}, pi.Adapter{})
	if err := runner.Preflight(context.Background(), planner.Input{Project: models.Project{RepoPath: t.TempDir()}, Session: models.PlannerSession{ID: "pln"}}); err == nil {
		t.Fatalf("expected preflight failure without worktree manager")
	}
}

func TestLocalPiPlannerRunsPlannerThenCriticReadOnlyBeforeApproval(t *testing.T) {
	repoPath := initTestGitRepo(t)
	root := t.TempDir()
	runtime := &scriptedRuntime{outputs: []string{
		`{"title":"Generated plan","objective":"Wire planner agents","focus":"planner flow","boundaries":"approval gate stays closed","done_when":"draft is reviewed","assumptions":["local only"],"risks":["parse errors"],"graph_preview":["Prompt","Planner agent","Critic agent","Approval"],"summary":"Planner made a draft."}`,
		`{"verdict":"approve-with-cautions","summary":"Critic reviewed the draft.","risks":["confirm scope before approval"],"questions":["Should local-pi stay opt-in?"]}`,
	}}
	runner := planner.NewLocalPiRunner(worktrees.NewLocalManager(root), runtime, pi.Adapter{})
	result, err := runner.Run(context.Background(), planner.Input{
		Project: models.Project{ID: "prj", Name: "Demo", RepoPath: repoPath},
		Session: models.PlannerSession{ID: "pln", Prompt: "Add planner agents"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Mode != planner.ModeLocalPi || result.Draft.Title != "Generated plan" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(result.Draft.Risks) != 2 {
		t.Fatalf("critic risk was not merged into draft: %+v", result.Draft.Risks)
	}
	if len(runtime.invocations) != 2 {
		t.Fatalf("expected planner and critic invocations, got %d", len(runtime.invocations))
	}
	if runtime.invocations[0].Profile != profiles.ProfilePlanner || runtime.invocations[1].Profile != profiles.ProfileCritic {
		t.Fatalf("unexpected profiles: %+v", runtime.invocations)
	}
	if runtime.sawPrivateDocs {
		t.Fatalf("planner/critic managed worktree exposed ignored private docs")
	}
	for _, invocation := range runtime.invocations {
		if invocation.Network != "none" || invocation.Privileged {
			t.Fatalf("planner invocation must stay sandboxed: %+v", invocation)
		}
		if len(invocation.Mounts) != 2 || invocation.Mounts[1].HostPath == repoPath {
			t.Fatalf("planner/critic must mount a managed worktree, not the raw repo: %+v", invocation.Mounts)
		}
		for _, mount := range invocation.Mounts {
			if mount.Mode != "ro" {
				t.Fatalf("planner/critic mounts must be read-only: %+v", invocation.Mounts)
			}
		}
	}
	assertNoPlannerWorktrees(t, root)
	assertNoPlannerBranches(t, repoPath)
}

func TestLocalPiPlannerRejectsInvalidPlannerJSON(t *testing.T) {
	repoPath := initTestGitRepo(t)
	root := t.TempDir()
	runtime := &scriptedRuntime{outputs: []string{"not-json"}}
	runner := planner.NewLocalPiRunner(worktrees.NewLocalManager(root), runtime, pi.Adapter{})
	if _, err := runner.Run(context.Background(), planner.Input{Project: models.Project{ID: "prj", Name: "Demo", RepoPath: repoPath}, Session: models.PlannerSession{ID: "pln", Prompt: "Add planner agents"}}); err == nil {
		t.Fatalf("expected invalid planner JSON failure")
	}
	if len(runtime.invocations) != 1 {
		t.Fatalf("critic should not run after invalid planner JSON, invocations=%d", len(runtime.invocations))
	}
	assertNoPlannerWorktrees(t, root)
	assertNoPlannerBranches(t, repoPath)
}

func TestLocalPiPlannerRejectsProseWrappedPlannerJSON(t *testing.T) {
	repoPath := initTestGitRepo(t)
	root := t.TempDir()
	runtime := &scriptedRuntime{outputs: []string{`Here is the plan: {"title":"Generated plan"}`}}
	runner := planner.NewLocalPiRunner(worktrees.NewLocalManager(root), runtime, pi.Adapter{})
	if _, err := runner.Run(context.Background(), planner.Input{Project: models.Project{ID: "prj", Name: "Demo", RepoPath: repoPath}, Session: models.PlannerSession{ID: "pln", Prompt: "Add planner agents"}}); err == nil {
		t.Fatalf("expected prose-wrapped planner JSON failure")
	}
	if len(runtime.invocations) != 1 {
		t.Fatalf("critic should not run after prose-wrapped planner JSON, invocations=%d", len(runtime.invocations))
	}
	assertNoPlannerWorktrees(t, root)
}

func TestLocalPiPlannerRejectsEmptyPlannerJSONShape(t *testing.T) {
	repoPath := initTestGitRepo(t)
	root := t.TempDir()
	runtime := &scriptedRuntime{outputs: []string{`{}`}}
	runner := planner.NewLocalPiRunner(worktrees.NewLocalManager(root), runtime, pi.Adapter{})
	if _, err := runner.Run(context.Background(), planner.Input{Project: models.Project{ID: "prj", Name: "Demo", RepoPath: repoPath}, Session: models.PlannerSession{ID: "pln", Prompt: "Add planner agents"}}); err == nil {
		t.Fatalf("expected empty planner JSON shape failure")
	}
	if len(runtime.invocations) != 1 {
		t.Fatalf("critic should not run after empty planner JSON shape, invocations=%d", len(runtime.invocations))
	}
	assertNoPlannerWorktrees(t, root)
}

func TestLocalPiPlannerRejectsPartialPlannerJSONShape(t *testing.T) {
	repoPath := initTestGitRepo(t)
	root := t.TempDir()
	runtime := &scriptedRuntime{outputs: []string{`{"title":"Generated plan","objective":"Wire planner agents","focus":"planner flow","boundaries":"approval gate stays closed","done_when":"draft is reviewed","summary":"Planner made a draft."}`}}
	runner := planner.NewLocalPiRunner(worktrees.NewLocalManager(root), runtime, pi.Adapter{})
	if _, err := runner.Run(context.Background(), planner.Input{Project: models.Project{ID: "prj", Name: "Demo", RepoPath: repoPath}, Session: models.PlannerSession{ID: "pln", Prompt: "Add planner agents"}}); err == nil {
		t.Fatalf("expected partial planner JSON shape failure")
	}
	if len(runtime.invocations) != 1 {
		t.Fatalf("critic should not run after partial planner JSON shape, invocations=%d", len(runtime.invocations))
	}
	assertNoPlannerWorktrees(t, root)
}

func TestLocalPiPlannerRejectsInvalidCriticJSON(t *testing.T) {
	repoPath := initTestGitRepo(t)
	root := t.TempDir()
	runtime := &scriptedRuntime{outputs: []string{
		`{"title":"Generated plan","objective":"Wire planner agents","focus":"planner flow","boundaries":"approval gate stays closed","done_when":"draft is reviewed","assumptions":["local only"],"risks":["parse errors"],"graph_preview":["Prompt","Planner agent","Critic agent","Approval"],"summary":"Planner made a draft."}`,
		`not-json`,
	}}
	runner := planner.NewLocalPiRunner(worktrees.NewLocalManager(root), runtime, pi.Adapter{})
	if _, err := runner.Run(context.Background(), planner.Input{Project: models.Project{ID: "prj", Name: "Demo", RepoPath: repoPath}, Session: models.PlannerSession{ID: "pln", Prompt: "Add planner agents"}}); err == nil {
		t.Fatalf("expected invalid critic JSON failure")
	}
	if len(runtime.invocations) != 2 {
		t.Fatalf("critic should run once after valid planner JSON, invocations=%d", len(runtime.invocations))
	}
	assertNoPlannerWorktrees(t, root)
}

func TestLocalPiPlannerRejectsEmptyCriticJSONShape(t *testing.T) {
	repoPath := initTestGitRepo(t)
	root := t.TempDir()
	runtime := &scriptedRuntime{outputs: []string{
		`{"title":"Generated plan","objective":"Wire planner agents","focus":"planner flow","boundaries":"approval gate stays closed","done_when":"draft is reviewed","assumptions":["local only"],"risks":["parse errors"],"graph_preview":["Prompt","Planner agent","Critic agent","Approval"],"summary":"Planner made a draft."}`,
		`{}`,
	}}
	runner := planner.NewLocalPiRunner(worktrees.NewLocalManager(root), runtime, pi.Adapter{})
	if _, err := runner.Run(context.Background(), planner.Input{Project: models.Project{ID: "prj", Name: "Demo", RepoPath: repoPath}, Session: models.PlannerSession{ID: "pln", Prompt: "Add planner agents"}}); err == nil {
		t.Fatalf("expected empty critic JSON shape failure")
	}
	if len(runtime.invocations) != 2 {
		t.Fatalf("critic should run once after valid planner JSON, invocations=%d", len(runtime.invocations))
	}
	assertNoPlannerWorktrees(t, root)
}

func TestLocalPiPlannerRejectsPartialCriticJSONShape(t *testing.T) {
	repoPath := initTestGitRepo(t)
	root := t.TempDir()
	runtime := &scriptedRuntime{outputs: []string{
		`{"title":"Generated plan","objective":"Wire planner agents","focus":"planner flow","boundaries":"approval gate stays closed","done_when":"draft is reviewed","assumptions":["local only"],"risks":["parse errors"],"graph_preview":["Prompt","Planner agent","Critic agent","Approval"],"summary":"Planner made a draft."}`,
		`{"verdict":"approve-with-cautions","summary":"Critic reviewed the draft."}`,
	}}
	runner := planner.NewLocalPiRunner(worktrees.NewLocalManager(root), runtime, pi.Adapter{})
	if _, err := runner.Run(context.Background(), planner.Input{Project: models.Project{ID: "prj", Name: "Demo", RepoPath: repoPath}, Session: models.PlannerSession{ID: "pln", Prompt: "Add planner agents"}}); err == nil {
		t.Fatalf("expected partial critic JSON shape failure")
	}
	if len(runtime.invocations) != 2 {
		t.Fatalf("critic should run once after valid planner JSON, invocations=%d", len(runtime.invocations))
	}
	assertNoPlannerWorktrees(t, root)
}

func assertNoPlannerWorktrees(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, "active", "prj", "planner", "pln")
	entries, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("planner worktrees should be cleaned up, remaining entries=%v", entries)
	}
}

func assertNoPlannerBranches(t *testing.T, repoPath string) {
	t.Helper()
	cmd := exec.Command("git", "branch", "--list", "parley/planner/*")
	cmd.Dir = repoPath
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git branch --list failed: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "" {
		t.Fatalf("planner branches should be cleaned up, remaining=%s", output)
	}
}

func initTestGitRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git unavailable")
	}
	dir := t.TempDir()
	runGit(t, dir, "init")
	runGit(t, dir, "config", "user.email", "parley@example.test")
	runGit(t, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# demo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("docs/\nplans/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "docs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "docs", "private.md"), []byte("private\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, "add", "README.md", ".gitignore")
	runGit(t, dir, "commit", "-m", "init")
	return dir
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, output)
	}
}
