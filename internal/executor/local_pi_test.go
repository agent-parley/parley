package executor_test

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/agent-parley/parley/internal/adapters/pi"
	"github.com/agent-parley/parley/internal/containers"
	"github.com/agent-parley/parley/internal/executor"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/profiles"
	"github.com/agent-parley/parley/internal/worktrees"
)

type preflightRuntime struct {
	images []string
}

type sequenceRuntime struct {
	exits       []int
	invocations []containers.Invocation
}

func (r *preflightRuntime) Preflight(ctx context.Context, image string) error {
	r.images = append(r.images, image)
	return nil
}

func (r *preflightRuntime) Run(ctx context.Context, invocation containers.Invocation) (containers.Result, error) {
	return containers.Result{}, nil
}

func (r *sequenceRuntime) Run(ctx context.Context, invocation containers.Invocation) (containers.Result, error) {
	r.invocations = append(r.invocations, invocation)
	exitCode := 0
	if len(r.exits) > 0 {
		exitCode = r.exits[0]
		r.exits = r.exits[1:]
	}
	return containers.Result{ExitCode: exitCode}, nil
}

func TestLocalPiWorkerFailureEmitsProgressAndSkipsReviewer(t *testing.T) {
	runtime := &sequenceRuntime{exits: []int{7}}
	runner := executor.NewLocalPiRunner(worktrees.NewLocalManager(t.TempDir()), runtime, pi.Adapter{})
	var events []string
	_, err := runner.RunAttempt(context.Background(), executor.AttemptInput{
		Project: models.Project{ID: "project", RepoPath: initializedGitRepo(t)},
		Run: models.Run{ID: "run", ProjectID: "project"},
		Task: models.Task{ID: "task", RunID: "run", ProjectID: "project", Objective: "do work", AcceptanceCriteria: "done"},
		Attempt: models.Attempt{Number: 1},
		ArtifactDir: t.TempDir(),
		Progress: func(eventType, summary string, data map[string]any) {
			events = append(events, eventType)
		},
	})
	if err == nil {
		t.Fatalf("expected worker failure")
	}
	want := []string{models.EventAttemptWorkerStarted, models.EventAttemptWorkerFinished}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("unexpected progress events: got %v want %v", events, want)
	}
	if len(runtime.invocations) != 1 {
		t.Fatalf("reviewer should not run after worker failure, invocations=%d", len(runtime.invocations))
	}
}

func initializedGitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	runGit(t, dir, "init")
	runGit(t, dir, "config", "user.email", "parley@example.test")
	runGit(t, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(dir+"/README.md", []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, "add", "README.md")
	runGit(t, dir, "commit", "-m", "initial")
	return dir
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}

func TestLocalPiPreflightAcceptsNonDefaultWorkerAdapter(t *testing.T) {
	runtime := &preflightRuntime{}
	runner := executor.NewLocalPiRunner(nil, runtime, pi.Adapter{})
	input := executor.AttemptInput{Task: models.Task{Adapter: "pi-high-context"}}
	if err := runner.Preflight(context.Background(), input); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if len(runtime.images) != 1 || runtime.images[0] != profiles.DefaultImage {
		t.Fatalf("unexpected image checks: %#v", runtime.images)
	}
}

func TestLocalPiPreflightUsesTaskAdapterForWorkerEligibility(t *testing.T) {
	runtime := &preflightRuntime{}
	runner := executor.NewLocalPiRunner(nil, runtime, pi.Adapter{})
	if err := runner.Preflight(context.Background(), executor.AttemptInput{Task: models.Task{Adapter: "pi-reviewer"}}); err == nil {
		t.Fatalf("reviewer-only task adapter must be rejected before runtime preflight")
	}
	if len(runtime.images) != 0 {
		t.Fatalf("runtime preflight should not run for disallowed task adapter: %#v", runtime.images)
	}
}

func TestLocalPiPreflightRejectsUnknownTaskAdapter(t *testing.T) {
	runner := executor.NewLocalPiRunner(nil, &preflightRuntime{}, pi.Adapter{})
	if err := runner.Preflight(context.Background(), executor.AttemptInput{Task: models.Task{Adapter: "missing-profile"}}); err == nil {
		t.Fatalf("expected unknown profile rejection")
	}
}

func TestLocalPiPreflightRejectsPlannedNonPiProfile(t *testing.T) {
	runtime := &preflightRuntime{}
	runner := executor.NewLocalPiRunner(nil, runtime, pi.Adapter{})
	if err := runner.Preflight(context.Background(), executor.AttemptInput{Task: models.Task{Adapter: "codex-worker"}}); err == nil {
		t.Fatalf("planned non-Pi profile must be rejected before runtime preflight")
	}
	if len(runtime.images) != 0 {
		t.Fatalf("runtime preflight should not run for planned non-Pi profile: %#v", runtime.images)
	}
}

func TestLocalPiPreflightDefaultsEmptyTaskAdapterToStandardWorker(t *testing.T) {
	runtime := &preflightRuntime{}
	runner := executor.NewLocalPiRunner(nil, runtime, pi.Adapter{})
	if err := runner.Preflight(context.Background(), executor.AttemptInput{}); err != nil {
		t.Fatalf("empty task adapter should default to standard worker: %v", err)
	}
	if len(runtime.images) != 1 || runtime.images[0] != profiles.DefaultImage {
		t.Fatalf("unexpected image checks: %#v", runtime.images)
	}
}
