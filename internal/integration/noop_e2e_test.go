package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	managerhttp "github.com/agent-parley/parley/internal/manager/http"
	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/runnerclient"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/runner/adapter"
	"github.com/agent-parley/parley/internal/runner/provider"
	"github.com/agent-parley/parley/internal/runner/runnerio"
	runnersession "github.com/agent-parley/parley/internal/runner/session"
	"github.com/agent-parley/parley/internal/runner/worktree"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/ids"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestFullLoopWithFakeSandboxProvider(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dataRoot := t.TempDir()
	projectID := "p1"
	source := initFullLoopSourceRepo(t, ctx)

	serverCtx, stop := context.WithCancel(context.Background())
	defer stop()
	srv, url, err := runnersession.Listen(runnersession.WithAdapters(
		fakeImplementationAdapter{dataRoot: dataRoot, projectID: projectID, sourceRepo: source},
		adapter.NewValidation(adapter.ValidationOptions{Provider: fakeSandboxProvider{}, DataRoot: dataRoot, ProjectID: projectID, Image: "fake-validation", Command: "go build ./... && go test ./..."}),
	))
	if err != nil {
		t.Fatalf("listen runner: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(serverCtx) }()

	client, err := runnerclient.Dial(ctx, url, ids.New("runner"))
	if err != nil {
		t.Fatalf("dial runner: %v", err)
	}
	st, err := store.Open(ctx, filepath.Join(dataRoot, "manager"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.EnsureProject(ctx, store.ProjectSpec{ID: projectID, Name: "p1", RepositoryPath: source, QueueAutoWhenReady: true, QueueMaxConcurrent: 1, QueueBacklogCap: 100}); err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	renderer, err := web.NewRenderer()
	if err != nil {
		t.Fatalf("renderer: %v", err)
	}
	hub := managerhttp.NewHub()
	engine := orchestrator.NewEngineWithOptions(st, client, renderer, hub, orchestrator.EngineOptions{ImplementationAdapter: "fake_impl", ValidationAdapter: "validation", DataRoot: dataRoot, ProjectID: projectID})
	client.SetHandlers(engine.HandleRunnerEvent, engine.HandleRunnerArtifact, engine.HandleRunnerReport, engine.HandleRunnerResult, engine.HandleRunnerLog)

	runID, err := engine.StartRun(ctx, "add a local-first harness")
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	var bundle store.RunBundle
	for {
		bundle, err = st.RunBundle(ctx, runID)
		if err != nil {
			t.Fatalf("bundle: %v", err)
		}
		if bundle.Run.Status == store.RunStatusCompleted {
			break
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("run did not complete; last status=%s", bundle.Run.Status)
		}
	}
	if len(bundle.Stages) != 5 {
		t.Fatalf("expected 5 stages, got %d", len(bundle.Stages))
	}
	for _, stage := range bundle.Stages {
		if stage.Status != store.RunStatusCompleted {
			t.Fatalf("stage not completed: %+v", stage)
		}
		if stage.StageBriefArtifactID == "" {
			t.Fatalf("stage missing Stage brief reference: %+v", stage)
		}
		_, content, err := st.GetArtifact(ctx, stage.StageBriefArtifactID)
		if err != nil {
			t.Fatalf("read stage brief %s: %v", stage.StageBriefArtifactID, err)
		}
		if !strings.Contains(string(content), "# Stage brief") || !strings.Contains(string(content), "## Source: workflow_snapshot") {
			t.Fatalf("stage brief missing source-labeled content:\n%s", content)
		}
	}
	var reportArtifacts, diffArtifacts, contractArtifacts, stageBriefArtifacts int
	for _, artifact := range bundle.Artifacts {
		switch artifact.Kind {
		case "report":
			reportArtifacts++
		case "diff_patch":
			diffArtifacts++
		case "task_contract":
			contractArtifacts++
		case "stage_brief":
			stageBriefArtifacts++
		}
	}
	if reportArtifacts != 5 {
		t.Fatalf("expected 5 report artifacts, got %d", reportArtifacts)
	}
	if diffArtifacts != 1 {
		t.Fatalf("expected 1 validation diff.patch artifact, got %d", diffArtifacts)
	}
	if contractArtifacts != 1 {
		t.Fatalf("expected 1 task contract artifact, got %d", contractArtifacts)
	}
	if stageBriefArtifacts != 5 {
		t.Fatalf("expected 5 stage brief artifacts, got %d", stageBriefArtifacts)
	}
	var completedEvent bool
	for _, ev := range bundle.Events {
		if ev.Type == "run.completed" {
			completedEvent = true
			branch, _ := ev.Data["branch"].(string)
			if !strings.HasPrefix(branch, "agent/"+runID+"/") {
				t.Fatalf("completed branch = %q", branch)
			}
		}
	}
	if !completedEvent {
		t.Fatal("missing run.completed event")
	}
	assertM5ActiveEventStream(t, bundle.Events)
	_ = client.Close(context.Background())
	stop()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func assertM5ActiveEventStream(t *testing.T, events []event.Event) {
	t.Helper()
	seen := map[string]bool{}
	for _, ev := range events {
		if strings.HasPrefix(ev.Type, "task.") {
			t.Fatalf("unexpected planned task.* event emitted: %s", ev.Type)
		}
		seen[ev.Type] = true
	}
	for _, typ := range []string{"run.created", "run.started", "stage.started", "adapter.invocation_prepared", "adapter.started", "adapter.completed", "harness.completed", "stage.completed", "run.completed"} {
		if !seen[typ] {
			t.Fatalf("missing active event type %s in stream", typ)
		}
	}
	assertEventOrder(t, events, "run.created", "run.started", "run.completed")
	assertEventOrder(t, events, "adapter.invocation_prepared", "adapter.started", "adapter.completed")
}

func assertEventOrder(t *testing.T, events []event.Event, ordered ...string) {
	t.Helper()
	pos := 0
	for _, ev := range events {
		if pos < len(ordered) && ev.Type == ordered[pos] {
			pos++
		}
	}
	if pos != len(ordered) {
		t.Fatalf("events did not contain ordered subsequence %#v", ordered)
	}
}

type fakeImplementationAdapter struct {
	dataRoot   string
	projectID  string
	sourceRepo string
}

func (a fakeImplementationAdapter) Name() string { return "fake_impl" }

func (a fakeImplementationAdapter) Run(ctx context.Context, disp contract.Dispatch, _ runnerio.Sink) (report.Report, error) {
	wt, err := worktree.Create(ctx, worktree.CreateOptions{DataRoot: a.dataRoot, ProjectID: a.projectID, RunID: disp.RunID, TaskID: disp.TaskID, AttemptID: disp.AttemptID, SourceRepo: a.sourceRepo})
	if err != nil {
		return report.Report{}, err
	}
	if err := os.WriteFile(filepath.Join(wt.Path, "feature.txt"), []byte("feature\n"), 0o600); err != nil {
		return report.Report{}, err
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: a.Name()},
		Status:        report.StatusCompleted,
		Summary:       "fake implementation changed worktree",
		EvidenceRefs:  []string{},
		Payload:       map[string]any{"worktree": wt.Path},
		Errors:        []string{},
	}, nil
}

type fakeSandboxProvider struct{}

func (fakeSandboxProvider) Name() string { return "fake" }

func (fakeSandboxProvider) Run(context.Context, provider.PreparedInvocation, runnerio.Sink) (provider.Result, error) {
	now := time.Now().UTC()
	return provider.Result{ExitCode: 0, StartedAt: now, EndedAt: now}, nil
}

func initFullLoopSourceRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	runFullLoopGit(t, ctx, dir, "init")
	runFullLoopGit(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runFullLoopGit(t, ctx, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runFullLoopGit(t, ctx, dir, "add", "README.md")
	runFullLoopGit(t, ctx, dir, "commit", "-m", "initial")
	return dir
}

func runFullLoopGit(t *testing.T, ctx context.Context, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}
