package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/protocol"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestCancelMidRunRoutesToCancelled(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	runner := newBlockingRunner()
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "cancel me", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun() error = %v", err)
	}
	<-runner.started
	if err := engine.CancelRun(ctx, runID); err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	if !runner.cancelCalled() {
		t.Fatal("runner CancelAttempt was not called")
	}
	waitForRunStatus(t, st, runID, store.RunStatusCancelled)
	events, err := st.ListEvents(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if !hasEventType(events, "run.cancelled") {
		t.Fatalf("missing run.cancelled event: %#v", eventTypes(events))
	}
}

func TestCancelAfterNaturalTerminalDoesNotOverride(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "already done")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("set completed: %v", err)
	}
	runner := newBlockingRunner()
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	if err := engine.CancelRun(ctx, wr.Run.ID); err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	run, err := st.GetRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != store.RunStatusCompleted {
		t.Fatalf("status = %s, want completed", run.Status)
	}
	if runner.cancelCalled() {
		t.Fatal("CancelAttempt called after natural terminal")
	}
}

func TestRunnerDownDuringCancellationRoutesToCancelled(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	runner := newBlockingRunner()
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "cancel then disconnect", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun() error = %v", err)
	}
	<-runner.started
	if err := engine.CancelRun(ctx, runID); err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	if err := engine.HandleRunnerDown(ctx, "runner_local", "session_done"); err != nil {
		t.Fatalf("HandleRunnerDown() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusCancelled)
	waitForEventType(t, st, runID, "stage.failed")
	events, err := st.ListEvents(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if !hasEventType(events, "run.cancelled") {
		t.Fatalf("missing run.cancelled event: %#v", eventTypes(events))
	}
	for _, ev := range events {
		if ev.Type == "run.failed" && ev.Data["reason"] == "runner_disconnected" {
			t.Fatalf("runner down overrode cancellation: %#v", ev)
		}
	}
}

func TestRunnerDownFailsInFlightRunStatePreservingly(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	runner := newBlockingRunner()
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "runner dies", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun() error = %v", err)
	}
	<-runner.started
	if err := engine.HandleRunnerDown(ctx, "runner_local", "process_exit"); err != nil {
		t.Fatalf("HandleRunnerDown() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusFailed)
	waitForEventType(t, st, runID, "stage.failed")
	events, err := st.ListEvents(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for _, ev := range events {
		if ev.Type == "run.failed" && ev.Data["reason"] == "runner_disconnected" {
			return
		}
	}
	t.Fatalf("missing runner_disconnected run.failed event: %#v", events)
}

func TestStageCompletionEmitsPerformerAndStageTerminal(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "freeze idea", RefinementLevel: contract.RefinementLevelDirect})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	engine := NewEngineWithOptions(st, nil, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	if _, err := engine.runIdeaIntake(ctx, wr); err != nil {
		t.Fatalf("runIdeaIntake() error = %v", err)
	}
	events, err := st.ListEvents(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	want := []string{"stage.started", "harness.completed", "stage.completed"}
	got := eventTypes(events)
	if len(got) != len(want) {
		t.Fatalf("event types = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event types = %#v, want %#v", got, want)
		}
	}
}

type blockingRunner struct {
	started chan contract.Dispatch
	cancel  chan struct{}
}

func newBlockingRunner() *blockingRunner {
	return &blockingRunner{started: make(chan contract.Dispatch, 1), cancel: make(chan struct{}, 1)}
}

func (r *blockingRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	r.started <- disp
	<-ctx.Done()
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        report.StatusFailed,
		Summary:       "dispatch interrupted",
		Payload:       map[string]any{},
		Errors:        []string{ctx.Err().Error()},
	}, nil
}

func (r *blockingRunner) CancelAttempt(context.Context, string, string, string) error {
	select {
	case r.cancel <- struct{}{}:
	default:
	}
	return nil
}

func (r *blockingRunner) cancelCalled() bool {
	select {
	case <-r.cancel:
		return true
	default:
		return false
	}
}

func waitForRunStatus(t *testing.T, st *store.Store, runID, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		run, err := st.GetRun(context.Background(), runID)
		if err == nil && run.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	run, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	t.Fatalf("run status = %s, want %s", run.Status, want)
}

func waitForEventType(t *testing.T, st *store.Store, runID, typ string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		events, err := st.ListEvents(context.Background(), runID)
		if err == nil && hasEventType(events, typ) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	events, err := st.ListEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	t.Fatalf("missing event %s in %#v", typ, eventTypes(events))
}

func hasEventType(events []event.Event, typ string) bool {
	for _, ev := range events {
		if ev.Type == typ {
			return true
		}
	}
	return false
}

func eventTypes(events []event.Event) []string {
	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Type)
	}
	return out
}

// TestDispatchSessionClosedRoutesToRunnerDisconnected exercises the failure path
// on its own — a dispatch returning ErrSessionClosed with no HandleRunnerDown
// call — to prove the stage path self-classifies the run terminal as
// runner_disconnected, so the reason is deterministic regardless of which path
// finalizes the run during a real runner crash.
func TestDispatchSessionClosedRoutesToRunnerDisconnected(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	engine := NewEngineWithOptions(st, sessionClosedRunner{}, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "runner vanishes mid-dispatch", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusFailed)
	events, err := st.ListEvents(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for _, ev := range events {
		if ev.Type == "run.failed" && ev.Data["reason"] == "runner_disconnected" {
			return
		}
	}
	t.Fatalf("expected run.failed with reason=runner_disconnected via the dispatch path; got %#v", eventTypes(events))
}

// sessionClosedRunner fails every dispatch as if the runner vanished mid-stage.
type sessionClosedRunner struct{}

func (sessionClosedRunner) Dispatch(context.Context, contract.Dispatch) (report.Report, error) {
	return report.Report{}, protocol.ErrSessionClosed
}

func (sessionClosedRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}
