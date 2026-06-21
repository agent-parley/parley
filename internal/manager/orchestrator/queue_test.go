package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
)

func TestStartRunEnqueuesWhenAutoDisabledAndManualStartDispatches(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	runner := newBlockingRunner()
	policy := QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{QueuePolicy: &policy})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "queued idea", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun() error = %v", err)
	}
	run, err := st.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != store.RunStatusPending {
		t.Fatalf("status = %s, want pending", run.Status)
	}
	assertNoDispatch(t, runner.started)
	if err := engine.StartQueuedRun(ctx, runID); err != nil {
		t.Fatalf("StartQueuedRun() error = %v", err)
	}
	<-runner.started
	waitForRunStatus(t, st, runID, store.RunStatusRunning)
	page, err := st.ListSystemEventsPage(ctx, 0, 20)
	if err != nil {
		t.Fatalf("list system events: %v", err)
	}
	if !systemEventsContain(page.Events, "queue.enqueued") || !systemEventsContain(page.Events, "queue.dispatched") {
		t.Fatalf("system events = %#v, want queue.enqueued and queue.dispatched", page.Events)
	}
	if err := engine.CancelRun(ctx, runID); err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusCancelled)
}

func TestCancelPendingRunDoesNotDispatch(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	runner := newBlockingRunner()
	policy := QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{QueuePolicy: &policy})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "cancel queued", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun() error = %v", err)
	}
	if err := engine.CancelRun(ctx, runID); err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusCancelled)
	assertNoDispatch(t, runner.started)
	if runner.cancelCalled() {
		t.Fatal("CancelAttempt called for a queued run")
	}
}

func TestBacklogCapRejectsNewEnqueuesAndEmitsSystemEvent(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	policy := QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 1}
	engine := NewEngineWithOptions(st, newBlockingRunner(), fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{QueuePolicy: &policy})
	if _, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "first queued", WorkflowTemplateID: workflow.AutonomousPRDeliveryID}); err != nil {
		t.Fatalf("StartRun(first) error = %v", err)
	}
	_, err = engine.StartRunInput(ctx, contract.TaskInput{Idea: "second queued", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	var backlogErr QueueBacklogFullError
	if !errors.As(err, &backlogErr) {
		t.Fatalf("StartRun(second) error = %v, want QueueBacklogFullError", err)
	}
	page, err := st.ListSystemEventsPage(ctx, 0, 20)
	if err != nil {
		t.Fatalf("list system events: %v", err)
	}
	if !systemEventsContain(page.Events, "queue.rejected_backlog_full") {
		t.Fatalf("system events = %#v, want queue.rejected_backlog_full", page.Events)
	}
}

func TestAutoQueueDispatchesOnlyUpToReadyRunnerSlots(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	runner := newBlockingRunner()
	policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 2, BacklogCap: 10}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{QueuePolicy: &policy, RunnerSlots: 1})
	firstID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "first", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun(first) error = %v", err)
	}
	<-runner.started
	waitForRunStatus(t, st, firstID, store.RunStatusRunning)
	secondID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "second", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun(second) error = %v", err)
	}
	assertNoDispatch(t, runner.started)
	second, err := st.GetRun(ctx, secondID)
	if err != nil {
		t.Fatalf("get second run: %v", err)
	}
	if second.Status != store.RunStatusPending {
		t.Fatalf("second status = %s, want pending while single runner slot is busy", second.Status)
	}
	if err := engine.CancelRun(ctx, firstID); err != nil {
		t.Fatalf("CancelRun(first) error = %v", err)
	}
	waitForRunStatus(t, st, firstID, store.RunStatusCancelled)
	<-runner.started
	waitForRunStatus(t, st, secondID, store.RunStatusRunning)
}

func TestRecoverAndDispatchFailsInterruptedRunningAndStartsPending(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	interrupted, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "interrupted", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create interrupted run: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, interrupted.Run.ID, store.RunStatusRunning); err != nil {
		t.Fatalf("mark interrupted running: %v", err)
	}
	queued, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "queued", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create queued run: %v", err)
	}
	runner := newBlockingRunner()
	policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{QueuePolicy: &policy})
	if err := engine.RecoverAndDispatch(ctx); err != nil {
		t.Fatalf("RecoverAndDispatch() error = %v", err)
	}
	waitForRunStatus(t, st, interrupted.Run.ID, store.RunStatusFailed)
	<-runner.started
	waitForRunStatus(t, st, queued.Run.ID, store.RunStatusRunning)
	events, err := st.ListEvents(ctx, interrupted.Run.ID)
	if err != nil {
		t.Fatalf("list interrupted events: %v", err)
	}
	for _, ev := range events {
		if ev.Type == "run.failed" && ev.Data["reason"] == "manager_restarted" && ev.Data["retryable"] == true {
			return
		}
	}
	t.Fatalf("interrupted events = %#v, want retryable manager_restarted failure", events)
}

func TestDispatchSkipsGateHeldRunWithoutBlockingOthers(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	runner := newBlockingRunner()
	policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{QueuePolicy: &policy, RunnerSlots: 1})
	engine.gate = func(_ context.Context, run store.Run) (bool, error) {
		return run.Idea != "held", nil
	}
	heldID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "held", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun(held) error = %v", err)
	}
	readyID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "ready", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun(ready) error = %v", err)
	}
	// The younger, un-gated run must dispatch even though an older run is held:
	// the test reaching this point at all proves the dispatcher does not spin.
	<-runner.started
	waitForRunStatus(t, st, readyID, store.RunStatusRunning)
	held, err := st.GetRun(ctx, heldID)
	if err != nil {
		t.Fatalf("get held run: %v", err)
	}
	if held.Status != store.RunStatusPending {
		t.Fatalf("held run status = %s, want pending", held.Status)
	}
	// Manual start of a gate-held run must report held, not a false success.
	if err := engine.StartQueuedRun(ctx, heldID); !errors.Is(err, ErrRunHeld) {
		t.Fatalf("StartQueuedRun(held) error = %v, want ErrRunHeld", err)
	}
}

func assertNoDispatch(t *testing.T, ch <-chan contract.Dispatch) {
	t.Helper()
	select {
	case disp := <-ch:
		t.Fatalf("unexpected dispatch: %#v", disp)
	case <-time.After(50 * time.Millisecond):
	}
}

func systemEventsContain(events []store.SystemEvent, typ string) bool {
	for _, entry := range events {
		if entry.Event.Type == typ {
			return true
		}
	}
	return false
}
