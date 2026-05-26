package dispatcher_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/artifacts"
	"github.com/agent-parley/parley/internal/dispatcher"
	"github.com/agent-parley/parley/internal/executor"
	"github.com/agent-parley/parley/internal/manager"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/store"
	"github.com/agent-parley/parley/internal/testsupport"
)

type fakeAttemptRunner struct {
	preflightErr error
	block        chan struct{}
	started      chan struct{}
	panicRun     bool
	runErr       error

	mu              sync.Mutex
	preflightInputs []executor.AttemptInput
}

func (f *fakeAttemptRunner) Preflight(ctx context.Context, input executor.AttemptInput) error {
	f.mu.Lock()
	f.preflightInputs = append(f.preflightInputs, input)
	f.mu.Unlock()
	return f.preflightErr
}

func (f *fakeAttemptRunner) RunAttempt(ctx context.Context, input executor.AttemptInput) (executor.AttemptResult, error) {
	if f.started != nil {
		select {
		case f.started <- struct{}{}:
		default:
		}
	}
	if f.panicRun {
		panic("secret panic /tmp/private")
	}
	if f.block != nil {
		<-f.block
	}
	if f.runErr != nil {
		return executor.AttemptResult{Summary: "worker failed"}, f.runErr
	}
	return executor.AttemptResult{Summary: "done", Files: []executor.OutputFile{{Name: "worker-output.md", Kind: models.ArtifactKindWorkerOutput, Body: "ok"}}}, nil
}

func (f *fakeAttemptRunner) lastPreflight() executor.AttemptInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.preflightInputs[len(f.preflightInputs)-1]
}

func newDispatcherHarness(t *testing.T, runner *fakeAttemptRunner, workers int) (*store.Store, *dispatcher.Dispatcher) {
	t.Helper()
	st := testsupport.OpenStore(t)
	wf := manager.NewWorkflowService(st, runner, artifacts.NewWriter(st), slog.New(slog.NewTextHandler(io.Discard, nil)))
	d := dispatcher.New(st, wf, runner, slog.New(slog.NewTextHandler(io.Discard, nil)), workers)
	return st, d
}

func eventLeaks(event models.Event, needles ...string) bool {
	text := event.Summary + " " + fmt.Sprint(event.Data)
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition was not met before timeout")
}

func TestEnqueueRunsTaskThroughWorkflow(t *testing.T) {
	runner := &fakeAttemptRunner{started: make(chan struct{}, 1)}
	st, d := newDispatcherHarness(t, runner, 1)
	_, run, task := testsupport.ProjectAndTask(t, st)

	if err := d.Enqueue(context.Background(), task.ID); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, func() bool {
		got, _ := st.GetTask(task.ID)
		return got.Status == models.TaskStatusAwaitingReview
	})
	events := st.EventsForRun(run.ID)
	if !testsupport.HasEventSummary(events, "Attempt accepted by durable dispatcher") || !testsupport.HasEventSummary(events, "Attempt dispatch started") {
		t.Fatalf("missing dispatcher events: %+v", events)
	}
	if !testsupport.HasEventSummary(events, "Runner slot released") {
		t.Fatalf("missing workflow release event: %+v", events)
	}
}

func TestPreflightUsesPersistedTaskAdapterAndSanitizesFailureEvent(t *testing.T) {
	runner := &fakeAttemptRunner{preflightErr: errors.New("secret /tmp/private image missing")}
	st, d := newDispatcherHarness(t, runner, 1)
	_, run, task := testsupport.ProjectAndTask(t, st)
	task.Adapter = "pi-high-context"
	if err := st.UpdateTask(task); err != nil {
		t.Fatal(err)
	}

	err := d.Enqueue(context.Background(), task.ID)
	if err == nil || !strings.Contains(err.Error(), "secret") {
		t.Fatalf("expected raw preflight error returned to caller, got %v", err)
	}
	if runner.lastPreflight().Task.Adapter != "pi-high-context" {
		t.Fatalf("preflight did not use persisted adapter: %+v", runner.lastPreflight().Task)
	}
	events := st.EventsForRun(run.ID)
	if len(events) != 1 || events[0].Summary != "Attempt preflight failed" || events[0].Data["reason"] != "preflight_failed" {
		t.Fatalf("unexpected sanitized event: %+v", events)
	}
	if eventLeaks(events[0], "secret", "/tmp/private") {
		t.Fatalf("event leaked raw failure details: %+v", events[0])
	}
}

func TestDuplicateEnqueueRejectedWhileAttemptRunning(t *testing.T) {
	block := make(chan struct{})
	runner := &fakeAttemptRunner{block: block, started: make(chan struct{}, 1)}
	st, d := newDispatcherHarness(t, runner, 1)
	_, _, task := testsupport.ProjectAndTask(t, st)

	if err := d.Enqueue(context.Background(), task.ID); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	<-runner.started
	if err := d.Enqueue(context.Background(), task.ID); err == nil {
		t.Fatalf("expected duplicate enqueue rejection")
	}
	attempts := st.AttemptsForTask(task.ID)
	if len(attempts) != 1 || attempts[0].Number != 1 || attempts[0].Status != models.AttemptStatusRunning {
		t.Fatalf("duplicate enqueue created or changed attempts: %+v", attempts)
	}
	close(block)
	waitFor(t, func() bool {
		got, _ := st.GetTask(task.ID)
		return got.Status == models.TaskStatusAwaitingReview
	})
}

func TestQueuedBacklogPersistsWhenInProcessQueueIsFull(t *testing.T) {
	block := make(chan struct{})
	runner := &fakeAttemptRunner{block: block, started: make(chan struct{}, 1)}
	st, d := newDispatcherHarness(t, runner, 1)
	project, _, first := testsupport.ProjectAndTask(t, st)
	_, second, err := st.CreateManualRunTask(project, "second", "second", "", "", "done")
	if err != nil {
		t.Fatal(err)
	}
	_, third, err := st.CreateManualRunTask(project, "third", "third", "", "", "done")
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Enqueue(context.Background(), first.ID); err != nil {
		t.Fatal(err)
	}
	<-runner.started
	if err := d.Enqueue(context.Background(), second.ID); err != nil {
		t.Fatal(err)
	}
	if err := d.Enqueue(context.Background(), third.ID); err != nil {
		t.Fatalf("queued backlog should persist even when in-process queue is full: %v", err)
	}
	gotThird, _ := st.GetTask(third.ID)
	if gotThird.Status != models.TaskStatusQueued {
		t.Fatalf("third task should remain durably queued, got %+v", gotThird)
	}
	events := st.EventsForRun(third.RunID)
	if len(events) != 1 || events[0].Summary != "Attempt accepted by durable dispatcher" || events[0].Data["durable"] != true {
		t.Fatalf("unexpected durable accepted event: %+v", events)
	}
	close(block)
	waitFor(t, func() bool {
		gotFirst, _ := st.GetTask(first.ID)
		gotSecond, _ := st.GetTask(second.ID)
		gotThird, _ := st.GetTask(third.ID)
		return gotFirst.Status == models.TaskStatusAwaitingReview && gotSecond.Status == models.TaskStatusAwaitingReview && gotThird.Status == models.TaskStatusAwaitingReview
	})
}

func TestRunnerSlotContentionLeavesAttemptQueuedUntilCapacityReturns(t *testing.T) {
	block := make(chan struct{})
	runner := &fakeAttemptRunner{block: block, started: make(chan struct{}, 2)}
	st, d := newDispatcherHarness(t, runner, 2)
	project, _, first := testsupport.ProjectAndTask(t, st)
	_, second, err := st.CreateManualRunTask(project, "second", "second", "", "", "done")
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Enqueue(context.Background(), first.ID); err != nil {
		t.Fatal(err)
	}
	<-runner.started
	if err := d.Enqueue(context.Background(), second.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		got, _ := st.GetTask(second.ID)
		return got.Status == models.TaskStatusQueued
	})
	deferredBefore := 0
	waitFor(t, func() bool {
		deferredBefore = 0
		for _, event := range st.EventsForRun(second.RunID) {
			if event.Summary == "Attempt dispatch deferred" {
				deferredBefore++
			}
		}
		return deferredBefore == 1
	})
	close(block)
	waitFor(t, func() bool {
		gotFirst, _ := st.GetTask(first.ID)
		return gotFirst.Status == models.TaskStatusAwaitingReview
	})
	deferredAfter := 0
	for _, event := range st.EventsForRun(second.RunID) {
		if event.Summary == "Attempt dispatch deferred" {
			deferredAfter++
		}
	}
	if deferredBefore != 1 || deferredAfter != 1 {
		t.Fatalf("expected one deferred event without retry spam, before=%d after=%d events=%+v", deferredBefore, deferredAfter, st.EventsForRun(second.RunID))
	}
	waitFor(t, func() bool {
		gotSecond, _ := st.GetTask(second.ID)
		return gotSecond.Status == models.TaskStatusAwaitingReview
	})
}

func TestDispatcherRecoversQueuedAttemptsOnStartup(t *testing.T) {
	root := t.TempDir()
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	_, _, task := testsupport.ProjectAndTask(t, st)
	if _, _, _, err := st.QueueAttempt(task.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	runner := &fakeAttemptRunner{started: make(chan struct{}, 1)}
	wf := manager.NewWorkflowService(reopened, runner, artifacts.NewWriter(reopened), slog.New(slog.NewTextHandler(io.Discard, nil)))
	d := dispatcher.New(reopened, wf, runner, slog.New(slog.NewTextHandler(io.Discard, nil)), 1)
	if err := d.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		got, _ := reopened.GetTask(task.ID)
		return got.Status == models.TaskStatusAwaitingReview
	})
}

func TestDispatcherFailsUnrecoverableQueuedSetupError(t *testing.T) {
	st := testsupport.OpenStore(t)
	project, _, task := testsupport.ProjectAndTask(t, st)
	if _, _, _, err := st.QueueAttempt(task.ID); err != nil {
		t.Fatal(err)
	}
	project.RepoPath = t.TempDir() + "/missing"
	if err := st.UpdateProjectSettings(project); err != nil {
		t.Fatal(err)
	}
	runner := &fakeAttemptRunner{started: make(chan struct{}, 1)}
	wf := manager.NewWorkflowService(st, runner, artifacts.NewWriter(st), slog.New(slog.NewTextHandler(io.Discard, nil)))
	d := dispatcher.New(st, wf, runner, slog.New(slog.NewTextHandler(io.Discard, nil)), 1)
	if err := d.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		got, _ := st.GetTask(task.ID)
		return got.Status == models.TaskStatusFailed
	})
	attempts := st.AttemptsForTask(task.ID)
	if len(attempts) != 1 || attempts[0].Status != models.AttemptStatusFailed {
		t.Fatalf("queued setup failure did not fail attempt: %+v", attempts)
	}
	events := st.EventsForRun(task.RunID)
	if !testsupport.HasEventSummary(events, "Attempt dispatch failed") {
		t.Fatalf("missing sanitized setup failure event: %+v", events)
	}
	for _, event := range events {
		if event.Summary == "Attempt dispatch failed" && fmt.Sprint(event.Data["reason"]) != "setup_failed" {
			t.Fatalf("unexpected failure reason event=%+v", event)
		}
		if eventLeaks(event, "missing") {
			t.Fatalf("setup failure event leaked raw path: %+v", event)
		}
	}
}

func TestDispatcherIgnoresCompletedAttemptsOnRecovery(t *testing.T) {
	runner := &fakeAttemptRunner{started: make(chan struct{}, 1)}
	st, d := newDispatcherHarness(t, runner, 1)
	_, _, task := testsupport.ProjectAndTask(t, st)
	if err := d.Enqueue(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		got, _ := st.GetTask(task.ID)
		return got.Status == models.TaskStatusAwaitingReview
	})
	select {
	case <-runner.started:
	default:
	}
	if err := d.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runner.started:
		t.Fatalf("completed attempt was scheduled again during recovery")
	default:
	}
}

func TestWorkerFailureEmitsSanitizedAttemptFailedEvent(t *testing.T) {
	runner := &fakeAttemptRunner{runErr: errors.New("secret /tmp/private worker error"), started: make(chan struct{}, 1)}
	st, d := newDispatcherHarness(t, runner, 1)
	_, run, task := testsupport.ProjectAndTask(t, st)

	if err := d.Enqueue(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, event := range st.EventsForRun(run.ID) {
			if event.Summary == "Attempt dispatch failed" && event.Data["reason"] == "attempt_failed" {
				return true
			}
		}
		return false
	})
	for _, event := range st.EventsForRun(run.ID) {
		if event.Summary == "Attempt dispatch failed" && eventLeaks(event, "secret", "/tmp/private") {
			t.Fatalf("attempt_failed event leaked details: %+v", event)
		}
	}
}

func TestDispatcherPanicBackstopFailsRunningAttempt(t *testing.T) {
	runner := &fakeAttemptRunner{started: make(chan struct{}, 1)}
	st := testsupport.OpenStore(t)
	wf := manager.NewWorkflowService(st, runner, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	d := dispatcher.New(st, wf, runner, slog.New(slog.NewTextHandler(io.Discard, nil)), 1)
	_, run, task := testsupport.ProjectAndTask(t, st)

	if err := d.Enqueue(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		got, _ := st.GetTask(task.ID)
		return got.Status == models.TaskStatusFailed && got.LeaseID == ""
	})
	if active := st.ActiveLeaseCountByExecutor()[models.LocalExecutorID]; active != 0 {
		t.Fatalf("dispatcher panic left active lease: %d", active)
	}
	foundPanicEvent := false
	for _, event := range st.EventsForRun(run.ID) {
		if event.Summary == "Attempt dispatch failed" && event.Data["reason"] == "panic" {
			foundPanicEvent = true
			if eventLeaks(event, "secret", "/tmp/private") {
				t.Fatalf("dispatcher panic event leaked details: %+v", event)
			}
		}
	}
	if !foundPanicEvent {
		t.Fatalf("missing sanitized dispatcher panic event: %+v", st.EventsForRun(run.ID))
	}
}

func TestWorkerPanicFailsAttemptAndSanitizesLifecycle(t *testing.T) {
	runner := &fakeAttemptRunner{panicRun: true, started: make(chan struct{}, 1)}
	st, d := newDispatcherHarness(t, runner, 1)
	_, run, task := testsupport.ProjectAndTask(t, st)

	if err := d.Enqueue(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, event := range st.EventsForRun(run.ID) {
			if event.Summary == "Attempt dispatch failed" && event.Data["reason"] == "attempt_failed" {
				return true
			}
		}
		return false
	})
	for _, event := range st.EventsForRun(run.ID) {
		if event.Summary == "Attempt dispatch failed" && eventLeaks(event, "secret", "/tmp/private") {
			t.Fatalf("panic event leaked details: %+v", event)
		}
	}
}

func TestAcceptedEventMarksQueueDurable(t *testing.T) {
	block := make(chan struct{})
	runner := &fakeAttemptRunner{block: block, started: make(chan struct{}, 1)}
	st, d := newDispatcherHarness(t, runner, 1)
	_, run, task := testsupport.ProjectAndTask(t, st)

	if err := d.Enqueue(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, event := range st.EventsForRun(run.ID) {
			if event.Summary == "Attempt accepted by durable dispatcher" {
				return event.Data["durable"] == true
			}
		}
		return false
	})
	close(block)
	waitFor(t, func() bool {
		got, _ := st.GetTask(task.ID)
		return got.Status == models.TaskStatusAwaitingReview
	})
}
