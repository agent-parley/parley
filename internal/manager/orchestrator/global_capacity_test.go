package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestGlobalCapBindsAcrossRunAndTurnPools(t *testing.T) {
	ctx := context.Background()
	runner := newGlobalConcurrencyRunner()
	policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 2, BacklogCap: 10}
	env := newTestEnv(t, runner, EngineOptions{
		QueuePolicy:         &policy,
		RunnerSlots:         2,
		ConversationBudget:  2,
		GlobalMaxConcurrent: 2,
		InteractiveReserve:  1,
	})
	ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)
	ensureGlobalConcurrencyProject(t, env.store, "second-project")

	_, err := env.engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "first turn")
	if err != nil {
		t.Fatalf("submit first turn: %v", err)
	}
	_, err = env.engine.SubmitConversationMessage(ctx, "second-project", "second turn")
	if err != nil {
		t.Fatalf("submit second turn: %v", err)
	}
	_ = receiveDispatch(t, runner.turnDispatches)
	_ = receiveDispatch(t, runner.turnDispatches)

	runID := startGlobalConcurrencyRun(t, ctx, env.engine, "held behind two turns")
	assertNoDispatch(t, runner.runDispatches)
	assertRunStatus(t, env.store, runID, store.RunStatusPending)
	assertGlobalCapacity(t, env.engine, 0, 2)

	runner.releaseTurn()
	env.recorder.waitUntil(t, func() bool {
		return env.engine.globalCapacitySnapshot().turnsInflight == 1
	})
	assertNoDispatch(t, runner.runDispatches)

	runner.releaseTurn()
	env.recorder.waitUntil(t, func() bool {
		return env.engine.globalCapacitySnapshot().turnsInflight == 0
	})
	_ = receiveDispatch(t, runner.runDispatches)
	assertGlobalCapacity(t, env.engine, 1, 0)

	if err := env.engine.CancelRun(ctx, runID); err != nil {
		t.Fatalf("cancel run: %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCancelled)
}

func TestInteractiveReserveAdmitsTurnAndHoldsAdditionalRun(t *testing.T) {
	ctx := context.Background()
	runner := newGlobalConcurrencyRunner()
	policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 2, BacklogCap: 10}
	env := newTestEnv(t, runner, EngineOptions{
		QueuePolicy:         &policy,
		RunnerSlots:         2,
		ConversationBudget:  2,
		GlobalMaxConcurrent: 2,
		InteractiveReserve:  1,
	})
	ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)

	firstRunID := startGlobalConcurrencyRun(t, ctx, env.engine, "first run")
	_ = receiveDispatch(t, runner.runDispatches)
	message, err := env.engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "reserved interactive turn")
	if err != nil {
		t.Fatalf("submit turn: %v", err)
	}
	_ = receiveDispatch(t, runner.turnDispatches)

	secondRunID := startGlobalConcurrencyRun(t, ctx, env.engine, "second run")
	assertNoDispatch(t, runner.runDispatches)
	assertRunStatus(t, env.store, secondRunID, store.RunStatusPending)
	assertGlobalCapacity(t, env.engine, 1, 1)
	queueState, err := env.engine.QueueState(ctx)
	if err != nil {
		t.Fatalf("QueueState() error = %v", err)
	}
	if queueState.RunsInflight != 1 || queueState.TurnsInflight != 1 || queueState.GlobalMaxConcurrent != 2 || queueState.InteractiveReserve != 1 {
		t.Fatalf("queue global state = %+v, want runs 1 turns 1 cap 2 reserve 1", queueState)
	}

	if err := env.engine.CancelRun(ctx, firstRunID); err != nil {
		t.Fatalf("cancel first run: %v", err)
	}
	waitForRunStatus(t, env.store, firstRunID, store.RunStatusCancelled)
	runner.releaseTurn()
	_ = waitForConversationEvent(t, env.recorder, "conversation.turn_completed", message.ID)
	_ = receiveDispatch(t, runner.runDispatches)
	if err := env.engine.CancelRun(ctx, secondRunID); err != nil {
		t.Fatalf("cancel second run: %v", err)
	}
	waitForRunStatus(t, env.store, secondRunID, store.RunStatusCancelled)
}

func TestGlobalCapCrossPoolWakeups(t *testing.T) {
	t.Run("turn finish wakes pending run", func(t *testing.T) {
		ctx := context.Background()
		runner := newGlobalConcurrencyRunner()
		policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
		env := newTestEnv(t, runner, EngineOptions{
			QueuePolicy:         &policy,
			RunnerSlots:         1,
			ConversationBudget:  1,
			GlobalMaxConcurrent: 1,
			InteractiveReserve:  0,
		})
		ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)

		message, err := env.engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "hold the cap")
		if err != nil {
			t.Fatalf("submit turn: %v", err)
		}
		_ = receiveDispatch(t, runner.turnDispatches)
		runID := startGlobalConcurrencyRun(t, ctx, env.engine, "wake after turn")
		assertNoDispatch(t, runner.runDispatches)

		runner.releaseTurn()
		_ = waitForConversationEvent(t, env.recorder, "conversation.turn_completed", message.ID)
		_ = receiveDispatch(t, runner.runDispatches)
		if err := env.engine.CancelRun(ctx, runID); err != nil {
			t.Fatalf("cancel run: %v", err)
		}
		waitForRunStatus(t, env.store, runID, store.RunStatusCancelled)
	})

	t.Run("run finish wakes ready turn", func(t *testing.T) {
		ctx := context.Background()
		runner := newGlobalConcurrencyRunner()
		policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
		env := newTestEnv(t, runner, EngineOptions{
			QueuePolicy:         &policy,
			RunnerSlots:         1,
			ConversationBudget:  1,
			GlobalMaxConcurrent: 1,
			InteractiveReserve:  0,
		})
		ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)

		runID := startGlobalConcurrencyRun(t, ctx, env.engine, "hold the cap")
		_ = receiveDispatch(t, runner.runDispatches)
		message, err := env.engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "wake after run")
		if err != nil {
			t.Fatalf("submit turn: %v", err)
		}
		assertNoDispatch(t, runner.turnDispatches)

		if err := env.engine.CancelRun(ctx, runID); err != nil {
			t.Fatalf("cancel run: %v", err)
		}
		waitForRunStatus(t, env.store, runID, store.RunStatusCancelled)
		_ = receiveDispatch(t, runner.turnDispatches)
		runner.releaseTurn()
		_ = waitForConversationEvent(t, env.recorder, "conversation.turn_completed", message.ID)
	})
}

func TestGlobalCapZeroLeavesPoolsIndependent(t *testing.T) {
	ctx := context.Background()
	runner := newGlobalConcurrencyRunner()
	policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
	env := newTestEnv(t, runner, EngineOptions{
		QueuePolicy:         &policy,
		RunnerSlots:         1,
		ConversationBudget:  1,
		GlobalMaxConcurrent: 0,
	})
	ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)

	runID := startGlobalConcurrencyRun(t, ctx, env.engine, "independent run")
	_ = receiveDispatch(t, runner.runDispatches)
	message, err := env.engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "independent turn")
	if err != nil {
		t.Fatalf("submit turn: %v", err)
	}
	_ = receiveDispatch(t, runner.turnDispatches)
	assertGlobalCapacity(t, env.engine, 1, 1)

	if err := env.engine.CancelRun(ctx, runID); err != nil {
		t.Fatalf("cancel run: %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCancelled)
	runner.releaseTurn()
	_ = waitForConversationEvent(t, env.recorder, "conversation.turn_completed", message.ID)
}

func TestPausedRunReleasesAndReacquiresGlobalSlot(t *testing.T) {
	ctx := context.Background()
	runner := newGlobalConcurrencyRunner()
	policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
	env := newTestEnv(t, runner, EngineOptions{
		QueuePolicy:         &policy,
		RunnerSlots:         1,
		ConversationBudget:  1,
		GlobalMaxConcurrent: 1,
		InteractiveReserve:  0,
	})
	ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)
	template := pauseWorkflowTemplate("global_capacity_pause")
	if err := env.store.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create pause workflow template: %v", err)
	}

	runID, err := env.engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{Idea: "pause and resume", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("start pause run: %v", err)
	}
	_ = receiveDispatch(t, runner.runDispatches)
	if err := env.engine.RequestPause(ctx, runID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
		t.Fatalf("request pause: %v", err)
	}
	runner.releaseRun()
	waitForRunStatus(t, env.store, runID, store.RunStatusPaused)
	env.recorder.waitUntil(t, func() bool {
		snapshot := env.engine.globalCapacitySnapshot()
		return snapshot.runsInflight == 0 && snapshot.turnsInflight == 0
	})
	assertGlobalCapacity(t, env.engine, 0, 0)

	message, err := env.engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "use parked capacity")
	if err != nil {
		t.Fatalf("submit turn: %v", err)
	}
	_ = receiveDispatch(t, runner.turnDispatches)
	if err := env.engine.ResumeRun(ctx, runID); !errors.Is(err, ErrNoRunnerSlots) {
		t.Fatalf("ResumeRun() error = %v, want ErrNoRunnerSlots", err)
	}
	assertRunStatus(t, env.store, runID, store.RunStatusPaused)

	runner.releaseTurn()
	_ = waitForConversationEvent(t, env.recorder, "conversation.turn_completed", message.ID)
	if err := env.engine.ResumeRun(ctx, runID); err != nil {
		t.Fatalf("ResumeRun() after turn: %v", err)
	}
	_ = receiveDispatch(t, runner.runDispatches)
	assertGlobalCapacity(t, env.engine, 1, 0)
	if err := env.engine.CancelRun(ctx, runID); err != nil {
		t.Fatalf("cancel resumed run: %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCancelled)
}

func TestGlobalCapHeldEventOncePerDispatchPass(t *testing.T) {
	t.Run("global cap is binding", func(t *testing.T) {
		ctx := context.Background()
		runner := newGlobalConcurrencyRunner()
		policy := QueuePolicy{AutoWhenReady: false, MaxConcurrent: 2, BacklogCap: 10}
		env := newTestEnv(t, runner, EngineOptions{
			QueuePolicy:         &policy,
			RunnerSlots:         2,
			ConversationBudget:  1,
			GlobalMaxConcurrent: 1,
			InteractiveReserve:  0,
		})
		ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)
		message, err := env.engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "hold cap")
		if err != nil {
			t.Fatalf("submit turn: %v", err)
		}
		_ = receiveDispatch(t, runner.turnDispatches)
		_ = startGlobalConcurrencyRun(t, ctx, env.engine, "pending one")
		_ = startGlobalConcurrencyRun(t, ctx, env.engine, "pending two")

		env.engine.queuePolicy.AutoWhenReady = true
		if err := env.engine.DispatchPending(ctx); err != nil {
			t.Fatalf("DispatchPending() error = %v", err)
		}
		events := globalHeldEvents(t, env.store)
		if len(events) != 1 {
			t.Fatalf("held event count = %d, want 1 for one pass", len(events))
		}
		if reportPayloadInt(events[0].Data, "runs_inflight") != 0 || reportPayloadInt(events[0].Data, "turns_inflight") != 1 || reportPayloadInt(events[0].Data, "global_max") != 1 || reportPayloadInt(events[0].Data, "reserve") != 0 {
			t.Fatalf("held event data = %#v, want runs 0 turns 1 global 1 reserve 0", events[0].Data)
		}
		runner.releaseTurn()
		_ = waitForConversationEvent(t, env.recorder, "conversation.turn_completed", message.ID)
	})

	t.Run("per-pool limit is binding", func(t *testing.T) {
		ctx := context.Background()
		runner := newGlobalConcurrencyRunner()
		policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
		env := newTestEnv(t, runner, EngineOptions{
			QueuePolicy:         &policy,
			RunnerSlots:         1,
			GlobalMaxConcurrent: 3,
			InteractiveReserve:  0,
		})
		ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)
		firstRunID := startGlobalConcurrencyRun(t, ctx, env.engine, "running")
		_ = receiveDispatch(t, runner.runDispatches)
		env.engine.queuePolicy.AutoWhenReady = false
		_ = startGlobalConcurrencyRun(t, ctx, env.engine, "pending")
		env.engine.queuePolicy.AutoWhenReady = true
		if err := env.engine.DispatchPending(ctx); err != nil {
			t.Fatalf("DispatchPending() error = %v", err)
		}
		if events := globalHeldEvents(t, env.store); len(events) != 0 {
			t.Fatalf("held events = %#v, want none when queue max is binding", events)
		}
		if err := env.engine.CancelRun(ctx, firstRunID); err != nil {
			t.Fatalf("cancel run: %v", err)
		}
		waitForRunStatus(t, env.store, firstRunID, store.RunStatusCancelled)
	})
}

func TestGlobalReservationRevertsWhenDispatchCannotSpawnAndQueuedCancelIsFree(t *testing.T) {
	ctx := context.Background()
	runner := newGlobalConcurrencyRunner()
	policy := QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}
	env := newTestEnv(t, runner, EngineOptions{
		QueuePolicy:         &policy,
		RunnerSlots:         1,
		GlobalMaxConcurrent: 1,
		InteractiveReserve:  0,
	})
	ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)

	cancelledID := startGlobalConcurrencyRun(t, ctx, env.engine, "cancel while queued")
	if err := env.engine.CancelRun(ctx, cancelledID); err != nil {
		t.Fatalf("cancel queued run: %v", err)
	}
	assertGlobalCapacity(t, env.engine, 0, 0)

	shutdownCtx, cancel := context.WithTimeout(ctx, testWaitTimeout)
	defer cancel()
	if err := env.engine.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	failedSpawnID := startGlobalConcurrencyRun(t, ctx, env.engine, "spawn after shutdown")
	if err := env.engine.StartQueuedRun(ctx, failedSpawnID); err != nil {
		t.Fatalf("StartQueuedRun() error = %v", err)
	}
	assertGlobalCapacity(t, env.engine, 0, 0)
}

func TestGlobalCapacityCountersAreRaceSafe(t *testing.T) {
	engine := &Engine{globalMaxConcurrent: 8, interactiveReserve: 2}
	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if (worker+i)%2 == 0 {
					if snapshot, ok := engine.tryReserveGlobalRun(); ok {
						if snapshot.runsInflight+snapshot.turnsInflight > snapshot.globalMaxConcurrent {
							t.Errorf("run reservation exceeded cap: %+v", snapshot)
						}
						engine.releaseGlobalRun()
					}
					continue
				}
				if snapshot, ok := engine.tryReserveGlobalTurn(); ok {
					if snapshot.runsInflight+snapshot.turnsInflight > snapshot.globalMaxConcurrent {
						t.Errorf("turn reservation exceeded cap: %+v", snapshot)
					}
					engine.releaseGlobalTurn()
				}
			}
		}(worker)
	}
	wg.Wait()
	snapshot := engine.globalCapacitySnapshot()
	if snapshot.runsInflight != 0 || snapshot.turnsInflight != 0 {
		t.Fatalf("final global capacity = %+v, want zero", snapshot)
	}
}

func startGlobalConcurrencyRun(t *testing.T, ctx context.Context, engine *Engine, idea string) string {
	t.Helper()
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: idea, WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRunInput(%q) error = %v", idea, err)
	}
	return runID
}

func ensureGlobalConcurrencyProject(t *testing.T, st *store.Store, projectID string) {
	t.Helper()
	_, err := st.EnsureProject(context.Background(), store.ProjectSpec{
		ID:                 projectID,
		Name:               projectID,
		RepositoryPath:     t.TempDir(),
		QueueAutoWhenReady: true,
		QueueMaxConcurrent: 2,
		QueueBacklogCap:    10,
	})
	if err != nil {
		t.Fatalf("ensure project %s: %v", projectID, err)
	}
}

func assertGlobalCapacity(t *testing.T, engine *Engine, runs, turns int) {
	t.Helper()
	snapshot := engine.globalCapacitySnapshot()
	if snapshot.runsInflight != runs || snapshot.turnsInflight != turns {
		t.Fatalf("global capacity = runs %d turns %d, want %d and %d", snapshot.runsInflight, snapshot.turnsInflight, runs, turns)
	}
}

func globalHeldEvents(t *testing.T, st *store.Store) []event.Event {
	t.Helper()
	page, err := st.ListSystemEventsPage(context.Background(), 0, 100)
	if err != nil {
		t.Fatalf("list system events: %v", err)
	}
	var events []event.Event
	for _, entry := range page.Events {
		if entry.Event.Type == "queue.held_global_cap" {
			events = append(events, entry.Event)
		}
	}
	return events
}

type globalConcurrencyRunner struct {
	runDispatches  chan contract.Dispatch
	turnDispatches chan contract.Dispatch
	runReleases    chan struct{}
	turnReleases   chan struct{}
}

func newGlobalConcurrencyRunner() *globalConcurrencyRunner {
	return &globalConcurrencyRunner{
		runDispatches:  make(chan contract.Dispatch, 16),
		turnDispatches: make(chan contract.Dispatch, 16),
		runReleases:    make(chan struct{}, 16),
		turnReleases:   make(chan struct{}, 16),
	}
}

func (r *globalConcurrencyRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	if disp.StageType == contract.StageTypeConversation {
		select {
		case r.turnDispatches <- disp:
		case <-ctx.Done():
			return report.Report{}, ctx.Err()
		}
		select {
		case <-r.turnReleases:
			return report.Report{
				SchemaVersion: report.SchemaVersion,
				RunID:         disp.RunID,
				TaskID:        disp.TaskID,
				AttemptID:     disp.AttemptID,
				StageID:       disp.StageID,
				StageType:     disp.StageType,
				Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
				Status:        report.StatusCompleted,
				Summary:       "conversation completed",
				Payload:       map[string]any{"reply_markdown": "done"},
				Errors:        []string{},
			}, nil
		case <-ctx.Done():
			return report.Report{}, ctx.Err()
		}
	}

	select {
	case r.runDispatches <- disp:
	case <-ctx.Done():
		return report.Report{}, ctx.Err()
	}
	select {
	case <-r.runReleases:
		return validAdapterReport(disp, "stage completed"), nil
	case <-ctx.Done():
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
}

func (r *globalConcurrencyRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func (r *globalConcurrencyRunner) releaseRun() {
	r.runReleases <- struct{}{}
}

func (r *globalConcurrencyRunner) releaseTurn() {
	r.turnReleases <- struct{}{}
}

var _ Runner = (*globalConcurrencyRunner)(nil)
