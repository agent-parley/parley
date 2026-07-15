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

func TestGlobalCapZeroResumePathsIgnoreRunPoolSlots(t *testing.T) {
	t.Run("human review", func(t *testing.T) {
		ctx := context.Background()
		runner := newGlobalConcurrencyRunner()
		policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
		env := newTestEnv(t, runner, EngineOptions{
			QueuePolicy:         &policy,
			RunnerSlots:         1,
			GlobalMaxConcurrent: 0,
		})
		ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)

		parkedRunID, err := env.engine.StartRunInput(ctx, contract.TaskInput{
			Idea:               "park for human review",
			RefinementLevel:    contract.RefinementLevelDirect,
			WorkflowTemplateID: workflow.BalancedPRDeliveryID,
		})
		if err != nil {
			t.Fatalf("start human-review run: %v", err)
		}
		waitForRunStatus(t, env.store, parkedRunID, store.RunStatusAwaitingHuman)

		activeRunID := startGlobalConcurrencyRun(t, ctx, env.engine, "occupy the run pool")
		_ = receiveRunDispatchFor(t, runner.runDispatches, activeRunID)
		stage := stageByWorkflowID(t, env.store, parkedRunID, "plan_review_human")
		if _, err := env.engine.SubmitHumanReview(ctx, parkedRunID, stage.ID, HumanReviewSubmission{
			ActorID: "reviewer",
			Verdict: string(report.ReviewVerdictPass),
			Summary: "approved while another run executes",
		}); err != nil {
			t.Fatalf("SubmitHumanReview() with G=0 error = %v", err)
		}
		_ = receiveRunDispatchFor(t, runner.runDispatches, parkedRunID)

		cancelGlobalConcurrencyRun(t, ctx, env, activeRunID)
		cancelGlobalConcurrencyRun(t, ctx, env, parkedRunID)
	})

	t.Run("human memory approval", func(t *testing.T) {
		ctx := context.Background()
		runner := newContendedHumanMemoryRunner()
		policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
		env := newTestEnv(t, runner, EngineOptions{
			QueuePolicy:         &policy,
			RunnerSlots:         1,
			GlobalMaxConcurrent: 0,
		})
		ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)
		templateID := "global_cap_zero_human_memory"
		createMemoryProducerTemplateWithActor(t, env.store, templateID, true, workflow.ActorHuman)

		parkedRunID, err := env.engine.StartRunInput(ctx, contract.TaskInput{
			Idea:               "park for human memory approval",
			RefinementLevel:    contract.RefinementLevelDirect,
			WorkflowTemplateID: templateID,
		})
		if err != nil {
			t.Fatalf("start human-memory run: %v", err)
		}
		waitForWorkflowStageAwaiting(t, env.store, parkedRunID, "memory_update")

		runner.blockFutureDispatches()
		activeRunID := startGlobalConcurrencyRun(t, ctx, env.engine, "occupy the run pool")
		_ = receiveRunDispatchFor(t, runner.started, activeRunID)
		memoryStage := stageByWorkflowID(t, env.store, parkedRunID, "memory_update")
		if _, err := env.engine.SubmitHumanReview(ctx, parkedRunID, memoryStage.ID, HumanReviewSubmission{
			ActorID: "reviewer",
			Summary: "memory decisions approved while another run executes",
			MemoryDecisions: []HumanMemoryDecision{
				{CandidateID: "candidate-1", Action: store.ProjectMemoryDecisionApprove},
				{CandidateID: "candidate-2", Action: store.ProjectMemoryDecisionEdit, Kind: store.ProjectMemoryKindLesson, Title: "Edited memory lesson", Body: "Edited memory remains useful.", SourceSummary: "edited during contention test"},
				{CandidateID: "candidate-3", Action: store.ProjectMemoryDecisionReject, Reason: "not durable"},
				{CandidateID: "candidate-4", Action: store.ProjectMemoryDecisionDefer, Reason: "needs evidence"},
			},
		}); err != nil {
			t.Fatalf("SubmitHumanReview(memory) with G=0 error = %v", err)
		}
		waitForRunStatus(t, env.store, parkedRunID, store.RunStatusCompleted)
		cancelGlobalConcurrencyRun(t, ctx, env, activeRunID)
	})

	t.Run("paused run", func(t *testing.T) {
		ctx := context.Background()
		runner := newGlobalConcurrencyRunner()
		policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
		env := newTestEnv(t, runner, EngineOptions{
			QueuePolicy:         &policy,
			RunnerSlots:         1,
			GlobalMaxConcurrent: 0,
		})
		ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)
		template := pauseWorkflowTemplate("global_cap_zero_pause_resume")
		if err := env.store.CreateWorkflowTemplate(ctx, template); err != nil {
			t.Fatalf("create pause template: %v", err)
		}

		parkedRunID, err := env.engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{
			Idea:               "park at implementation boundary",
			RefinementLevel:    contract.RefinementLevelDirect,
			WorkflowTemplateID: template.ID,
		})
		if err != nil {
			t.Fatalf("start paused run: %v", err)
		}
		_ = receiveRunDispatchFor(t, runner.runDispatches, parkedRunID)
		if err := env.engine.RequestPause(ctx, parkedRunID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
			t.Fatalf("request pause: %v", err)
		}
		runner.releaseRun()
		waitForRunStatus(t, env.store, parkedRunID, store.RunStatusPaused)

		activeRunID := startGlobalConcurrencyRun(t, ctx, env.engine, "occupy the run pool")
		_ = receiveRunDispatchFor(t, runner.runDispatches, activeRunID)
		if err := env.engine.ResumeRun(ctx, parkedRunID); err != nil {
			t.Fatalf("ResumeRun() with G=0 error = %v", err)
		}
		_ = receiveRunDispatchFor(t, runner.runDispatches, parkedRunID)

		cancelGlobalConcurrencyRun(t, ctx, env, activeRunID)
		cancelGlobalConcurrencyRun(t, ctx, env, parkedRunID)
	})

	t.Run("stage rerun", func(t *testing.T) {
		ctx := context.Background()
		runner := newGlobalConcurrencyRunner()
		policy := QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 10}
		env := newTestEnv(t, runner, EngineOptions{
			QueuePolicy:         &policy,
			RunnerSlots:         1,
			GlobalMaxConcurrent: 0,
		})
		ensureGlobalConcurrencyProject(t, env.store, store.DefaultProjectID)
		template := stageRerunTemplate("global_cap_zero_stage_rerun", true)
		if err := env.store.CreateWorkflowTemplate(ctx, template); err != nil {
			t.Fatalf("create rerun template: %v", err)
		}
		terminal, err := env.store.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "rerun while pool is occupied", WorkflowTemplateID: template.ID})
		if err != nil {
			t.Fatalf("create terminal run: %v", err)
		}
		if err := env.store.SaveWorkflowSnapshot(ctx, terminal.Run.ID, map[string]any{"workflow_template_snapshot": template}); err != nil {
			t.Fatalf("save workflow snapshot: %v", err)
		}
		if err := env.store.UpdateRunStatus(ctx, terminal.Run.ID, store.RunStatusCompleted); err != nil {
			t.Fatalf("complete rerun target: %v", err)
		}

		activeRunID := startGlobalConcurrencyRun(t, ctx, env.engine, "occupy the run pool")
		_ = receiveRunDispatchFor(t, runner.runDispatches, activeRunID)
		if _, err := env.engine.ReRunStage(ctx, terminal.Run.ID, terminal.ImplementationStage.ID, stageReRunOperatorActor); err != nil {
			t.Fatalf("ReRunStage() with G=0 error = %v", err)
		}
		_ = receiveRunDispatchFor(t, runner.runDispatches, terminal.Run.ID)

		cancelGlobalConcurrencyRun(t, ctx, env, activeRunID)
		cancelGlobalConcurrencyRun(t, ctx, env, terminal.Run.ID)
	})
}

func TestGlobalBlockedResumeDoesNotConsumeReservation(t *testing.T) {
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
	template := pauseWorkflowTemplate("global_cap_blocked_resume_counter")
	if err := env.store.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create pause template: %v", err)
	}

	pausedRunID, err := env.engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{
		Idea:               "blocked resume retains counters",
		RefinementLevel:    contract.RefinementLevelDirect,
		WorkflowTemplateID: template.ID,
	})
	if err != nil {
		t.Fatalf("start paused run: %v", err)
	}
	_ = receiveRunDispatchFor(t, runner.runDispatches, pausedRunID)
	if err := env.engine.RequestPause(ctx, pausedRunID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
		t.Fatalf("request pause: %v", err)
	}
	runner.releaseRun()
	waitForRunStatus(t, env.store, pausedRunID, store.RunStatusPaused)
	env.recorder.waitUntil(t, func() bool {
		return env.engine.globalCapacitySnapshot().runsInflight == 0
	})

	message, err := env.engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "hold the global slot")
	if err != nil {
		t.Fatalf("submit conversation turn: %v", err)
	}
	_ = receiveDispatch(t, runner.turnDispatches)
	before := env.engine.globalCapacitySnapshot()
	if err := env.engine.ResumeRun(ctx, pausedRunID); !errors.Is(err, ErrNoRunnerSlots) {
		t.Fatalf("ResumeRun() error = %v, want ErrNoRunnerSlots", err)
	}
	after := env.engine.globalCapacitySnapshot()
	if after != before {
		t.Fatalf("global capacity changed after blocked resume: before=%+v after=%+v", before, after)
	}
	assertRunStatus(t, env.store, pausedRunID, store.RunStatusPaused)

	runner.releaseTurn()
	_ = waitForConversationEvent(t, env.recorder, "conversation.turn_completed", message.ID)
}

type contendedHumanMemoryRunner struct {
	base    humanMemoryApprovalRunner
	mu      sync.Mutex
	block   bool
	started chan contract.Dispatch
}

func newContendedHumanMemoryRunner() *contendedHumanMemoryRunner {
	return &contendedHumanMemoryRunner{started: make(chan contract.Dispatch, 1)}
}

func (r *contendedHumanMemoryRunner) blockFutureDispatches() {
	r.mu.Lock()
	r.block = true
	r.mu.Unlock()
}

func (r *contendedHumanMemoryRunner) Dispatch(ctx context.Context, dispatch contract.Dispatch) (report.Report, error) {
	r.mu.Lock()
	block := r.block
	r.mu.Unlock()
	if !block {
		return r.base.Dispatch(ctx, dispatch)
	}
	select {
	case r.started <- dispatch:
	case <-ctx.Done():
		return report.Report{}, ctx.Err()
	}
	<-ctx.Done()
	return report.Report{}, ctx.Err()
}

func (r *contendedHumanMemoryRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func receiveRunDispatchFor(t *testing.T, dispatches <-chan contract.Dispatch, runID string) contract.Dispatch {
	t.Helper()
	dispatch := receiveDispatch(t, dispatches)
	if dispatch.RunID != runID {
		t.Fatalf("dispatch run = %s, want %s", dispatch.RunID, runID)
	}
	return dispatch
}

func cancelGlobalConcurrencyRun(t *testing.T, ctx context.Context, env *testEnv, runID string) {
	t.Helper()
	if err := env.engine.CancelRun(ctx, runID); err != nil {
		t.Fatalf("CancelRun(%s) error = %v", runID, err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCancelled)
}
