package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestHumanReviewSuspendsAndRestartResumeAcceptsHumanEnvelope(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	firstEngine := NewEngineWithOptions(st, &capturingRunner{}, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	runID, err := firstEngine.StartRunInput(ctx, contract.TaskInput{Idea: "needs plan review", WorkflowTemplateID: workflow.BalancedPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
	queue, err := firstEngine.QueueState(ctx)
	if err != nil {
		t.Fatalf("queue state: %v", err)
	}
	if queue.Running != 0 {
		t.Fatalf("running slots = %d, want 0 after suspend", queue.Running)
	}
	if !hasArtifactKind(t, st, runID, "human_review_packet") {
		t.Fatal("missing human_review_packet artifact")
	}

	stage := stageByWorkflowID(t, st, runID, "plan_review_human")
	restartedEngine := NewEngineWithOptions(st, &capturingRunner{}, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	rep, err := restartedEngine.SubmitHumanReview(ctx, runID, stage.ID, HumanReviewSubmission{ActorID: "alice", Verdict: string(report.ReviewVerdictPass), Summary: "plan approved"})
	if err != nil {
		t.Fatalf("SubmitHumanReview() error = %v", err)
	}
	if rep.Actor.Kind != report.ActorKindHuman || rep.Actor.ID != "alice" || rep.Status != report.StatusCompleted || verdictString(rep.Verdict) != string(report.ReviewVerdictPass) {
		t.Fatalf("human report = %#v", rep)
	}
	waitForNotRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
	updatedStage := stageByWorkflowID(t, st, runID, "plan_review_human")
	if updatedStage.Status != report.StatusCompleted {
		t.Fatalf("human stage status = %s, want completed", updatedStage.Status)
	}
}

func TestAgentEscalationRoutesToWiredHumanReviewAndDoubleSubmitRejected(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	runner := &humanTestReviewRunner{verdict: report.ReviewVerdictEscalate}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "escalate review", WorkflowTemplateID: workflow.CarefulReviewID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
	planStage := stageByWorkflowID(t, st, runID, "plan_review_human")
	if _, err := engine.SubmitHumanReview(ctx, runID, planStage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictPass), Summary: "plan ok"}); err != nil {
		t.Fatalf("submit plan review: %v", err)
	}
	waitForWorkflowStageAwaiting(t, st, runID, "change_review_human")
	changeStage := stageByWorkflowID(t, st, runID, "change_review_human")
	if _, err := engine.SubmitHumanReview(ctx, runID, changeStage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictBlocked), Summary: "blocked by operator"}); err != nil {
		t.Fatalf("submit blocked review: %v", err)
	}
	if _, err := engine.SubmitHumanReview(ctx, runID, changeStage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictPass), Summary: "late"}); err == nil {
		t.Fatal("double submit succeeded")
	}
	waitForRunStatus(t, st, runID, store.RunStatusNeedsInput)
}

func TestMalformedHumanReviewRejectedWhileAwaiting(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	engine := NewEngineWithOptions(st, &capturingRunner{}, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "bad human form", WorkflowTemplateID: workflow.BalancedPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
	stage := stageByWorkflowID(t, st, runID, "plan_review_human")
	if _, err := engine.SubmitHumanReview(ctx, runID, stage.ID, HumanReviewSubmission{Verdict: "bogus", Summary: "bad"}); err == nil {
		t.Fatal("invalid verdict accepted")
	}
	run, err := st.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != store.RunStatusAwaitingHuman {
		t.Fatalf("run status = %s, want awaiting_human", run.Status)
	}
}

func stageByWorkflowID(t *testing.T, st *store.Store, runID, workflowStageID string) store.Stage {
	t.Helper()
	stages, err := st.ListStages(ctxBackground(), runID)
	if err != nil {
		t.Fatalf("list stages: %v", err)
	}
	for _, stage := range stages {
		if stage.WorkflowStageID == workflowStageID {
			return stage
		}
	}
	t.Fatalf("workflow stage %s not found", workflowStageID)
	return store.Stage{}
}

func waitForWorkflowStageAwaiting(t *testing.T, st *store.Store, runID, workflowStageID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		run, _ := st.GetRun(ctxBackground(), runID)
		stage := stageByWorkflowID(t, st, runID, workflowStageID)
		if run.Status == store.RunStatusAwaitingHuman && stage.Status == store.StageStatusRunning {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("workflow stage %s did not become awaiting_human", workflowStageID)
}

func waitForNotRunStatus(t *testing.T, st *store.Store, runID, status string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		run, err := st.GetRun(ctxBackground(), runID)
		if err == nil && run.Status != status {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run stayed in status %s", status)
}

func hasArtifactKind(t *testing.T, st *store.Store, runID, kind string) bool {
	t.Helper()
	artifacts, err := st.ListArtifacts(ctxBackground(), runID)
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	for _, artifact := range artifacts {
		if artifact.Kind == kind {
			return true
		}
	}
	return false
}

type humanTestReviewRunner struct {
	capturingRunner
	verdict report.Verdict
}

func (r *humanTestReviewRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	rep, err := r.capturingRunner.Dispatch(ctx, disp)
	if err != nil || disp.StageType != contract.StageTypeReview || disp.Input["review_role"] != contract.ReviewRoleArbiter {
		return rep, err
	}
	rep.Verdict = &r.verdict
	if r.verdict == report.ReviewVerdictChangesRequested {
		rep.Payload["arbitration_decisions"] = []any{map[string]any{"finding_id": "finding-1", "classification": report.ReviewFindingAccepted, "rationale": "real issue"}}
	}
	return rep, nil
}

func ctxBackground() context.Context { return context.Background() }
