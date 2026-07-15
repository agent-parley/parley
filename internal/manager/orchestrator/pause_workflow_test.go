package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestOperatorPauseEditRemainingStagesAndResume(t *testing.T) {
	ctx := context.Background()
	runner := newPauseBoundaryRunner()
	env := newTestEnv(t, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}})
	template := pauseWorkflowTemplate("pause_edit_template")
	if err := env.store.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create workflow template: %v", err)
	}

	runID, err := env.engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{Idea: "pause and edit", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("StartProjectRunInput() error = %v", err)
	}
	first := runner.waitForDispatch(t)
	if first.StageType != contract.StageTypeImplementation {
		t.Fatalf("first dispatch stage = %s, want implementation", first.StageType)
	}
	if err := env.engine.RequestPause(ctx, runID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
		t.Fatalf("RequestPause() error = %v", err)
	}
	run, err := env.store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != store.RunStatusRunning {
		t.Fatalf("run status while implementation is in flight = %s, want running", run.Status)
	}
	runner.releaseOne()
	waitForRunStatus(t, env.store, runID, store.RunStatusPaused)
	if runner.dispatchCountByRunAndType(runID, contract.StageTypeValidation) != 0 {
		t.Fatalf("validation dispatched before resume despite pause: %#v", runner.dispatchTypes())
	}
	secondRunID, err := env.engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{Idea: "second run uses freed slot", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("StartProjectRunInput(second) error = %v", err)
	}
	second := runner.waitForDispatch(t)
	if second.RunID != secondRunID || second.StageType != contract.StageTypeImplementation {
		t.Fatalf("second dispatch = run %s stage %s, want run %s implementation", second.RunID, second.StageType, secondRunID)
	}
	runner.releaseOne()
	waitForRunStatus(t, env.store, secondRunID, store.RunStatusCompleted)

	badEdit, err := env.store.LatestWorkflowTemplateSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("latest workflow template snapshot: %v", err)
	}
	for i := range badEdit.Stages {
		if badEdit.Stages[i].ID == "implementation" {
			badEdit.Stages[i].Instructions = "rewrite executed history"
		}
	}
	if err := env.engine.UpdateRunWorkflowSnapshot(ctx, runID, badEdit, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err == nil || !strings.Contains(err.Error(), "executed workflow prefix") {
		t.Fatalf("executed-prefix edit error = %v, want executed workflow prefix rejection", err)
	}
	assertRunStatus(t, env.store, runID, store.RunStatusPaused)

	edited, err := env.store.LatestWorkflowTemplateSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("latest workflow template snapshot: %v", err)
	}
	var remaining []workflow.StageTemplate
	for _, stage := range edited.Stages {
		if stage.ID != "validation" {
			remaining = append(remaining, stage)
		}
	}
	edited.Stages = remaining
	edited.Edges = workflow.DeriveTemplateEdges(edited)
	if err := env.engine.UpdateRunWorkflowSnapshot(ctx, runID, edited, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
		t.Fatalf("UpdateRunWorkflowSnapshot() error = %v", err)
	}
	latest, err := env.store.LatestWorkflowTemplateSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("latest workflow template snapshot after edit: %v", err)
	}
	if workflowTemplateHasStage(latest, "validation") {
		t.Fatalf("validation stage still present after run-local edit: %+v", latest.Stages)
	}

	if err := env.engine.ResumeRun(ctx, runID); err != nil {
		t.Fatalf("ResumeRun() error = %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCompleted)
	if runner.dispatchCountByRunAndType(runID, contract.StageTypeValidation) != 0 {
		t.Fatalf("validation dispatched after it was removed while paused: %#v", runner.dispatchTypes())
	}
	events, err := env.store.ListEvents(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for _, typ := range []string{"run.pause_requested", "run.paused", "run.workflow_snapshot_edited", "run.resumed"} {
		if !hasEventType(events, typ) {
			t.Fatalf("missing %s event in %#v", typ, eventTypes(events))
		}
	}
}

func TestCancelWhilePausedTerminatesAndRejectsResume(t *testing.T) {
	ctx := context.Background()
	runner := newPauseBoundaryRunner()
	env := newTestEnv(t, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}})
	template := pauseWorkflowTemplate("pause_cancel_template")
	if err := env.store.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create workflow template: %v", err)
	}
	runID := startPausedRunAtImplementationBoundary(t, ctx, env, runner, template.ID, "cancel while paused")

	if err := env.engine.CancelRun(ctx, runID); err != nil {
		t.Fatalf("CancelRun() error = %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCancelled)
	events, err := env.store.ListEvents(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if !hasEventType(events, "run.cancelled") {
		t.Fatalf("missing run.cancelled event: %#v", eventTypes(events))
	}
	if err := env.engine.ResumeRun(ctx, runID); !errors.Is(err, ErrRunNotPaused) {
		t.Fatalf("ResumeRun() after cancel error = %v, want ErrRunNotPaused", err)
	}
}

func TestResumePausedRunWithoutEditsMatchesUnpausedDispatchSequence(t *testing.T) {
	ctx := context.Background()
	template := pauseWorkflowTemplate("pause_plain_resume_template")

	unpausedRunner := newPauseBoundaryRunner()
	unpausedEnv := newTestEnv(t, unpausedRunner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}})
	if err := unpausedEnv.store.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create unpaused workflow template: %v", err)
	}
	unpausedID, err := unpausedEnv.engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{Idea: "unpaused baseline", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("StartProjectRunInput(unpaused) error = %v", err)
	}
	unpausedRunner.expectDispatch(t, unpausedID, "implementation")
	unpausedRunner.releaseOne()
	waitForRunStatus(t, unpausedEnv.store, unpausedID, store.RunStatusCompleted)
	unpausedSeq := unpausedRunner.workflowStageIDsForRun(unpausedID)

	pausedRunner := newPauseBoundaryRunner()
	pausedEnv := newTestEnv(t, pausedRunner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}})
	if err := pausedEnv.store.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create paused workflow template: %v", err)
	}
	pausedID := startPausedRunAtImplementationBoundary(t, ctx, pausedEnv, pausedRunner, template.ID, "plain paused resume")
	if err := pausedEnv.engine.ResumeRun(ctx, pausedID); err != nil {
		t.Fatalf("ResumeRun() error = %v", err)
	}
	waitForRunStatus(t, pausedEnv.store, pausedID, store.RunStatusCompleted)
	pausedSeq := pausedRunner.workflowStageIDsForRun(pausedID)

	if strings.Join(pausedSeq, ",") != strings.Join(unpausedSeq, ",") {
		t.Fatalf("paused dispatch sequence = %#v, want unpaused sequence %#v", pausedSeq, unpausedSeq)
	}
}

func TestPausedRunSurvivesRestartRecoveryAndResumes(t *testing.T) {
	ctx := context.Background()
	runner := newPauseBoundaryRunner()
	env := newTestEnv(t, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}})
	template := pauseWorkflowTemplate("pause_restart_template")
	if err := env.store.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create workflow template: %v", err)
	}
	runID := startPausedRunAtImplementationBoundary(t, ctx, env, runner, template.ID, "restart while paused")

	if err := env.engine.failInterruptedRunning(ctx); err != nil {
		t.Fatalf("failInterruptedRunning() error = %v", err)
	}
	assertRunStatus(t, env.store, runID, store.RunStatusPaused)
	if err := env.engine.ResumeRun(ctx, runID); err != nil {
		t.Fatalf("ResumeRun() error = %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCompleted)
}

func TestPausedRunSurvivesRunnerDownAndResumes(t *testing.T) {
	ctx := context.Background()
	runner := newPauseBoundaryRunner()
	env := newTestEnv(t, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}})
	template := pauseWorkflowTemplate("pause_runner_down_template")
	if err := env.store.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create workflow template: %v", err)
	}
	runID := startPausedRunAtImplementationBoundary(t, ctx, env, runner, template.ID, "runner down while paused")

	if err := env.engine.HandleRunnerDown(ctx, "runner-test", "connection lost"); err != nil {
		t.Fatalf("HandleRunnerDown() error = %v", err)
	}
	assertRunStatus(t, env.store, runID, store.RunStatusPaused)
	if err := env.engine.ResumeRun(ctx, runID); err != nil {
		t.Fatalf("ResumeRun() error = %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCompleted)
}

func TestAddedPendingStageExecutesInEditedOrderOnResume(t *testing.T) {
	ctx := context.Background()
	runner := newPauseBoundaryRunner()
	env := newTestEnv(t, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}})
	template := pauseWorkflowTemplate("pause_add_stage_template")
	if err := env.store.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create workflow template: %v", err)
	}
	runID := startPausedRunAtImplementationBoundary(t, ctx, env, runner, template.ID, "add a stage while paused")

	edited, err := env.store.LatestWorkflowTemplateSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("latest workflow template snapshot: %v", err)
	}
	added := workflow.StageTemplate{ID: "extra_validation", Type: workflow.StageTypeValidation, Label: "Extra validation", Actor: workflow.ActorHarness}
	var reordered []workflow.StageTemplate
	for _, stage := range edited.Stages {
		reordered = append(reordered, stage)
		if stage.ID == "implementation" {
			reordered = append(reordered, added)
		}
	}
	edited.Stages = reordered
	edited.Edges = workflow.DeriveTemplateEdges(edited)
	if err := env.engine.UpdateRunWorkflowSnapshot(ctx, runID, edited, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
		t.Fatalf("UpdateRunWorkflowSnapshot() error = %v", err)
	}
	if err := env.engine.ResumeRun(ctx, runID); err != nil {
		t.Fatalf("ResumeRun() error = %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCompleted)
	got := runner.workflowStageIDsForRun(runID)
	want := []string{"implementation", "extra_validation", "validation"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("workflow dispatch sequence = %#v, want %#v", got, want)
	}
}

func TestRequestPauseRejectsRunningRunWithoutActiveGoroutine(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, &capturingRunner{}, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100}})
	runID, err := env.engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{Idea: "stale running", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: workflow.QuickFixDeliveryID})
	if err != nil {
		t.Fatalf("StartProjectRunInput() error = %v", err)
	}
	if err := env.store.UpdateRunStatus(ctx, runID, store.RunStatusRunning); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := env.engine.RequestPause(ctx, runID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); !errors.Is(err, ErrRunNotRunning) {
		t.Fatalf("RequestPause() error = %v, want ErrRunNotRunning", err)
	}
	if env.engine.isPausing(runID) {
		t.Fatal("inactive running run left a stale pausing flag")
	}
	events, err := env.store.ListEvents(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if hasEventType(events, "run.pause_requested") {
		t.Fatalf("inactive running run emitted run.pause_requested: %#v", eventTypes(events))
	}
}

func TestOperatorPauseRequestedDuringStopReportExpiresOnCompletion(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	recorder := newEventRecorder()
	renderer := newStopReportBlockingRenderer()
	engine := NewEngineWithOptions(st, &capturingRunner{}, renderer, recorder, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}})
	testRecorders.Store(st, recorder)
	t.Cleanup(func() { testRecorders.Delete(st) })
	registerEngineTeardown(t, engine, st)

	template := pauseWorkflowTemplate("final_pause_template")
	if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create workflow template: %v", err)
	}
	runID, err := engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{Idea: "pause during stop report", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("StartProjectRunInput() error = %v", err)
	}
	renderer.waitForStopReport(t)
	if err := engine.RequestPause(ctx, runID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
		t.Fatalf("RequestPause() during stop_report error = %v", err)
	}
	renderer.release()
	waitForRunStatus(t, st, runID, store.RunStatusCompleted)
	events, err := st.ListEvents(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if !hasEventType(events, "run.pause_requested") || !hasEventType(events, "run.completed") {
		t.Fatalf("events missing pause_requested/completed: %#v", eventTypes(events))
	}
	if hasEventType(events, "run.paused") {
		t.Fatalf("pause requested during stop_report parked the completed run: %#v", eventTypes(events))
	}
}

func TestOperatorPauseRejectedOutsideRunning(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, &capturingRunner{}, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100}})
	runID, err := env.engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{Idea: "queued", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: workflow.QuickFixDeliveryID})
	if err != nil {
		t.Fatalf("StartProjectRunInput() error = %v", err)
	}
	if err := env.engine.RequestPause(ctx, runID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err == nil || !strings.Contains(err.Error(), store.RunStatusPending) {
		t.Fatalf("RequestPause() error = %v, want pending-state rejection", err)
	}
	if err := env.engine.ResumeRun(ctx, runID); err == nil || !strings.Contains(err.Error(), store.RunStatusPending) {
		t.Fatalf("ResumeRun() error = %v, want pending-state rejection", err)
	}
}

func pauseWorkflowTemplate(id string) workflow.Template {
	template := workflow.Template{
		SchemaVersion: workflow.SchemaVersion,
		ID:            id,
		Name:          id,
		Description:   "pause workflow test",
		Editable:      true,
		Stages: []workflow.StageTemplate{
			{ID: "idea_refinement", Type: workflow.StageTypeIdeaRefinement, Label: "Idea refinement", Actor: workflow.ActorHarness},
			{ID: "implementation", Type: workflow.StageTypeImplementation, Label: "Implementation", Actor: workflow.ActorAgent, ProfileID: agentregistry.ProfilePiHeadlessWorker, Instructions: "implement"},
			{ID: "validation", Type: workflow.StageTypeValidation, Label: "Validation", Actor: workflow.ActorHarness},
			{ID: "stop_report", Type: workflow.StageTypeStopReport, Label: "Stop/report", Actor: workflow.ActorHarness},
		},
		Settings: map[string]any{"pr_behavior": "none"},
	}
	template.Edges = workflow.DeriveTemplateEdges(template)
	return workflow.NormalizeTemplate(template)
}

func assertRunStatus(t *testing.T, st *store.Store, runID, want string) {
	t.Helper()
	run, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != want {
		t.Fatalf("run status = %s, want %s", run.Status, want)
	}
}

func workflowTemplateHasStage(template workflow.Template, id string) bool {
	for _, stage := range template.Stages {
		if stage.ID == id {
			return true
		}
	}
	return false
}

func startPausedRunAtImplementationBoundary(t *testing.T, ctx context.Context, env *testEnv, runner *pauseBoundaryRunner, templateID, idea string) string {
	t.Helper()
	runID, err := env.engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{Idea: idea, RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: templateID})
	if err != nil {
		t.Fatalf("StartProjectRunInput() error = %v", err)
	}
	runner.expectDispatch(t, runID, "implementation")
	if err := env.engine.RequestPause(ctx, runID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
		t.Fatalf("RequestPause() error = %v", err)
	}
	runner.releaseOne()
	waitForRunStatus(t, env.store, runID, store.RunStatusPaused)
	return runID
}

type stopReportBlockingRenderer struct {
	mu        sync.Mutex
	blocked   bool
	started   chan struct{}
	releaseCh chan struct{}
}

func newStopReportBlockingRenderer() *stopReportBlockingRenderer {
	return &stopReportBlockingRenderer{started: make(chan struct{}), releaseCh: make(chan struct{})}
}

func (r *stopReportBlockingRenderer) RenderRunFragments(bundle store.RunBundle) (string, error) {
	for _, stage := range bundle.Stages {
		if stage.StageType != workflow.StageTypeStopReport || stage.Status != store.StageStatusRunning {
			continue
		}
		r.mu.Lock()
		shouldBlock := !r.blocked
		if shouldBlock {
			r.blocked = true
		}
		r.mu.Unlock()
		if shouldBlock {
			close(r.started)
			<-r.releaseCh
		}
		break
	}
	return "", nil
}

func (r *stopReportBlockingRenderer) waitForStopReport(t *testing.T) {
	t.Helper()
	select {
	case <-r.started:
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for stop_report rendering")
	}
}

func (r *stopReportBlockingRenderer) release() {
	close(r.releaseCh)
}

type pauseBoundaryRunner struct {
	mu       sync.Mutex
	disps    []contract.Dispatch
	started  chan contract.Dispatch
	releases chan struct{}
}

func newPauseBoundaryRunner() *pauseBoundaryRunner {
	return &pauseBoundaryRunner{started: make(chan contract.Dispatch, 8), releases: make(chan struct{}, 8)}
}

func (r *pauseBoundaryRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	r.mu.Lock()
	r.disps = append(r.disps, disp)
	r.mu.Unlock()
	r.started <- disp
	if disp.StageType == contract.StageTypeImplementation {
		select {
		case <-r.releases:
		case <-ctx.Done():
			return validAdapterReport(disp, "implementation interrupted"), nil
		}
	}
	return validAdapterReport(disp, "dispatch completed"), nil
}

func (r *pauseBoundaryRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func (r *pauseBoundaryRunner) waitForDispatch(t *testing.T) contract.Dispatch {
	t.Helper()
	select {
	case disp := <-r.started:
		return disp
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for dispatch")
		return contract.Dispatch{}
	}
}

func (r *pauseBoundaryRunner) expectDispatch(t *testing.T, runID, workflowStageID string) contract.Dispatch {
	t.Helper()
	disp := r.waitForDispatch(t)
	if disp.RunID != runID || workflowStageIDFromDispatch(disp) != workflowStageID {
		t.Fatalf("dispatch = run %s workflow_stage %s stage_type %s, want run %s workflow_stage %s", disp.RunID, workflowStageIDFromDispatch(disp), disp.StageType, runID, workflowStageID)
	}
	return disp
}

func (r *pauseBoundaryRunner) releaseOne() {
	r.releases <- struct{}{}
}

func (r *pauseBoundaryRunner) dispatchCountByRunAndType(runID, stageType string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, disp := range r.disps {
		if disp.RunID == runID && disp.StageType == stageType {
			count++
		}
	}
	return count
}

func (r *pauseBoundaryRunner) workflowStageIDsForRun(runID string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []string{}
	for _, disp := range r.disps {
		if disp.RunID == runID {
			out = append(out, workflowStageIDFromDispatch(disp))
		}
	}
	return out
}

func workflowStageIDFromDispatch(disp contract.Dispatch) string {
	workflowStageID, _ := disp.Input["workflow_stage_id"].(string)
	if workflowStageID != "" {
		return workflowStageID
	}
	return disp.StageType
}

func (r *pauseBoundaryRunner) dispatchTypes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.disps))
	for _, disp := range r.disps {
		out = append(out, disp.StageType)
	}
	return out
}
