package orchestrator

import (
	"context"
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

func (r *pauseBoundaryRunner) dispatchTypes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.disps))
	for _, disp := range r.disps {
		out = append(out, disp.StageType)
	}
	return out
}
