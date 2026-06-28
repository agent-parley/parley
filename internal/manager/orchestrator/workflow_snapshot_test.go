package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
)

func TestRunWorkflowSnapshotEditIsRunOnlyAndFrozenSnapshotExecutes(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	template := workflowSnapshotEditTemplate("run_only_template", "before")
	if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create workflow template: %v", err)
	}
	runner := &capturingRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{})

	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "edit snapshot only", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingWorkflowAdjustment)

	edited, err := st.LatestWorkflowTemplateSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("latest workflow template snapshot: %v", err)
	}
	if edited.Settings == nil {
		edited.Settings = map[string]any{}
	}
	edited.Settings["branch_policy"] = "target_branch"
	for i := range edited.Stages {
		if edited.Stages[i].ID == "implementation" {
			edited.Stages[i].Instructions = "after"
		}
	}
	if err := engine.UpdateRunWorkflowSnapshot(ctx, runID, edited, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
		t.Fatalf("UpdateRunWorkflowSnapshot() error = %v", err)
	}

	storedTemplate, err := st.GetWorkflowTemplate(ctx, template.ID)
	if err != nil {
		t.Fatalf("get stored template: %v", err)
	}
	if storedTemplate.Settings["branch_policy"] == "target_branch" {
		t.Fatalf("stored template mutated: %#v", storedTemplate.Settings)
	}
	if got := storedTemplate.Stages[1].Instructions; got != "before" {
		t.Fatalf("stored template implementation instructions = %v, want before", got)
	}

	if err := engine.FreezeRunWorkflowSnapshot(ctx, runID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
		t.Fatalf("FreezeRunWorkflowSnapshot() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusCompleted)
	if len(runner.disps) == 0 {
		t.Fatal("implementation was not dispatched")
	}
	if runner.disps[0].Input["workflow_stage_instructions"] != "after" {
		t.Fatalf("implementation dispatch instructions = %#v, want edited instructions", runner.disps[0].Input["workflow_stage_instructions"])
	}
	latest, err := st.LatestWorkflowSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("latest snapshot: %v", err)
	}
	if latest["frozen"] != true || latest["workflow_template_frozen"] != true {
		t.Fatalf("latest snapshot not frozen: %#v", latest)
	}
}

func TestRunWorkflowSnapshotEditRejectedAfterFreeze(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	template := workflowSnapshotEditTemplate("post_freeze_template", "before")
	if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create workflow template: %v", err)
	}
	engine := newRecordingEngine(t, st, &capturingRunner{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "reject post-freeze", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	freezeRunWorkflowSnapshot(t, engine, st, runID)
	waitForRunStatus(t, st, runID, store.RunStatusCompleted)

	edited := template
	edited.Settings["branch_policy"] = "target_branch"
	if err := engine.UpdateRunWorkflowSnapshot(ctx, runID, edited, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); !errors.Is(err, ErrWorkflowSnapshotNotEditable) && !errors.Is(err, ErrWorkflowSnapshotFrozen) {
		t.Fatalf("post-freeze edit error = %v, want not editable/frozen", err)
	}
}

func TestInvalidRunWorkflowSnapshotBlockedAtFreeze(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	template := workflowSnapshotEditTemplate("invalid_freeze_template", "before")
	if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create workflow template: %v", err)
	}
	engine := newRecordingEngine(t, st, &capturingRunner{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "block invalid freeze", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingWorkflowAdjustment)

	snapshot, err := st.LatestWorkflowSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("latest snapshot: %v", err)
	}
	snapshot["workflow_template_snapshot"] = workflow.Template{
		SchemaVersion: workflow.SchemaVersion,
		ID:            template.ID,
		Name:          "Invalid missing start",
		Editable:      true,
		Stages: []workflow.StageTemplate{
			{ID: "implementation", Type: workflow.StageTypeImplementation, Label: "Implementation", Actor: workflow.ActorAgent},
			{ID: "stop_report", Type: workflow.StageTypeStopReport, Label: "Stop/report", Actor: workflow.ActorHarness},
		},
		Edges: []workflow.Edge{{From: "implementation", To: "stop_report", On: workflow.OnCompleted}},
	}
	if err := st.SaveWorkflowSnapshot(ctx, runID, snapshot); err != nil {
		t.Fatalf("save invalid snapshot: %v", err)
	}
	if err := engine.FreezeRunWorkflowSnapshot(ctx, runID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err == nil {
		t.Fatal("FreezeRunWorkflowSnapshot accepted invalid snapshot")
	}
	run, err := st.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != store.RunStatusAwaitingWorkflowAdjustment {
		t.Fatalf("run status = %s, want awaiting workflow adjustment", run.Status)
	}
}

func workflowSnapshotEditTemplate(id, instructions string) workflow.Template {
	template := workflow.Template{
		SchemaVersion: workflow.SchemaVersion,
		ID:            id,
		Name:          id,
		Description:   "test template",
		Editable:      true,
		Stages: []workflow.StageTemplate{
			{ID: "idea_refinement", Type: workflow.StageTypeIdeaRefinement, Label: "Idea refinement", Actor: workflow.ActorHarness},
			{ID: "implementation", Type: workflow.StageTypeImplementation, Label: "Implementation", Actor: workflow.ActorAgent, Instructions: instructions},
			{ID: "stop_report", Type: workflow.StageTypeStopReport, Label: "Stop/report", Actor: workflow.ActorHarness},
		},
		Settings: map[string]any{"branch_policy": "feature_branch"},
	}
	template.Edges = workflow.DeriveTemplateEdges(template)
	return template
}
