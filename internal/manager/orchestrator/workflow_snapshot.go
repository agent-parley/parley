package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/event"
)

var (
	ErrWorkflowSnapshotNotEditable = errors.New("workflow snapshot is not editable")
	ErrWorkflowSnapshotFrozen      = errors.New("workflow snapshot is frozen")
)

func (e *Engine) UpdateRunWorkflowSnapshot(ctx context.Context, runID string, template workflow.Template, actor event.Actor) error {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	if wr.Run.Status != store.RunStatusAwaitingWorkflowAdjustment {
		return fmt.Errorf("%w: run %s has status %q", ErrWorkflowSnapshotNotEditable, runID, wr.Run.Status)
	}
	contractArtifactID, planArtifactID, frozen, err := e.workflowSnapshotMetadata(ctx, runID)
	if err != nil {
		return err
	}
	if frozen {
		return ErrWorkflowSnapshotFrozen
	}
	template = editableRunSnapshotTemplate(template)
	if err := workflow.ValidateTemplate(template); err != nil {
		return err
	}
	if err := e.store.ReconcileWorkflowSnapshotStages(ctx, runID, template); err != nil {
		return err
	}
	wr, err = e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	wr, err = e.configureRuntimeStageAdapters(ctx, wr, template)
	if err != nil {
		return err
	}
	if err := e.store.SaveWorkflowSnapshot(ctx, wr.Run.ID, e.workflowSnapshot(wr, template, contractArtifactID, planArtifactID, false)); err != nil {
		return err
	}
	if actor.Kind == "" {
		actor = event.Actor{Kind: event.ActorKindOperator, ID: "operator"}
	}
	_, err = e.emit(ctx, runEvent(wr, "run.workflow_snapshot_edited", actor, "run workflow snapshot edited", map[string]any{
		"status":                   wr.Run.Status,
		"workflow_template_id":     template.ID,
		"workflow_snapshot_frozen": false,
		"stage_count":              len(template.Stages),
	}))
	return err
}

func (e *Engine) FreezeRunWorkflowSnapshot(ctx context.Context, runID string, actor event.Actor) error {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	if wr.Run.Status != store.RunStatusAwaitingWorkflowAdjustment {
		return fmt.Errorf("%w: run %s has status %q", ErrWorkflowSnapshotNotEditable, runID, wr.Run.Status)
	}
	contractArtifactID, planArtifactID, frozen, err := e.workflowSnapshotMetadata(ctx, runID)
	if err != nil {
		return err
	}
	if frozen {
		return ErrWorkflowSnapshotFrozen
	}
	template, err := e.store.LatestWorkflowTemplateSnapshot(ctx, runID)
	if err != nil {
		return err
	}
	template = editableRunSnapshotTemplate(template)
	if err := workflow.ValidateTemplate(template); err != nil {
		return err
	}
	if err := e.store.ReconcileWorkflowSnapshotStages(ctx, runID, template); err != nil {
		return err
	}
	wr, err = e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	wr, err = e.configureRuntimeStageAdapters(ctx, wr, template)
	if err != nil {
		return err
	}
	if err := e.store.SaveWorkflowSnapshot(ctx, wr.Run.ID, e.workflowSnapshot(wr, template, contractArtifactID, planArtifactID, true)); err != nil {
		return err
	}
	changed, err := e.store.UpdateRunStatusFrom(ctx, wr.Run.ID, store.RunStatusAwaitingWorkflowAdjustment, store.RunStatusRunning)
	if err != nil {
		return err
	}
	if !changed {
		return ErrWorkflowSnapshotNotEditable
	}
	wr.Run.Status = store.RunStatusRunning
	if actor.Kind == "" {
		actor = event.Actor{Kind: event.ActorKindOperator, ID: "operator"}
	}
	if _, err := e.emit(ctx, runEvent(wr, "run.workflow_snapshot_frozen", actor, "run workflow snapshot frozen", map[string]any{
		"status":                   store.RunStatusRunning,
		"workflow_template_id":     template.ID,
		"workflow_snapshot_frozen": true,
		"stage_count":              len(template.Stages),
	})); err != nil {
		_, _ = e.store.UpdateRunStatusFrom(context.Background(), wr.Run.ID, store.RunStatusRunning, store.RunStatusAwaitingWorkflowAdjustment)
		return err
	}
	runCtx, cancel := context.WithCancel(e.rootCtx)
	e.registerActiveRun(wr.Run.ID, cancel)
	resumeAfter := wr.IdeaIntakeStage.WorkflowStageID
	if resumeAfter == "" {
		resumeAfter = workflow.StageTypeIdeaRefinement
	}
	if !e.spawn(func() { e.executeRunAfter(runCtx, wr.Run.ID, resumeAfter) }) {
		cancel()
		e.unregisterActiveRun(wr.Run.ID)
	}
	return nil
}

func (e *Engine) pauseForWorkflowSnapshotAdjustment(ctx context.Context, wr store.WorkflowRun, template workflow.Template) error {
	persisted, changed, err := e.store.UpdateRunStatusFromAndAppendEvent(ctx, wr.Run.ID, store.RunStatusRunning, store.RunStatusAwaitingWorkflowAdjustment, runEvent(wr, "run.awaiting_workflow_adjustment", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run awaiting workflow snapshot adjustment", map[string]any{
		"status":                   store.RunStatusAwaitingWorkflowAdjustment,
		"workflow_template_id":     template.ID,
		"workflow_snapshot_frozen": false,
		"stage_count":              len(template.Stages),
	}))
	if err != nil {
		return err
	}
	if !changed {
		return ErrWorkflowSnapshotNotEditable
	}
	_, err = e.publishEvent(ctx, persisted)
	return err
}

func editableRunSnapshotTemplate(template workflow.Template) workflow.Template {
	template = workflow.NormalizeTemplate(template)
	template.Predefined = false
	template.Editable = true
	return template
}

func (e *Engine) workflowSnapshotMetadata(ctx context.Context, runID string) (contractArtifactID string, planArtifactID string, frozen bool, err error) {
	snapshot, err := e.store.LatestWorkflowSnapshot(ctx, runID)
	if err != nil {
		return "", "", false, err
	}
	contractArtifactID = snapshotString(snapshot, "task_contract_artifact_id")
	planArtifactID = snapshotString(snapshot, "task_plan_artifact_id")
	return contractArtifactID, planArtifactID, workflowSnapshotFrozen(snapshot), nil
}

func workflowSnapshotFrozen(snapshot map[string]any) bool {
	return snapshotBool(snapshot, "frozen") || snapshotBool(snapshot, "workflow_template_frozen") || snapshotBool(snapshot, "workflow_snapshot_frozen")
}

func snapshotBool(snapshot map[string]any, key string) bool {
	value, ok := snapshot[key]
	if !ok || value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "1" || v == "yes" || v == "on"
	default:
		return fmt.Sprint(v) == "true"
	}
}

func snapshotString(snapshot map[string]any, key string) string {
	value, ok := snapshot[key]
	if !ok || value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprint(value)
}
