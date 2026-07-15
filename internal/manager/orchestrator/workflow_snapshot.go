package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/event"
)

var ErrWorkflowSnapshotNotEditable = errors.New("workflow snapshot is not editable")

func (e *Engine) UpdateRunWorkflowSnapshot(ctx context.Context, runID string, template workflow.Template, actor event.Actor) error {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	if wr.Run.Status != store.RunStatusPaused {
		return fmt.Errorf("%w: run %s has status %q", ErrWorkflowSnapshotNotEditable, runID, wr.Run.Status)
	}
	current, err := e.store.LatestWorkflowTemplateSnapshot(ctx, runID)
	if err != nil {
		return err
	}
	anchor, err := e.latestPausedWorkflowStageID(ctx, runID)
	if err != nil {
		return err
	}
	contractArtifactID, planArtifactID, err := e.workflowSnapshotMetadata(ctx, runID)
	if err != nil {
		return err
	}
	template = editableRunSnapshotTemplate(template)
	registry := registryForRuntimeTemplate(template)
	template = workflow.NormalizeTemplateWithRegistry(template, registry)
	if err := workflow.ValidateTemplateWithRegistry(template, registry); err != nil {
		return err
	}
	if err := validateAmendedSnapshotPrefix(current, template, anchor); err != nil {
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
	if actor.Kind == "" {
		actor = event.Actor{Kind: event.ActorKindOperator, ID: "operator"}
	}
	_, err = e.emit(ctx, runEvent(wr, "run.workflow_snapshot_edited", actor, "run workflow snapshot edited", map[string]any{
		"status":                         wr.Run.Status,
		"workflow_template_id":           template.ID,
		"stage_count":                    len(template.Stages),
		"paused_after_workflow_stage_id": anchor,
	}))
	return err
}

func validateAmendedSnapshotPrefix(current, edited workflow.Template, anchor string) error {
	current = workflow.NormalizeTemplate(current)
	edited = workflow.NormalizeTemplate(edited)
	anchorIndex := -1
	for i, stage := range current.Stages {
		if stage.ID == anchor {
			anchorIndex = i
			break
		}
	}
	if anchorIndex < 0 {
		return fmt.Errorf("executed workflow prefix is locked: pause anchor %q is not in the current snapshot", anchor)
	}
	if len(edited.Stages) <= anchorIndex {
		return fmt.Errorf("executed workflow prefix is locked: edited snapshot removes stage %q", anchor)
	}
	for i := 0; i <= anchorIndex; i++ {
		if current.Stages[i].ID != edited.Stages[i].ID {
			return fmt.Errorf("executed workflow prefix is locked: stage %d changed from %q to %q", i+1, current.Stages[i].ID, edited.Stages[i].ID)
		}
		if !reflect.DeepEqual(executedPrefixStage(current.Stages[i]), executedPrefixStage(edited.Stages[i])) {
			return fmt.Errorf("executed workflow prefix is locked: workflow stage %q cannot be changed after it started", current.Stages[i].ID)
		}
	}
	return nil
}

type executedPrefixStageSnapshot struct {
	ID              string
	Type            string
	Label           string
	Actor           string
	Target          string
	Instructions    string
	ProfileID       string
	RequiredSet     bool
	Required        bool
	Settings        map[string]any
	ContextSettings map[string]any
	Timeout         string
	MaxAttempts     int
}

func executedPrefixStage(stage workflow.StageTemplate) executedPrefixStageSnapshot {
	return executedPrefixStageSnapshot{
		ID:              stage.ID,
		Type:            stage.Type,
		Label:           stage.Label,
		Actor:           stage.Actor,
		Target:          stage.Target,
		Instructions:    stage.Instructions,
		ProfileID:       stage.ProfileID,
		RequiredSet:     stage.Required != nil,
		Required:        stageRequiredValue(stage.Required),
		Settings:        stage.Settings,
		ContextSettings: stage.ContextSettings,
		Timeout:         stage.Timeout,
		MaxAttempts:     stage.MaxAttempts,
	}
}

func stageRequiredValue(required *bool) bool {
	return required != nil && *required
}

func editableRunSnapshotTemplate(template workflow.Template) workflow.Template {
	template = workflow.NormalizeTemplate(template)
	template.Predefined = false
	template.Recommended = false
	template.Editable = true
	template.Edges = workflow.DeriveTemplateEdges(template)
	return workflow.NormalizeTemplate(template)
}

func (e *Engine) workflowSnapshotMetadata(ctx context.Context, runID string) (contractArtifactID string, planArtifactID string, err error) {
	snapshot, err := e.store.LatestWorkflowSnapshot(ctx, runID)
	if err != nil {
		return "", "", err
	}
	return snapshotString(snapshot, "task_contract_artifact_id"), snapshotString(snapshot, "task_plan_artifact_id"), nil
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
