package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

type executionState struct {
	lastReport           report.Report
	lastValidationReport report.Report
	lastDeliveryReport   report.Report
	snapshot             workerSnapshot
	snapshotErr          error
}

func (e *Engine) reconstructExecutionState(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, throughWorkflowStageID string) (executionState, error) {
	var state executionState
	for _, runtimeStage := range runtime.Stages {
		rep, ok, err := e.reportForStage(ctx, wr.Run.ID, runtimeStage.Stage.ID)
		if err != nil {
			return executionState{}, err
		}
		if ok {
			state.lastReport = rep
			if runtimeStage.Template.Type == workflow.StageTypeValidation {
				state.lastValidationReport = rep
			}
			if reportCarriesDeliveryPayload(rep) {
				state.lastDeliveryReport = rep
			}
			if runtimeStage.Template.Type == workflow.StageTypeImplementation && rep.Status == report.StatusCompleted {
				state.snapshot, state.snapshotErr = e.snapshotWorktree(ctx, wr, rep)
			}
		}
		if runtimeStage.Template.ID == throughWorkflowStageID {
			if !ok {
				return executionState{}, fmt.Errorf("resume stage %q has no completed report", throughWorkflowStageID)
			}
			return state, nil
		}
	}
	return executionState{}, fmt.Errorf("resume stage %q not found", throughWorkflowStageID)
}

func (e *Engine) reportForStage(ctx context.Context, runID, stageID string) (report.Report, bool, error) {
	events, err := e.store.ListEvents(ctx, runID)
	if err != nil {
		return report.Report{}, false, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if ev.Data == nil || eventStageID(ev) != stageID {
			continue
		}
		artifactID, _ := ev.Data["report_artifact_id"].(string)
		if artifactID == "" {
			continue
		}
		_, content, err := e.store.GetArtifact(ctx, artifactID)
		if err != nil {
			return report.Report{}, false, err
		}
		var rep report.Report
		if err := json.Unmarshal(content, &rep); err != nil {
			return report.Report{}, false, fmt.Errorf("decode report artifact %s: %w", artifactID, err)
		}
		return rep, true, nil
	}
	return report.Report{}, false, nil
}

func eventStageID(ev event.Event) string {
	if ev.Data == nil {
		return ""
	}
	stageID, _ := ev.Data["stage_id"].(string)
	return stageID
}
