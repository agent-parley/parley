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

const worktreeLostOnResumeSummary = "worktree lost on restart - cannot commit; re-run implementation"

type worktreeLostOnResumeError struct {
	cause    error
	snapshot workerSnapshot
}

func (e worktreeLostOnResumeError) Error() string {
	if e.snapshot.BaseSHA != "" && e.snapshot.DiffArtifactID != "" {
		return fmt.Sprintf("%s (base_sha=%s diff_artifact_id=%s): %v", worktreeLostOnResumeSummary, e.snapshot.BaseSHA, e.snapshot.DiffArtifactID, e.cause)
	}
	if e.snapshot.BaseSHA != "" {
		return fmt.Sprintf("%s (base_sha=%s): %v", worktreeLostOnResumeSummary, e.snapshot.BaseSHA, e.cause)
	}
	if e.cause == nil {
		return worktreeLostOnResumeSummary
	}
	return fmt.Sprintf("%s: %v", worktreeLostOnResumeSummary, e.cause)
}

func (e worktreeLostOnResumeError) Unwrap() error {
	return e.cause
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
				if state.snapshotErr != nil {
					liveWorktreeMissing := state.snapshot.WorktreePath == ""
					persisted, ok, err := e.suspendedImplementationSnapshot(ctx, wr.Run.ID, throughWorkflowStageID)
					if err != nil {
						return executionState{}, err
					}
					if ok {
						state.snapshot = mergeWorkerSnapshot(state.snapshot, persisted)
					}
					if liveWorktreeMissing {
						state.snapshotErr = worktreeLostOnResumeError{cause: state.snapshotErr, snapshot: state.snapshot}
					}
				}
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

func (e *Engine) suspendedImplementationSnapshot(ctx context.Context, runID, workflowStageID string) (workerSnapshot, bool, error) {
	events, err := e.store.ListEvents(ctx, runID)
	if err != nil {
		return workerSnapshot{}, false, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if ev.Type != "stage.awaiting_human" || ev.Data == nil {
			continue
		}
		if got, _ := ev.Data["workflow_stage_id"].(string); got != workflowStageID {
			continue
		}
		raw, ok := ev.Data["implementation_snapshot"].(map[string]any)
		if !ok {
			continue
		}
		return workerSnapshotFromPayload(raw), true, nil
	}
	return workerSnapshot{}, false, nil
}

func mergeWorkerSnapshot(current, persisted workerSnapshot) workerSnapshot {
	if current.WorktreePath == "" {
		current.WorktreePath = persisted.WorktreePath
	}
	if current.BaseSHA == "" {
		current.BaseSHA = persisted.BaseSHA
	}
	if current.BaseTreeSHA == "" {
		current.BaseTreeSHA = persisted.BaseTreeSHA
	}
	if current.WorkerTreeSHA == "" {
		current.WorkerTreeSHA = persisted.WorkerTreeSHA
	}
	if current.DiffArtifactID == "" {
		current.DiffArtifactID = persisted.DiffArtifactID
	}
	return current
}

func workerSnapshotPayload(snapshot workerSnapshot, snapshotErr error) map[string]any {
	payload := map[string]any{}
	if snapshot.BaseSHA != "" {
		payload["base_sha"] = snapshot.BaseSHA
	}
	if snapshot.BaseTreeSHA != "" {
		payload["base_tree_sha"] = snapshot.BaseTreeSHA
	}
	if snapshot.WorkerTreeSHA != "" {
		payload["worker_tree_sha"] = snapshot.WorkerTreeSHA
	}
	if snapshot.DiffArtifactID != "" {
		payload["diff_artifact_id"] = snapshot.DiffArtifactID
	}
	if snapshotErr != nil {
		payload["snapshot_error"] = snapshotErr.Error()
	}
	return payload
}

func workerSnapshotFromPayload(payload map[string]any) workerSnapshot {
	return workerSnapshot{
		BaseSHA:        payloadString(payload, "base_sha"),
		BaseTreeSHA:    payloadString(payload, "base_tree_sha"),
		WorkerTreeSHA:  payloadString(payload, "worker_tree_sha"),
		DiffArtifactID: payloadString(payload, "diff_artifact_id"),
	}
}

func eventStageID(ev event.Event) string {
	if ev.Data == nil {
		return ""
	}
	stageID, _ := ev.Data["stage_id"].(string)
	return stageID
}
