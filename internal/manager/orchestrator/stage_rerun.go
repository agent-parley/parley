package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

var (
	ErrStageReRunInvalidTarget   = errors.New("stage re-run target is invalid")
	ErrStageReRunRunNotTerminal  = errors.New("stage re-run requires a terminal run")
	ErrStageReRunFrozenSnapshot  = errors.New("stage re-run requires a frozen workflow snapshot")
	ErrStageReRunPrerequisiteGap = errors.New("stage re-run target prerequisites are incomplete")
)

// ReRunStage creates a new attempt for a terminal run and starts execution at
// the requested workflow stage, continuing forward through the run's frozen
// workflow snapshot. The target may be either a workflow stage ID from the
// frozen template or a persisted stage ID from any attempt of the run.
func (e *Engine) ReRunStage(ctx context.Context, runID, stageID string, actor event.Actor) (store.Attempt, error) {
	runID = strings.TrimSpace(runID)
	stageID = strings.TrimSpace(stageID)
	if runID == "" || stageID == "" {
		return store.Attempt{}, fmt.Errorf("%w: run_id and stage_id are required", ErrStageReRunInvalidTarget)
	}

	e.queueMu.Lock()
	defer e.queueMu.Unlock()

	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return store.Attempt{}, err
	}
	if !stageReRunStatusAllowed(wr.Run.Status) {
		return store.Attempt{}, fmt.Errorf("%w: run %s has status %q; completed, failed, invalid, or cancelled runs may be re-run; cancel pending/running runs first and resume awaiting_human/needs_input runs instead", ErrStageReRunRunNotTerminal, runID, wr.Run.Status)
	}

	runtime, err := e.loadFrozenRuntimeWorkflow(ctx, wr)
	if err != nil {
		return store.Attempt{}, err
	}
	target, err := e.resolveStageReRunTarget(ctx, runID, runtime, stageID)
	if err != nil {
		return store.Attempt{}, err
	}
	if !stageReRunTargetTypeAllowed(target.Template.Type) {
		return store.Attempt{}, fmt.Errorf("%w: workflow stage %q has type %q; choose implementation, validation, or review", ErrStageReRunInvalidTarget, target.Template.ID, target.Template.Type)
	}

	seed, err := e.executionStateBeforeStage(ctx, wr, runtime, target.Template.ID)
	if err != nil {
		return store.Attempt{}, err
	}
	if err := e.validateStageReRunPrerequisites(ctx, wr, runtime, target, seed); err != nil {
		return store.Attempt{}, err
	}
	attempt, _, err := e.store.CreateAttemptForRun(ctx, wr.Run.ID, runtime.Template)
	if err != nil {
		return store.Attempt{}, err
	}
	newWR, err := e.store.GetWorkflowRun(ctx, wr.Run.ID)
	if err != nil {
		return store.Attempt{}, err
	}
	newWR, err = e.configureRuntimeStageAdapters(ctx, newWR, runtime.Template)
	if err != nil {
		return store.Attempt{}, err
	}
	newRuntime, err := e.loadFrozenRuntimeWorkflow(ctx, newWR)
	if err != nil {
		return store.Attempt{}, err
	}
	newTarget, ok := newRuntime.ByID[target.Template.ID]
	if !ok {
		return store.Attempt{}, fmt.Errorf("%w: workflow stage %q not found in new attempt", ErrStageReRunInvalidTarget, target.Template.ID)
	}
	seed = e.prepareStageReRunSeedForAttempt(ctx, newWR, newTarget, seed)

	if err := e.store.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusRunning); err != nil {
		return store.Attempt{}, err
	}
	newWR.Run.Status = store.RunStatusRunning
	if _, err := e.emit(ctx, runEvent(newWR, "run.stage_rerun_started", actor, "stage re-run started", map[string]any{
		"run_id":                   wr.Run.ID,
		"attempt_id":               attempt.ID,
		"previous_attempt_id":      wr.Attempt.ID,
		"requested_stage_id":       stageID,
		"target_stage_id":          newTarget.Stage.ID,
		"target_workflow_stage_id": newTarget.Template.ID,
		"target_stage_type":        newTarget.Template.Type,
	})); err != nil {
		return store.Attempt{}, err
	}

	runCtx, cancel := context.WithCancel(e.rootCtx)
	e.registerActiveRun(wr.Run.ID, cancel)
	if !e.spawn(func() {
		e.executeRunWithCleanup(runCtx, wr.Run.ID, func() error {
			return e.executeRunFromStage(runCtx, wr.Run.ID, target.Template.ID, seed)
		})
	}) {
		cancel()
		e.unregisterActiveRun(wr.Run.ID)
	}
	return attempt, nil
}

func (e *Engine) loadFrozenRuntimeWorkflow(ctx context.Context, wr store.WorkflowRun) (runtimeWorkflow, error) {
	template, err := e.store.LatestWorkflowTemplateSnapshot(ctx, wr.Run.ID)
	if err != nil {
		return runtimeWorkflow{}, fmt.Errorf("%w: run %s: %v", ErrStageReRunFrozenSnapshot, wr.Run.ID, err)
	}
	stages, err := e.store.ListStagesForAttempt(ctx, wr.Run.ID, wr.Attempt.ID)
	if err != nil {
		return runtimeWorkflow{}, err
	}
	return newRuntimeWorkflow(template, stages)
}

func stageReRunStatusAllowed(status string) bool {
	return store.RunStatusIsTerminal(status) && status != store.RunStatusNeedsInput
}

func stageReRunTargetTypeAllowed(stageType string) bool {
	switch stageType {
	case workflow.StageTypeImplementation, workflow.StageTypeValidation, workflow.StageTypeReview:
		return true
	default:
		return false
	}
}

func (e *Engine) validateStageReRunPrerequisites(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, target runtimeStage, seed executionState) error {
	switch target.Template.Type {
	case workflow.StageTypeImplementation:
		return nil
	case workflow.StageTypeValidation:
		return e.requireImplementationSnapshotForReRun(ctx, wr, target.Template.ID, seed)
	case workflow.StageTypeReview:
		if target.Template.Target != workflow.TargetCodeChanges {
			return nil
		}
		if err := e.requireImplementationSnapshotForReRun(ctx, wr, target.Template.ID, seed); err != nil {
			return err
		}
		if validationBefore(runtime, target.Template.ID) && seed.lastValidationReport.Status != report.StatusCompleted {
			return fmt.Errorf("%w: workflow stage %q requires a completed validation report before it can be re-run", ErrStageReRunPrerequisiteGap, target.Template.ID)
		}
		return nil
	default:
		return nil
	}
}

func (e *Engine) requireImplementationSnapshotForReRun(ctx context.Context, wr store.WorkflowRun, targetWorkflowStageID string, seed executionState) error {
	if seed.lastImplementationReport.Status != report.StatusCompleted {
		return fmt.Errorf("%w: workflow stage %q requires a completed implementation report before it can be re-run", ErrStageReRunPrerequisiteGap, targetWorkflowStageID)
	}
	if seed.snapshotErr != nil {
		return fmt.Errorf("%w: workflow stage %q implementation snapshot is unavailable: %v", ErrStageReRunPrerequisiteGap, targetWorkflowStageID, seed.snapshotErr)
	}
	if seed.snapshot.BaseSHA == "" || seed.snapshot.WorkerTreeSHA == "" || seed.snapshot.DiffArtifactID == "" {
		return fmt.Errorf("%w: workflow stage %q requires an implementation snapshot before it can be re-run", ErrStageReRunPrerequisiteGap, targetWorkflowStageID)
	}
	artifact, _, err := e.store.GetArtifact(ctx, seed.snapshot.DiffArtifactID)
	if err != nil {
		return fmt.Errorf("%w: workflow stage %q implementation diff artifact %q is unavailable: %v", ErrStageReRunPrerequisiteGap, targetWorkflowStageID, seed.snapshot.DiffArtifactID, err)
	}
	if artifact.RunID != wr.Run.ID || artifact.TaskID != wr.Task.ID || artifact.Kind != "diff_patch" {
		return fmt.Errorf("%w: workflow stage %q implementation diff artifact %q is not a diff_patch for this run/task", ErrStageReRunPrerequisiteGap, targetWorkflowStageID, seed.snapshot.DiffArtifactID)
	}
	return nil
}

func validationBefore(runtime runtimeWorkflow, workflowStageID string) bool {
	for _, runtimeStage := range runtime.Stages {
		if runtimeStage.Template.ID == workflowStageID {
			return false
		}
		if runtimeStage.Template.Type == workflow.StageTypeValidation {
			return true
		}
	}
	return false
}

func (e *Engine) resolveStageReRunTarget(ctx context.Context, runID string, runtime runtimeWorkflow, stageID string) (runtimeStage, error) {
	if runtimeStage, ok := runtime.ByID[stageID]; ok {
		return runtimeStage, nil
	}
	for _, runtimeStage := range runtime.Stages {
		if runtimeStage.Stage.ID == stageID {
			return runtimeStage, nil
		}
	}
	stages, err := e.store.ListStages(ctx, runID)
	if err != nil {
		return runtimeStage{}, err
	}
	for _, stage := range stages {
		if stage.ID != stageID {
			continue
		}
		if stage.WorkflowStageID != "" {
			if runtimeStage, ok := runtime.ByID[stage.WorkflowStageID]; ok {
				return runtimeStage, nil
			}
		}
		return runtimeStage{}, fmt.Errorf("%w: stage %q is not in run %s frozen workflow snapshot", ErrStageReRunInvalidTarget, stageID, runID)
	}
	return runtimeStage{}, fmt.Errorf("%w: stage %q is not in run %s frozen workflow snapshot", ErrStageReRunInvalidTarget, stageID, runID)
}

func (e *Engine) executionStateBeforeStage(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, targetWorkflowStageID string) (executionState, error) {
	var state executionState
	for _, runtimeStage := range runtime.Stages {
		if runtimeStage.Template.ID == targetWorkflowStageID {
			return state, nil
		}
		rep, ok, err := e.reportForStage(ctx, wr.Run.ID, runtimeStage.Stage.ID)
		if err != nil {
			return executionState{}, err
		}
		if !ok {
			continue
		}
		state.lastReport = rep
		if runtimeStage.Template.Type == workflow.StageTypeImplementation {
			state.lastImplementationReport = rep
		}
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
				persisted, ok, err := e.suspendedImplementationSnapshot(ctx, wr.Run.ID, targetWorkflowStageID)
				if err != nil {
					return executionState{}, err
				}
				if ok {
					state.snapshot = mergeWorkerSnapshot(state.snapshot, persisted)
				}
				if liveWorktreeMissing {
					recovered, recoverErr := e.rematerializeWorktreeFromSnapshot(ctx, wr, state.snapshot)
					if recoverErr == nil {
						state.snapshot = recovered
						state.snapshotErr = nil
					} else {
						state.snapshotErr = worktreeLostOnResumeError{cause: fmt.Errorf("%w; re-materialize worktree: %v", state.snapshotErr, recoverErr), snapshot: state.snapshot}
					}
				}
			}
		}
	}
	return executionState{}, fmt.Errorf("%w: workflow stage %q is not in run %s frozen workflow snapshot", ErrStageReRunInvalidTarget, targetWorkflowStageID, wr.Run.ID)
}

func (e *Engine) prepareStageReRunSeedForAttempt(ctx context.Context, wr store.WorkflowRun, target runtimeStage, seed executionState) executionState {
	if target.Template.Type == workflow.StageTypeImplementation {
		return seed
	}
	if seed.snapshot.BaseSHA == "" || seed.snapshot.DiffArtifactID == "" {
		return seed
	}
	recovered, err := e.rematerializeWorktreeFromSnapshot(ctx, wr, seed.snapshot)
	if err != nil {
		seed.snapshotErr = worktreeLostOnResumeError{cause: fmt.Errorf("re-materialize worktree for stage re-run: %w", err), snapshot: seed.snapshot}
		return seed
	}
	seed.snapshot = recovered
	seed.snapshotErr = nil
	return seed
}
