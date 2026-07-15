package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/event"
)

var (
	errRunPaused     = errors.New("run paused")
	ErrRunNotRunning = errors.New("run is not running")
	ErrRunNotPaused  = errors.New("run is not paused")
)

func (e *Engine) RequestPause(ctx context.Context, runID string, actor event.Actor) error {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	if wr.Run.Status != store.RunStatusRunning {
		return fmt.Errorf("%w: run %s has status %q", ErrRunNotRunning, runID, wr.Run.Status)
	}
	if !e.markPausing(runID) {
		return nil
	}
	if actor.Kind == "" {
		actor = event.Actor{Kind: event.ActorKindOperator, ID: "operator"}
	}
	if _, err := e.emit(ctx, runEvent(wr, "run.pause_requested", actor, "run pause requested", map[string]any{
		"status": store.RunStatusRunning,
	})); err != nil {
		e.clearPausing(runID)
		return err
	}
	return nil
}

func (e *Engine) pauseRunAtBoundary(ctx context.Context, wr store.WorkflowRun, workflowStageID string) error {
	e.clearPausing(wr.Run.ID)
	persisted, changed, err := e.store.UpdateRunStatusFromAndAppendEvent(ctx, wr.Run.ID, store.RunStatusRunning, store.RunStatusPaused, runEvent(wr, "run.paused", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run paused at workflow boundary", map[string]any{
		"status":                         store.RunStatusPaused,
		"paused_after_workflow_stage_id": workflowStageID,
	}))
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if _, err := e.publishEvent(ctx, persisted); err != nil {
		return err
	}
	return errRunPaused
}

func (e *Engine) ResumeRun(ctx context.Context, runID string) error {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	if wr.Run.Status != store.RunStatusPaused {
		return fmt.Errorf("%w: run %s has status %q", ErrRunNotPaused, runID, wr.Run.Status)
	}
	anchor, err := e.latestPausedWorkflowStageID(ctx, runID)
	if err != nil {
		return err
	}
	changed, err := e.store.UpdateRunStatusFrom(ctx, runID, store.RunStatusPaused, store.RunStatusRunning)
	if err != nil {
		return err
	}
	if !changed {
		return ErrRunNotPaused
	}
	wr.Run.Status = store.RunStatusRunning
	rollbackResume := func() {
		changed, err := e.store.UpdateRunStatusFrom(context.Background(), runID, store.RunStatusRunning, store.RunStatusPaused)
		if err == nil && changed {
			wr.Run.Status = store.RunStatusPaused
		}
	}
	if _, err := e.emit(ctx, runEvent(wr, "run.resumed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run resumed from operator pause", map[string]any{
		"status":                         store.RunStatusRunning,
		"source":                         "operator_pause",
		"paused_after_workflow_stage_id": anchor,
	})); err != nil {
		rollbackResume()
		return err
	}
	runCtx, cancel := context.WithCancel(e.rootCtx)
	e.registerActiveRun(runID, cancel)
	if !e.spawn(func() { e.executeRunAfter(runCtx, runID, anchor) }) {
		cancel()
		e.unregisterActiveRun(runID)
	}
	return nil
}

func (e *Engine) latestPausedWorkflowStageID(ctx context.Context, runID string) (string, error) {
	events, err := e.store.ListEvents(ctx, runID)
	if err != nil {
		return "", err
	}
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if ev.Type != "run.paused" || ev.Data == nil {
			continue
		}
		anchor, _ := ev.Data["paused_after_workflow_stage_id"].(string)
		if anchor == "" {
			return "", fmt.Errorf("run %s pause event is missing paused_after_workflow_stage_id", runID)
		}
		return anchor, nil
	}
	return "", fmt.Errorf("run %s has no pause anchor", runID)
}
