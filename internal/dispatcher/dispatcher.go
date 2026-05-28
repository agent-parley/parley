package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/agent-parley/parley/internal/executor"
	"github.com/agent-parley/parley/internal/manager"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/store"
)

type dispatchItem struct {
	TaskID    string
	AttemptID string
}

type Dispatcher struct {
	store    *store.Store
	workflow *manager.WorkflowService
	runner   manager.AttemptRunner
	logger   *slog.Logger
	queue    chan dispatchItem
	mu       sync.Mutex
	queued   map[string]struct{}
}

func New(store *store.Store, workflow *manager.WorkflowService, runner manager.AttemptRunner, logger *slog.Logger, workers int) *Dispatcher {
	if workers <= 0 {
		workers = 1
	}
	d := &Dispatcher{store: store, workflow: workflow, runner: runner, logger: logger, queue: make(chan dispatchItem, workers), queued: map[string]struct{}{}}
	for i := 0; i < workers; i++ {
		go d.worker()
	}
	return d
}

func (d *Dispatcher) Enqueue(ctx context.Context, taskID string) error {
	if d == nil || d.workflow == nil || d.store == nil {
		return fmt.Errorf("attempt dispatcher is not configured")
	}
	if task, ok := d.store.GetTask(taskID); ok && task.Status == models.TaskStatusQueued {
		return fmt.Errorf("attempt is already queued for this task")
	}
	input, err := d.preflightInput(taskID)
	if err != nil {
		return err
	}
	if runner, ok := d.runner.(executor.PreflightRunner); ok {
		d.emitEventForTask(taskID, models.EventAttemptPreflightStarted, "Attempt preflight started", map[string]any{"runner": input.Runner.ID})
		if err := runner.Preflight(ctx, input); err != nil {
			d.emitEventForTask(taskID, models.EventAttemptPreflightFailed, "Attempt preflight failed", map[string]any{"reason": "preflight_failed"})
			return err
		}
	}
	_, _, attempt, err := d.store.QueueAttempt(taskID)
	if err != nil {
		if strings.Contains(err.Error(), "queue backlog is full") {
			d.emitForTask(taskID, "Attempt dispatch failed", map[string]any{"reason": "queue_full"})
		}
		return err
	}
	d.emitForTask(taskID, "Attempt accepted by durable dispatcher", map[string]any{"durable": true, "attempt": attempt.Number})
	d.fillQueue()
	return nil
}

func (d *Dispatcher) Recover(ctx context.Context) error {
	if d == nil || d.store == nil {
		return fmt.Errorf("attempt dispatcher is not configured")
	}
	attempts, err := d.store.RecoverDispatchState()
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		d.emitForTask(attempt.TaskID, "Queued attempt recovered for dispatch", map[string]any{"reason": "recovered", "durable": true, "attempt": attempt.Number})
	}
	d.fillQueue()
	return nil
}

func (d *Dispatcher) fillQueue() {
	if d == nil || d.store == nil {
		return
	}
	for _, attempt := range d.store.QueuedAttempts() {
		d.mu.Lock()
		if _, ok := d.queued[attempt.ID]; ok {
			d.mu.Unlock()
			continue
		}
		d.queued[attempt.ID] = struct{}{}
		item := dispatchItem{TaskID: attempt.TaskID, AttemptID: attempt.ID}
		select {
		case d.queue <- item:
			d.mu.Unlock()
		default:
			delete(d.queued, attempt.ID)
			d.mu.Unlock()
			return
		}
	}
}

func (d *Dispatcher) preflightInput(taskID string) (executor.AttemptInput, error) {
	task, ok := d.store.GetTask(taskID)
	if !ok {
		return executor.AttemptInput{}, fmt.Errorf("task not found")
	}
	run, ok := d.store.GetRun(task.RunID)
	if !ok {
		return executor.AttemptInput{}, fmt.Errorf("run not found")
	}
	project, ok := d.store.GetProject(task.ProjectID)
	if !ok {
		return executor.AttemptInput{}, fmt.Errorf("project not found")
	}
	runnerID := task.AssignedExecutorID
	if runnerID == "" {
		runnerID = project.DefaultExecutorID
	}
	runner, _ := d.store.GetExecutor(runnerID)
	return executor.AttemptInput{Project: project, Run: run, Task: task, Runner: runner}, nil
}

func (d *Dispatcher) worker() {
	for item := range d.queue {
		d.run(item)
	}
}

func (d *Dispatcher) run(item dispatchItem) {
	refill := true
	defer func() {
		d.forget(item.AttemptID)
		if refill {
			d.fillQueue()
		}
	}()
	defer func() {
		if recovered := recover(); recovered != nil {
			_, _ = d.store.FailRunningAttemptForDispatch(item.TaskID, item.AttemptID, "Attempt interrupted by dispatcher panic")
			d.emitForTask(item.TaskID, "Attempt dispatch failed", map[string]any{"reason": "panic"})
			if d.logger != nil {
				d.logger.Error("attempt dispatcher panic", "task_id", item.TaskID, "panic", recovered, "stack", string(debug.Stack()))
			}
		}
	}()
	if err := d.workflow.StartAttempt(context.Background(), item.TaskID); err != nil {
		status, queued := d.store.AttemptStatus(item.AttemptID)
		if queued && status == models.AttemptStatusQueued && errors.Is(err, store.ErrRunnerCapacityUnavailable) {
			refill = false
			d.emitForTask(item.TaskID, "Attempt dispatch deferred", map[string]any{"reason": "capacity_unavailable"})
		} else if queued && status == models.AttemptStatusQueued {
			if failErr := d.store.FailQueuedAttempt(item.TaskID, item.AttemptID, "Attempt failed before runner execution. Request a fix to retry."); failErr != nil {
				if d.logger != nil {
					d.logger.Error("failed to mark queued attempt failed", "task_id", item.TaskID, "error", failErr)
				}
				d.emitForTask(item.TaskID, "Attempt dispatch failed", map[string]any{"reason": "attempt_failed"})
			} else {
				d.emitForTask(item.TaskID, "Attempt dispatch failed", map[string]any{"reason": "setup_failed"})
			}
		} else {
			d.emitForTask(item.TaskID, "Attempt dispatch failed", map[string]any{"reason": "attempt_failed"})
		}
		if d.logger != nil {
			d.logger.Error("attempt failed", "task_id", item.TaskID, "error", err)
		}
	}
}

func (d *Dispatcher) forget(attemptID string) {
	d.mu.Lock()
	delete(d.queued, attemptID)
	d.mu.Unlock()
}

func (d *Dispatcher) emitForTask(taskID, summary string, data map[string]any) {
	d.emitEventForTask(taskID, models.EventTaskStateChanged, summary, data)
}

func (d *Dispatcher) emitEventForTask(taskID, eventType, summary string, data map[string]any) {
	task, ok := d.store.GetTask(taskID)
	if !ok {
		return
	}
	_, _ = d.store.AppendEvent(models.Event{RunID: task.RunID, TaskID: task.ID, Type: eventType, ActorKind: models.ActorKindManager, ActorID: models.ActorKindManager, Summary: summary, Data: data})
}
