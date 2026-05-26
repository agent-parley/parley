package store

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/models"
)

// maxPersistedQueuedAttempts is a pre-alpha local prototype cap. Keep it
// hardcoded until backlog sizing/configuration is designed for real local use.
const maxPersistedQueuedAttempts = 100

func (s *Store) QueueAttempt(taskID string) (models.Run, models.Task, models.Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	task, ok := s.state.Tasks[taskID]
	if !ok {
		return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("task not found")
	}
	run, ok := s.state.Runs[task.RunID]
	if !ok {
		return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("run not found")
	}
	if task.Status == models.TaskStatusQueued {
		return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("attempt is already queued for this task")
	}
	if task.Status != models.TaskStatusDraft && task.Status != models.TaskStatusNeedsFix {
		return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("task is not ready to queue")
	}
	if len(s.queuedAttemptsLocked()) >= maxPersistedQueuedAttempts {
		return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("attempt queue backlog is full")
	}
	maxAttempts := task.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if task.Attempts >= maxAttempts {
		if task.Status == models.TaskStatusNeedsFix && task.Attempts > 0 {
			task.MaxAttempts = task.Attempts + 1
		} else {
			return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("task has reached its retry limit")
		}
	}
	number := task.Attempts + 1
	attempt, ok := s.attemptForTaskNumberLocked(task.ID, number)
	if ok {
		if attempt.Status != models.AttemptStatusQueued && attempt.Status != models.AttemptStatusRequested {
			return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("attempt %d is already %s", number, attempt.Status)
		}
	} else {
		kind := models.AttemptKindWorker
		summary := "Worker attempt queued for durable dispatch."
		if task.Status == models.TaskStatusNeedsFix {
			kind = models.AttemptKindFix
			summary = "Fix attempt queued for durable dispatch."
		}
		attempt = models.Attempt{ID: newID("att"), ProjectID: task.ProjectID, RunID: run.ID, TaskID: task.ID, Number: number, Kind: kind, Status: models.AttemptStatusQueued, Summary: summary, CreatedAt: now, UpdatedAt: now}
	}
	attempt.Status = models.AttemptStatusQueued
	attempt.Summary = "Worker attempt queued for durable dispatch."
	if attempt.Kind == models.AttemptKindFix {
		attempt.Summary = "Fix attempt queued for durable dispatch."
	}
	attempt.UpdatedAt = now
	task.Status = models.TaskStatusQueued
	task.UpdatedAt = now
	run.Status = models.RunStatusQueued
	run.UpdatedAt = now
	s.state.Attempts[attempt.ID] = attempt
	s.state.Tasks[task.ID] = task
	s.state.Runs[run.ID] = run
	return run, task, attempt, s.saveLocked()
}

func (s *Store) QueuedAttempts() []models.Attempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queuedAttemptsLocked()
}

func (s *Store) queuedAttemptsLocked() []models.Attempt {
	attempts := make([]models.Attempt, 0)
	for _, attempt := range s.state.Attempts {
		if attempt.Status != models.AttemptStatusQueued {
			continue
		}
		task, ok := s.state.Tasks[attempt.TaskID]
		if !ok || task.Status != models.TaskStatusQueued || task.Attempts+1 != attempt.Number {
			continue
		}
		attempts = append(attempts, attempt)
	}
	sort.Slice(attempts, func(i, j int) bool {
		if attempts[i].CreatedAt.Equal(attempts[j].CreatedAt) {
			return attempts[i].ID < attempts[j].ID
		}
		return attempts[i].CreatedAt.Before(attempts[j].CreatedAt)
	})
	return attempts
}

func (s *Store) RecoverDispatchState() ([]models.Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	var events []models.Event
	for _, attempt := range s.state.Attempts {
		if attempt.Status != models.AttemptStatusRunning {
			continue
		}
		task, run, current, err := s.transitionRecordsLocked(attempt.TaskID, attempt.ID)
		if err != nil || task.Status != models.TaskStatusRunning {
			continue
		}
		leaseID := task.LeaseID
		if leaseID == "" {
			if attemptLease := s.leaseForTaskLocked(task.ID); attemptLease.ID != "" {
				leaseID = attemptLease.ID
			}
		}
		current.Status = models.AttemptStatusFailed
		current.Summary = "Attempt interrupted by manager restart. Request a fix to retry."
		current.UpdatedAt = now
		task.Status = models.TaskStatusFailed
		task.LeaseID = ""
		task.UpdatedAt = now
		run.Status = models.RunStatusFailed
		run.UpdatedAt = now
		if leaseID != "" {
			s.releaseLeaseLocked(leaseID, now)
		}
		s.state.Attempts[current.ID] = current
		s.state.Tasks[task.ID] = task
		s.state.Runs[run.ID] = run
		events = append(events, s.appendEventLocked(models.Event{RunID: run.ID, TaskID: task.ID, ExecutorID: task.AssignedExecutorID, LeaseID: leaseID, Type: models.EventTaskStateChanged, ActorKind: models.ActorKindManager, ActorID: models.ActorKindManager, Summary: "Attempt interrupted by manager restart", Data: map[string]any{"reason": "manager_restarted"}}))
		if leaseID != "" {
			events = append(events, s.appendEventLocked(models.Event{RunID: run.ID, TaskID: task.ID, ExecutorID: task.AssignedExecutorID, LeaseID: leaseID, Type: models.EventLeaseReleased, ActorKind: models.ActorKindManager, ActorID: models.ActorKindManager, Summary: "Runner slot released after interrupted attempt", Data: map[string]any{"reason": "manager_restarted"}}))
		}
	}
	if err := s.saveLocked(); err != nil {
		return nil, err
	}
	s.notifyEventsLocked(events)
	return s.queuedAttemptsLocked(), nil
}

func (s *Store) FailRunningAttemptForDispatch(taskID, attemptID, summary string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	task, run, attempt, err := s.transitionRecordsLocked(taskID, attemptID)
	if err != nil {
		return false, err
	}
	if attempt.Status != models.AttemptStatusRunning || task.Status != models.TaskStatusRunning {
		return false, nil
	}
	leaseID := task.LeaseID
	attempt.Status = models.AttemptStatusFailed
	attempt.Summary = strings.TrimSpace(summary)
	attempt.UpdatedAt = now
	task.Status = models.TaskStatusFailed
	task.LeaseID = ""
	task.UpdatedAt = now
	run.Status = models.RunStatusFailed
	run.UpdatedAt = now
	if leaseID != "" {
		s.releaseLeaseLocked(leaseID, now)
	}
	s.state.Attempts[attempt.ID] = attempt
	s.state.Tasks[task.ID] = task
	s.state.Runs[run.ID] = run
	events := []models.Event{s.appendEventLocked(models.Event{RunID: run.ID, TaskID: task.ID, ExecutorID: task.AssignedExecutorID, LeaseID: leaseID, Type: models.EventTaskStateChanged, ActorKind: models.ActorKindManager, ActorID: models.ActorKindManager, Summary: "Attempt failed", Data: map[string]any{"reason": "dispatcher_interrupted"}})}
	if leaseID != "" {
		events = append(events, s.appendEventLocked(models.Event{RunID: run.ID, TaskID: task.ID, ExecutorID: task.AssignedExecutorID, LeaseID: leaseID, Type: models.EventLeaseReleased, ActorKind: models.ActorKindManager, ActorID: models.ActorKindManager, Summary: "Runner slot released after interrupted attempt", Data: map[string]any{"reason": "dispatcher_interrupted"}}))
	}
	if err := s.saveLocked(); err != nil {
		return false, err
	}
	s.notifyEventsLocked(events)
	return true, nil
}

func (s *Store) FailQueuedAttempt(taskID, attemptID, summary string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	task, run, attempt, err := s.transitionRecordsLocked(taskID, attemptID)
	if err != nil {
		return err
	}
	if attempt.Status != models.AttemptStatusQueued {
		return nil
	}
	attempt.Status = models.AttemptStatusFailed
	attempt.Summary = strings.TrimSpace(summary)
	attempt.UpdatedAt = now
	task.Status = models.TaskStatusFailed
	task.UpdatedAt = now
	run.Status = models.RunStatusFailed
	run.UpdatedAt = now
	s.state.Attempts[attempt.ID] = attempt
	s.state.Tasks[task.ID] = task
	s.state.Runs[run.ID] = run
	return s.saveLocked()
}

func (s *Store) leaseForTaskLocked(taskID string) models.Lease {
	for _, lease := range s.state.Leases {
		if lease.TaskID == taskID && lease.Status == models.LeaseStatusActive {
			return lease
		}
	}
	return models.Lease{}
}
