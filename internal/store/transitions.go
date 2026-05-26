package store

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/models"
)

var ErrRunnerCapacityUnavailable = errors.New("runner capacity unavailable")

type StartAttemptResult struct {
	Project models.Project
	Run     models.Run
	Task    models.Task
	Attempt models.Attempt
	Runner  models.Executor
	Lease   models.Lease
}

func (s *Store) BeginAttempt(taskID string) (StartAttemptResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	changed, events := s.expireLeasesLocked(now)
	if changed {
		if err := s.saveLocked(); err != nil {
			return StartAttemptResult{}, err
		}
		s.notifyEventsLocked(events)
	}

	task, ok := s.state.Tasks[taskID]
	if !ok {
		return StartAttemptResult{}, fmt.Errorf("task not found")
	}
	run, ok := s.state.Runs[task.RunID]
	if !ok {
		return StartAttemptResult{}, fmt.Errorf("run not found")
	}
	project, ok := s.state.Projects[task.ProjectID]
	if !ok {
		return StartAttemptResult{}, fmt.Errorf("project not found")
	}
	canonicalRepoPath, err := canonicalizeRepoPath(project.RepoPath)
	if err != nil {
		return StartAttemptResult{}, fmt.Errorf("project repo path is unavailable: %w", err)
	}
	if canonicalRepoPath != project.RepoPath {
		return StartAttemptResult{}, fmt.Errorf("project repo path changed; re-register the canonical repo root before execution")
	}
	if task.Status != models.TaskStatusDraft && task.Status != models.TaskStatusNeedsFix && task.Status != models.TaskStatusQueued {
		return StartAttemptResult{}, fmt.Errorf("task is not ready to run")
	}
	maxAttempts := task.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if task.Attempts >= maxAttempts {
		if task.Status == models.TaskStatusNeedsFix && task.Attempts > 0 {
			maxAttempts = task.Attempts + 1
			task.MaxAttempts = maxAttempts
		} else {
			return StartAttemptResult{}, fmt.Errorf("task has reached its retry limit")
		}
	}

	runnerID := task.AssignedExecutorID
	if runnerID == "" {
		runnerID = project.DefaultExecutorID
	}
	runnerID = s.localExecutionRunnerIDLocked(runnerID)
	runner, ok := s.state.Executors[runnerID]
	if !ok {
		return StartAttemptResult{}, fmt.Errorf("runner %s not found", runnerID)
	}
	if runner.Kind != models.ExecutorKindLocal {
		return StartAttemptResult{}, fmt.Errorf("runner %s is not a local execution runner", runnerID)
	}
	if runner.Status != models.ExecutorStatusOnline {
		return StartAttemptResult{}, fmt.Errorf("runner %s is %s", runnerID, runner.Status)
	}

	lease, ok := s.activeLeaseForTaskRunnerLocked(task.ID, runnerID, now)
	if !ok {
		if conflicting, ok := s.activeLeaseForTaskLocked(task.ID, now); ok && conflicting.ExecutorID != runnerID {
			return StartAttemptResult{}, fmt.Errorf("task already has an active run slot on runner %s", conflicting.ExecutorID)
		}
		maxSlots := runner.MaxSlots
		if maxSlots <= 0 {
			maxSlots = 1
		}
		if s.activeLeaseCountForExecutorLocked(runnerID, now) >= maxSlots {
			return StartAttemptResult{}, fmt.Errorf("%w: runner %s has no available run slots", ErrRunnerCapacityUnavailable, runnerID)
		}
		expiresAt := now.Add(defaultLeaseTTL)
		lease = models.Lease{ID: newID("lse"), TaskID: task.ID, ExecutorID: runnerID, Status: models.LeaseStatusActive, GrantedAt: now, ExpiresAt: &expiresAt, Reason: "prototype dry-run attempt"}
		s.state.Leases[lease.ID] = lease
	}

	nextNumber := task.Attempts + 1
	attempt, ok := s.attemptForTaskNumberLocked(task.ID, nextNumber)
	if ok {
		if attempt.Status != models.AttemptStatusQueued {
			return StartAttemptResult{}, fmt.Errorf("attempt %d is already %s", nextNumber, attempt.Status)
		}
		attempt.Status = models.AttemptStatusRunning
		attempt.Summary = "Prototype worker attempt running on the selected runner."
		attempt.UpdatedAt = now
		s.state.Attempts[attempt.ID] = attempt
	} else {
		if task.Status == models.TaskStatusQueued {
			return StartAttemptResult{}, fmt.Errorf("queued attempt not found")
		}
		attempt = models.Attempt{ID: newID("att"), ProjectID: project.ID, RunID: run.ID, TaskID: task.ID, Number: nextNumber, Kind: models.AttemptKindWorker, Status: models.AttemptStatusRunning, Summary: "Prototype worker attempt running on the selected runner.", CreatedAt: now, UpdatedAt: now}
		if task.Status == models.TaskStatusNeedsFix {
			attempt.Kind = models.AttemptKindFix
		}
		s.state.Attempts[attempt.ID] = attempt
	}

	run.Status = models.RunStatusRunning
	run.UpdatedAt = now
	task.Status = models.TaskStatusRunning
	task.AssignedExecutorID = runnerID
	task.LeaseID = lease.ID
	task.Attempts = nextNumber
	task.UpdatedAt = now
	s.state.Runs[run.ID] = run
	s.state.Tasks[task.ID] = task

	if err := s.saveLocked(); err != nil {
		return StartAttemptResult{}, err
	}
	return StartAttemptResult{Project: project, Run: run, Task: task, Attempt: attempt, Runner: runner, Lease: lease}, nil
}

func (s *Store) CompleteAttempt(taskID, attemptID, leaseID, summary string) (models.Run, models.Task, models.Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	if changed, events := s.expireLeasesLocked(now); changed {
		if err := s.saveLocked(); err != nil {
			return models.Run{}, models.Task{}, models.Attempt{}, err
		}
		s.notifyEventsLocked(events)
	}
	task, run, attempt, err := s.transitionRecordsLocked(taskID, attemptID)
	if err != nil {
		return models.Run{}, models.Task{}, models.Attempt{}, err
	}
	if task.Status != models.TaskStatusRunning {
		lease, leaseOK := s.state.Leases[leaseID]
		if task.Status == models.TaskStatusAwaitingReview && attempt.Status == models.AttemptStatusReviewed && leaseOK && lease.TaskID == taskID && lease.Status == models.LeaseStatusReleased {
			return run, task, attempt, nil
		}
		if attempt.Status == models.AttemptStatusExpired || (leaseOK && lease.Status == models.LeaseStatusExpired) {
			return run, task, attempt, fmt.Errorf("attempt lease expired")
		}
		return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("task is not running")
	}
	if task.LeaseID != leaseID {
		return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("task run slot changed")
	}

	attempt.Status = models.AttemptStatusReviewed
	attempt.Summary = strings.TrimSpace(summary)
	attempt.UpdatedAt = now
	task.Status = models.TaskStatusAwaitingReview
	task.LeaseID = ""
	task.UpdatedAt = now
	run.Status = models.RunStatusAwaitingReview
	run.UpdatedAt = now
	s.releaseLeaseLocked(leaseID, now)
	s.state.Attempts[attempt.ID] = attempt
	s.state.Tasks[task.ID] = task
	s.state.Runs[run.ID] = run
	return run, task, attempt, s.saveLocked()
}

func (s *Store) FailAttempt(taskID, attemptID, leaseID, summary string) (models.Run, models.Task, models.Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	if changed, events := s.expireLeasesLocked(now); changed {
		if err := s.saveLocked(); err != nil {
			return models.Run{}, models.Task{}, models.Attempt{}, err
		}
		s.notifyEventsLocked(events)
	}
	task, run, attempt, err := s.transitionRecordsLocked(taskID, attemptID)
	if err != nil {
		return models.Run{}, models.Task{}, models.Attempt{}, err
	}
	if task.Status != models.TaskStatusRunning {
		lease, leaseOK := s.state.Leases[leaseID]
		if task.Status == models.TaskStatusFailed && attempt.Status == models.AttemptStatusFailed && leaseOK && lease.TaskID == taskID && lease.Status == models.LeaseStatusReleased {
			return run, task, attempt, nil
		}
		if attempt.Status == models.AttemptStatusExpired || (leaseOK && lease.Status == models.LeaseStatusExpired) {
			return run, task, attempt, nil
		}
		return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("task is not running")
	}
	if task.LeaseID != leaseID {
		return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("task run slot changed")
	}
	attempt.Status = models.AttemptStatusFailed
	attempt.Summary = strings.TrimSpace(summary)
	attempt.UpdatedAt = now
	task.Status = models.TaskStatusFailed
	task.LeaseID = ""
	task.UpdatedAt = now
	run.Status = models.RunStatusFailed
	run.UpdatedAt = now
	s.releaseLeaseLocked(leaseID, now)
	s.state.Attempts[attempt.ID] = attempt
	s.state.Tasks[task.ID] = task
	s.state.Runs[run.ID] = run
	return run, task, attempt, s.saveLocked()
}

func (s *Store) RequestFix(taskID string) (models.Run, models.Task, models.Attempt, error) {
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
	if task.Status == models.TaskStatusNeedsFix {
		attempt, err := s.ensureRequestedFixAttemptLocked(task, run, now)
		if err != nil {
			return models.Run{}, models.Task{}, models.Attempt{}, err
		}
		return run, task, attempt, s.saveLocked()
	}
	if task.Status != models.TaskStatusAwaitingReview && task.Status != models.TaskStatusFailed {
		return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("task is not awaiting review or failed")
	}
	maxAttempts := task.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if task.Attempts >= maxAttempts {
		if task.Status == models.TaskStatusFailed {
			maxAttempts = task.Attempts + 1
			task.MaxAttempts = maxAttempts
		} else {
			return models.Run{}, models.Task{}, models.Attempt{}, fmt.Errorf("task has reached its retry limit")
		}
	}
	task.Status = models.TaskStatusNeedsFix
	task.UpdatedAt = now
	run.Status = models.RunStatusNeedsFix
	run.UpdatedAt = now
	attempt, err := s.ensureRequestedFixAttemptLocked(task, run, now)
	if err != nil {
		return models.Run{}, models.Task{}, models.Attempt{}, err
	}
	s.state.Tasks[task.ID] = task
	s.state.Runs[run.ID] = run
	return run, task, attempt, s.saveLocked()
}

func (s *Store) AcceptTask(taskID string) (models.Run, models.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	task, ok := s.state.Tasks[taskID]
	if !ok {
		return models.Run{}, models.Task{}, fmt.Errorf("task not found")
	}
	run, ok := s.state.Runs[task.RunID]
	if !ok {
		return models.Run{}, models.Task{}, fmt.Errorf("run not found")
	}
	if task.Status == models.TaskStatusDone {
		return run, task, nil
	}
	if task.Status != models.TaskStatusAwaitingReview {
		return models.Run{}, models.Task{}, fmt.Errorf("task is not awaiting review")
	}
	task.Status = models.TaskStatusDone
	task.UpdatedAt = now
	run.Status = models.RunStatusCompleted
	run.UpdatedAt = now
	s.state.Tasks[task.ID] = task
	s.state.Runs[run.ID] = run
	return run, task, s.saveLocked()
}

func (s *Store) RecordHandoffApproval(id string) (models.Handoff, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	handoff, ok := s.state.Handoffs[id]
	if !ok {
		return models.Handoff{}, false, fmt.Errorf("handoff not found")
	}
	if handoff.Status == models.HandoffStatusRecorded {
		return handoff, false, nil
	}
	if handoff.Status != models.HandoffStatusPreview {
		return models.Handoff{}, false, fmt.Errorf("handoff is not awaiting approval")
	}
	if _, ok := s.state.Tasks[handoff.TaskID]; !ok {
		return models.Handoff{}, false, fmt.Errorf("task not found")
	}
	destination, ok := s.state.Executors[handoff.DestinationExecutorID]
	if !ok {
		return models.Handoff{}, false, fmt.Errorf("destination runner not found")
	}
	if destination.Status != models.ExecutorStatusOnline {
		return models.Handoff{}, false, fmt.Errorf("destination runner %s is %s", destination.ID, destination.Status)
	}
	now := time.Now().UTC()
	handoff.Status = models.HandoffStatusRecorded
	handoff.ResultSummary = "Prototype handoff approval recorded. No code, files, credentials, database, container socket, or task runner assignment moved."
	handoff.CompletedAt = &now
	handoff.UpdatedAt = now
	s.state.Handoffs[handoff.ID] = handoff
	return handoff, true, s.saveLocked()
}

func (s *Store) transitionRecordsLocked(taskID, attemptID string) (models.Task, models.Run, models.Attempt, error) {
	task, ok := s.state.Tasks[taskID]
	if !ok {
		return models.Task{}, models.Run{}, models.Attempt{}, fmt.Errorf("task not found")
	}
	run, ok := s.state.Runs[task.RunID]
	if !ok {
		return models.Task{}, models.Run{}, models.Attempt{}, fmt.Errorf("run not found")
	}
	attempt, ok := s.state.Attempts[attemptID]
	if !ok || attempt.TaskID != taskID {
		return models.Task{}, models.Run{}, models.Attempt{}, fmt.Errorf("attempt not found")
	}
	return task, run, attempt, nil
}

func (s *Store) ensureRequestedFixAttemptLocked(task models.Task, run models.Run, now time.Time) (models.Attempt, error) {
	number := task.Attempts + 1
	if attempt, ok := s.attemptForTaskNumberLocked(task.ID, number); ok {
		if attempt.Status != models.AttemptStatusRequested && attempt.Status != models.AttemptStatusQueued {
			return models.Attempt{}, fmt.Errorf("attempt %d is already %s", number, attempt.Status)
		}
		if task.Status == models.TaskStatusNeedsFix && attempt.Status == models.AttemptStatusQueued {
			attempt.Status = models.AttemptStatusRequested
			attempt.Summary = "Fix requested; queue the next attempt when ready."
			attempt.UpdatedAt = now
			s.state.Attempts[attempt.ID] = attempt
		}
		return attempt, nil
	}
	attempt := models.Attempt{ID: newID("att"), ProjectID: task.ProjectID, RunID: run.ID, TaskID: task.ID, Number: number, Kind: models.AttemptKindFix, Status: models.AttemptStatusRequested, Summary: "Fix requested; queue the next attempt when ready.", CreatedAt: now, UpdatedAt: now}
	s.state.Attempts[attempt.ID] = attempt
	return attempt, nil
}

func (s *Store) activeLeaseForTaskRunnerLocked(taskID, executorID string, now time.Time) (models.Lease, bool) {
	for _, lease := range s.state.Leases {
		if lease.TaskID == taskID && lease.ExecutorID == executorID && s.isLeaseActiveLocked(lease, now) {
			return lease, true
		}
	}
	return models.Lease{}, false
}

func (s *Store) activeLeaseForTaskLocked(taskID string, now time.Time) (models.Lease, bool) {
	for _, lease := range s.state.Leases {
		if lease.TaskID == taskID && s.isLeaseActiveLocked(lease, now) {
			return lease, true
		}
	}
	return models.Lease{}, false
}

func (s *Store) activeLeaseCountForExecutorLocked(executorID string, now time.Time) int {
	count := 0
	for _, lease := range s.state.Leases {
		if lease.ExecutorID == executorID && s.isLeaseActiveLocked(lease, now) {
			count++
		}
	}
	return count
}

func (s *Store) attemptForTaskNumberLocked(taskID string, number int) (models.Attempt, bool) {
	for _, attempt := range s.state.Attempts {
		if attempt.TaskID == taskID && attempt.Number == number {
			return attempt, true
		}
	}
	return models.Attempt{}, false
}

func (s *Store) releaseLeaseLocked(id string, now time.Time) {
	lease, ok := s.state.Leases[id]
	if !ok || lease.Status == models.LeaseStatusReleased {
		return
	}
	lease.Status = models.LeaseStatusReleased
	lease.ReleasedAt = &now
	s.state.Leases[id] = lease
}
