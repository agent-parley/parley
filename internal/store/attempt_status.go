package store

import (
	"time"

	"github.com/agent-parley/parley/internal/models"
)

func (s *Store) AttemptStillRunning(taskID, attemptID, leaseID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	changed, events := s.expireLeasesLocked(now)
	if changed {
		if err := s.saveLocked(); err == nil {
			s.notifyEventsLocked(events)
		}
	}
	task, ok := s.state.Tasks[taskID]
	if !ok || task.Status != models.TaskStatusRunning || task.LeaseID != leaseID {
		return false
	}
	attempt, ok := s.state.Attempts[attemptID]
	if !ok || attempt.TaskID != taskID || attempt.Status != models.AttemptStatusRunning {
		return false
	}
	lease, ok := s.state.Leases[leaseID]
	return ok && lease.Status == models.LeaseStatusActive
}
