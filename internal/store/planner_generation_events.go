package store

import (
	"sort"
	"time"

	"github.com/agent-parley/parley/internal/models"
)

func (s *Store) AppendPlannerGenerationEvent(event models.PlannerGenerationEvent) (models.PlannerGenerationEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	event = s.appendPlannerGenerationEventLocked(event)
	return event, s.saveLocked()
}

func (s *Store) appendPlannerGenerationEventLocked(event models.PlannerGenerationEvent) models.PlannerGenerationEvent {
	if s.state.PlannerGenerationEvents == nil {
		s.state.PlannerGenerationEvents = map[string]models.PlannerGenerationEvent{}
	}
	if event.ID == "" {
		event.ID = newID("pgevt")
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	if event.Sequence == 0 {
		event.Sequence = s.nextPlannerGenerationEventSequenceLocked(event.GenerationID)
	}
	s.state.PlannerGenerationEvents[event.ID] = event
	return event
}

func (s *Store) nextPlannerGenerationEventSequenceLocked(generationID string) int {
	seq := 0
	for _, event := range s.state.PlannerGenerationEvents {
		if event.GenerationID == generationID && event.Sequence > seq {
			seq = event.Sequence
		}
	}
	return seq + 1
}

func (s *Store) PlannerGenerationEventsForSession(sessionID string) []models.PlannerGenerationEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]models.PlannerGenerationEvent, 0)
	for _, event := range s.state.PlannerGenerationEvents {
		if event.SessionID == sessionID {
			events = append(events, event)
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if !events[i].CreatedAt.Equal(events[j].CreatedAt) {
			return events[i].CreatedAt.Before(events[j].CreatedAt)
		}
		if events[i].GenerationID != events[j].GenerationID {
			return events[i].GenerationID < events[j].GenerationID
		}
		return events[i].Sequence < events[j].Sequence
	})
	return events
}

func (s *Store) PlannerGenerationEventsForGeneration(generationID string) []models.PlannerGenerationEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]models.PlannerGenerationEvent, 0)
	for _, event := range s.state.PlannerGenerationEvents {
		if event.GenerationID == generationID {
			events = append(events, event)
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Sequence != events[j].Sequence {
			return events[i].Sequence < events[j].Sequence
		}
		return events[i].CreatedAt.Before(events[j].CreatedAt)
	})
	return events
}
