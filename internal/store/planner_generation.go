package store

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/models"
)

func (s *Store) BeginPlannerGeneration(sessionID, mode, plannerProfile, criticProfile string) (models.PlannerGeneration, models.PlannerSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.state.PlannerSessions[sessionID]
	if !ok {
		return models.PlannerGeneration{}, models.PlannerSession{}, fmt.Errorf("planner session not found")
	}
	if session.Status != models.PlannerStatusPlanning {
		return models.PlannerGeneration{}, models.PlannerSession{}, fmt.Errorf("planner session is %s", session.Status)
	}
	if active := s.activePlannerGenerationLocked(session.ID); active.ID != "" {
		return models.PlannerGeneration{}, session, fmt.Errorf("planner generation %s is already %s", active.ID, active.Status)
	}
	now := time.Now().UTC()
	generation := models.PlannerGeneration{
		ID:              newID("gen"),
		ProjectID:       session.ProjectID,
		SessionID:       session.ID,
		Status:          models.PlannerGenerationStatusRunning,
		Mode:            strings.TrimSpace(mode),
		PlannerProfile:  strings.TrimSpace(plannerProfile),
		CriticProfile:   strings.TrimSpace(criticProfile),
		PlannerRevision: session.PlannerRevision,
		Summary:         "Planner/critic generation is running before task approval.",
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	session.ActiveGenerationID = generation.ID
	session.AgentMode = generation.Mode
	session.AgentStatus = models.PlannerAgentStatusRunning
	session.AgentSummary = generation.Summary
	session.PlannerProfile = generation.PlannerProfile
	session.CriticProfile = generation.CriticProfile
	session.UpdatedAt = now
	s.state.PlannerGenerations[generation.ID] = generation
	s.state.PlannerSessions[session.ID] = session
	return generation, session, s.saveLocked()
}

func (s *Store) CompletePlannerGeneration(generationID string, session models.PlannerSession, status, summary, diagnostic string) (models.PlannerSession, models.PlannerGeneration, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	generation, ok := s.state.PlannerGenerations[generationID]
	if !ok {
		return models.PlannerSession{}, models.PlannerGeneration{}, false, fmt.Errorf("planner generation not found")
	}
	current, ok := s.state.PlannerSessions[generation.SessionID]
	if !ok {
		return models.PlannerSession{}, models.PlannerGeneration{}, false, fmt.Errorf("planner session not found")
	}
	now := time.Now().UTC()
	completed := now
	generation.CompletedAt = &completed
	generation.UpdatedAt = now
	generation.Summary = strings.TrimSpace(summary)
	generation.Diagnostic = strings.TrimSpace(diagnostic)
	if generation.Summary == "" {
		generation.Summary = "Planner/critic generation finished before approval."
	}

	staleStatus := current.Status != models.PlannerStatusPlanning
	staleRevision := current.PlannerRevision != generation.PlannerRevision
	staleGeneration := current.ActiveGenerationID != generation.ID
	if staleStatus || staleRevision || staleGeneration {
		generation.Status = models.PlannerGenerationStatusDiscarded
		switch {
		case staleStatus:
			generation.Summary = fmt.Sprintf("Planner/critic result discarded because the session is now %s.", current.Status)
		case staleRevision:
			generation.Summary = "Planner/critic result discarded because the planning thread changed while it was running."
		default:
			generation.Summary = "Planner/critic result discarded because another generation is active."
		}
		if staleStatus && current.ActiveGenerationID == generation.ID {
			current.ActiveGenerationID = ""
			current.UpdatedAt = now
			s.state.PlannerSessions[current.ID] = current
		}
		s.state.PlannerGenerations[generation.ID] = generation
		return current, generation, false, s.saveLocked()
	}

	session.CreatedAt = current.CreatedAt
	session.Status = current.Status
	session.PlannerRevision = current.PlannerRevision
	session.ActiveGenerationID = ""
	session.ApprovedRunID = current.ApprovedRunID
	session.ApprovedTaskID = current.ApprovedTaskID
	session.UpdatedAt = now
	generation.Status = status
	if generation.Status == "" {
		generation.Status = models.PlannerGenerationStatusCompleted
	}
	generation.Mode = firstNonEmptyStore(session.AgentMode, generation.Mode)
	generation.PlannerProfile = firstNonEmptyStore(session.PlannerProfile, generation.PlannerProfile)
	generation.CriticProfile = firstNonEmptyStore(session.CriticProfile, generation.CriticProfile)
	generation.Summary = firstNonEmptyStore(session.AgentSummary, generation.Summary)
	s.state.PlannerSessions[session.ID] = session
	s.state.PlannerGenerations[generation.ID] = generation
	return session, generation, true, s.saveLocked()
}

func (s *Store) PlannerGenerationsForSession(sessionID string) []models.PlannerGeneration {
	s.mu.Lock()
	defer s.mu.Unlock()
	generations := make([]models.PlannerGeneration, 0)
	for _, generation := range s.state.PlannerGenerations {
		if generation.SessionID == sessionID {
			generations = append(generations, generation)
		}
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i].CreatedAt.After(generations[j].CreatedAt) })
	return generations
}

func (s *Store) SavePlannerDiagnostic(diagnostic models.PlannerDiagnostic) (models.PlannerDiagnostic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if diagnostic.ID == "" {
		diagnostic.ID = newID("pdiag")
	}
	if diagnostic.CreatedAt.IsZero() {
		diagnostic.CreatedAt = time.Now().UTC()
	}
	if diagnostic.Sensitivity == "" {
		diagnostic.Sensitivity = models.SensitivityInternal
	}
	s.state.PlannerDiagnostics[diagnostic.ID] = diagnostic
	return diagnostic, s.saveLocked()
}

func (s *Store) GetPlannerDiagnostic(id string) (models.PlannerDiagnostic, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	diagnostic, ok := s.state.PlannerDiagnostics[id]
	return diagnostic, ok
}

func (s *Store) PlannerDiagnosticsForSession(sessionID string) []models.PlannerDiagnostic {
	s.mu.Lock()
	defer s.mu.Unlock()
	diagnostics := make([]models.PlannerDiagnostic, 0)
	for _, diagnostic := range s.state.PlannerDiagnostics {
		if diagnostic.SessionID == sessionID {
			diagnostics = append(diagnostics, diagnostic)
		}
	}
	sort.Slice(diagnostics, func(i, j int) bool { return diagnostics[i].CreatedAt.After(diagnostics[j].CreatedAt) })
	return diagnostics
}

func (s *Store) PlannerDiagnosticsForGeneration(generationID string) []models.PlannerDiagnostic {
	s.mu.Lock()
	defer s.mu.Unlock()
	diagnostics := make([]models.PlannerDiagnostic, 0)
	for _, diagnostic := range s.state.PlannerDiagnostics {
		if diagnostic.GenerationID == generationID {
			diagnostics = append(diagnostics, diagnostic)
		}
	}
	sort.Slice(diagnostics, func(i, j int) bool { return diagnostics[i].CreatedAt.Before(diagnostics[j].CreatedAt) })
	return diagnostics
}

func (s *Store) PrunePlannerDiagnosticsForSession(sessionID string, keepGenerations int) ([]models.PlannerDiagnostic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if keepGenerations <= 0 {
		return nil, nil
	}
	generations := make([]models.PlannerGeneration, 0)
	for _, generation := range s.state.PlannerGenerations {
		if generation.SessionID == sessionID {
			generations = append(generations, generation)
		}
	}
	sort.Slice(generations, func(i, j int) bool {
		if generations[i].CreatedAt.Equal(generations[j].CreatedAt) {
			return generations[i].ID > generations[j].ID
		}
		return generations[i].CreatedAt.After(generations[j].CreatedAt)
	})
	keep := map[string]bool{}
	for i, generation := range generations {
		if i >= keepGenerations {
			break
		}
		keep[generation.ID] = true
	}
	removed := make([]models.PlannerDiagnostic, 0)
	for id, diagnostic := range s.state.PlannerDiagnostics {
		if diagnostic.SessionID == sessionID && !keep[diagnostic.GenerationID] {
			removed = append(removed, diagnostic)
			delete(s.state.PlannerDiagnostics, id)
		}
	}
	if len(removed) == 0 {
		return nil, nil
	}
	sort.Slice(removed, func(i, j int) bool { return removed[i].CreatedAt.Before(removed[j].CreatedAt) })
	return removed, s.saveLocked()
}

func (s *Store) activePlannerGenerationLocked(sessionID string) models.PlannerGeneration {
	if session, ok := s.state.PlannerSessions[sessionID]; ok && session.ActiveGenerationID != "" {
		generation := s.state.PlannerGenerations[session.ActiveGenerationID]
		if isPlannerGenerationActive(generation.Status) {
			return generation
		}
	}
	for _, generation := range s.state.PlannerGenerations {
		if generation.SessionID == sessionID && isPlannerGenerationActive(generation.Status) {
			return generation
		}
	}
	return models.PlannerGeneration{}
}

func (s *Store) failInterruptedPlannerGenerationsLocked(now time.Time) {
	for id, generation := range s.state.PlannerGenerations {
		if !isPlannerGenerationActive(generation.Status) {
			continue
		}
		generation.Status = models.PlannerGenerationStatusFailed
		generation.Summary = "Planner/critic generation was interrupted before completion; regenerate the draft."
		generation.Diagnostic = "Diagnostic: planner/critic execution was interrupted by process restart."
		generation.CompletedAt = &now
		generation.UpdatedAt = now
		s.state.PlannerGenerations[id] = generation
		s.appendPlannerGenerationEventLocked(models.PlannerGenerationEvent{ProjectID: generation.ProjectID, SessionID: generation.SessionID, GenerationID: generation.ID, Type: models.PlannerGenerationEventResultFailed, Summary: generation.Summary, Data: map[string]any{"reason": "interrupted"}, CreatedAt: now})
		if session, ok := s.state.PlannerSessions[generation.SessionID]; ok && session.ActiveGenerationID == generation.ID {
			session.ActiveGenerationID = ""
			if session.Status == models.PlannerStatusPlanning {
				session.AgentStatus = models.PlannerAgentStatusFailed
				session.AgentSummary = generation.Summary + " " + generation.Diagnostic
				session.UpdatedAt = now
				messageID := newID("msg")
				s.state.PlannerMessages[messageID] = models.PlannerMessage{ID: messageID, SessionID: session.ID, Role: "planner", Body: session.AgentSummary, CreatedAt: now}
			}
			s.state.PlannerSessions[session.ID] = session
		}
	}
}

func isPlannerGenerationActive(status string) bool {
	return status == models.PlannerGenerationStatusRunning
}

func firstNonEmptyStore(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
