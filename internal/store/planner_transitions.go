package store

import (
	"fmt"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/models"
)

type PlannerApprovalResult struct {
	Session models.PlannerSession
	Run     models.Run
	Task    models.Task
	Created bool
}

func (s *Store) AddPlannerReply(sessionID, body string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.state.PlannerSessions[sessionID]
	if !ok {
		return fmt.Errorf("planner session not found")
	}
	if session.Status != models.PlannerStatusPlanning {
		return fmt.Errorf("planner session is %s", session.Status)
	}
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}
	now := time.Now().UTC()
	userMessage := models.PlannerMessage{ID: newID("msg"), SessionID: sessionID, Role: "user", Body: body, CreatedAt: now}
	plannerMessage := models.PlannerMessage{ID: newID("msg"), SessionID: sessionID, Role: "planner", Body: "Noted. Generate the planner/critic draft again if this note should reshape it, approve when it looks right, or record another note.", CreatedAt: now.Add(time.Millisecond)}
	s.state.PlannerMessages[userMessage.ID] = userMessage
	s.state.PlannerMessages[plannerMessage.ID] = plannerMessage
	session.PlannerRevision++
	markActivePlannerGenerationStaleLocked(&session)
	session.UpdatedAt = now
	s.state.PlannerSessions[sessionID] = session
	return s.saveLocked()
}

func (s *Store) RevisePlannerSession(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.state.PlannerSessions[sessionID]
	if !ok {
		return fmt.Errorf("planner session not found")
	}
	if session.Status != models.PlannerStatusPlanning {
		return fmt.Errorf("planner session is %s", session.Status)
	}
	now := time.Now().UTC()
	session.Assumptions = append(session.Assumptions, "Prototype revision note recorded before approval; keep the plan editable and review-gated.")
	session.Risks = append(session.Risks, "Review the generated task graph before letting a worker run.")
	session.PlannerRevision++
	markActivePlannerGenerationStaleLocked(&session)
	session.UpdatedAt = now
	plannerMessage := models.PlannerMessage{ID: newID("msg"), SessionID: sessionID, Role: "planner", Body: "Revision note recorded. Generate the planner/critic draft again before approval if needed.", CreatedAt: now}
	s.state.PlannerMessages[plannerMessage.ID] = plannerMessage
	s.state.PlannerSessions[sessionID] = session
	return s.saveLocked()
}

func (s *Store) ApprovePlannerSession(project models.Project, sessionID string) (PlannerApprovalResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.state.PlannerSessions[sessionID]
	if !ok {
		return PlannerApprovalResult{}, fmt.Errorf("planner session not found")
	}
	if session.ProjectID != project.ID {
		return PlannerApprovalResult{}, fmt.Errorf("planner session project mismatch")
	}
	if session.Status == models.PlannerStatusApproved {
		run, runOK := s.state.Runs[session.ApprovedRunID]
		task, taskOK := s.state.Tasks[session.ApprovedTaskID]
		if runOK && taskOK {
			return PlannerApprovalResult{Session: session, Run: run, Task: task, Created: false}, nil
		}
		return PlannerApprovalResult{}, fmt.Errorf("approved planner session is missing its task")
	}
	if session.Status != models.PlannerStatusPlanning {
		return PlannerApprovalResult{}, fmt.Errorf("planner session is %s", session.Status)
	}
	now := time.Now().UTC()
	run, task := s.createManualRunTaskLocked(now, project, session.DraftTitle, session.DraftObjective, session.DraftFocus, session.DraftBoundaries, session.DraftDoneWhen)
	session.Status = models.PlannerStatusApproved
	session.ApprovedRunID = run.ID
	session.ApprovedTaskID = task.ID
	session.UpdatedAt = now
	s.state.PlannerSessions[session.ID] = session
	if err := s.saveLocked(); err != nil {
		return PlannerApprovalResult{}, err
	}
	return PlannerApprovalResult{Session: session, Run: run, Task: task, Created: true}, nil
}

func markActivePlannerGenerationStaleLocked(session *models.PlannerSession) {
	if session.ActiveGenerationID == "" {
		return
	}
	session.ActiveGenerationID = ""
	session.AgentStatus = models.PlannerAgentStatusDiscarded
	session.AgentSummary = "Planning thread changed; the in-flight planner/critic generation will be discarded. Regenerate the draft when the running pass finishes."
}

func (s *Store) DismissPlannerSession(sessionID string) (models.PlannerSession, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.state.PlannerSessions[sessionID]
	if !ok {
		return models.PlannerSession{}, false, fmt.Errorf("planner session not found")
	}
	if session.Status == models.PlannerStatusDismissed {
		return session, false, nil
	}
	if session.Status != models.PlannerStatusPlanning {
		return models.PlannerSession{}, false, fmt.Errorf("planner session is %s", session.Status)
	}
	session.Status = models.PlannerStatusDismissed
	session.UpdatedAt = time.Now().UTC()
	s.state.PlannerSessions[session.ID] = session
	return session, true, s.saveLocked()
}
