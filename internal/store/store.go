package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/agent-parley/parley/internal/models"
)

const (
	defaultLeaseTTL = 30 * time.Minute
	sqliteFileName = "parley.db"
)

type Store struct {
	mu             sync.Mutex
	root           string
	path           string
	dbPath         string
	db             *sql.DB
	legacyJSONPath string
	state          state
	watchers       map[string]map[chan models.Event]struct{}
}

type state struct {
	Executors         map[string]models.Executor         `json:"executors"`
	Projects          map[string]models.Project          `json:"projects"`
	WorkflowTemplates map[string]models.WorkflowTemplate `json:"workflow_templates"`
	PlannerSessions    map[string]models.PlannerSession    `json:"planner_sessions"`
	PlannerMessages    map[string]models.PlannerMessage    `json:"planner_messages"`
	PlannerGenerations map[string]models.PlannerGeneration `json:"planner_generations"`
	PlannerDiagnostics map[string]models.PlannerDiagnostic `json:"planner_diagnostics"`
	Runs               map[string]models.Run              `json:"runs"`
	Tasks             map[string]models.Task             `json:"tasks"`
	Attempts          map[string]models.Attempt          `json:"attempts"`
	Handoffs          map[string]models.Handoff          `json:"handoffs"`
	Leases            map[string]models.Lease            `json:"leases"`
	Artifacts         map[string]models.Artifact         `json:"artifacts"`
	Events            []models.Event                     `json:"events"`
}

func newState() state {
	return state{
		Executors:         map[string]models.Executor{},
		Projects:          map[string]models.Project{},
		WorkflowTemplates: map[string]models.WorkflowTemplate{},
		PlannerSessions:    map[string]models.PlannerSession{},
		PlannerMessages:    map[string]models.PlannerMessage{},
		PlannerGenerations: map[string]models.PlannerGeneration{},
		PlannerDiagnostics: map[string]models.PlannerDiagnostic{},
		Runs:               map[string]models.Run{},
		Tasks:             map[string]models.Task{},
		Attempts:          map[string]models.Attempt{},
		Handoffs:          map[string]models.Handoff{},
		Leases:            map[string]models.Lease{},
		Artifacts:         map[string]models.Artifact{},
	}
}

func (s *Store) ensureStateMapsLocked() {
	if s.state.Executors == nil {
		s.state.Executors = map[string]models.Executor{}
	}
	if s.state.Projects == nil {
		s.state.Projects = map[string]models.Project{}
	}
	if s.state.WorkflowTemplates == nil {
		s.state.WorkflowTemplates = map[string]models.WorkflowTemplate{}
	}
	if s.state.PlannerSessions == nil {
		s.state.PlannerSessions = map[string]models.PlannerSession{}
	}
	if s.state.PlannerMessages == nil {
		s.state.PlannerMessages = map[string]models.PlannerMessage{}
	}
	if s.state.PlannerGenerations == nil {
		s.state.PlannerGenerations = map[string]models.PlannerGeneration{}
	}
	if s.state.PlannerDiagnostics == nil {
		s.state.PlannerDiagnostics = map[string]models.PlannerDiagnostic{}
	}
	if s.state.Runs == nil {
		s.state.Runs = map[string]models.Run{}
	}
	if s.state.Tasks == nil {
		s.state.Tasks = map[string]models.Task{}
	}
	if s.state.Attempts == nil {
		s.state.Attempts = map[string]models.Attempt{}
	}
	if s.state.Handoffs == nil {
		s.state.Handoffs = map[string]models.Handoff{}
	}
	if s.state.Leases == nil {
		s.state.Leases = map[string]models.Lease{}
	}
	if s.state.Artifacts == nil {
		s.state.Artifacts = map[string]models.Artifact{}
	}
}

func Open(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	st := &Store{
		root:           root,
		path:           filepath.Join(root, "state.json"),
		dbPath:         filepath.Join(root, sqliteFileName),
		legacyJSONPath: filepath.Join(root, "state.json"),
		watchers:       map[string]map[chan models.Event]struct{}{},
		state:          newState(),
	}
	db, err := openSQLite(st.dbPath)
	if err != nil {
		return nil, err
	}
	st.db = db
	if err := st.loadSQLiteState(); err != nil {
		_ = db.Close()
		return nil, err
	}
	st.ensureStateMapsLocked()
	st.ensurePrototypeExecutorsLocked()
	now := time.Now().UTC()
	st.failInterruptedPlannerGenerationsLocked(now)
	_, _ = st.expireLeasesLocked(now)
	st.ensureProjectDefaultsLocked()
	st.normalizeEventSequencesLocked()
	if err := st.saveLocked(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

func (s *Store) DataRoot() string { return s.root }

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *Store) LocalExecutor() models.Executor {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePrototypeExecutorsLocked()
	id := s.localExecutionRunnerIDLocked(models.LocalExecutorID)
	return s.state.Executors[id]
}

func (s *Store) ListExecutors() []models.Executor {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePrototypeExecutorsLocked()
	executors := make([]models.Executor, 0, len(s.state.Executors))
	for _, executor := range s.state.Executors {
		executors = append(executors, executor)
	}
	sort.Slice(executors, func(i, j int) bool { return executors[i].ID < executors[j].ID })
	return executors
}

func (s *Store) GetExecutor(id string) (models.Executor, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePrototypeExecutorsLocked()
	executor, ok := s.state.Executors[id]
	return executor, ok
}

func (s *Store) ActiveLeaseCountByExecutor() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	changed, events := s.expireLeasesLocked(now)
	if changed {
		if err := s.saveLocked(); err == nil {
			s.notifyEventsLocked(events)
		}
	}
	counts := map[string]int{}
	for _, lease := range s.state.Leases {
		if s.isLeaseActiveLocked(lease, now) {
			counts[lease.ExecutorID]++
		}
	}
	return counts
}

func (s *Store) ListProjects() []models.Project {
	s.mu.Lock()
	defer s.mu.Unlock()
	projects := make([]models.Project, 0, len(s.state.Projects))
	for _, project := range s.state.Projects {
		projects = append(projects, project)
	}
	sort.Slice(projects, func(i, j int) bool { return projects[i].CreatedAt.Before(projects[j].CreatedAt) })
	return projects
}

func (s *Store) GetProject(id string) (models.Project, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	project, ok := s.state.Projects[id]
	return project, ok
}

func (s *Store) CreateProject(name, description, repoPath, defaultBranch string) (models.Project, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	canonicalRepoPath, err := canonicalizeRepoPath(repoPath)
	if err != nil {
		return models.Project{}, err
	}
	now := time.Now().UTC()
	project := models.Project{
		ID:                  newID("prj"),
		Name:                strings.TrimSpace(name),
		Description:         strings.TrimSpace(description),
		RepoPath:            canonicalRepoPath,
		DefaultBranch:       strings.TrimSpace(defaultBranch),
		DefaultExecutorID:   models.LocalExecutorID,
		AgentContext:        strings.TrimSpace(description),
		DefaultAgentProfile: "pi-standard",
		ReviewLoopCount:     1,
		RetryCount:          1,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	if project.Name == "" {
		project.Name = filepath.Base(repoPath)
	}
	if project.DefaultBranch == "" {
		project.DefaultBranch = "main"
	}
	templates := defaultWorkflowTemplates(project.ID, now)
	project.DefaultWorkflowTemplateID = templates[0].ID
	s.state.Projects[project.ID] = project
	for _, template := range templates {
		s.state.WorkflowTemplates[template.ID] = template
	}
	return project, s.saveLocked()
}

func (s *Store) UpdateProjectSettings(project models.Project) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.state.Projects[project.ID]
	if !ok {
		return fmt.Errorf("project not found")
	}
	project.CreatedAt = current.CreatedAt
	project.UpdatedAt = time.Now().UTC()
	s.state.Projects[project.ID] = project
	return s.saveLocked()
}

func (s *Store) WorkflowTemplatesForProject(projectID string) []models.WorkflowTemplate {
	s.mu.Lock()
	defer s.mu.Unlock()
	var templates []models.WorkflowTemplate
	for _, template := range s.state.WorkflowTemplates {
		if template.ProjectID == projectID {
			templates = append(templates, template)
		}
	}
	sort.Slice(templates, func(i, j int) bool { return templates[i].CreatedAt.Before(templates[j].CreatedAt) })
	return templates
}

func (s *Store) GetWorkflowTemplate(id string) (models.WorkflowTemplate, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	template, ok := s.state.WorkflowTemplates[id]
	return template, ok
}

func (s *Store) UpdateWorkflowTemplate(template models.WorkflowTemplate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.state.WorkflowTemplates[template.ID]
	if !ok {
		return fmt.Errorf("workflow template not found")
	}
	template.ProjectID = current.ProjectID
	template.CreatedAt = current.CreatedAt
	template.UpdatedAt = time.Now().UTC()
	s.state.WorkflowTemplates[template.ID] = template
	return s.saveLocked()
}

func (s *Store) PlannerSessionsForProject(projectID string) []models.PlannerSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sessions []models.PlannerSession
	for _, session := range s.state.PlannerSessions {
		if session.ProjectID == projectID {
			sessions = append(sessions, session)
		}
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].CreatedAt.After(sessions[j].CreatedAt) })
	return sessions
}

func (s *Store) GetPlannerSession(id string) (models.PlannerSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.state.PlannerSessions[id]
	return session, ok
}

func (s *Store) CreatePlannerSession(projectID, prompt string) (models.PlannerSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	title := summarizeForStore(prompt)
	session := models.PlannerSession{
		ID:             newID("pln"),
		ProjectID:      projectID,
		Title:          title,
		Prompt:         strings.TrimSpace(prompt),
		Status:         models.PlannerStatusPlanning,
		DraftTitle:     title,
		DraftObjective: strings.TrimSpace(prompt),
		DraftFocus:     "Starter draft uses stored project context and your notes. Generate a planner/critic draft before approval when you want an agent-reviewed plan.",
		DraftBoundaries: "Respect project settings, excluded paths, secrets, generated assets, and any boundaries added in this planning session.",
		DraftDoneWhen:  "The plan is approved, a worker attempt is reviewed from fresh context, and final review accepts the result.",
		Assumptions:    []string{"Local runner is available for the prototype.", "A review-gated workflow is preferred unless changed before approval."},
		Risks:          []string{"Scope may need narrowing before approval.", "Generated files and secrets should remain out of bounds."},
			GraphPreview:   []string{"Prompt", "Planner agent", "Critic agent", "Approval", "Worker", "Fresh review", "Final review"},
		PlannerRevision: 1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	s.state.PlannerSessions[session.ID] = session
	userMessage := models.PlannerMessage{ID: newID("msg"), SessionID: session.ID, Role: "user", Body: session.Prompt, CreatedAt: now}
	plannerMessage := models.PlannerMessage{ID: newID("msg"), SessionID: session.ID, Role: "planner", Body: "I drafted a starter plan with assumptions, boundaries, and a review-gated chain. Generate a planner/critic draft, record follow-up notes here, or approve the draft when it looks right.", CreatedAt: now.Add(time.Millisecond)}
	s.state.PlannerMessages[userMessage.ID] = userMessage
	s.state.PlannerMessages[plannerMessage.ID] = plannerMessage
	return session, s.saveLocked()
}

func (s *Store) PlannerMessages(sessionID string) []models.PlannerMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	var messages []models.PlannerMessage
	for _, message := range s.state.PlannerMessages {
		if message.SessionID == sessionID {
			messages = append(messages, message)
		}
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].CreatedAt.Before(messages[j].CreatedAt) })
	return messages
}

func (s *Store) AppendPlannerMessage(sessionID, role, body string) (models.PlannerMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.state.PlannerSessions[sessionID]
	if !ok {
		return models.PlannerMessage{}, fmt.Errorf("planner session not found")
	}
	now := time.Now().UTC()
	message := models.PlannerMessage{ID: newID("msg"), SessionID: sessionID, Role: role, Body: strings.TrimSpace(body), CreatedAt: now}
	s.state.PlannerMessages[message.ID] = message
	session.UpdatedAt = now
	s.state.PlannerSessions[sessionID] = session
	return message, s.saveLocked()
}

func (s *Store) UpdatePlannerSession(session models.PlannerSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.state.PlannerSessions[session.ID]
	if !ok {
		return fmt.Errorf("planner session not found")
	}
	session.CreatedAt = current.CreatedAt
	session.UpdatedAt = time.Now().UTC()
	s.state.PlannerSessions[session.ID] = session
	return s.saveLocked()
}

func (s *Store) UpdatePlanningSession(session models.PlannerSession) (models.PlannerSession, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.state.PlannerSessions[session.ID]
	if !ok {
		return models.PlannerSession{}, false, fmt.Errorf("planner session not found")
	}
	if current.Status != models.PlannerStatusPlanning {
		return current, false, nil
	}
	session.CreatedAt = current.CreatedAt
	session.Status = current.Status
	session.PlannerRevision = current.PlannerRevision
	session.ActiveGenerationID = current.ActiveGenerationID
	session.ApprovedRunID = current.ApprovedRunID
	session.ApprovedTaskID = current.ApprovedTaskID
	session.UpdatedAt = time.Now().UTC()
	s.state.PlannerSessions[session.ID] = session
	return session, true, s.saveLocked()
}

func (s *Store) CreateManualRunTask(project models.Project, title, objective, focus, excludedPaths, acceptance string) (models.Run, models.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, task := s.createManualRunTaskLocked(time.Now().UTC(), project, title, objective, focus, excludedPaths, acceptance)
	return run, task, s.saveLocked()
}

func (s *Store) createManualRunTaskLocked(now time.Time, project models.Project, title, objective, focus, excludedPaths, acceptance string) (models.Run, models.Task) {
	run := models.Run{ID: newID("run"), ProjectID: project.ID, Title: title, Goal: objective, Status: models.RunStatusAwaitingApproval, CreatedAt: now, UpdatedAt: now}
	assignedRunner := s.localExecutionRunnerIDLocked(project.DefaultExecutorID)
	agentProfile := project.DefaultAgentProfile
	if agentProfile == "" {
		agentProfile = "pi-standard"
	}
	maxAttempts := project.RetryCount + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	task := models.Task{
		ID:                 newID("tsk"),
		RunID:              run.ID,
		ProjectID:          project.ID,
		AssignedExecutorID: assignedRunner,
		Title:              title,
		Objective:          objective,
		Focus:              strings.TrimSpace(focus),
		ExcludedPaths:      strings.TrimSpace(excludedPaths),
		AcceptanceCriteria: acceptance,
		Status:             models.TaskStatusDraft,
		RiskLevel:          "medium",
		Adapter:            agentProfile,
		Role:               models.AttemptKindWorker,
		MaxAttempts:        maxAttempts,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	s.state.Runs[run.ID] = run
	s.state.Tasks[task.ID] = task
	return run, task
}

func (s *Store) GetRun(id string) (models.Run, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.state.Runs[id]
	return run, ok
}

func (s *Store) GetTask(id string) (models.Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.state.Tasks[id]
	return task, ok
}

func (s *Store) TasksForRun(runID string) []models.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tasks []models.Task
	for _, task := range s.state.Tasks {
		if task.RunID == runID {
			tasks = append(tasks, task)
		}
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].CreatedAt.Before(tasks[j].CreatedAt) })
	return tasks
}

func (s *Store) RunsForProject(projectID string) []models.Run {
	s.mu.Lock()
	defer s.mu.Unlock()
	var runs []models.Run
	for _, run := range s.state.Runs {
		if run.ProjectID == projectID {
			runs = append(runs, run)
		}
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].CreatedAt.After(runs[j].CreatedAt) })
	return runs
}

func (s *Store) UpdateRun(run models.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run.UpdatedAt = time.Now().UTC()
	s.state.Runs[run.ID] = run
	return s.saveLocked()
}

func (s *Store) UpdateTask(task models.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task.UpdatedAt = time.Now().UTC()
	s.state.Tasks[task.ID] = task
	return s.saveLocked()
}

func (s *Store) GrantLease(taskID, executorID, reason string) (models.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	changed, events := s.expireLeasesLocked(now)
	if changed {
		if err := s.saveLocked(); err != nil {
			return models.Lease{}, err
		}
		s.notifyEventsLocked(events)
	}
	executor, ok := s.state.Executors[executorID]
	if !ok {
		return models.Lease{}, fmt.Errorf("runner %s not found", executorID)
	}
	if executor.Status != models.ExecutorStatusOnline {
		return models.Lease{}, fmt.Errorf("runner %s is %s", executorID, executor.Status)
	}
	maxSlots := executor.MaxSlots
	if maxSlots <= 0 {
		maxSlots = 1
	}
	activeSlots := 0
	for _, lease := range s.state.Leases {
		if !s.isLeaseActiveLocked(lease, now) {
			continue
		}
		if lease.TaskID == taskID && lease.ExecutorID == executorID {
			return lease, nil
		}
		if lease.TaskID == taskID && lease.ExecutorID != executorID {
			return models.Lease{}, fmt.Errorf("task already has an active run slot on runner %s", lease.ExecutorID)
		}
		if lease.ExecutorID == executorID {
			activeSlots++
		}
	}
	if activeSlots >= maxSlots {
		return models.Lease{}, fmt.Errorf("runner %s has no available run slots", executorID)
	}
	expiresAt := now.Add(defaultLeaseTTL)
	lease := models.Lease{ID: newID("lse"), TaskID: taskID, ExecutorID: executorID, Status: models.LeaseStatusActive, GrantedAt: now, ExpiresAt: &expiresAt, Reason: reason}
	s.state.Leases[lease.ID] = lease
	return lease, s.saveLocked()
}

func (s *Store) ReleaseLease(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.state.Leases[id]
	if !ok {
		return nil
	}
	now := time.Now().UTC()
	lease.Status = models.LeaseStatusReleased
	lease.ReleasedAt = &now
	s.state.Leases[id] = lease
	return s.saveLocked()
}

func (s *Store) expireLeasesLocked(now time.Time) (bool, []models.Event) {
	changed := false
	var events []models.Event
	for id, lease := range s.state.Leases {
		if lease.Status != models.LeaseStatusActive || lease.ExpiresAt == nil || now.Before(*lease.ExpiresAt) {
			continue
		}
		lease.Status = models.LeaseStatusExpired
		s.state.Leases[id] = lease
		changed = true
		task, ok := s.state.Tasks[lease.TaskID]
		if !ok || task.LeaseID != lease.ID || task.Status != models.TaskStatusRunning {
			continue
		}
		if attempt, ok := s.attemptForTaskNumberLocked(task.ID, task.Attempts); ok && attempt.Status == models.AttemptStatusRunning {
			attempt.Status = models.AttemptStatusExpired
			attempt.Summary = "Run slot expired before the attempt completed. Run a fix attempt to retry."
			attempt.UpdatedAt = now
			s.state.Attempts[attempt.ID] = attempt
		}
		task.LeaseID = ""
		task.Status = models.TaskStatusNeedsFix
		task.UpdatedAt = now
		s.state.Tasks[task.ID] = task
		if run, ok := s.state.Runs[task.RunID]; ok && run.Status == models.RunStatusRunning {
			run.Status = models.RunStatusNeedsFix
			run.UpdatedAt = now
			s.state.Runs[run.ID] = run
			event := s.appendEventLocked(models.Event{RunID: run.ID, TaskID: task.ID, ExecutorID: lease.ExecutorID, LeaseID: lease.ID, Type: models.EventLeaseExpired, ActorKind: models.ActorKindManager, ActorID: models.ActorKindManager, Summary: "Run slot expired; task is ready for a retry"})
			events = append(events, event)
		}
	}
	return changed, events
}

func (s *Store) isLeaseActiveLocked(lease models.Lease, now time.Time) bool {
	if lease.Status != models.LeaseStatusActive {
		return false
	}
	return lease.ExpiresAt == nil || now.Before(*lease.ExpiresAt)
}

func (s *Store) AddArtifact(artifact models.Artifact) error {
	_, err := s.SaveArtifact(artifact)
	return err
}

func (s *Store) SaveArtifact(artifact models.Artifact) (models.Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if artifact.ID == "" {
		artifact.ID = newID("art")
	}
	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = time.Now().UTC()
	}
	s.state.Artifacts[artifact.ID] = artifact
	return artifact, s.saveLocked()
}

func (s *Store) GetArtifact(id string) (models.Artifact, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	artifact, ok := s.state.Artifacts[id]
	return artifact, ok
}

func (s *Store) ArtifactsForTask(taskID string) []models.Artifact {
	s.mu.Lock()
	defer s.mu.Unlock()
	var artifacts []models.Artifact
	for _, artifact := range s.state.Artifacts {
		if artifact.TaskID == taskID {
			artifacts = append(artifacts, artifact)
		}
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].CreatedAt.Before(artifacts[j].CreatedAt) })
	return artifacts
}

func (s *Store) CheckpointArtifactsForTaskBeforeAttempt(taskID string, beforeAttemptNumber int) []models.Artifact {
	s.mu.Lock()
	defer s.mu.Unlock()
	var artifacts []models.Artifact
	for _, artifact := range s.state.Artifacts {
		if artifact.TaskID == taskID && artifact.Kind == models.ArtifactKindCheckpoint && artifact.Sensitivity == models.SensitivityInternal && artifact.AttemptNumber < beforeAttemptNumber {
			artifacts = append(artifacts, artifact)
		}
	}
	sort.Slice(artifacts, func(i, j int) bool {
		if artifacts[i].AttemptNumber == artifacts[j].AttemptNumber {
			return artifacts[i].CreatedAt.Before(artifacts[j].CreatedAt)
		}
		return artifacts[i].AttemptNumber < artifacts[j].AttemptNumber
	})
	return artifacts
}

func (s *Store) ArtifactsForTaskAttempt(taskID string, attemptNumber int) []models.Artifact {
	s.mu.Lock()
	defer s.mu.Unlock()
	var artifacts []models.Artifact
	for _, artifact := range s.state.Artifacts {
		if artifact.TaskID == taskID && artifact.AttemptNumber == attemptNumber {
			artifacts = append(artifacts, artifact)
		}
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].CreatedAt.Before(artifacts[j].CreatedAt) })
	return artifacts
}

func (s *Store) CreateAttempt(projectID, runID, taskID string, number int, kind, status, summary string) (models.Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, attempt := range s.state.Attempts {
		if attempt.TaskID == taskID && attempt.Number == number {
			if attempt.Status != models.AttemptStatusQueued && attempt.Status != models.AttemptStatusRequested {
				return models.Attempt{}, fmt.Errorf("attempt %d is already %s", number, attempt.Status)
			}
			attempt.Kind = kind
			attempt.Status = status
			attempt.Summary = strings.TrimSpace(summary)
			attempt.UpdatedAt = time.Now().UTC()
			s.state.Attempts[attempt.ID] = attempt
			return attempt, s.saveLocked()
		}
	}
	now := time.Now().UTC()
	attempt := models.Attempt{ID: newID("att"), ProjectID: projectID, RunID: runID, TaskID: taskID, Number: number, Kind: kind, Status: status, Summary: strings.TrimSpace(summary), CreatedAt: now, UpdatedAt: now}
	s.state.Attempts[attempt.ID] = attempt
	return attempt, s.saveLocked()
}

func (s *Store) UpdateAttempt(attempt models.Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.state.Attempts[attempt.ID]
	if !ok {
		return fmt.Errorf("attempt not found")
	}
	attempt.CreatedAt = current.CreatedAt
	attempt.UpdatedAt = time.Now().UTC()
	s.state.Attempts[attempt.ID] = attempt
	return s.saveLocked()
}

func (s *Store) AttemptStatus(attemptID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempt, ok := s.state.Attempts[attemptID]
	return attempt.Status, ok
}

func (s *Store) AttemptsForTask(taskID string) []models.Attempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	var attempts []models.Attempt
	for _, attempt := range s.state.Attempts {
		if attempt.TaskID == taskID {
			attempts = append(attempts, attempt)
		}
	}
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].Number < attempts[j].Number })
	return attempts
}

func (s *Store) SaveHandoff(handoff models.Handoff) (models.Handoff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if handoff.ID == "" {
		handoff.ID = newID("hnd")
		handoff.CreatedAt = now
	}
	if handoff.CreatedAt.IsZero() {
		handoff.CreatedAt = now
	}
	handoff.UpdatedAt = now
	s.state.Handoffs[handoff.ID] = handoff
	return handoff, s.saveLocked()
}

func (s *Store) GetHandoff(id string) (models.Handoff, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	handoff, ok := s.state.Handoffs[id]
	return handoff, ok
}

func (s *Store) UpdateHandoff(handoff models.Handoff) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.state.Handoffs[handoff.ID]
	if !ok {
		return fmt.Errorf("handoff not found")
	}
	handoff.CreatedAt = current.CreatedAt
	handoff.UpdatedAt = time.Now().UTC()
	s.state.Handoffs[handoff.ID] = handoff
	return s.saveLocked()
}

func (s *Store) HandoffsForTask(taskID string) []models.Handoff {
	s.mu.Lock()
	defer s.mu.Unlock()
	var handoffs []models.Handoff
	for _, handoff := range s.state.Handoffs {
		if handoff.TaskID == taskID {
			handoffs = append(handoffs, handoff)
		}
	}
	sort.Slice(handoffs, func(i, j int) bool { return handoffs[i].CreatedAt.After(handoffs[j].CreatedAt) })
	return handoffs
}

func (s *Store) AppendEvent(event models.Event) (models.Event, error) {
	s.mu.Lock()
	event = s.appendEventLocked(event)
	watchers := make([]chan models.Event, 0, len(s.watchers[event.RunID]))
	for ch := range s.watchers[event.RunID] {
		watchers = append(watchers, ch)
	}
	err := s.saveLocked()
	s.mu.Unlock()
	if err != nil {
		return event, err
	}
	for _, ch := range watchers {
		select {
		case ch <- event:
		default:
		}
	}
	return event, nil
}

func (s *Store) appendEventLocked(event models.Event) models.Event {
	if event.ID == "" {
		event.ID = newID("evt")
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	event.Sequence = s.nextSequenceLocked(event.RunID)
	s.state.Events = append(s.state.Events, event)
	return event
}

func (s *Store) notifyEventWatchersLocked(event models.Event) {
	for ch := range s.watchers[event.RunID] {
		select {
		case ch <- event:
		default:
		}
	}
}

func (s *Store) notifyEventsLocked(events []models.Event) {
	for _, event := range events {
		s.notifyEventWatchersLocked(event)
	}
}

func (s *Store) EventsForRun(runID string) []models.Event {
	return s.EventsForRunAfter(runID, 0)
}

func (s *Store) EventsForRunAfter(runID string, afterSequence int) []models.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eventsForRunAfterLocked(runID, afterSequence)
}

func (s *Store) eventsForRunAfterLocked(runID string, afterSequence int) []models.Event {
	var events []models.Event
	for _, event := range s.state.Events {
		if event.RunID == runID && event.Sequence > afterSequence {
			events = append(events, event)
		}
	}
	return events
}

func (s *Store) Subscribe(runID string) (<-chan models.Event, func()) {
	_, ch, unsubscribe := s.SubscribeWithSnapshot(runID)
	return ch, unsubscribe
}

func (s *Store) SubscribeWithSnapshot(runID string) ([]models.Event, <-chan models.Event, func()) {
	return s.SubscribeWithSnapshotAfter(runID, 0)
}

func (s *Store) SubscribeWithSnapshotAfter(runID string, afterSequence int) ([]models.Event, <-chan models.Event, func()) {
	ch := make(chan models.Event, 16)
	s.mu.Lock()
	if s.watchers[runID] == nil {
		s.watchers[runID] = map[chan models.Event]struct{}{}
	}
	s.watchers[runID][ch] = struct{}{}
	events := s.eventsForRunAfterLocked(runID, afterSequence)
	s.mu.Unlock()
	return events, ch, func() {
		s.mu.Lock()
		delete(s.watchers[runID], ch)
		s.mu.Unlock()
	}
}

func (s *Store) ProjectRunDir(projectID, runID string) string {
	return filepath.Join(s.root, "projects", projectID, "runs", runID)
}

func (s *Store) PlannerGenerationDir(projectID, sessionID, generationID string) string {
	return filepath.Join(s.root, "projects", projectID, "planner", sessionID, "generations", generationID)
}

func (s *Store) TaskDir(projectID, runID, taskID string) string {
	return filepath.Join(s.ProjectRunDir(projectID, runID), "tasks", taskID)
}

func (s *Store) AttemptDir(projectID, runID, taskID string, attempt int) string {
	return filepath.Join(s.TaskDir(projectID, runID, taskID), "attempts", fmt.Sprintf("%d", attempt))
}

func (s *Store) ensurePrototypeExecutorsLocked() {
	now := time.Now().UTC()
	defaults := []models.Executor{
		{
			ID: models.LocalExecutorID, Name: "Local all-in-one runner", Endpoint: "in-process", Status: models.ExecutorStatusOnline, Kind: models.ExecutorKindLocal,
			Description: "Runs manager and local runner together for the prototype.", ResourceClass: "local-machine", ContainerRuntime: "Podman preferred", RepoAvailability: "Project folders mounted locally", MaxSlots: 1,
			Capabilities: []string{"Manager + runner", "Podman preferred", "Pi agent planned", "Dry-run prototype"}, AgentProfiles: []string{"Pi standard", "Pi high-context", "Read-only scout"},
			Notes: "Only this runner performs actual prototype actions today.", LastSeenAt: now,
		},
		{
			ID: "homelab", Name: "Homelab runner", Endpoint: "https://homelab.runner.local", Status: models.ExecutorStatusOnline, Kind: models.ExecutorKindRemote,
			Description: "Mock always-on runner for larger background jobs and long reviews.", ResourceClass: "medium", ContainerRuntime: "Podman planned", RepoAvailability: "Repo must be cloned by runner", MaxSlots: 2,
			Capabilities: []string{"Remote runner preview", "Worktrees planned", "Safe handoff preview"}, AgentProfiles: []string{"Pi standard", "Pi reviewer"},
			Notes: "Prototype card only; no network calls are made.", LastSeenAt: now.Add(-2 * time.Minute),
		},
		{
			ID: "workstation", Name: "Workstation runner", Endpoint: "ssh://workstation.example", Status: models.ExecutorStatusOffline, Kind: models.ExecutorKindRemote,
			Description: "Mock high-power workstation for future parallel workers.", ResourceClass: "high", ContainerRuntime: "Docker fallback planned", RepoAvailability: "Repo availability unknown", MaxSlots: 4,
			Capabilities: []string{"Remote runner preview", "Parallel workers planned", "Optional GPU support"}, AgentProfiles: []string{"Pi high-context", "Critic", "Fresh reviewer"},
			Notes: "Shown to make offline/remote state tangible.", LastSeenAt: now.Add(-6 * time.Hour),
		},
	}
	for _, executor := range defaults {
		current, ok := s.state.Executors[executor.ID]
		if !ok {
			s.state.Executors[executor.ID] = executor
			continue
		}
		s.state.Executors[executor.ID] = mergeExecutorDefaults(current, executor)
	}
}

func (s *Store) localExecutionRunnerIDLocked(preferred string) string {
	if preferred != "" {
		if runner, ok := s.state.Executors[preferred]; ok && runner.Kind == models.ExecutorKindLocal {
			return preferred
		}
	}
	if runner, ok := s.state.Executors[models.LocalExecutorID]; ok && runner.Kind == models.ExecutorKindLocal {
		return runner.ID
	}
	for _, runner := range s.state.Executors {
		if runner.Kind == models.ExecutorKindLocal {
			return runner.ID
		}
	}
	return models.LocalExecutorID
}

func mergeExecutorDefaults(current, defaults models.Executor) models.Executor {
	if current.Name == "" {
		current.Name = defaults.Name
	}
	if current.Endpoint == "" {
		current.Endpoint = defaults.Endpoint
	}
	if current.Status == "" {
		current.Status = defaults.Status
	}
	if current.Kind == "" {
		current.Kind = defaults.Kind
	}
	if current.Description == "" {
		current.Description = defaults.Description
	}
	if current.ResourceClass == "" {
		current.ResourceClass = defaults.ResourceClass
	}
	if current.ContainerRuntime == "" {
		current.ContainerRuntime = defaults.ContainerRuntime
	}
	if current.RepoAvailability == "" {
		current.RepoAvailability = defaults.RepoAvailability
	}
	if current.MaxSlots <= 0 {
		current.MaxSlots = defaults.MaxSlots
	}
	if len(current.Capabilities) == 0 {
		current.Capabilities = defaults.Capabilities
	}
	if len(current.AgentProfiles) == 0 {
		current.AgentProfiles = defaults.AgentProfiles
	}
	if current.Notes == "" {
		current.Notes = defaults.Notes
	}
	if current.LastSeenAt.IsZero() {
		current.LastSeenAt = defaults.LastSeenAt
	}
	return current
}

func (s *Store) ensureProjectDefaultsLocked() {
	now := time.Now().UTC()
	for id, project := range s.state.Projects {
		changed := false
		if project.DefaultExecutorID == "" {
			project.DefaultExecutorID = s.localExecutionRunnerIDLocked("")
			changed = true
		}
		if !isWorkerAgentProfile(project.DefaultAgentProfile) {
			project.DefaultAgentProfile = "pi-standard"
			changed = true
		}
		if project.AgentContext == "" {
			project.AgentContext = project.Description
			changed = true
		}
		templates := s.workflowTemplatesForProjectLocked(project.ID)
		if len(templates) == 0 {
			for _, template := range defaultWorkflowTemplates(project.ID, now) {
				s.state.WorkflowTemplates[template.ID] = template
				if template.IsDefault {
					project.DefaultWorkflowTemplateID = template.ID
					changed = true
				}
			}
		} else if project.DefaultWorkflowTemplateID == "" {
			project.DefaultWorkflowTemplateID = templates[0].ID
			changed = true
		}
		if changed {
			project.UpdatedAt = now
			s.state.Projects[id] = project
		}
	}
}

func (s *Store) workflowTemplatesForProjectLocked(projectID string) []models.WorkflowTemplate {
	var templates []models.WorkflowTemplate
	for _, template := range s.state.WorkflowTemplates {
		if template.ProjectID == projectID {
			templates = append(templates, template)
		}
	}
	sort.Slice(templates, func(i, j int) bool { return templates[i].CreatedAt.Before(templates[j].CreatedAt) })
	return templates
}

func defaultWorkflowTemplates(projectID string, now time.Time) []models.WorkflowTemplate {
	return []models.WorkflowTemplate{
		{
			ID: projectID + "_standard", ProjectID: projectID, Name: "Standard implementation", Summary: "Planner → worker step → review artifact → final review", UseCase: "Default planning preview for normal feature work", ReviewLoops: 1, RetryCount: 1, IsDefault: true, CreatedAt: now, UpdatedAt: now,
			Steps: []models.WorkflowStep{{Name: "Prompt", Role: "intake", Description: "Capture the goal and project context."}, {Name: "Planner draft", Role: "planner", Description: "Draft assumptions, boundaries, and editable plan."}, {Name: "Approval", Role: "review", Description: "Approve before execution starts."}, {Name: "Worker", Role: "worker", Description: "Dry-run writes placeholder artifacts by default; experimental local-pi can use a guarded worktree/container path."}, {Name: "Fresh review", Role: "reviewer", Description: "Dry-run writes a placeholder review by default; experimental local-pi can run a guarded reviewer step."}, {Name: "Final review", Role: "review", Description: "Accept, request fix, retry, or block."}},
		},
		{
			ID: projectID + "_highrisk", ProjectID: projectID, Name: "High-risk change", Summary: "Planner → critic preview → worker step → review artifact → fix loop → final review", UseCase: "Planning preview for auth, data, infra, or security", ReviewLoops: 2, RetryCount: 2, CreatedAt: now.Add(time.Millisecond), UpdatedAt: now.Add(time.Millisecond),
			Steps: []models.WorkflowStep{{Name: "Prompt", Role: "intake", Description: "Capture goal, constraints, and risk."}, {Name: "Scout", Role: "scout", Description: "Planned repo inspection step; not active in the default dry-run path.", Optional: true}, {Name: "Planner draft", Role: "planner", Description: "Draft plan and boundaries."}, {Name: "Critic", Role: "critic", Description: "Planned risk critique step; not active in the default dry-run path."}, {Name: "Worker", Role: "worker", Description: "Dry-run writes placeholder artifacts by default; experimental local-pi can run a guarded worker."}, {Name: "Fresh review", Role: "reviewer", Description: "Dry-run writes placeholder review output by default; experimental local-pi can run a guarded reviewer."}, {Name: "Final review", Role: "review", Description: "Approve or request another pass."}},
		},
		{
			ID: projectID + "_scout", ProjectID: projectID, Name: "Quick scout", Summary: "Scout → summary → final review", UseCase: "Planning preview for read-only investigation", ReviewLoops: 0, RetryCount: 0, CreatedAt: now.Add(2*time.Millisecond), UpdatedAt: now.Add(2*time.Millisecond),
			Steps: []models.WorkflowStep{{Name: "Prompt", Role: "intake", Description: "Capture the question."}, {Name: "Scout", Role: "scout", Description: "Planned read-only inspection step; not active in dry-run execution."}, {Name: "Summary", Role: "writer", Description: "Planned concise findings step."}, {Name: "Final review", Role: "review", Description: "Decide next step."}},
		},
		{
			ID: projectID + "_custom", ProjectID: projectID, Name: "Custom chain", Summary: "Choose planned agents, loops, retries, and gates before task start", UseCase: "Editable planning metadata; execution uses dry-run by default with explicit experimental local-pi opt-in", ReviewLoops: 1, RetryCount: 1, CreatedAt: now.Add(3*time.Millisecond), UpdatedAt: now.Add(3*time.Millisecond),
			Steps: []models.WorkflowStep{{Name: "Prompt", Role: "intake", Description: "Capture the goal."}, {Name: "Planner draft", Role: "planner", Description: "Draft an editable plan."}, {Name: "Worker", Role: "worker", Description: "Preview selected agent profile; dry-run is default and experimental local-pi is opt-in."}, {Name: "Final review", Role: "review", Description: "Accept or request a fix."}},
		},
	}
}

func canonicalizeRepoPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("repo path is required")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	canonicalPath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonicalPath)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("repo path is not a directory")
	}
	if _, err := os.Stat(filepath.Join(canonicalPath, ".git")); err != nil {
		return "", fmt.Errorf("repo path must contain a .git directory or file")
	}
	return canonicalPath, nil
}

func summarizeForStore(prompt string) string {
	prompt = strings.Join(strings.Fields(prompt), " ")
	if len(prompt) <= 72 {
		return prompt
	}
	return prompt[:69] + "..."
}

func (s *Store) normalizeEventSequencesLocked() {
	nextByRun := map[string]int{}
	for i, event := range s.state.Events {
		if event.Sequence <= nextByRun[event.RunID] {
			event.Sequence = nextByRun[event.RunID] + 1
		}
		nextByRun[event.RunID] = event.Sequence
		s.state.Events[i] = event
	}
}

func (s *Store) nextSequenceLocked(runID string) int {
	seq := 0
	for _, event := range s.state.Events {
		if event.RunID == runID && event.Sequence > seq {
			seq = event.Sequence
		}
	}
	return seq + 1
}

func (s *Store) saveLocked() error {
	return s.saveSQLiteSnapshotLocked()
}

func newID(prefix string) string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}
