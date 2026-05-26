package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/artifacts"
	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/dispatcher"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/pathsafe"
	plannerexec "github.com/agent-parley/parley/internal/planner"
	"github.com/agent-parley/parley/internal/profiles"
	"github.com/agent-parley/parley/internal/store"
)

const plannerGenerationTimeout = 15 * time.Minute

type Dependencies struct {
	Config config.Config
	Store  *store.Store
	Logger *slog.Logger
	Dispatcher     *dispatcher.Dispatcher
	PlannerRunner plannerexec.Runner
}

type Server struct {
	cfg       config.Config
	store     *store.Store
	logger    *slog.Logger
	artifacts  *artifacts.Writer
	dispatcher     *dispatcher.Dispatcher
	plannerRunner plannerexec.Runner
	csrfToken     string
}

func New(deps Dependencies) http.Handler {
	artifactWriter := artifacts.NewWriter(deps.Store)
	if deps.Dispatcher == nil {
		panic("app dispatcher dependency is required")
	}
	s := &Server{cfg: deps.Config, store: deps.Store, logger: deps.Logger, artifacts: artifactWriter, dispatcher: deps.Dispatcher, plannerRunner: deps.PlannerRunner, csrfToken: newCSRFToken()}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.home)
	mux.HandleFunc("/projects", s.projects)
	mux.HandleFunc("/projects/", s.projectRoutes)
	mux.HandleFunc("/runners", s.runners)
	mux.HandleFunc("/runners/", s.runnerRoutes)
	mux.HandleFunc("/runs/", s.runRoutes)
	mux.HandleFunc("/tasks/", s.taskRoutes)
	mux.HandleFunc("/artifacts/", s.artifact)
	mux.HandleFunc("/static/app.css", s.css)
	return withSecurityHeaders(withHostAllowlist(withRequestSafety(mux, s.csrfToken), deps.Config.BindAddr))
}

func (s *Server) home(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.Redirect(w, r, "/projects", http.StatusSeeOther)
}

func (s *Server) projects(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.render(w, "Projects", projectsTemplate, map[string]any{
			"Executor": s.store.LocalExecutor(),
			"Projects": s.store.ListProjects(),
			"DataRoot": s.cfg.DataRoot,
		})
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			s.badRequest(w, err)
			return
		}
		repoPath, err := validateGitRepo(strings.TrimSpace(r.FormValue("repo_path")))
		if err != nil {
			s.render(w, "Projects", projectsTemplate, map[string]any{
				"Executor": s.store.LocalExecutor(),
				"Projects": s.store.ListProjects(),
				"DataRoot": s.cfg.DataRoot,
				"Error": err.Error(),
			})
			return
		}
		project, err := s.store.CreateProject(r.FormValue("name"), r.FormValue("description"), repoPath, r.FormValue("default_branch"))
		if err != nil {
			s.serverError(w, err)
			return
		}
		http.Redirect(w, r, "/projects/"+project.ID, http.StatusSeeOther)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) projectRoutes(w http.ResponseWriter, r *http.Request) {
	parts := splitPath(strings.TrimPrefix(r.URL.Path, "/projects/"))
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	project, ok := s.store.GetProject(parts[0])
	if !ok {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.render(w, project.Name, projectTemplate, map[string]any{
			"Project": project,
			"Runs": s.store.RunsForProject(project.ID),
			"Executor": s.store.LocalExecutor(),
			"Templates": s.store.WorkflowTemplatesForProject(project.ID),
			"Sessions": s.store.PlannerSessionsForProject(project.ID),
		})
		return
	}
	if len(parts) == 2 && parts[1] == "settings" {
		s.projectSettings(w, r, project)
		return
	}
	if len(parts) == 3 && parts[1] == "templates" {
		s.workflowTemplate(w, r, project, parts[2])
		return
	}
	if len(parts) == 3 && parts[1] == "tasks" && parts[2] == "new" {
		s.newTask(w, r, project)
		return
	}
	if len(parts) >= 2 && parts[1] == "planner" {
		s.planner(w, r, project, parts[2:])
		return
	}
	http.NotFound(w, r)
}

func (s *Server) projectSettings(w http.ResponseWriter, r *http.Request, project models.Project) {
	switch r.Method {
	case http.MethodGet:
		s.render(w, "Project settings", projectSettingsTemplate, s.projectSettingsData(project, ""))
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			s.badRequest(w, err)
			return
		}
		project.Description = strings.TrimSpace(r.FormValue("description"))
		project.AgentContext = strings.TrimSpace(r.FormValue("agent_context"))
		defaultRunner := strings.TrimSpace(r.FormValue("default_runner"))
		if defaultRunner == "" {
			defaultRunner = models.LocalExecutorID
		}
		runner, ok := s.store.GetExecutor(defaultRunner)
		if !ok {
			s.render(w, "Project settings", projectSettingsTemplate, s.projectSettingsData(project, "choose a known runner"))
			return
		}
		if runner.Kind != models.ExecutorKindLocal {
			s.render(w, "Project settings", projectSettingsTemplate, s.projectSettingsData(project, "choose a local execution runner until remote runner APIs exist"))
			return
		}
		if runner.Status != models.ExecutorStatusOnline {
			s.render(w, "Project settings", projectSettingsTemplate, s.projectSettingsData(project, "choose an online runner"))
			return
		}
		project.DefaultExecutorID = defaultRunner
		project.DefaultAgentProfile = strings.TrimSpace(r.FormValue("agent_profile"))
		if !isAllowedAgentProfile(project.DefaultAgentProfile) {
			s.render(w, "Project settings", projectSettingsTemplate, s.projectSettingsData(project, fmt.Sprintf("unsupported agent profile %q", project.DefaultAgentProfile)))
			return
		}
		project.DefaultWorkflowTemplateID = strings.TrimSpace(r.FormValue("workflow_template"))
		project.ReviewLoopCount = parseSmallInt(r.FormValue("review_loops"), 1)
		project.RetryCount = parseSmallInt(r.FormValue("retry_count"), 1)
		if err := s.store.UpdateProjectSettings(project); err != nil {
			s.serverError(w, err)
			return
		}
		http.Redirect(w, r, "/projects/"+project.ID+"/settings", http.StatusSeeOther)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) projectSettingsData(project models.Project, errorMessage string) map[string]any {
	data := map[string]any{"Project": project, "Templates": s.store.WorkflowTemplatesForProject(project.ID), "Executor": s.store.LocalExecutor(), "Runners": s.store.ListExecutors(), "ExecutionRunnerID": project.DefaultExecutorID, "AgentProfiles": profiles.WorkerDefaultIDs()}
	if errorMessage != "" {
		data["Error"] = errorMessage
	}
	return data
}

func isAllowedAgentProfile(profile string) bool {
	return profiles.IsWorkerDefault(profile)
}

func (s *Server) workflowTemplate(w http.ResponseWriter, r *http.Request, project models.Project, templateID string) {
	tmpl, ok := s.store.GetWorkflowTemplate(templateID)
	if !ok || tmpl.ProjectID != project.ID {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.render(w, tmpl.Name, workflowTemplateTemplate, map[string]any{"Project": project, "Template": tmpl})
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			s.badRequest(w, err)
			return
		}
		tmpl.Name = strings.TrimSpace(r.FormValue("name"))
		tmpl.Summary = strings.TrimSpace(r.FormValue("summary"))
		tmpl.UseCase = strings.TrimSpace(r.FormValue("use_case"))
		tmpl.ReviewLoops = parseSmallInt(r.FormValue("review_loops"), tmpl.ReviewLoops)
		tmpl.RetryCount = parseSmallInt(r.FormValue("retry_count"), tmpl.RetryCount)
		for i := range tmpl.Steps {
			prefix := fmt.Sprintf("step_%d_", i)
			tmpl.Steps[i].Name = strings.TrimSpace(r.FormValue(prefix + "name"))
			tmpl.Steps[i].Role = strings.TrimSpace(r.FormValue(prefix + "role"))
			tmpl.Steps[i].Description = strings.TrimSpace(r.FormValue(prefix + "description"))
			tmpl.Steps[i].Optional = r.FormValue(prefix+"optional") == "on"
		}
		if r.FormValue("make_default") == "on" {
			project.DefaultWorkflowTemplateID = tmpl.ID
			if err := s.store.UpdateProjectSettings(project); err != nil {
				s.serverError(w, err)
				return
			}
		}
		if err := s.store.UpdateWorkflowTemplate(tmpl); err != nil {
			s.serverError(w, err)
			return
		}
		http.Redirect(w, r, "/projects/"+project.ID+"/templates/"+tmpl.ID, http.StatusSeeOther)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) planner(w http.ResponseWriter, r *http.Request, project models.Project, parts []string) {
	if len(parts) == 0 {
		s.plannerStart(w, r, project)
		return
	}
	session, ok := s.store.GetPlannerSession(parts[0])
	if !ok || session.ProjectID != project.ID {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		generations := s.store.PlannerGenerationsForSession(session.ID)
		diagnostics := s.store.PlannerDiagnosticsForSession(session.ID)
		s.render(w, session.Title, plannerSessionTemplate, map[string]any{"Project": project, "Session": session, "Messages": s.store.PlannerMessages(session.ID), "Executor": s.store.LocalExecutor(), "Generations": generations, "GenerationViews": plannerGenerationViews(generations, diagnostics), "GenerationRunning": plannerGenerationRunning(generations)})
		return
	}
	if len(parts) == 3 && parts[1] == "diagnostics" && r.Method == http.MethodGet {
		s.plannerDiagnostic(w, r, project, session, parts[2])
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		s.plannerSessionAction(w, r, project, session, parts[1])
		return
	}
	http.NotFound(w, r)
}

func (s *Server) plannerStart(w http.ResponseWriter, r *http.Request, project models.Project) {
	switch r.Method {
	case http.MethodGet:
		s.render(w, "Planner", plannerTemplate, map[string]any{"Project": project, "Executor": s.store.LocalExecutor(), "Sessions": s.store.PlannerSessionsForProject(project.ID)})
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			s.badRequest(w, err)
			return
		}
		prompt := strings.TrimSpace(r.FormValue("prompt"))
		if prompt == "" {
			s.render(w, "Planner", plannerTemplate, map[string]any{"Project": project, "Executor": s.store.LocalExecutor(), "Sessions": s.store.PlannerSessionsForProject(project.ID), "Error": "describe what you want Parley to plan"})
			return
		}
		session, err := s.store.CreatePlannerSession(project.ID, prompt)
		if err != nil {
			s.serverError(w, err)
			return
		}
		http.Redirect(w, r, "/projects/"+project.ID+"/planner/"+session.ID, http.StatusSeeOther)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) plannerSessionAction(w http.ResponseWriter, r *http.Request, project models.Project, session models.PlannerSession, action string) {
	if err := r.ParseForm(); err != nil {
		s.badRequest(w, err)
		return
	}
	switch action {
	case "messages":
		if err := s.store.AddPlannerReply(session.ID, r.FormValue("message")); err != nil {
			s.serverError(w, err)
			return
		}
	case "revise":
		if err := s.store.RevisePlannerSession(session.ID); err != nil {
			s.serverError(w, err)
			return
		}
	case "run-agents":
		if err := s.runPlannerAgents(r.Context(), project, session); err != nil {
			s.serverError(w, err)
			return
		}
	case "approve":
		result, err := s.store.ApprovePlannerSession(project, session.ID)
		if err != nil {
			s.serverError(w, err)
			return
		}
		if err := s.ensureContractArtifacts(project, result.Run, result.Task); err != nil {
			s.serverError(w, err)
			return
		}
		if result.Created || !s.hasEvent(result.Run.ID, models.EventPlannerPromptReceived) {
			s.emit(result.Run.ID, result.Task.ID, models.EventPlannerPromptReceived, models.ActorKindManager, models.ActorKindManager, "Planner prompt captured", map[string]any{"mode": "interactive-prototype", "session_id": result.Session.ID})
		}
		if result.Created || !s.hasEvent(result.Run.ID, models.EventPlannerDraftCreated) {
			s.emit(result.Run.ID, result.Task.ID, models.EventPlannerDraftCreated, models.ActorKindManager, models.ActorKindManager, "Interactive planner session approved a task plan", nil)
		}
		http.Redirect(w, r, "/tasks/"+result.Task.ID, http.StatusSeeOther)
		return
	case "dismiss":
		if _, _, err := s.store.DismissPlannerSession(session.ID); err != nil {
			s.serverError(w, err)
			return
		}
		http.Redirect(w, r, "/projects/"+project.ID, http.StatusSeeOther)
		return
	default:
		http.NotFound(w, r)
		return
	}
	http.Redirect(w, r, "/projects/"+project.ID+"/planner/"+session.ID, http.StatusSeeOther)
}

func (s *Server) runPlannerAgents(_ context.Context, project models.Project, session models.PlannerSession) error {
	if session.Status != models.PlannerStatusPlanning {
		return fmt.Errorf("planner session is %s", session.Status)
	}
	generation, startedSession, err := s.store.BeginPlannerGeneration(session.ID, plannerModeForConfig(s.cfg), profiles.ProfilePlanner, profiles.ProfileCritic)
	if err != nil {
		if strings.Contains(err.Error(), "already") {
			_, _ = s.store.AppendPlannerMessage(session.ID, "planner", "Planner/critic generation is already running for this session. Refresh this page to inspect progress.")
			return nil
		}
		return err
	}
	messages := s.store.PlannerMessages(session.ID)
	input := plannerexec.Input{Project: project, Session: startedSession, Messages: messages}
	if _, err := s.store.AppendPlannerMessage(session.ID, "planner", fmt.Sprintf("Planner/critic generation %s started in %s mode. Refresh this page to inspect progress; task creation and worker execution remain approval-gated.", generation.ID, generation.Mode)); err != nil && s.logger != nil {
		s.logger.Error("failed to append planner generation start message", "generation_id", generation.ID, "session_id", session.ID, "error", err)
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), plannerGenerationTimeout)
		defer cancel()
		if err := s.runPlannerGeneration(ctx, generation, input); err != nil && s.logger != nil {
			s.logger.Error("planner/critic generation update failed", "generation_id", generation.ID, "session_id", session.ID, "error", err)
		}
	}()
	return nil
}

func (s *Server) runPlannerGeneration(ctx context.Context, generation models.PlannerGeneration, input plannerexec.Input) error {
	runner := s.activePlannerRunner()
	var result plannerexec.Result
	var runErr error
	if preflight, ok := runner.(plannerexec.PreflightRunner); ok {
		if err := preflight.Preflight(ctx, input); err != nil {
			result = plannerexec.Result{Mode: plannerModeForConfig(s.cfg), PlannerProfile: profiles.ProfilePlanner, CriticProfile: profiles.ProfileCritic, Summary: "Planner/critic readiness check failed before approval.", Diagnostics: []plannerexec.Diagnostic{{Name: "preflight-error.txt", Kind: models.PlannerDiagnosticKindError, Body: err.Error()}}}
			runErr = fmt.Errorf("planner preflight failed: %w", err)
		} else {
			result, runErr = runner.Run(ctx, input)
		}
	} else {
		result, runErr = runner.Run(ctx, input)
	}
	status := models.PlannerGenerationStatusCompleted
	agentStatus := models.PlannerAgentStatusCompleted
	diagnostic := ""
	if runErr != nil {
		status = models.PlannerGenerationStatusFailed
		agentStatus = models.PlannerAgentStatusFailed
		diagnostic = plannerFailureDiagnostic(runErr)
		result.Diagnostics = append(result.Diagnostics, plannerexec.Diagnostic{Name: "generation-error.txt", Kind: models.PlannerDiagnosticKindError, Body: runErr.Error()})
		if s.logger != nil {
			s.logger.Error("planner/critic generation failed", "generation_id", generation.ID, "session_id", input.Session.ID, "error", runErr)
		}
		if strings.TrimSpace(result.Summary) == "" {
			result.Summary = "Planner/critic agent execution failed before approval."
		}
		if diagnostic != "" && !strings.Contains(result.Summary, diagnostic) {
			result.Summary = strings.TrimSpace(result.Summary) + " " + diagnostic
		}
	}
	s.writePlannerDiagnostics(generation, result.Diagnostics)
	updated := applyPlannerExecutionResult(input.Session, result, agentStatus, plannerModeForConfig(s.cfg))
	stored, completedGeneration, applied, err := s.store.CompletePlannerGeneration(generation.ID, updated, status, updated.AgentSummary, diagnostic)
	if err != nil {
		return err
	}
	if !applied {
		if _, err := s.store.AppendPlannerMessage(input.Session.ID, "planner", completedGeneration.Summary); err != nil {
			return err
		}
		return nil
	}
	if agentStatus == models.PlannerAgentStatusCompleted {
		if _, err := s.store.AppendPlannerMessage(input.Session.ID, "planner", plannerDraftSnapshot(stored)); err != nil {
			return err
		}
	}
	if strings.TrimSpace(result.PlannerMessage) != "" {
		if _, err := s.store.AppendPlannerMessage(input.Session.ID, "planner", result.PlannerMessage); err != nil {
			return err
		}
	}
	if strings.TrimSpace(result.CriticMessage) != "" {
		if _, err := s.store.AppendPlannerMessage(input.Session.ID, "critic", result.CriticMessage); err != nil {
			return err
		}
	}
	if runErr != nil {
		body := "Planner/critic execution failed before approval. " + plannerFailureDiagnostic(runErr) + " The task approval gate remains closed until you approve a draft manually or regenerate the planner/critic draft successfully."
		if _, err := s.store.AppendPlannerMessage(input.Session.ID, "planner", body); err != nil {
			return err
		}
	}
	return nil
}

func plannerDraftSnapshot(session models.PlannerSession) string {
	return fmt.Sprintf("Generated planner draft snapshot:\nTitle: %s\nObjective: %s\nFocus: %s\nBoundaries: %s\nDone when: %s\nAssumptions: %s\nRisks: %s\nGraph: %s", session.DraftTitle, session.DraftObjective, session.DraftFocus, session.DraftBoundaries, session.DraftDoneWhen, strings.Join(session.Assumptions, "; "), strings.Join(session.Risks, "; "), strings.Join(session.GraphPreview, " → "))
}

func (s *Server) writePlannerDiagnostics(generation models.PlannerGeneration, diagnostics []plannerexec.Diagnostic) {
	for _, diagnostic := range diagnostics {
		if strings.TrimSpace(diagnostic.Body) == "" {
			continue
		}
		if err := s.writePlannerDiagnostic(generation, diagnostic); err != nil && s.logger != nil {
			s.logger.Error("failed to save planner diagnostic", "generation_id", generation.ID, "error", err)
		}
	}
}

func (s *Server) writePlannerDiagnostic(generation models.PlannerGeneration, diagnostic plannerexec.Diagnostic) error {
	root, err := filepath.Abs(s.store.DataRoot())
	if err != nil {
		return err
	}
	dir := s.store.PlannerGenerationDir(generation.ProjectID, generation.SessionID, generation.ID)
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if rel, err := filepath.Rel(root, absDir); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("planner diagnostic directory is outside the data root")
	}
	if err := pathsafe.MkdirAllNoSymlink(absDir, 0o700); err != nil {
		return err
	}
	name := plannerDiagnosticFileName(diagnostic.Name, diagnostic.Kind)
	path := filepath.Join(absDir, name)
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if rel, err := filepath.Rel(root, absPath); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("planner diagnostic path is outside the data root")
	}
	body := []byte(diagnostic.Body)
	if err := pathsafe.WriteFileNoFollow(absPath, body, 0o600); err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	_, err = s.store.SavePlannerDiagnostic(models.PlannerDiagnostic{ProjectID: generation.ProjectID, SessionID: generation.SessionID, GenerationID: generation.ID, Kind: firstNonEmpty(diagnostic.Kind, models.PlannerDiagnosticKindTrace), Path: absPath, MediaType: "text/plain; charset=utf-8", Sensitivity: models.SensitivityInternal, SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:]), CreatedAt: time.Now().UTC()})
	return err
}

func plannerDiagnosticFileName(name, kind string) string {
	name = strings.TrimSpace(filepath.ToSlash(name))
	if name == "" {
		name = strings.TrimSpace(kind) + ".txt"
	}
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.Trim(name, " .")
	if name == "" || name == "." || name == ".." {
		return "planner-diagnostic.txt"
	}
	return name
}

func plannerFailureDiagnostic(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "Diagnostic: planner/critic execution was canceled."
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "Diagnostic: planner/critic execution timed out."
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "json"):
		return "Diagnostic: an agent returned invalid structured JSON."
	case strings.Contains(message, "image"):
		return "Diagnostic: the configured Pi container image is unavailable or invalid."
	case strings.Contains(message, "podman") || strings.Contains(message, "container"):
		return "Diagnostic: the local container runtime is unavailable or rejected the invocation."
	case strings.Contains(message, "profile") || strings.Contains(message, "command"):
		return "Diagnostic: a planner/critic agent profile is misconfigured."
	case strings.Contains(message, "worktree") || strings.Contains(message, "git"):
		return "Diagnostic: the managed planner worktree could not be prepared."
	case strings.Contains(message, "repo") || strings.Contains(message, "path"):
		return "Diagnostic: the project repository could not be prepared for planner inspection."
	default:
		return "Diagnostic: planner/critic execution failed; check server logs for local-only details."
	}
}

func (s *Server) activePlannerRunner() plannerexec.Runner {
	if s.plannerRunner != nil {
		return s.plannerRunner
	}
	return plannerexec.NewDryRunRunner()
}

func plannerModeForConfig(cfg config.Config) string {
	if cfg.ExecutionMode == config.ExecutionModeLocalPi {
		return plannerexec.ModeLocalPi
	}
	return plannerexec.ModeDryRun
}

func applyPlannerExecutionResult(session models.PlannerSession, result plannerexec.Result, status, fallbackMode string) models.PlannerSession {
	draft := result.Draft
	if strings.TrimSpace(draft.Title) != "" {
		session.DraftTitle = strings.TrimSpace(draft.Title)
	}
	if strings.TrimSpace(draft.Objective) != "" {
		session.DraftObjective = strings.TrimSpace(draft.Objective)
	}
	if strings.TrimSpace(draft.Focus) != "" {
		session.DraftFocus = strings.TrimSpace(draft.Focus)
	}
	if strings.TrimSpace(draft.Boundaries) != "" {
		session.DraftBoundaries = strings.TrimSpace(draft.Boundaries)
	}
	if strings.TrimSpace(draft.DoneWhen) != "" {
		session.DraftDoneWhen = strings.TrimSpace(draft.DoneWhen)
	}
	if len(draft.Assumptions) > 0 {
		session.Assumptions = append([]string(nil), draft.Assumptions...)
	}
	if len(draft.Risks) > 0 {
		session.Risks = append([]string(nil), draft.Risks...)
	}
	if len(draft.GraphPreview) > 0 {
		session.GraphPreview = append([]string(nil), draft.GraphPreview...)
	}
	session.AgentMode = firstNonEmpty(result.Mode, fallbackMode, plannerexec.ModeDryRun)
	session.AgentStatus = status
	session.AgentSummary = strings.TrimSpace(result.Summary)
	session.PlannerProfile = strings.TrimSpace(result.PlannerProfile)
	session.CriticProfile = strings.TrimSpace(result.CriticProfile)
	now := time.Now().UTC()
	session.AgentExecutedAt = &now
	return session
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (s *Server) newTask(w http.ResponseWriter, r *http.Request, project models.Project) {
	switch r.Method {
	case http.MethodGet:
		s.render(w, "New task", newTaskTemplate, map[string]any{"Project": project})
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			s.badRequest(w, err)
			return
		}
		title := strings.TrimSpace(r.FormValue("title"))
		objective := strings.TrimSpace(r.FormValue("objective"))
		if title == "" || objective == "" {
			s.render(w, "New task", newTaskTemplate, map[string]any{"Project": project, "Error": "title and objective are required"})
			return
		}
		run, task, err := s.store.CreateManualRunTask(project, title, objective, r.FormValue("focus"), r.FormValue("excluded_paths"), r.FormValue("acceptance"))
		if err != nil {
			s.serverError(w, err)
			return
		}
		if err := s.ensureContractArtifacts(project, run, task); err != nil {
			s.serverError(w, err)
			return
		}
		s.emit(run.ID, task.ID, models.EventTaskPlanCreated, models.ActorKindManager, models.ActorKindManager, "Draft task plan created", nil)
		http.Redirect(w, r, "/tasks/"+task.ID, http.StatusSeeOther)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) runRoutes(w http.ResponseWriter, r *http.Request) {
	parts := splitPath(strings.TrimPrefix(r.URL.Path, "/runs/"))
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	run, ok := s.store.GetRun(parts[0])
	if !ok {
		http.NotFound(w, r)
		return
	}
	if (len(parts) == 2 && parts[1] == "events") || (len(parts) == 3 && parts[1] == "events" && parts[2] == "stream") {
		s.eventsStream(w, r, run.ID)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		project, _ := s.store.GetProject(run.ProjectID)
		s.render(w, run.Title, runTemplate, map[string]any{
			"Run": run,
			"Project": project,
			"Tasks": s.store.TasksForRun(run.ID),
			"Events": s.store.EventsForRun(run.ID),
		})
		return
	}
	http.NotFound(w, r)
}

func (s *Server) taskRoutes(w http.ResponseWriter, r *http.Request) {
	parts := splitPath(strings.TrimPrefix(r.URL.Path, "/tasks/"))
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	task, ok := s.store.GetTask(parts[0])
	if !ok {
		http.NotFound(w, r)
		return
	}
	run, _ := s.store.GetRun(task.RunID)
	project, _ := s.store.GetProject(task.ProjectID)
	if len(parts) == 1 && r.Method == http.MethodGet {
		artifacts := s.store.ArtifactsForTask(task.ID)
		events := s.store.EventsForRun(run.ID)
		attempts := attemptTimeline(task, s.store.AttemptsForTask(task.ID), artifacts)
		s.render(w, task.Title, taskTemplate, map[string]any{
			"Task": task,
			"Run": run,
			"Project": project,
			"Artifacts": artifacts,
			"Events": events,
			"Attempts": attempts,
			"Handoffs": s.store.HandoffsForTask(task.ID),
			"RunnerNames": s.runnerNames(),
			"Chain": chainForTask(task, artifacts, events),
			"DiffArtifacts": visibleDiffArtifacts(artifacts),
			"Diagnostics": internalDiagnosticArtifacts(artifacts),
		})
		return
	}
	if len(parts) == 3 && parts[1] == "diagnostics" {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.taskDiagnosticArtifact(w, r, task, parts[2])
		return
	}
	if len(parts) >= 2 && parts[1] == "handoff" {
		s.taskHandoff(w, r, project, run, task, parts[2:])
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		switch parts[1] {
		case "approve":
			if err := s.dispatcher.Enqueue(r.Context(), task.ID); err != nil {
				s.logAttemptQueueError(task.ID, err)
				s.renderTask(w, task.ID, "Attempt could not be queued. Check activity and server logs for details.")
				return
			}
		case "accept":
			updatedRun, updatedTask, err := s.store.AcceptTask(task.ID)
			if err != nil {
				s.serverError(w, err)
				return
			}
			s.emit(updatedRun.ID, updatedTask.ID, models.EventTaskCompleted, models.ActorKindUser, models.ActorKindUser, "Task accepted by user", nil)
		case "request-fix":
			updatedRun, updatedTask, _, err := s.store.RequestFix(task.ID)
			if err != nil {
				s.serverError(w, err)
				return
			}
			s.emit(updatedRun.ID, updatedTask.ID, models.EventTaskStateChanged, models.ActorKindUser, models.ActorKindUser, "Fix requested; queue the next attempt when ready", map[string]any{"assigned_runner": updatedTask.AssignedExecutorID})
		case "resume-fix":
			if err := s.dispatcher.Enqueue(r.Context(), task.ID); err != nil {
				s.logAttemptQueueError(task.ID, err)
				s.renderTask(w, task.ID, "Attempt could not be queued. Check activity and server logs for details.")
				return
			}
		default:
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/tasks/"+task.ID, http.StatusSeeOther)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) logAttemptQueueError(taskID string, err error) {
	if s.logger != nil {
		s.logger.Error("attempt queue failed", "task_id", taskID, "error", err)
	}
}

func (s *Server) renderTask(w http.ResponseWriter, taskID, errorMessage string) {
	task, ok := s.store.GetTask(taskID)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	run, _ := s.store.GetRun(task.RunID)
	project, _ := s.store.GetProject(task.ProjectID)
	artifacts := s.store.ArtifactsForTask(task.ID)
	events := s.store.EventsForRun(run.ID)
	attempts := attemptTimeline(task, s.store.AttemptsForTask(task.ID), artifacts)
	data := map[string]any{"Task": task, "Run": run, "Project": project, "Artifacts": artifacts, "Events": events, "Attempts": attempts, "Handoffs": s.store.HandoffsForTask(task.ID), "RunnerNames": s.runnerNames(), "Chain": chainForTask(task, artifacts, events), "DiffArtifacts": visibleDiffArtifacts(artifacts), "Diagnostics": internalDiagnosticArtifacts(artifacts)}
	if errorMessage != "" {
		data["Error"] = errorMessage
	}
	s.render(w, task.Title, taskTemplate, data)
}

func (s *Server) redirectIfAlreadyProgressed(w http.ResponseWriter, r *http.Request, taskID string) bool {
	task, ok := s.store.GetTask(taskID)
	if !ok {
		return false
	}
	switch task.Status {
	case models.TaskStatusQueued, models.TaskStatusRunning, models.TaskStatusAwaitingReview, models.TaskStatusDone, models.TaskStatusFailed:
		http.Redirect(w, r, "/tasks/"+taskID, http.StatusSeeOther)
		return true
	default:
		return false
	}
}

func (s *Server) artifact(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/artifacts/")
	artifact, ok := s.store.GetArtifact(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if artifact.Sensitivity != "" && artifact.Sensitivity != models.SensitivityNormal {
		http.Error(w, "artifact is not available for browser preview", http.StatusForbidden)
		return
	}
	s.serveArtifactBody(w, artifact)
}

func (s *Server) plannerDiagnostic(w http.ResponseWriter, r *http.Request, project models.Project, session models.PlannerSession, diagnosticID string) {
	diagnostic, ok := s.store.GetPlannerDiagnostic(diagnosticID)
	if !ok || diagnostic.ProjectID != project.ID || diagnostic.SessionID != session.ID {
		http.NotFound(w, r)
		return
	}
	if diagnostic.Sensitivity != models.SensitivityInternal {
		http.Error(w, "planner diagnostic is not available on this route", http.StatusForbidden)
		return
	}
	s.serveDataRootPath(w, diagnostic.Path)
}

func (s *Server) taskDiagnosticArtifact(w http.ResponseWriter, r *http.Request, task models.Task, artifactID string) {
	artifact, ok := s.store.GetArtifact(artifactID)
	if !ok || artifact.TaskID != task.ID {
		http.NotFound(w, r)
		return
	}
	if !isDiagnosticArtifact(artifact) {
		http.Error(w, "diagnostic artifact is not available on this route", http.StatusForbidden)
		return
	}
	s.serveArtifactBody(w, artifact)
}

func (s *Server) serveArtifactBody(w http.ResponseWriter, artifact models.Artifact) {
	s.serveDataRootPath(w, artifact.Path)
}

func (s *Server) serveDataRootPath(w http.ResponseWriter, path string) {
	root, err := filepath.EvalSymlinks(s.store.DataRoot())
	if err != nil {
		s.serverError(w, err)
		return
	}
	root, err = filepath.Abs(root)
	if err != nil {
		s.serverError(w, err)
		return
	}
	artifactPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		s.serverError(w, err)
		return
	}
	artifactPath, err = filepath.Abs(artifactPath)
	if err != nil {
		s.serverError(w, err)
		return
	}
	if rel, err := filepath.Rel(root, artifactPath); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		http.Error(w, "artifact path is outside the data root", http.StatusForbidden)
		return
	}
	data, err := pathsafe.ReadFileNoFollow(artifactPath)
	if err != nil {
		http.Error(w, "artifact preview is not available on this platform", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) eventsStream(w http.ResponseWriter, r *http.Request, runID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	lastSequence := parseLastEventID(r.Header.Get("Last-Event-ID"))
	events, ch, unsubscribe := s.store.SubscribeWithSnapshotAfter(runID, lastSequence)
	for _, event := range events {
		writeSSE(w, event)
		if event.Sequence > lastSequence {
			lastSequence = event.Sequence
		}
	}
	flusher.Flush()
	defer unsubscribe()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ch:
			for _, event := range s.store.EventsForRunAfter(runID, lastSequence) {
				writeSSE(w, event)
				if event.Sequence > lastSequence {
					lastSequence = event.Sequence
				}
			}
			flusher.Flush()
		}
	}
}

func (s *Server) ensureContractArtifacts(project models.Project, run models.Run, task models.Task) error {
	hasYAML := false
	hasMarkdown := false
	for _, artifact := range s.store.ArtifactsForTask(task.ID) {
		switch artifactName(artifact.Path) {
		case "plan.v1.yaml":
			hasYAML = true
		case "plan.v1.md":
			hasMarkdown = true
		}
	}
	if hasYAML && hasMarkdown {
		return nil
	}
	return s.writeContractArtifacts(project, run, task, !hasYAML, !hasMarkdown)
}

func (s *Server) writeContractArtifacts(project models.Project, run models.Run, task models.Task, writeYAML, writeMarkdown bool) error {
	dir := s.store.TaskDir(project.ID, run.ID, task.ID)
	plan := fmt.Sprintf("title: %q\nobjective: %q\nproject_description: %q\nfocus: %q\nexcluded_paths: %q\ndone_when: %q\nagent_profile: %q\nrunner_id: %q\n", task.Title, task.Objective, project.Description, task.Focus, task.ExcludedPaths, task.AcceptanceCriteria, task.Adapter, task.AssignedExecutorID)
	markdown := fmt.Sprintf("# %s\n\n## Objective\n\n%s\n\n## Project context\n\n%s\n\n## Focus\n\n%s\n\n## Boundaries\n\n%s\n\n## Done when\n\n%s\n", task.Title, task.Objective, project.Description, task.Focus, task.ExcludedPaths, task.AcceptanceCriteria)
	if writeYAML {
		if _, err := s.artifacts.Write(run.ID, task.ID, dir, "plan.v1.yaml", models.ArtifactKindPlan, plan); err != nil {
			return err
		}
	}
	if writeMarkdown {
		_, err := s.artifacts.Write(run.ID, task.ID, dir, "plan.v1.md", models.ArtifactKindPlan, markdown)
		return err
	}
	return nil
}

func (s *Server) hasEvent(runID, typ string) bool {
	for _, event := range s.store.EventsForRun(runID) {
		if event.Type == typ {
			return true
		}
	}
	return false
}

func (s *Server) emit(runID, taskID, typ, actorKind, actorID, summary string, data map[string]any) {
	_, err := s.store.AppendEvent(models.Event{RunID: runID, TaskID: taskID, Type: typ, ActorKind: actorKind, ActorID: actorID, Summary: summary, Data: data})
	if err != nil && s.logger != nil {
		s.logger.Error("failed to append event", "error", err)
	}
}

func (s *Server) render(w http.ResponseWriter, title, body string, data map[string]any) {
	base := template.Must(template.New("base").Funcs(template.FuncMap{"since": since, "statusLabel": statusLabel, "artifactName": artifactName, "artifactLabel": artifactLabel, "agentProfileLabel": agentProfileLabel, "shortStep": shortStep, "eventLabel": eventLabel, "eventDetail": eventDetail}).Parse(layoutTemplate + body))
	data["Title"] = title
	data["Now"] = time.Now().UTC()
	data["CSRFToken"] = s.csrfToken
	data["K"] = templateConstants()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := base.ExecuteTemplate(w, "layout", data); err != nil {
		s.logger.Error("render failed", "error", err)
	}
}

func (s *Server) badRequest(w http.ResponseWriter, err error) { http.Error(w, err.Error(), http.StatusBadRequest) }
func (s *Server) serverError(w http.ResponseWriter, err error) { http.Error(w, err.Error(), http.StatusInternalServerError) }

func validateGitRepo(path string) (string, error) {
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

func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

func writeSSE(w http.ResponseWriter, event models.Event) {
	data, _ := json.Marshal(event)
	fmt.Fprintf(w, "id: %d\n", event.Sequence)
	fmt.Fprintf(w, "event: %s\n", event.Type)
	fmt.Fprintf(w, "data: %s\n\n", data)
}

func parseLastEventID(value string) int {
	seq, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seq < 0 {
		return 0
	}
	return seq
}

func since(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return time.Since(t).Round(time.Second).String() + " ago"
}

type plannerGenerationView struct {
	Generation  models.PlannerGeneration
	Diagnostics []models.PlannerDiagnostic
}

func plannerGenerationViews(generations []models.PlannerGeneration, diagnostics []models.PlannerDiagnostic) []plannerGenerationView {
	views := make([]plannerGenerationView, 0, len(generations))
	indexByGeneration := map[string]int{}
	for _, generation := range generations {
		indexByGeneration[generation.ID] = len(views)
		views = append(views, plannerGenerationView{Generation: generation})
	}
	for _, diagnostic := range diagnostics {
		if index, ok := indexByGeneration[diagnostic.GenerationID]; ok {
			views[index].Diagnostics = append(views[index].Diagnostics, diagnostic)
		}
	}
	return views
}

func plannerGenerationRunning(generations []models.PlannerGeneration) bool {
	for _, generation := range generations {
		if generation.Status == models.PlannerGenerationStatusRunning {
			return true
		}
	}
	return false
}

func parseSmallInt(value string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed < 0 {
		return fallback
	}
	if parsed > 9 {
		return 9
	}
	return parsed
}

type attemptView struct {
	Attempt   models.Attempt
	Artifacts []models.Artifact
}

func visibleDiffArtifacts(artifacts []models.Artifact) []models.Artifact {
	visible := make([]models.Artifact, 0)
	for _, artifact := range artifacts {
		if artifact.Sensitivity != "" && artifact.Sensitivity != models.SensitivityNormal {
			continue
		}
		if artifact.Kind != models.ArtifactKindDiff && artifact.Kind != models.ArtifactKindChangedFiles {
			continue
		}
		if artifact.SizeBytes <= 0 {
			continue
		}
		visible = append(visible, artifact)
	}
	return visible
}

func internalDiagnosticArtifacts(artifacts []models.Artifact) []models.Artifact {
	diagnostics := make([]models.Artifact, 0)
	for _, artifact := range artifacts {
		if isDiagnosticArtifact(artifact) {
			diagnostics = append(diagnostics, artifact)
		}
	}
	return diagnostics
}

func isDiagnosticArtifact(artifact models.Artifact) bool {
	if artifact.Sensitivity != models.SensitivityInternal {
		return false
	}
	path := filepath.ToSlash(artifact.Path)
	name := artifactName(artifact.Path)
	switch artifact.Kind {
	case models.ArtifactKindWorkerInput:
		return name == "worker-input.md"
	case models.ArtifactKindCheckpoint:
		return strings.Contains(path, "/checkpoints/")
	case models.ArtifactKindWorkerOutput, models.ArtifactKindReview:
		return strings.Contains(path, "/runtime/") && (name == "stdout.txt" || name == "stderr.txt")
	default:
		return false
	}
}

func attemptTimeline(task models.Task, attempts []models.Attempt, artifacts []models.Artifact) []attemptView {
	if len(attempts) == 0 && (task.Attempts > 0 || len(artifacts) > 0) {
		attempts = append(attempts, models.Attempt{TaskID: task.ID, Number: 1, Kind: models.AttemptKindWorker, Status: models.AttemptStatusReviewed, Summary: "Prototype attempt outputs captured before timeline metadata existed.", CreatedAt: task.UpdatedAt, UpdatedAt: task.UpdatedAt})
	}
	views := make([]attemptView, 0, len(attempts))
	for _, attempt := range attempts {
		view := attemptView{Attempt: attempt}
		for _, artifact := range artifacts {
			artifactAttempt := artifact.AttemptNumber
			if artifactAttempt == 0 && attempt.Number == 1 && artifact.Kind != models.ArtifactKindPlan {
				artifactAttempt = 1
			}
			if artifactAttempt == attempt.Number {
				view.Artifacts = append(view.Artifacts, artifact)
			}
		}
		views = append(views, view)
	}
	return views
}

type chainStep struct {
	Name        string
	Description string
	State       string
}

func chainForTask(task models.Task, artifacts []models.Artifact, events []models.Event) []chainStep {
	hasEvent := func(kind string) bool {
		for _, event := range events {
			if event.Type == kind {
				return true
			}
		}
		return false
	}
	hasArtifact := func(kind string) bool {
		for _, artifact := range artifacts {
			if artifact.Kind == kind {
				return true
			}
		}
		return false
	}
	state := func(done bool, active bool) string {
		if done {
			return "done"
		}
		if active {
			return "active"
		}
		return "queued"
	}
	return []chainStep{
		{Name: "1. Prompt / intake", Description: "Capture the goal and project context.", State: state(hasEvent(models.EventPlannerPromptReceived) || hasEvent(models.EventTaskPlanCreated) || hasEvent(models.EventTaskContractCreated), task.Status == models.TaskStatusDraft)},
		{Name: "2. Planner draft", Description: "Turn the prompt into assumptions, boundaries, and an editable plan.", State: state(hasEvent(models.EventPlannerDraftCreated) || hasArtifact(models.ArtifactKindPlan) || hasArtifact(models.ArtifactKindContract), false)},
		{Name: "3. Approval gate", Description: "Save the plan before execution starts.", State: state(task.Status != models.TaskStatusDraft, task.Status == models.TaskStatusDraft)},
		{Name: "4. Runner slot", Description: "Reserve one runner for this attempt.", State: state(hasEvent(models.EventLeaseGranted) || hasEvent(models.EventLeaseReleased), task.Status == models.TaskStatusRunning && task.LeaseID != "")},
		{Name: "5. Worker attempt", Description: "Dry-run writes placeholder worker artifacts by default; experimental local-pi can run a guarded worktree/container worker.", State: state(hasArtifact(models.ArtifactKindWorkerOutput), task.Status == models.TaskStatusRunning)},
		{Name: "6. Fresh review", Description: "Dry-run writes placeholder review output by default; experimental local-pi can run a guarded reviewer step.", State: state(hasArtifact(models.ArtifactKindReview), task.Status == models.TaskStatusAwaitingReview)},
		{Name: "7. Final review", Description: "Accept, request fix, retry, block, or abandon.", State: state(task.Status == models.TaskStatusDone || task.Status == models.TaskStatusNeedsFix, task.Status == models.TaskStatusAwaitingReview)},
	}
}

func summarizeTitle(prompt string) string {
	prompt = strings.Join(strings.Fields(prompt), " ")
	if len(prompt) <= 72 {
		return prompt
	}
	return prompt[:69] + "..."
}

func shortStep(name string) string {
	if idx := strings.Index(name, ". "); idx >= 0 && idx+2 < len(name) {
		return name[idx+2:]
	}
	return name
}

func withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}
