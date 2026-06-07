package managerhttp

import (
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
)

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	s.handleProjectIndex(w, r, store.DefaultProjectID)
}

func (s *Server) handleProjectPath(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/projects/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	projectID := parts[0]
	if len(parts) == 1 {
		s.handleProjectIndex(w, r, projectID)
		return
	}
	if parts[1] != "runs" {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 2 {
		s.handleProjectRuns(w, r, projectID)
		return
	}
	runID := parts[2]
	if runID == "" {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 4 && parts[3] == "events" {
		s.handleRunEvents(w, r, projectID, runID)
		return
	}
	if len(parts) == 4 && parts[3] == "cancel" {
		s.handleCancelRun(w, r, projectID, runID)
		return
	}
	if len(parts) == 4 && parts[3] == "start" {
		s.handleStartQueuedRun(w, r, projectID, runID)
		return
	}
	if len(parts) == 3 {
		s.handleRunDetail(w, r, projectID, runID)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleProjectIndex(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	data, err := s.indexData(r, projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writePage(w, "index.html", data)
}

func (s *Server) indexData(r *http.Request, projectID string) (web.IndexData, error) {
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		return web.IndexData{}, err
	}
	runs, err := s.store.ListRunsForProject(r.Context(), projectID)
	if err != nil {
		return web.IndexData{}, err
	}
	runners, err := s.store.ListRunners(r.Context())
	if err != nil {
		return web.IndexData{}, err
	}
	runnerEventPage, err := s.store.ListSystemEventsPage(r.Context(), parseInt64Query(r, "runner_events_before"), 50)
	if err != nil {
		return web.IndexData{}, err
	}
	queueState, err := s.engine.QueueState(r.Context())
	if err != nil {
		return web.IndexData{}, err
	}
	return web.IndexData{Project: project, Runs: runs, Runners: runners, RunnerEventPage: runnerEventPage, Queue: web.NewQueueView(queueState), CSRF: csrfFromContext(r.Context()), Title: "Parley · " + project.Name}, nil
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/runs" {
		http.NotFound(w, r)
		return
	}
	s.handleProjectRuns(w, r, store.DefaultProjectID)
}

func (s *Server) handleProjectRuns(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	idea := strings.TrimSpace(r.Form.Get("idea"))
	if idea == "" {
		http.Error(w, "idea is required", http.StatusBadRequest)
		return
	}
	input := contract.TaskInput{Idea: idea, RefinementLevel: r.Form.Get("refinement_level")}
	input.RefinementLevel = contract.NormalizeRefinementLevel(input.RefinementLevel)
	if err := contract.ValidateRefinementLevel(input.RefinementLevel); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	runID, err := s.engine.StartProjectRunInput(r.Context(), projectID, input)
	if err != nil {
		var backlogErr orchestrator.QueueBacklogFullError
		if errors.As(err, &backlogErr) {
			data, pageErr := s.indexData(r, projectID)
			if pageErr != nil {
				http.Error(w, pageErr.Error(), http.StatusInternalServerError)
				return
			}
			data.Notice = backlogFullNotice(backlogErr, data.Queue)
			s.writePageStatus(w, "index.html", data, http.StatusTooManyRequests)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, projectRunPath(projectID, runID), http.StatusSeeOther)
}

func (s *Server) handleRunPath(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/runs/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	runID := parts[0]
	run, err := s.store.GetRun(r.Context(), runID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	projectID := run.ProjectID
	if len(parts) == 2 && parts[1] == "events" {
		s.handleRunEvents(w, r, projectID, runID)
		return
	}
	if len(parts) == 2 && parts[1] == "cancel" {
		s.handleCancelRun(w, r, projectID, runID)
		return
	}
	if len(parts) == 2 && parts[1] == "start" {
		s.handleStartQueuedRun(w, r, projectID, runID)
		return
	}
	if len(parts) == 1 {
		http.Redirect(w, r, projectRunPath(projectID, runID), http.StatusSeeOther)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleRunDetail(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil || bundle.Run.ProjectID != projectID {
		http.NotFound(w, r)
		return
	}
	s.writePage(w, "run.html", web.NewRunData(bundle, csrfFromContext(r.Context()), "Run "+runID))
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.runBelongsToProject(r, projectID, runID) {
		http.NotFound(w, r)
		return
	}
	if err := s.engine.CancelRun(r.Context(), runID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, projectRunPath(projectID, runID), http.StatusSeeOther)
}

func (s *Server) handleStartQueuedRun(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.runBelongsToProject(r, projectID, runID) {
		http.NotFound(w, r)
		return
	}
	if err := s.engine.StartQueuedRun(r.Context(), runID); err != nil {
		if errors.Is(err, orchestrator.ErrNoRunnerSlots) ||
			errors.Is(err, orchestrator.ErrRunNotPending) ||
			errors.Is(err, orchestrator.ErrRunHeld) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, projectRunPath(projectID, runID), http.StatusSeeOther)
}

func (s *Server) handleRunEvents(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.security.requireSession(r) {
		http.Error(w, "session required", http.StatusUnauthorized)
		return
	}
	last := parseLastEventID(r)
	if _, err := s.store.ListEventsAfter(r.Context(), runID, last); err != nil {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil || bundle.Run.ProjectID != projectID {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	fragment, err := s.renderer.RenderRunFragments(bundle)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writer, ok := NewSSEWriter(w)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	seq := last
	if len(bundle.Events) > 0 {
		seq = bundle.Events[len(bundle.Events)-1].Sequence
	}
	writer.Patch(event.Event{ProjectID: projectID, Sequence: seq}, fragment)

	ch, unsubscribe := s.hub.Subscribe(runID)
	defer unsubscribe()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			writer.Patch(msg.Event, msg.Fragment)
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) handleArtifact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	artifactID := strings.TrimPrefix(r.URL.Path, "/artifacts/")
	if artifactID == "" || strings.Contains(artifactID, "/") {
		http.NotFound(w, r)
		return
	}
	artifact, content, err := s.store.GetArtifact(r.Context(), artifactID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if artifactIsHTML(artifact.MediaType) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", artifact.ID+".html"))
		_, _ = w.Write(content)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, "<!doctype html><title>%s</title><pre>%s</pre>", template.HTMLEscapeString(artifact.ID), template.HTMLEscapeString(string(content)))
}

func (s *Server) runBelongsToProject(r *http.Request, projectID, runID string) bool {
	run, err := s.store.GetRun(r.Context(), runID)
	return err == nil && run.ProjectID == projectID
}

func projectRunPath(projectID, runID string) string {
	return "/projects/" + projectID + "/runs/" + runID
}

func artifactIsHTML(mediaType string) bool {
	mediaType = strings.ToLower(mediaType)
	return strings.HasPrefix(mediaType, "text/html") || strings.Contains(mediaType, "html")
}

func parseInt64Query(r *http.Request, key string) int64 {
	value := r.URL.Query().Get(key)
	if value == "" {
		return 0
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func backlogFullNotice(backlogErr orchestrator.QueueBacklogFullError, queue web.QueueView) *web.Notice {
	return &web.Notice{
		Title: "Queue is full",
		Message: fmt.Sprintf(
			"%d pending runs are already waiting, which reaches backlog_cap %d. Effective policy: auto_when_ready=%t, max_concurrent=%d (effective %d), ready_slots=%d/%d.",
			backlogErr.Pending,
			backlogErr.Cap,
			queue.AutoWhenReady,
			queue.MaxConcurrent,
			queue.EffectiveMaxConcurrent,
			queue.ReadyRunnerSlots,
			queue.RunnerSlots,
		),
	}
}

func (s *Server) writePage(w http.ResponseWriter, name string, data any) {
	s.writePageStatus(w, name, data, http.StatusOK)
}

func (s *Server) writePageStatus(w http.ResponseWriter, name string, data any, status int) {
	html, err := s.renderer.ExecutePage(name, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(html)))
	w.WriteHeader(status)
	_, _ = w.Write([]byte(html))
}
