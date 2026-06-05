package managerhttp

import (
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/shared/event"
)

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	runs, err := s.store.ListRuns(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	runners, err := s.store.ListRunners(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	runnerEventPage, err := s.store.ListSystemEventsPage(r.Context(), parseInt64Query(r, "runner_events_before"), 50)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writePage(w, "index.html", web.IndexData{Runs: runs, Runners: runners, RunnerEventPage: runnerEventPage, CSRF: csrfFromContext(r.Context()), Title: "Parley"})
}

func (s *Server) handlePrototype(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/prototype" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	query := r.URL.Query()
	s.writePage(w, "prototype.html", web.NewPrototypeDataWithOptions(web.PrototypeOptions{
		RunID:      query.Get("run"),
		Tab:        query.Get("tab"),
		View:       query.Get("view"),
		Mock:       query.Get("mock"),
		CancelMode: query.Get("cancel"),
	}))
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/runs" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	idea := strings.TrimSpace(r.Form.Get("idea"))
	if idea == "" {
		http.Error(w, "idea is required", http.StatusBadRequest)
		return
	}
	runID, err := s.engine.StartRun(r.Context(), idea)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/runs/"+runID, http.StatusSeeOther)
}

func (s *Server) handleRunPath(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/runs/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	runID := parts[0]
	if len(parts) == 2 && parts[1] == "events" {
		s.handleRunEvents(w, r, runID)
		return
	}
	if len(parts) == 2 && parts[1] == "cancel" {
		s.handleCancelRun(w, r, runID)
		return
	}
	if len(parts) == 1 {
		s.handleRunDetail(w, r, runID)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleRunDetail(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	s.writePage(w, "run.html", web.NewRunData(bundle, csrfFromContext(r.Context()), "Run "+runID))
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if err := s.engine.CancelRun(r.Context(), runID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/runs/"+runID, http.StatusSeeOther)
}

func (s *Server) handleRunEvents(w http.ResponseWriter, r *http.Request, runID string) {
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
	if err != nil {
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
	writer.Patch(event.Event{Sequence: seq}, fragment)

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

func (s *Server) writePage(w http.ResponseWriter, name string, data any) {
	html, err := s.renderer.ExecutePage(name, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(html)))
	_, _ = w.Write([]byte(html))
}
