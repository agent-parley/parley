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
	s.writePage(w, "index.html", web.IndexData{Runs: runs, CSRF: csrfFromContext(r.Context()), Title: "Parley"})
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
	s.writePage(w, "run.html", web.RunData{Bundle: bundle, CSRF: csrfFromContext(r.Context()), Title: "Run " + runID})
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
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, "<!doctype html><title>%s</title><pre>%s</pre>", template.HTMLEscapeString(artifact.ID), template.HTMLEscapeString(string(content)))
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
