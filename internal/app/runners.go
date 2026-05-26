package app

import (
	"net/http"
	"strings"

	"github.com/agent-parley/parley/internal/models"
)

type runnerView struct {
	Runner      models.Executor
	ActiveSlots int
}

func (s *Server) runners(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/runners" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	project, hasProject := s.projectFromQuery(r)
	data := map[string]any{"Runners": s.runnerViews(), "ProjectQuery": ""}
	if hasProject {
		data["Project"] = project
		data["ProjectQuery"] = "?project=" + project.ID
	}
	s.render(w, "Runners", runnersTemplate, data)
}

func (s *Server) runnerRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	parts := splitPath(strings.TrimPrefix(r.URL.Path, "/runners/"))
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	runner, ok := s.store.GetExecutor(parts[0])
	if !ok {
		http.NotFound(w, r)
		return
	}
	project, hasProject := s.projectFromQuery(r)
	counts := s.store.ActiveLeaseCountByExecutor()
	data := map[string]any{"Runner": runner, "ActiveSlots": counts[runner.ID], "ProjectQuery": ""}
	if hasProject {
		data["Project"] = project
		data["ProjectQuery"] = "?project=" + project.ID
	}
	s.render(w, runner.Name, runnerDetailTemplate, data)
}

func (s *Server) runnerViews() []runnerView {
	counts := s.store.ActiveLeaseCountByExecutor()
	runners := s.store.ListExecutors()
	views := make([]runnerView, 0, len(runners))
	for _, runner := range runners {
		views = append(views, runnerView{Runner: runner, ActiveSlots: counts[runner.ID]})
	}
	return views
}

func (s *Server) runnerNames() map[string]string {
	names := map[string]string{}
	for _, runner := range s.store.ListExecutors() {
		names[runner.ID] = runner.Name
	}
	return names
}

func (s *Server) projectFromQuery(r *http.Request) (models.Project, bool) {
	id := strings.TrimSpace(r.URL.Query().Get("project"))
	if id == "" {
		return models.Project{}, false
	}
	project, ok := s.store.GetProject(id)
	return project, ok
}
