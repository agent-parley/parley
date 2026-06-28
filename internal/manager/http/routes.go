package managerhttp

import (
	"net/http"

	"github.com/agent-parley/parley/internal/manager/web"
)

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/assets/", http.StripPrefix("/", http.FileServer(http.FS(web.Embedded))))
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/projects", s.handleProjectsIndex)
	mux.HandleFunc("/projects/", s.handleProjectPath)
	mux.HandleFunc("/notifications/events", s.handleNotificationEvents)
	mux.HandleFunc("/notifications/ack-all", s.handleAcknowledgeAllNotifications)
	mux.HandleFunc("/notifications/", s.handleNotificationPath)
	mux.HandleFunc("/templates", s.handleTemplatesIndex)
	mux.HandleFunc("/templates/", s.handleTemplatePath)
	mux.HandleFunc("/settings", s.handleSystemSettings)
	mux.HandleFunc("/settings/", s.handleSystemSettingsPath)
	mux.HandleFunc("/runs", s.handleRuns)
	mux.HandleFunc("/runs/", s.handleRunPath)
	mux.HandleFunc("/artifacts/", s.handleArtifact)
	return s.security.middleware(mux)
}
