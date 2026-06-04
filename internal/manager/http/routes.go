package managerhttp

import (
	"net/http"

	"github.com/agent-parley/parley/internal/manager/web"
)

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/assets/", http.StripPrefix("/", http.FileServer(http.FS(web.Embedded))))
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/prototype", s.handlePrototype)
	mux.HandleFunc("/runs", s.handleRuns)
	mux.HandleFunc("/runs/", s.handleRunPath)
	mux.HandleFunc("/artifacts/", s.handleArtifact)
	return s.security.middleware(mux)
}
