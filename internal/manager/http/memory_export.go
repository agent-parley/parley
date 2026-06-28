package managerhttp

import (
	"fmt"
	"net/http"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
)

func (s *Server) handleProjectMemoryExport(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := r.ParseForm(); err != nil {
		s.writeProjectMemoryExportFragment(w, r, http.StatusBadRequest, project, "Could not parse memory export selection.", "", nil)
		return
	}
	repoPath, err := s.projectRepositoryPath(r, projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if repoPath == "" {
		s.writeProjectMemoryExportFragment(w, r, http.StatusBadRequest, project, "No repository configured; memory export is unavailable.", "", nil)
		return
	}
	result, err := s.store.ExportProjectMemoryEntries(r.Context(), store.ProjectMemoryExportRequest{
		ProjectID:      projectID,
		RepositoryPath: repoPath,
		EntryIDs:       r.Form["memory_entry_id"],
	})
	if err != nil {
		s.writeProjectMemoryExportFragment(w, r, http.StatusBadRequest, project, err.Error(), "", nil)
		return
	}
	exportedFiles := make([]string, 0, len(result.Files))
	for _, file := range result.Files {
		exportedFiles = append(exportedFiles, file.RelativePath)
	}
	status := fmt.Sprintf("exported %d selected memory %s to %s", len(result.Files), plural(len(result.Files), "entry", "entries"), store.ProjectMemoryExportDir)
	s.writeProjectMemoryExportFragment(w, r, http.StatusAccepted, project, "", status, exportedFiles)
}

func (s *Server) projectMemoryExportData(r *http.Request, project store.Project, notice, status string, exportedFiles []string) (web.ProjectMemoryExportData, error) {
	entries, err := s.store.ListProjectMemoryEntries(r.Context(), project.ID)
	if err != nil {
		return web.ProjectMemoryExportData{}, err
	}
	repoPath, err := s.projectRepositoryPath(r, project.ID)
	if err != nil {
		return web.ProjectMemoryExportData{}, err
	}
	return web.ProjectMemoryExportData{
		Project:              project,
		Entries:              entries,
		ExportDir:            store.ProjectMemoryExportDir,
		ExportActionPath:     "/projects/" + project.ID + "/settings/memory/export",
		RepositoryConfigured: repoPath != "",
		Notice:               notice,
		Status:               status,
		ExportedFiles:        exportedFiles,
		CSRF:                 csrfFromContext(r.Context()),
	}, nil
}

func (s *Server) writeProjectMemoryExportFragment(w http.ResponseWriter, r *http.Request, statusCode int, project store.Project, notice, status string, exportedFiles []string) {
	data, err := s.projectMemoryExportData(r, project, notice, status, exportedFiles)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fragment, err := s.renderer.ExecutePage("memory_export.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(fragment))
}

func plural(count int, singular, plural string) string {
	if count == 1 {
		return singular
	}
	return plural
}
