package managerhttp

import (
	"errors"
	"net/http"
	"strings"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/event"
)

func (s *Server) handleRunWorkflowSnapshot(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if !s.runBelongsToProject(r, projectID, runID) {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleRunWorkflowSnapshotEdit(w, r, projectID, runID)
	case http.MethodPost:
		s.handleRunWorkflowSnapshotSave(w, r, projectID, runID)
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleRunWorkflowSnapshotEdit(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil || bundle.Run.ProjectID != projectID {
		http.NotFound(w, r)
		return
	}
	if bundle.Run.Status != store.RunStatusPaused {
		http.Error(w, "workflow snapshot is editable only while the run is paused", http.StatusConflict)
		return
	}
	template, err := s.store.LatestWorkflowTemplateSnapshot(r.Context(), runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, err := s.runWorkflowSnapshotEditData(r, bundle, template, runWorkflowSnapshotNotice(r), "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writePage(w, "workflow_template_edit.html", data)
}

func (s *Server) handleRunWorkflowSnapshotSave(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil || bundle.Run.ProjectID != projectID {
		http.NotFound(w, r)
		return
	}
	if bundle.Run.Status != store.RunStatusPaused {
		http.Error(w, "workflow snapshot is editable only while the run is paused", http.StatusConflict)
		return
	}
	current, err := s.store.LatestWorkflowTemplateSnapshot(r.Context(), runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	registry, err := s.store.ResolveAgentRegistry(r.Context(), projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	current = editableRunSnapshotTemplateForHTTP(current)
	updated, err := workflowTemplateFromForm(r, current, registry)
	if err != nil {
		s.writeRunWorkflowSnapshotFormError(w, r, bundle, current, err.Error(), http.StatusBadRequest)
		return
	}
	updated = editableRunSnapshotTemplateForHTTP(updated)
	updated.Edges = workflow.DeriveTemplateEdges(updated)
	updated = workflow.NormalizeTemplateWithRegistry(updated, registry)
	if err := workflow.ValidateTemplateWithRegistry(updated, registry); err != nil {
		s.writeRunWorkflowSnapshotFormError(w, r, bundle, updated, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.engine.UpdateRunWorkflowSnapshot(r.Context(), runID, updated, event.Actor{Kind: event.ActorKindOperator, ID: "operator"}); err != nil {
		if errors.Is(err, orchestrator.ErrWorkflowSnapshotNotEditable) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if errors.Is(err, store.ErrWorkflowSnapshotStageLocked) || strings.Contains(err.Error(), "executed workflow prefix") || strings.Contains(err.Error(), "workflow template") || strings.Contains(err.Error(), "workflow stage") || strings.Contains(err.Error(), "exactly one") {
			s.writeRunWorkflowSnapshotFormError(w, r, bundle, updated, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, projectRunPath(projectID, runID), http.StatusSeeOther)
}

func (s *Server) writeRunWorkflowSnapshotFormError(w http.ResponseWriter, r *http.Request, bundle store.RunBundle, template workflow.Template, message string, status int) {
	data, err := s.runWorkflowSnapshotEditData(r, bundle, template, nil, message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writePageStatus(w, "workflow_template_edit.html", data, status)
}

func (s *Server) runWorkflowSnapshotEditData(r *http.Request, bundle store.RunBundle, template workflow.Template, notice *web.Notice, message string) (web.WorkflowTemplateEditData, error) {
	data, err := s.workflowTemplateEditData(r, editableRunSnapshotTemplateForHTTP(template), notice, message)
	if err != nil {
		return web.WorkflowTemplateEditData{}, err
	}
	registry, err := s.store.ResolveAgentRegistry(r.Context(), bundle.Project.ID)
	if err != nil {
		return web.WorkflowTemplateEditData{}, err
	}
	editable := editableRunSnapshotTemplateForHTTP(template)
	data.Template = workflow.NormalizeTemplateWithRegistry(editable, registry)
	data.Settings = workflowTemplateSettingsData(editable.Settings)
	data.StageRows = workflowTemplateStageRowsWithRegistry(editable, registry)
	data.AgentProfiles = registry.Profiles
	data.SavePath = runWorkflowSnapshotPath(bundle.Project.ID, bundle.Run.ID)
	data.BackPath = projectRunPath(bundle.Project.ID, bundle.Run.ID)
	data.Breadcrumb = "Run " + bundle.Run.ID
	data.Heading = "Customize run workflow"
	data.Help = "Edit only not-yet-started stages while this run is paused. Each save appends a run-local workflow snapshot version; project templates are untouched."
	data.SubmitLabel = "Save run snapshot"
	data.Title = "Parley · Customize run workflow"
	return data, nil
}

func editableRunSnapshotTemplateForHTTP(template workflow.Template) workflow.Template {
	template = workflow.NormalizeTemplate(template)
	template.Predefined = false
	template.Recommended = false
	template.Editable = true
	template.Edges = workflow.DeriveTemplateEdges(template)
	return workflow.NormalizeTemplate(template)
}

func runWorkflowSnapshotPath(projectID, runID string) string {
	return projectRunPath(projectID, runID) + "/workflow"
}

func runWorkflowSnapshotNotice(r *http.Request) *web.Notice {
	switch r.URL.Query().Get("notice") {
	case "saved":
		return &web.Notice{Title: "Run snapshot saved", Message: "The workflow snapshot was validated, versioned, and project templates were left untouched."}
	default:
		return nil
	}
}
