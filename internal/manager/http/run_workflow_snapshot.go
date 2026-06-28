package managerhttp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/event"
)

type workflowSnapshotController interface {
	UpdateRunWorkflowSnapshot(context.Context, string, workflow.Template, event.Actor) error
	FreezeRunWorkflowSnapshot(context.Context, string, event.Actor) error
}

func (s *Server) handleRunWorkflowSnapshotPath(w http.ResponseWriter, r *http.Request, projectID, runID, action string) {
	if !s.runBelongsToProject(r, projectID, runID) {
		http.NotFound(w, r)
		return
	}
	switch action {
	case "edit":
		s.handleRunWorkflowSnapshotEdit(w, r, projectID, runID)
	case "save":
		s.handleRunWorkflowSnapshotSave(w, r, projectID, runID)
	case "freeze":
		s.handleRunWorkflowSnapshotFreeze(w, r, projectID, runID)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleRunWorkflowSnapshotEdit(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil || bundle.Run.ProjectID != projectID {
		http.NotFound(w, r)
		return
	}
	if bundle.Run.Status != store.RunStatusAwaitingWorkflowAdjustment {
		http.Error(w, "workflow snapshot is not editable outside the final adjustment step", http.StatusConflict)
		return
	}
	if frozen, err := s.latestWorkflowSnapshotFrozen(r.Context(), runID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else if frozen {
		http.Error(w, "workflow snapshot is frozen", http.StatusConflict)
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
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	controller, ok := s.engine.(workflowSnapshotController)
	if !ok {
		http.Error(w, "workflow snapshot editing unavailable", http.StatusNotImplemented)
		return
	}
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil || bundle.Run.ProjectID != projectID {
		http.NotFound(w, r)
		return
	}
	current, err := s.store.LatestWorkflowTemplateSnapshot(r.Context(), runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	current.Predefined = false
	current.Editable = true
	updated, err := workflowTemplateFromForm(r, current)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	updated.Predefined = false
	updated.Editable = true
	if err := workflow.ValidateTemplate(updated); err != nil {
		data, dataErr := s.runWorkflowSnapshotEditData(r, bundle, updated, nil, err.Error())
		if dataErr != nil {
			http.Error(w, dataErr.Error(), http.StatusInternalServerError)
			return
		}
		s.writePageStatus(w, "workflow_template_edit.html", data, http.StatusBadRequest)
		return
	}
	if err := controller.UpdateRunWorkflowSnapshot(r.Context(), runID, updated, event.Actor{Kind: event.ActorKindOperator, ID: "operator"}); err != nil {
		s.writeRunWorkflowSnapshotError(w, err)
		return
	}
	http.Redirect(w, r, runWorkflowSnapshotEditPath(projectID, runID)+"?notice=saved", http.StatusSeeOther)
}

func (s *Server) handleRunWorkflowSnapshotFreeze(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	controller, ok := s.engine.(workflowSnapshotController)
	if !ok {
		http.Error(w, "workflow snapshot freeze unavailable", http.StatusNotImplemented)
		return
	}
	if err := controller.FreezeRunWorkflowSnapshot(r.Context(), runID, event.Actor{Kind: event.ActorKindOperator, ID: "operator"}); err != nil {
		s.writeRunWorkflowSnapshotError(w, err)
		return
	}
	http.Redirect(w, r, projectRunPath(projectID, runID), http.StatusSeeOther)
}

func (s *Server) runWorkflowSnapshotEditData(r *http.Request, bundle store.RunBundle, template workflow.Template, notice *web.Notice, message string) (web.WorkflowTemplateEditData, error) {
	data, err := s.workflowTemplateEditData(r, editableRunSnapshotTemplateForHTTP(template), notice, message)
	if err != nil {
		return web.WorkflowTemplateEditData{}, err
	}
	data.SavePath = runWorkflowSnapshotSavePath(bundle.Project.ID, bundle.Run.ID)
	data.BackPath = projectRunPath(bundle.Project.ID, bundle.Run.ID)
	data.Breadcrumb = "Run " + bundle.Run.ID
	data.Heading = "Customize run workflow"
	data.Help = "Use the same light controls as template editing. These changes apply only to this run snapshot; project templates are untouched. Freeze the snapshot from the run page to continue execution."
	data.SubmitLabel = "Save run snapshot"
	data.Title = "Parley · Customize run workflow"
	return data, nil
}

func (s *Server) runWorkflowSnapshotView(r *http.Request, bundle store.RunBundle) *web.WorkflowSnapshotView {
	frozen, err := s.latestWorkflowSnapshotFrozen(r.Context(), bundle.Run.ID)
	if err != nil {
		return nil
	}
	editable := !frozen && bundle.Run.Status == store.RunStatusAwaitingWorkflowAdjustment
	if !editable && !frozen {
		return nil
	}
	return &web.WorkflowSnapshotView{
		Editable:   editable,
		Frozen:     frozen,
		EditPath:   runWorkflowSnapshotEditPath(bundle.Project.ID, bundle.Run.ID),
		FreezePath: runWorkflowSnapshotFreezePath(bundle.Project.ID, bundle.Run.ID),
	}
}

func (s *Server) latestWorkflowSnapshotFrozen(ctx context.Context, runID string) (bool, error) {
	snapshot, err := s.store.LatestWorkflowSnapshot(ctx, runID)
	if err != nil {
		return false, err
	}
	return snapshotBool(snapshot, "frozen") || snapshotBool(snapshot, "workflow_template_frozen") || snapshotBool(snapshot, "workflow_snapshot_frozen"), nil
}

func editableRunSnapshotTemplateForHTTP(template workflow.Template) workflow.Template {
	template = workflow.NormalizeTemplate(template)
	template.Predefined = false
	template.Editable = true
	return template
}

func snapshotBool(snapshot map[string]any, key string) bool {
	value, ok := snapshot[key]
	if !ok || value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		v = strings.ToLower(strings.TrimSpace(v))
		return v == "true" || v == "1" || v == "yes" || v == "on"
	default:
		return strings.EqualFold(strings.TrimSpace(fmt.Sprint(v)), "true")
	}
}

func runWorkflowSnapshotEditPath(projectID, runID string) string {
	return projectRunPath(projectID, runID) + "/workflow/edit"
}

func runWorkflowSnapshotSavePath(projectID, runID string) string {
	return projectRunPath(projectID, runID) + "/workflow/save"
}

func runWorkflowSnapshotFreezePath(projectID, runID string) string {
	return projectRunPath(projectID, runID) + "/workflow/freeze"
}

func runWorkflowSnapshotNotice(r *http.Request) *web.Notice {
	switch r.URL.Query().Get("notice") {
	case "saved":
		return &web.Notice{Title: "Run snapshot saved", Message: "The workflow snapshot was validated, edges were re-derived, and project templates were left untouched."}
	default:
		return nil
	}
}

func (s *Server) writeRunWorkflowSnapshotError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, orchestrator.ErrWorkflowSnapshotNotEditable) || errors.Is(err, orchestrator.ErrWorkflowSnapshotFrozen) || errors.Is(err, store.ErrWorkflowSnapshotStageLocked) {
		status = http.StatusConflict
	} else if strings.Contains(err.Error(), "workflow template") || strings.Contains(err.Error(), "workflow stage") || strings.Contains(err.Error(), "exactly one") {
		status = http.StatusBadRequest
	}
	http.Error(w, err.Error(), status)
}
