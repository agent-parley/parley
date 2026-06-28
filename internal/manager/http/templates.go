package managerhttp

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/ids"
)

func (s *Server) handleTemplatesIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/templates" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	data, err := s.workflowTemplatesData(r, workflowTemplateNotice(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writePage(w, "workflow_templates.html", data)
}

func (s *Server) handleTemplatePath(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/templates/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	templateID, err := url.PathUnescape(parts[0])
	if err != nil || strings.TrimSpace(templateID) == "" {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 {
		http.Redirect(w, r, workflowTemplateEditPath(templateID), http.StatusSeeOther)
		return
	}
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	switch parts[1] {
	case "copy":
		s.handleWorkflowTemplateCopy(w, r, templateID)
	case "edit":
		s.handleWorkflowTemplateEdit(w, r, templateID)
	case "save":
		s.handleWorkflowTemplateSave(w, r, templateID)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleWorkflowTemplateCopy(w http.ResponseWriter, r *http.Request, sourceID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimSpace(r.Form.Get("name"))
	newID := ids.New("workflow_template")
	copied, err := s.store.CopyWorkflowTemplate(r.Context(), sourceID, newID, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, workflowTemplateEditPath(copied.ID)+"?notice=copied", http.StatusSeeOther)
}

func (s *Server) handleWorkflowTemplateEdit(w http.ResponseWriter, r *http.Request, templateID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	template, err := s.store.GetWorkflowTemplate(r.Context(), templateID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	data, err := s.workflowTemplateEditData(r, template, workflowTemplateNotice(r), "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writePage(w, "workflow_template_edit.html", data)
}

func (s *Server) handleWorkflowTemplateSave(w http.ResponseWriter, r *http.Request, templateID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	current, err := s.store.GetWorkflowTemplate(r.Context(), templateID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	updated, err := workflowTemplateFromForm(r, current)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := workflow.ValidateTemplate(updated); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.store.UpdateWorkflowTemplate(r.Context(), updated); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, store.ErrWorkflowTemplateInUse) {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	http.Redirect(w, r, workflowTemplateEditPath(templateID)+"?notice=saved", http.StatusSeeOther)
}

func (s *Server) workflowTemplatesData(r *http.Request, notice *web.Notice) (web.WorkflowTemplatesData, error) {
	templates, err := s.store.ListWorkflowTemplates(r.Context())
	if err != nil {
		return web.WorkflowTemplatesData{}, err
	}
	summaries := make([]web.WorkflowTemplateSummaryData, 0, len(templates))
	for _, template := range templates {
		summaries = append(summaries, web.WorkflowTemplateSummaryData{
			ID:          template.ID,
			Name:        template.Name,
			Description: template.Description,
			Predefined:  template.Predefined,
			Recommended: template.Recommended,
			Editable:    template.Editable,
			StageCount:  len(template.Stages),
			CopyPath:    "/templates/" + url.PathEscape(template.ID) + "/copy",
			EditPath:    workflowTemplateEditPath(template.ID),
		})
	}
	csrf := csrfFromContext(r.Context())
	notifications, err := s.notificationCenterData(r.Context(), csrf)
	if err != nil {
		return web.WorkflowTemplatesData{}, err
	}
	return web.WorkflowTemplatesData{Templates: summaries, Notifications: notifications, Notice: notice, CSRF: csrf, Title: "Parley · Workflow templates"}, nil
}

func (s *Server) workflowTemplateEditData(r *http.Request, template workflow.Template, notice *web.Notice, message string) (web.WorkflowTemplateEditData, error) {
	csrf := csrfFromContext(r.Context())
	notifications, err := s.notificationCenterData(r.Context(), csrf)
	if err != nil {
		return web.WorkflowTemplateEditData{}, err
	}
	return web.WorkflowTemplateEditData{
		Template:      workflow.NormalizeTemplate(template),
		Settings:      workflowTemplateSettingsData(template.Settings),
		StageRows:     workflowTemplateStageRows(template),
		SavePath:      "/templates/" + url.PathEscape(template.ID) + "/save",
		Notifications: notifications,
		Notice:        notice,
		Error:         message,
		CSRF:          csrf,
		Title:         "Parley · Edit " + template.Name,
	}, nil
}

func workflowTemplateFromForm(r *http.Request, current workflow.Template) (workflow.Template, error) {
	current = workflow.NormalizeTemplate(current)
	updated := current
	updated.Name = strings.TrimSpace(r.Form.Get("name"))
	updated.Description = strings.TrimSpace(r.Form.Get("description"))
	updated.Settings = copySettings(current.Settings)
	setStringSetting(updated.Settings, "branch_policy", r.Form.Get("branch_policy"))
	setStringSetting(updated.Settings, "pr_behavior", r.Form.Get("pr_behavior"))
	setStringSetting(updated.Settings, "merge_policy", r.Form.Get("merge_policy"))
	updated.Settings["fix_loop"] = r.Form.Get("fix_loop") != ""
	if raw := strings.TrimSpace(r.Form.Get("max_fix_loops")); raw != "" {
		maxLoops, err := strconv.Atoi(raw)
		if err != nil || maxLoops < 0 {
			return workflow.Template{}, fmt.Errorf("max_fix_loops must be a non-negative integer")
		}
		updated.Settings["max_fix_loops"] = maxLoops
	} else {
		delete(updated.Settings, "max_fix_loops")
	}

	existing := map[string]workflow.StageTemplate{}
	for _, stage := range current.Stages {
		existing[stage.ID] = stage
	}
	var starts, middles, stops []orderedWorkflowStage
	for _, stageID := range r.Form["stage_id"] {
		stageID = strings.TrimSpace(stageID)
		if stageID == "" {
			continue
		}
		stage, ok := existing[stageID]
		if !ok {
			stage = workflow.StageTemplate{ID: stageID}
		}
		fieldKey := stageID
		stage.Type = strings.TrimSpace(r.Form.Get("stage_type_" + fieldKey))
		if stage.Type == "" {
			return workflow.Template{}, fmt.Errorf("stage %q type is required", stageID)
		}
		enabled := r.Form.Get("enabled_"+fieldKey) != "" || workflowStageAlwaysEnabled(stage.Type)
		if !enabled {
			continue
		}
		stage.Label = strings.TrimSpace(r.Form.Get("label_" + fieldKey))
		if stage.Label == "" {
			stage.Label = defaultWorkflowStageLabel(stage.Type)
		}
		stage.Actor = strings.TrimSpace(r.Form.Get("actor_" + fieldKey))
		if stage.Actor == "" {
			stage.Actor = defaultWorkflowStageActor(stage.Type)
		}
		if stage.Type == workflow.StageTypeReview {
			stage.Target = strings.TrimSpace(r.Form.Get("target_" + fieldKey))
			if stage.Target == "" {
				stage.Target = workflow.TargetCodeChanges
			}
		} else {
			stage.Target = ""
		}
		if existingStage, ok := existing[stageID]; !ok || existingStage.Type != stage.Type {
			stage.Settings = defaultWorkflowStageSettings(stage.Type)
		} else {
			stage.Settings = copySettings(existingStage.Settings)
		}
		setStringSetting(stage.Settings, "instructions", r.Form.Get("instructions_"+fieldKey))
		if stage.Type == workflow.StageTypeReview {
			setStringSetting(stage.Settings, "profile", r.Form.Get("profile_"+fieldKey))
			setStringSetting(stage.Settings, "intensity", r.Form.Get("intensity_"+fieldKey))
		} else {
			delete(stage.Settings, "profile")
			delete(stage.Settings, "intensity")
		}
		if len(stage.Settings) == 0 {
			stage.Settings = nil
		}
		order, err := parseWorkflowStageOrder(r.Form.Get("order_" + fieldKey))
		if err != nil {
			return workflow.Template{}, fmt.Errorf("stage %q order %w", stageID, err)
		}
		ordered := orderedWorkflowStage{Stage: stage, Order: order}
		switch stage.Type {
		case workflow.StageTypeIdeaRefinement:
			starts = append(starts, ordered)
		case workflow.StageTypeStopReport:
			stops = append(stops, ordered)
		default:
			middles = append(middles, ordered)
		}
	}
	sortWorkflowStages(starts)
	sortWorkflowStages(middles)
	sortWorkflowStages(stops)
	updated.Stages = appendWorkflowStages(nil, starts)
	updated.Stages = appendWorkflowStages(updated.Stages, middles)
	updated.Stages = appendWorkflowStages(updated.Stages, stops)
	updated.Edges = workflow.DeriveTemplateEdges(updated)
	return workflow.NormalizeTemplate(updated), nil
}

type orderedWorkflowStage struct {
	Stage workflow.StageTemplate
	Order int
}

func sortWorkflowStages(stages []orderedWorkflowStage) {
	sort.SliceStable(stages, func(i, j int) bool {
		if stages[i].Order == stages[j].Order {
			return stages[i].Stage.ID < stages[j].Stage.ID
		}
		return stages[i].Order < stages[j].Order
	})
}

func appendWorkflowStages(out []workflow.StageTemplate, ordered []orderedWorkflowStage) []workflow.StageTemplate {
	for _, item := range ordered {
		out = append(out, item.Stage)
	}
	return out
}

func parseWorkflowStageOrder(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 1000, nil
	}
	order, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("must be a number")
	}
	return order, nil
}

func workflowTemplateStageRows(template workflow.Template) []web.WorkflowTemplateStageRowData {
	template = workflow.NormalizeTemplate(template)
	existingIDs := map[string]bool{}
	existingTypes := map[string]bool{}
	existingReviews := map[string]bool{}
	var starts, middles, stops []web.WorkflowTemplateStageRowData
	for i, stage := range template.Stages {
		existingIDs[stage.ID] = true
		if stage.Type == workflow.StageTypeReview {
			existingReviews[reviewStageKey(stage)] = true
		} else {
			existingTypes[stage.Type] = true
		}
		row := workflowTemplateStageRow(stage, i+1, true, true)
		switch stage.Type {
		case workflow.StageTypeIdeaRefinement:
			starts = append(starts, row)
		case workflow.StageTypeStopReport:
			stops = append(stops, row)
		default:
			middles = append(middles, row)
		}
	}
	rows := append([]web.WorkflowTemplateStageRowData{}, starts...)
	rows = append(rows, middles...)
	optionalOrder := len(middles) + 2
	for _, stage := range optionalWorkflowStages() {
		if existingIDs[stage.ID] {
			continue
		}
		if stage.Type == workflow.StageTypeReview {
			if existingReviews[reviewStageKey(stage)] {
				continue
			}
		} else if existingTypes[stage.Type] {
			continue
		}
		rows = append(rows, workflowTemplateStageRow(stage, optionalOrder, false, false))
		optionalOrder++
	}
	rows = append(rows, stops...)
	return rows
}

func workflowTemplateStageRow(stage workflow.StageTemplate, order int, enabled bool, existing bool) web.WorkflowTemplateStageRowData {
	stage = workflow.NormalizeTemplate(workflow.Template{Stages: []workflow.StageTemplate{stage}}).Stages[0]
	mandatory := workflowStageAlwaysEnabled(stage.Type)
	return web.WorkflowTemplateStageRowData{
		ID:           stage.ID,
		Type:         stage.Type,
		Label:        stage.Label,
		Actor:        stage.Actor,
		Target:       stage.Target,
		Order:        order,
		Enabled:      enabled || mandatory,
		Existing:     existing,
		Mandatory:    mandatory,
		Disableable:  !mandatory,
		Reorderable:  stage.Type != workflow.StageTypeIdeaRefinement && stage.Type != workflow.StageTypeStopReport,
		Review:       stage.Type == workflow.StageTypeReview,
		Instructions: settingString(stage.Settings, "instructions"),
		Profile:      settingString(stage.Settings, "profile"),
		Intensity:    settingString(stage.Settings, "intensity"),
	}
}

func optionalWorkflowStages() []workflow.StageTemplate {
	return []workflow.StageTemplate{
		{ID: "plan_review_human", Type: workflow.StageTypeReview, Label: "Plan review", Actor: workflow.ActorHuman, Target: workflow.TargetPlan, Settings: defaultWorkflowStageSettings(workflow.StageTypeReview)},
		{ID: "validation", Type: workflow.StageTypeValidation, Label: "Validation", Actor: workflow.ActorHarness},
		{ID: "change_review_agent", Type: workflow.StageTypeReview, Label: "Code review", Actor: workflow.ActorAgent, Target: workflow.TargetCodeChanges, Settings: defaultWorkflowStageSettings(workflow.StageTypeReview)},
		{ID: "change_review_human", Type: workflow.StageTypeReview, Label: "Human code review", Actor: workflow.ActorHuman, Target: workflow.TargetCodeChanges, Settings: defaultWorkflowStageSettings(workflow.StageTypeReview)},
		{ID: "commit_feature_branch", Type: workflow.StageTypeCommit, Label: "Commit to feature branch", Actor: workflow.ActorHarness},
		{ID: "pr_creation", Type: workflow.StageTypePRCreation, Label: "PR creation", Actor: workflow.ActorHarness},
		{ID: "memory_update", Type: workflow.StageTypeMemoryUpdate, Label: "Memory update", Actor: workflow.ActorAgent},
	}
}

func workflowStageAlwaysEnabled(stageType string) bool {
	switch stageType {
	case workflow.StageTypeIdeaRefinement, workflow.StageTypeImplementation, workflow.StageTypeStopReport:
		return true
	default:
		return false
	}
}

func defaultWorkflowStageLabel(stageType string) string {
	switch stageType {
	case workflow.StageTypeIdeaRefinement:
		return "Idea refinement"
	case workflow.StageTypeImplementation:
		return "Implementation"
	case workflow.StageTypeValidation:
		return "Validation"
	case workflow.StageTypeReview:
		return "Review"
	case workflow.StageTypeCommit:
		return "Commit"
	case workflow.StageTypePRCreation:
		return "PR creation"
	case workflow.StageTypeMemoryUpdate:
		return "Memory update"
	case workflow.StageTypeStopReport:
		return "Stop/report"
	default:
		return strings.ReplaceAll(stageType, "_", " ")
	}
}

func defaultWorkflowStageActor(stageType string) string {
	switch stageType {
	case workflow.StageTypeImplementation, workflow.StageTypeReview, workflow.StageTypeMemoryUpdate:
		return workflow.ActorAgent
	default:
		return workflow.ActorHarness
	}
}

func defaultWorkflowStageSettings(stageType string) map[string]any {
	if stageType == workflow.StageTypeReview {
		return map[string]any{
			"profile":   contract.ReviewProfileGeneralist,
			"intensity": contract.ReviewIntensityNormal,
		}
	}
	return map[string]any{}
}

func reviewStageKey(stage workflow.StageTemplate) string {
	return stage.Actor + "\x00" + stage.Target
}

func workflowTemplateSettingsData(settings map[string]any) web.WorkflowTemplateSettingsData {
	return web.WorkflowTemplateSettingsData{
		BranchPolicy: settingString(settings, "branch_policy"),
		PRBehavior:   settingString(settings, "pr_behavior"),
		MergePolicy:  settingString(settings, "merge_policy"),
		FixLoop:      settingBool(settings, "fix_loop"),
		MaxFixLoops:  settingInt(settings, "max_fix_loops"),
	}
}

func copySettings(in map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

func setStringSetting(settings map[string]any, key, raw string) {
	value := strings.TrimSpace(raw)
	if value == "" {
		delete(settings, key)
		return
	}
	settings[key] = value
}

func settingString(settings map[string]any, key string) string {
	if settings == nil {
		return ""
	}
	value, ok := settings[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func settingBool(settings map[string]any, key string) bool {
	if settings == nil {
		return false
	}
	value, ok := settings[key]
	if !ok || value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes", "on", "enabled":
			return true
		default:
			return false
		}
	default:
		return strings.EqualFold(strings.TrimSpace(fmt.Sprint(value)), "true")
	}
}

func settingInt(settings map[string]any, key string) int {
	if settings == nil {
		return 0
	}
	value, ok := settings[key]
	if !ok || value == nil {
		return 0
	}
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(v))
		return n
	default:
		n, _ := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
		return n
	}
}

func workflowTemplateEditPath(templateID string) string {
	return "/templates/" + url.PathEscape(templateID) + "/edit"
}

func workflowTemplateNotice(r *http.Request) *web.Notice {
	switch r.URL.Query().Get("notice") {
	case "saved":
		return &web.Notice{Title: "Template saved", Message: "The workflow template was validated, edges were re-derived, and the editable copy was persisted."}
	case "copied":
		return &web.Notice{Title: "Template copied", Message: "You are editing a project template copy. Predefined templates remain unchanged."}
	default:
		return nil
	}
}
