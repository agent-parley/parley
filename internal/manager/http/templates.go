package managerhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
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
	registry, err := s.store.ResolveAgentRegistry(r.Context(), store.DefaultProjectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	updated, err := workflowTemplateFromForm(r, current, registry)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := workflow.ValidateTemplateWithRegistry(updated, registry); err != nil {
		data, dataErr := s.workflowTemplateEditData(r, updated, nil, err.Error())
		if dataErr != nil {
			http.Error(w, dataErr.Error(), http.StatusInternalServerError)
			return
		}
		s.writePageStatus(w, "workflow_template_edit.html", data, http.StatusBadRequest)
		return
	}
	if err := s.store.UpdateWorkflowTemplateWithRegistry(r.Context(), updated, registry); err != nil {
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
	registry, err := s.store.ResolveAgentRegistry(r.Context(), store.DefaultProjectID)
	if err != nil {
		return web.WorkflowTemplateEditData{}, err
	}
	return web.WorkflowTemplateEditData{
		Template:      workflow.NormalizeTemplateWithRegistry(template, registry),
		Settings:      workflowTemplateSettingsData(template.Settings),
		StageRows:     workflowTemplateStageRowsWithRegistry(template, registry),
		ReviewTargets: contract.ReviewTargetOptions(),
		AgentProfiles: registry.Profiles,
		SavePath:      "/templates/" + url.PathEscape(template.ID) + "/save",
		BackPath:      "/templates",
		Breadcrumb:    "Workflow templates · " + template.ID,
		Heading:       "Edit workflow template",
		Help:          "Use forms for stage settings and order. Start, end, and fix-loop edges are derived by the harness every time this template is saved.",
		SubmitLabel:   "Save template",
		Notifications: notifications,
		Notice:        notice,
		Error:         message,
		CSRF:          csrf,
		Title:         "Parley · Edit " + template.Name,
	}, nil
}

func (s *Server) newRunWorkflowData(r *http.Request, projectID, selectedTemplateID, message string) (web.NewRunWorkflowData, error) {
	templates, err := s.store.ListWorkflowTemplates(r.Context())
	if err != nil {
		return web.NewRunWorkflowData{}, err
	}
	selectedTemplateID = strings.TrimSpace(selectedTemplateID)
	if selectedTemplateID == "" {
		selectedTemplateID = strings.TrimSpace(r.URL.Query().Get("workflow_template_id"))
	}
	if selectedTemplateID == "" {
		selectedTemplateID = workflow.DefaultTemplateID
	}
	summaries := make([]web.WorkflowTemplateSummaryData, 0, len(templates))
	var selected workflow.Template
	for _, template := range templates {
		summaries = append(summaries, web.WorkflowTemplateSummaryData{
			ID:          template.ID,
			Name:        template.Name,
			Description: template.Description,
			Predefined:  template.Predefined,
			Recommended: template.Recommended,
			Editable:    template.Editable,
			StageCount:  len(template.Stages),
		})
		if template.ID == selectedTemplateID {
			selected = template
		}
	}
	if selected.ID == "" {
		selectedTemplateID = workflow.DefaultTemplateID
		for _, template := range templates {
			if template.ID == selectedTemplateID {
				selected = template
				break
			}
		}
	}
	if selected.ID == "" {
		return web.NewRunWorkflowData{}, fmt.Errorf("workflow template %s not found", selectedTemplateID)
	}
	registry, err := s.store.ResolveAgentRegistry(r.Context(), projectID)
	if err != nil {
		return web.NewRunWorkflowData{}, err
	}
	selected = workflow.NormalizeTemplateWithRegistry(selected, registry)
	return web.NewRunWorkflowData{
		Templates:          summaries,
		SelectedTemplateID: selectedTemplateID,
		Template:           selected,
		Settings:           workflowTemplateSettingsData(selected.Settings),
		StageRows:          workflowTemplateStageRowsWithRegistry(selected, registry),
		ReviewTargets:      contract.ReviewTargetOptions(),
		AgentProfiles:      registry.Profiles,
		Error:              message,
	}, nil
}

func workflowTemplateFromForm(r *http.Request, current workflow.Template, registry agentregistry.Registry) (workflow.Template, error) {
	current = workflow.NormalizeTemplateWithRegistry(current, registry)
	updated := current
	updated.Name = strings.TrimSpace(r.Form.Get("name"))
	updated.Description = strings.TrimSpace(r.Form.Get("description"))
	updated.Settings = copySettings(current.Settings)
	setStringSetting(updated.Settings, "branch_policy", r.Form.Get("branch_policy"))
	setStringSetting(updated.Settings, "pr_behavior", r.Form.Get("pr_behavior"))
	setStringSetting(updated.Settings, "merge_policy", r.Form.Get("merge_policy"))
	setStringSetting(updated.Settings, "required_checks", r.Form.Get("required_checks"))
	setStringSetting(updated.Settings, "forge_credential", r.Form.Get("forge_credential"))
	setStringSetting(updated.Settings, "merge_wait_timeout", r.Form.Get("merge_wait_timeout"))
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
		stage.Instructions = strings.TrimSpace(r.Form.Get("instructions_" + fieldKey))
		stage.ProfileID = ""
		if stage.Actor == workflow.ActorAgent {
			stage.ProfileID = strings.TrimSpace(r.Form.Get("profile_id_" + fieldKey))
		}
		required := r.Form.Get("required_"+fieldKey) != ""
		stage.Required = &required
		contextSettings, err := parseWorkflowStageContextSettings(r.Form.Get("context_settings_" + fieldKey))
		if err != nil {
			return workflow.Template{}, fmt.Errorf("stage %q context_settings %w", stageID, err)
		}
		stage.ContextSettings = contextSettings
		stage.Timeout = strings.TrimSpace(r.Form.Get("timeout_" + fieldKey))
		maxAttempts, err := parseWorkflowStageMaxAttempts(r.Form.Get("max_attempts_" + fieldKey))
		if err != nil {
			return workflow.Template{}, fmt.Errorf("stage %q max_attempts %w", stageID, err)
		}
		stage.MaxAttempts = maxAttempts
		if existingStage, ok := existing[stageID]; !ok || existingStage.Type != stage.Type {
			stage.Settings = defaultWorkflowStageSettings(stage.Type)
		} else {
			stage.Settings = copySettings(existingStage.Settings)
		}
		delete(stage.Settings, "instructions")
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
	return workflow.NormalizeTemplateWithRegistry(updated, registry), nil
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

func parseWorkflowStageContextSettings(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var settings map[string]any
	if err := json.Unmarshal([]byte(raw), &settings); err != nil {
		return nil, fmt.Errorf("must be a JSON object")
	}
	if len(settings) == 0 {
		return nil, nil
	}
	return settings, nil
}

func parseWorkflowStageMaxAttempts(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 1, nil
	}
	attempts, err := strconv.Atoi(raw)
	if err != nil || attempts < 1 {
		return 0, fmt.Errorf("must be a positive integer")
	}
	return attempts, nil
}

func workflowTemplateStageRows(template workflow.Template) []web.WorkflowTemplateStageRowData {
	return workflowTemplateStageRowsWithRegistry(template, agentregistry.Defaults())
}

func workflowTemplateStageRowsWithRegistry(template workflow.Template, registry agentregistry.Registry) []web.WorkflowTemplateStageRowData {
	template = workflow.NormalizeTemplateWithRegistry(template, registry)
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
		row := workflowTemplateStageRow(stage, registry, i+1, true, true)
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
		rows = append(rows, workflowTemplateStageRow(stage, registry, optionalOrder, false, false))
		optionalOrder++
	}
	rows = append(rows, stops...)
	return rows
}

func workflowTemplateStageRow(stage workflow.StageTemplate, registry agentregistry.Registry, order int, enabled bool, existing bool) web.WorkflowTemplateStageRowData {
	stage = workflow.NormalizeTemplateWithRegistry(workflow.Template{Stages: []workflow.StageTemplate{stage}}, registry).Stages[0]
	mandatory := workflowStageAlwaysEnabled(stage.Type)
	return web.WorkflowTemplateStageRowData{
		ID:              stage.ID,
		Type:            stage.Type,
		Label:           stage.Label,
		Actor:           stage.Actor,
		Target:          stage.Target,
		Order:           order,
		Enabled:         enabled || mandatory,
		Existing:        existing,
		Mandatory:       mandatory,
		Disableable:     !mandatory,
		Reorderable:     stage.Type != workflow.StageTypeIdeaRefinement && stage.Type != workflow.StageTypeStopReport,
		Agent:           stage.Actor == workflow.ActorAgent,
		Review:          stage.Type == workflow.StageTypeReview,
		Instructions:    stage.Instructions,
		ProfileID:       stage.ProfileID,
		Required:        workflow.StageRequired(stage),
		ContextSettings: workflowTemplateContextSettingsValue(stage.ContextSettings),
		Timeout:         stage.Timeout,
		MaxAttempts:     stage.MaxAttempts,
		Profile:         settingString(stage.Settings, "profile"),
		Intensity:       settingString(stage.Settings, "intensity"),
	}
}

func workflowTemplateContextSettingsValue(settings map[string]any) string {
	if len(settings) == 0 {
		return ""
	}
	content, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return ""
	}
	return string(content)
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
		BranchPolicy:     settingString(settings, "branch_policy"),
		PRBehavior:       settingString(settings, "pr_behavior"),
		MergePolicy:      settingString(settings, "merge_policy"),
		RequiredChecks:   strings.Join(settingStringList(settings, "required_checks"), ", "),
		ForgeCredential:  firstSettingString(settings, "forge_credential", "forge_credential_id", "credential_ref"),
		MergeWaitTimeout: settingString(settings, "merge_wait_timeout"),
		FixLoop:          settingBool(settings, "fix_loop"),
		MaxFixLoops:      settingInt(settings, "max_fix_loops"),
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

func firstSettingString(settings map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := settingString(settings, key); value != "" {
			return value
		}
	}
	return ""
}

func settingStringList(settings map[string]any, key string) []string {
	if settings == nil {
		return nil
	}
	value, ok := settings[key]
	if !ok || value == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	add := func(raw string) {
		for _, part := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ';' || r == '\n' || r == '\r' }) {
			part = strings.TrimSpace(part)
			if part == "" || seen[part] {
				continue
			}
			seen[part] = true
			out = append(out, part)
		}
	}
	switch v := value.(type) {
	case []string:
		for _, item := range v {
			add(item)
		}
	case []any:
		for _, item := range v {
			add(fmt.Sprint(item))
		}
	default:
		add(fmt.Sprint(v))
	}
	return out
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
