package managerhttp

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
)

func (s *Server) handleGlobalAgentProfileSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	profile, err := agentProfileFromForm(r)
	if err != nil {
		s.writeGlobalAgentProfilesFragment(w, r, http.StatusBadRequest, err.Error(), "")
		return
	}
	overrides, err := s.store.GetGlobalAgentRegistryOverrides(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	baseline, baselineExists, err := globalAgentProfileSaveBaseline(overrides, profile.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	upsertAgentProfileOverride(&overrides, profile, baseline, baselineExists, r.Form.Get("default_profile") != "")
	if _, err := s.store.UpdateGlobalAgentRegistryOverrides(r.Context(), overrides); err != nil {
		s.writeGlobalAgentProfilesFragment(w, r, http.StatusBadRequest, err.Error(), "")
		return
	}
	s.writeGlobalAgentProfilesFragment(w, r, http.StatusAccepted, "", "saved global profile "+profile.ID)
}

func (s *Server) handleProjectAgentProfileSave(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	profile, err := agentProfileFromForm(r)
	if err != nil {
		s.writeProjectAgentProfilesFragment(w, r, http.StatusBadRequest, project, err.Error(), "")
		return
	}
	globalOverrides, err := s.store.GetGlobalAgentRegistryOverrides(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	overrides, err := s.store.GetProjectAgentRegistryOverrides(r.Context(), project.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	baseline, baselineExists, err := projectAgentProfileSaveBaseline(globalOverrides, overrides, profile.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	upsertAgentProfileOverride(&overrides, profile, baseline, baselineExists, r.Form.Get("default_profile") != "")
	if _, err := s.store.UpdateProjectAgentRegistryOverrides(r.Context(), project.ID, overrides); err != nil {
		s.writeProjectAgentProfilesFragment(w, r, http.StatusBadRequest, project, err.Error(), "")
		return
	}
	project, err = s.store.GetProject(r.Context(), project.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeProjectAgentProfilesFragment(w, r, http.StatusAccepted, project, "", "saved project profile "+profile.ID)
}

func (s *Server) handleGlobalAgentProfileDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		s.writeGlobalAgentProfilesFragment(w, r, http.StatusBadRequest, err.Error(), "")
		return
	}
	profileID := agentProfileIDFromForm(r)
	if profileID == "" {
		s.writeGlobalAgentProfilesFragment(w, r, http.StatusBadRequest, "profile_id is required", "")
		return
	}
	overrides, err := s.store.GetGlobalAgentRegistryOverrides(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	beforeDeleteOverrides := cloneAgentRegistryOverrides(overrides)
	deletedOverride, clearedDefault := deleteAgentProfileOverride(&overrides, profileID)
	if _, err := agentregistry.Resolve(overrides, agentregistry.Overrides{}); err != nil {
		s.writeGlobalAgentProfilesFragment(w, r, http.StatusBadRequest, err.Error(), "")
		return
	}
	var projectUpdates []projectAgentProfileOverrideUpdate
	if deletedOverride || clearedDefault {
		projectUpdates, err = s.projectAgentProfileOverrideUpdatesForGlobalDelete(r.Context(), profileID, beforeDeleteOverrides, overrides)
		if err != nil {
			s.writeGlobalAgentProfilesFragment(w, r, http.StatusBadRequest, err.Error(), "")
			return
		}
	}
	if err := s.ensureGlobalAgentProfileDeleteDoesNotBreakTemplates(r.Context(), profileID, overrides, projectUpdates); err != nil {
		s.writeGlobalAgentProfilesFragment(w, r, http.StatusBadRequest, err.Error(), "")
		return
	}
	if err := s.ensureProjectAgentRegistriesResolveAfterGlobalUpdate(r.Context(), profileID, overrides, projectUpdates); err != nil {
		s.writeGlobalAgentProfilesFragment(w, r, http.StatusBadRequest, err.Error(), "")
		return
	}
	if len(projectUpdates) > 0 && r.Form.Get("confirm_rebase") != "1" {
		s.writeGlobalAgentProfilesDeleteConfirmation(w, r, profileID, projectUpdates)
		return
	}
	if err := s.store.UpdateAgentRegistryOverridesAtomically(r.Context(), overrides, projectAgentProfileOverrideUpdatesByID(projectUpdates)); err != nil {
		s.writeGlobalAgentProfilesFragment(w, r, http.StatusBadRequest, err.Error(), "")
		return
	}
	s.writeGlobalAgentProfilesFragment(w, r, http.StatusAccepted, "", agentProfileDeleteStatus("global", profileID, deletedOverride, clearedDefault))
}

func (s *Server) handleProjectAgentProfileDelete(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		s.writeProjectAgentProfilesFragment(w, r, http.StatusBadRequest, project, err.Error(), "")
		return
	}
	profileID := agentProfileIDFromForm(r)
	if profileID == "" {
		s.writeProjectAgentProfilesFragment(w, r, http.StatusBadRequest, project, "profile_id is required", "")
		return
	}
	overrides, err := s.store.GetProjectAgentRegistryOverrides(r.Context(), project.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	deletedOverride, clearedDefault := deleteAgentProfileOverride(&overrides, profileID)
	globalOverrides, err := s.store.GetGlobalAgentRegistryOverrides(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	registryAfter, err := agentregistry.Resolve(globalOverrides, overrides)
	if err != nil {
		s.writeProjectAgentProfilesFragment(w, r, http.StatusBadRequest, project, err.Error(), "")
		return
	}
	if err := s.ensureAgentProfileDeleteDoesNotBreakTemplates(r.Context(), profileID, registryAfter); err != nil {
		s.writeProjectAgentProfilesFragment(w, r, http.StatusBadRequest, project, err.Error(), "")
		return
	}
	if _, err := s.store.UpdateProjectAgentRegistryOverrides(r.Context(), project.ID, overrides); err != nil {
		s.writeProjectAgentProfilesFragment(w, r, http.StatusBadRequest, project, err.Error(), "")
		return
	}
	project, err = s.store.GetProject(r.Context(), project.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeProjectAgentProfilesFragment(w, r, http.StatusAccepted, project, "", agentProfileDeleteStatus("project", profileID, deletedOverride, clearedDefault))
}

func (s *Server) handleGlobalAgentProfileClearDefault(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	overrides, err := s.store.GetGlobalAgentRegistryOverrides(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	alreadyInherited := overrides.DefaultProfileID == nil
	overrides.DefaultProfileID = nil
	if _, err := s.store.UpdateGlobalAgentRegistryOverrides(r.Context(), overrides); err != nil {
		s.writeGlobalAgentProfilesFragment(w, r, http.StatusBadRequest, err.Error(), "")
		return
	}
	s.writeGlobalAgentProfilesFragment(w, r, http.StatusAccepted, "", agentProfileClearDefaultStatus("global", alreadyInherited))
}

func (s *Server) handleProjectAgentProfileClearDefault(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	overrides, err := s.store.GetProjectAgentRegistryOverrides(r.Context(), project.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	alreadyInherited := overrides.DefaultProfileID == nil
	overrides.DefaultProfileID = nil
	if _, err := s.store.UpdateProjectAgentRegistryOverrides(r.Context(), project.ID, overrides); err != nil {
		s.writeProjectAgentProfilesFragment(w, r, http.StatusBadRequest, project, err.Error(), "")
		return
	}
	project, err = s.store.GetProject(r.Context(), project.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeProjectAgentProfilesFragment(w, r, http.StatusAccepted, project, "", agentProfileClearDefaultStatus("project", alreadyInherited))
}

func (s *Server) globalAgentProfileEditorData(r *http.Request, notice, status string) (web.AgentProfileEditorData, error) {
	overrides, err := s.store.GetGlobalAgentRegistryOverrides(r.Context())
	if err != nil {
		return web.AgentProfileEditorData{}, err
	}
	registry, err := s.store.ResolveGlobalAgentRegistry(r.Context())
	if err != nil {
		return web.AgentProfileEditorData{}, err
	}
	savePath := "/settings/agent-profiles"
	return agentProfileEditorData(agentProfileEditorInput{
		Scope:             "global",
		Title:             "Global agent profiles",
		Help:              "View, create, and edit global default profile definitions. TOML agent settings are still readable; this in-app editor is the supported path for ongoing changes.",
		SavePath:          savePath,
		DeletePath:        savePath + "/delete",
		ClearDefaultPath:  savePath + "/clear-default",
		Registry:          registry,
		InheritedRegistry: agentregistry.Defaults(),
		Overrides:         overrides,
		OverrideLabel:     "global override",
		InheritedLabel:    "built-in default",
		Notice:            notice,
		Status:            status,
		CSRF:              csrfFromContext(r.Context()),
		DefaultProfileID:  registry.DefaultProfileID,
	}), nil
}

func (s *Server) projectAgentProfileEditorData(r *http.Request, project store.Project, notice, status string) (web.AgentProfileEditorData, error) {
	overrides, err := s.store.GetProjectAgentRegistryOverrides(r.Context(), project.ID)
	if err != nil {
		return web.AgentProfileEditorData{}, err
	}
	inherited, err := s.store.ResolveGlobalAgentRegistry(r.Context())
	if err != nil {
		return web.AgentProfileEditorData{}, err
	}
	registry, err := s.store.ResolveAgentRegistry(r.Context(), project.ID)
	if err != nil {
		return web.AgentProfileEditorData{}, err
	}
	savePath := "/projects/" + project.ID + "/settings/agent-profiles"
	return agentProfileEditorData(agentProfileEditorInput{
		Scope:             "project-" + project.ID,
		Title:             "Project agent profile overrides",
		Help:              "View the resolved profile set and create or edit project-layer overrides without mutating global defaults.",
		SavePath:          savePath,
		DeletePath:        savePath + "/delete",
		ClearDefaultPath:  savePath + "/clear-default",
		Registry:          registry,
		InheritedRegistry: inherited,
		Overrides:         overrides,
		OverrideLabel:     "project override",
		InheritedLabel:    "inherited global",
		Notice:            notice,
		Status:            status,
		CSRF:              csrfFromContext(r.Context()),
		DefaultProfileID:  registry.DefaultProfileID,
	}), nil
}

type agentProfileEditorInput struct {
	Scope             string
	Title             string
	Help              string
	SavePath          string
	DeletePath        string
	ClearDefaultPath  string
	Registry          agentregistry.Registry
	InheritedRegistry agentregistry.Registry
	Overrides         agentregistry.Overrides
	OverrideLabel     string
	InheritedLabel    string
	Notice            string
	Status            string
	CSRF              string
	DefaultProfileID  string
}

func agentProfileEditorData(input agentProfileEditorInput) web.AgentProfileEditorData {
	profiles := append([]agentregistry.Profile(nil), input.Registry.Profiles...)
	sort.Slice(profiles, func(i, j int) bool {
		if profiles[i].Name == profiles[j].Name {
			return profiles[i].ID < profiles[j].ID
		}
		return profiles[i].Name < profiles[j].Name
	})
	views := make([]web.AgentProfileFormData, 0, len(profiles))
	for _, profile := range profiles {
		layer := input.InheritedLabel
		canDelete := profileOverrideExists(input.Overrides, profile.ID)
		deleteLabel := ""
		if canDelete {
			layer = input.OverrideLabel
			deleteLabel = "Delete profile"
			if _, ok := agentregistry.ProfileByID(input.InheritedRegistry, profile.ID); ok {
				deleteLabel = "Revert override"
			}
		}
		views = append(views, agentProfileFormData(profile, input.DefaultProfileID, layer, canDelete, deleteLabel))
	}
	return web.AgentProfileEditorData{
		Scope:            input.Scope,
		Title:            input.Title,
		Help:             input.Help,
		Profiles:         views,
		Create:           defaultNewAgentProfileForm(),
		SavePath:         input.SavePath,
		DeletePath:       input.DeletePath,
		ClearDefaultPath: input.ClearDefaultPath,
		DefaultProfileID: input.DefaultProfileID,
		CanClearDefault:  input.Overrides.DefaultProfileID != nil,
		Notice:           input.Notice,
		Status:           input.Status,
		CSRF:             input.CSRF,
	}
}

func agentProfileFormData(profile agentregistry.Profile, defaultProfileID, layer string, canDelete bool, deleteLabel string) web.AgentProfileFormData {
	return web.AgentProfileFormData{
		ID:                  profile.ID,
		FamilyID:            profile.FamilyID,
		Name:                profile.Name,
		Description:         profile.Description,
		Role:                profile.Role,
		Headless:            profile.Headless,
		Prompt:              profile.Prompt,
		DefaultInstructions: profile.DefaultInstructions,
		Model:               profile.Model,
		ContextPolicy:       profile.ContextPolicy,
		OutputStyle:         profile.OutputStyle,
		SuggestedStageTypes: strings.Join(profile.SuggestedStageTypes, ", "),
		Layer:               layer,
		IsDefault:           profile.ID == defaultProfileID,
		CanDelete:           canDelete,
		DeleteLabel:         deleteLabel,
	}
}

func defaultNewAgentProfileForm() web.AgentProfileFormData {
	return web.AgentProfileFormData{
		FamilyID:            agentregistry.FamilyPi,
		Role:                "implementation",
		Headless:            true,
		ContextPolicy:       "task_contract_only",
		OutputStyle:         "structured_report",
		SuggestedStageTypes: contract.StageTypeImplementation,
	}
}

func agentProfileFromForm(r *http.Request) (agentregistry.Profile, error) {
	profile := agentregistry.Profile{
		ID:                  strings.ToLower(strings.TrimSpace(r.Form.Get("profile_id"))),
		FamilyID:            strings.ToLower(strings.TrimSpace(r.Form.Get("family_id"))),
		Name:                strings.TrimSpace(r.Form.Get("name")),
		Description:         strings.TrimSpace(r.Form.Get("description")),
		Role:                strings.ToLower(strings.TrimSpace(r.Form.Get("role"))),
		Headless:            r.Form.Get("headless") != "",
		Prompt:              strings.TrimSpace(r.Form.Get("prompt")),
		DefaultInstructions: strings.TrimSpace(r.Form.Get("default_instructions")),
		Model:               strings.TrimSpace(r.Form.Get("model")),
		ContextPolicy:       strings.TrimSpace(r.Form.Get("context_policy")),
		OutputStyle:         strings.TrimSpace(r.Form.Get("output_style")),
		SuggestedStageTypes: parseSuggestedStageTypes(r.Form.Get("suggested_stage_types")),
	}
	if profile.ID == "" {
		return agentregistry.Profile{}, fmt.Errorf("profile_id is required")
	}
	if profile.FamilyID == "" {
		profile.FamilyID = agentregistry.FamilyPi
	}
	if profile.Name == "" {
		return agentregistry.Profile{}, fmt.Errorf("name is required")
	}
	if profile.Role == "" {
		return agentregistry.Profile{}, fmt.Errorf("role is required")
	}
	if profile.ContextPolicy == "" {
		return agentregistry.Profile{}, fmt.Errorf("context_policy is required")
	}
	if profile.OutputStyle == "" {
		return agentregistry.Profile{}, fmt.Errorf("output_style is required")
	}
	return profile, nil
}

func parseSuggestedStageTypes(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == '\n' || r == '\r' || r == '\t' })
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}

func upsertAgentProfileOverride(overrides *agentregistry.Overrides, profile agentregistry.Profile, baseline agentregistry.Profile, baselineExists bool, makeDefault bool) {
	override := agentregistry.ProfileOverrideFromProfileDiff(profile, baseline)
	if baselineExists && agentProfileOverrideIsEmpty(override) {
		removeProfileOverrideEntry(overrides.Profiles, profile.ID)
	} else {
		if overrides.Profiles == nil {
			overrides.Profiles = map[string]agentregistry.ProfileOverride{}
		}
		overrides.Profiles[profile.ID] = override
	}
	if makeDefault {
		profileID := profile.ID
		overrides.DefaultProfileID = &profileID
		return
	}
	if profileIDPtrEqual(overrides.DefaultProfileID, profile.ID) {
		overrides.DefaultProfileID = nil
	}
}

func deleteAgentProfileOverride(overrides *agentregistry.Overrides, profileID string) (bool, bool) {
	profileID = normalizeAgentProfileID(profileID)
	deletedOverride := false
	for id := range overrides.Profiles {
		if agentProfileIDEqual(id, profileID) {
			delete(overrides.Profiles, id)
			deletedOverride = true
		}
	}
	clearedDefault := false
	if profileIDPtrEqual(overrides.DefaultProfileID, profileID) {
		overrides.DefaultProfileID = nil
		clearedDefault = true
	}
	return deletedOverride, clearedDefault
}

func profileOverrideExists(overrides agentregistry.Overrides, profileID string) bool {
	for id := range overrides.Profiles {
		if agentProfileIDEqual(id, profileID) {
			return true
		}
	}
	return false
}

func agentProfileIDFromForm(r *http.Request) string {
	return normalizeAgentProfileID(r.Form.Get("profile_id"))
}

func normalizeAgentProfileID(profileID string) string {
	return strings.ToLower(strings.TrimSpace(profileID))
}

func agentProfileIDEqual(a, b string) bool {
	return normalizeAgentProfileID(a) == normalizeAgentProfileID(b)
}

func profileIDPtrEqual(ptr *string, profileID string) bool {
	return ptr != nil && agentProfileIDEqual(*ptr, profileID)
}

func agentProfileDeleteStatus(scope, profileID string, deletedOverride, clearedDefault bool) string {
	if deletedOverride && clearedDefault {
		return "deleted " + scope + " profile override " + profileID + " and cleared the " + scope + " default profile override"
	}
	if deletedOverride {
		return "deleted " + scope + " profile override " + profileID
	}
	if clearedDefault {
		return "cleared the " + scope + " default profile override for " + profileID
	}
	return "no " + scope + " profile override existed for " + profileID
}

func agentProfileClearDefaultStatus(scope string, alreadyInherited bool) string {
	if alreadyInherited {
		return scope + " default profile already inherits from the lower layer"
	}
	return "cleared " + scope + " default profile override"
}

func (s *Server) ensureAgentProfileDeleteDoesNotBreakTemplates(ctx context.Context, profileID string, registryAfter agentregistry.Registry) error {
	if _, ok := agentregistry.ProfileByID(registryAfter, profileID); ok {
		return nil
	}
	referencing, err := s.workflowTemplatesReferencingProfile(ctx, profileID)
	if err != nil {
		return err
	}
	if len(referencing) > 0 {
		return fmt.Errorf("cannot delete profile %s; workflow template(s) %s reference it", profileID, strings.Join(referencing, ", "))
	}
	return nil
}

type projectAgentProfileOverrideUpdate struct {
	project   store.Project
	overrides agentregistry.Overrides
}

func (s *Server) ensureGlobalAgentProfileDeleteDoesNotBreakTemplates(ctx context.Context, profileID string, globalOverrides agentregistry.Overrides, projectUpdates []projectAgentProfileOverrideUpdate) error {
	referencing, err := s.workflowTemplatesReferencingProfile(ctx, profileID)
	if err != nil {
		return err
	}
	if len(referencing) == 0 {
		return nil
	}
	projects, err := s.store.ListProjects(ctx)
	if err != nil {
		return err
	}
	projectUpdatesByID := projectAgentProfileOverrideUpdatesByID(projectUpdates)
	var unresolvedProjects []string
	for _, project := range projects {
		registry, err := s.resolveAgentRegistryAfterGlobalUpdate(ctx, project.ID, globalOverrides, projectUpdatesByID)
		if err != nil {
			return err
		}
		if _, ok := agentregistry.ProfileByID(registry, profileID); ok {
			continue
		}
		unresolvedProjects = append(unresolvedProjects, agentProfileProjectLabel(project))
	}
	if len(unresolvedProjects) > 0 {
		sort.Strings(unresolvedProjects)
		return fmt.Errorf("cannot delete profile %s; workflow template(s) %s reference it and project(s) %s would no longer resolve it", profileID, strings.Join(referencing, ", "), strings.Join(unresolvedProjects, ", "))
	}
	return nil
}

func (s *Server) workflowTemplatesReferencingProfile(ctx context.Context, profileID string) ([]string, error) {
	templates, err := s.store.ListWorkflowTemplates(ctx)
	if err != nil {
		return nil, err
	}
	var referencing []string
	for _, template := range templates {
		if workflowTemplateReferencesProfile(template, profileID) {
			label := template.ID
			if strings.TrimSpace(template.Name) != "" {
				label = template.Name + " (" + template.ID + ")"
			}
			referencing = append(referencing, label)
		}
	}
	sort.Strings(referencing)
	return referencing, nil
}

func workflowTemplateReferencesProfile(template workflow.Template, profileID string) bool {
	for _, stage := range template.Stages {
		if agentProfileIDEqual(stage.ProfileID, profileID) {
			return true
		}
	}
	return false
}

func (s *Server) ensureProjectAgentRegistriesResolveAfterGlobalUpdate(ctx context.Context, profileID string, globalOverrides agentregistry.Overrides, projectUpdates []projectAgentProfileOverrideUpdate) error {
	projects, err := s.store.ListProjects(ctx)
	if err != nil {
		return err
	}
	projectUpdatesByID := projectAgentProfileOverrideUpdatesByID(projectUpdates)
	for _, project := range projects {
		if _, err := s.resolveAgentRegistryAfterGlobalUpdate(ctx, project.ID, globalOverrides, projectUpdatesByID); err != nil {
			return fmt.Errorf("cannot delete profile %s; project %s has agent registry overrides that would no longer resolve: %w", profileID, project.ID, err)
		}
	}
	return nil
}

func (s *Server) resolveAgentRegistryAfterGlobalUpdate(ctx context.Context, projectID string, globalOverrides agentregistry.Overrides, projectUpdates map[string]agentregistry.Overrides) (agentregistry.Registry, error) {
	if projectOverrides, ok := projectUpdates[projectID]; ok {
		return agentregistry.Resolve(globalOverrides, projectOverrides)
	}
	projectOverrides, err := s.store.GetProjectAgentRegistryOverrides(ctx, projectID)
	if err != nil {
		return agentregistry.Registry{}, err
	}
	return agentregistry.Resolve(globalOverrides, projectOverrides)
}

func (s *Server) projectAgentProfileOverrideUpdatesForGlobalDelete(ctx context.Context, profileID string, beforeGlobalOverrides, afterGlobalOverrides agentregistry.Overrides) ([]projectAgentProfileOverrideUpdate, error) {
	projects, err := s.store.ListProjects(ctx)
	if err != nil {
		return nil, err
	}
	var updates []projectAgentProfileOverrideUpdate
	for _, project := range projects {
		projectOverrides, err := s.store.GetProjectAgentRegistryOverrides(ctx, project.ID)
		if err != nil {
			return nil, err
		}
		if !profileOverrideExists(projectOverrides, profileID) {
			continue
		}
		registryBeforeDelete, err := agentregistry.Resolve(beforeGlobalOverrides, projectOverrides)
		if err != nil {
			return nil, fmt.Errorf("cannot delete profile %s; project %s has agent registry overrides that do not currently resolve: %w", profileID, project.ID, err)
		}
		profileBeforeDelete, ok := agentregistry.ProfileByID(registryBeforeDelete, profileID)
		if !ok {
			continue
		}
		rebased := cloneAgentRegistryOverrides(projectOverrides)
		baseline, baselineExists, err := projectAgentProfileSaveBaseline(afterGlobalOverrides, rebased, profileID)
		if err != nil {
			return nil, fmt.Errorf("cannot delete profile %s; project %s profile override cannot be rebased: %w", profileID, project.ID, err)
		}
		makeDefault := agentProfileIDEqual(registryBeforeDelete.DefaultProfileID, profileID)
		upsertAgentProfileOverride(&rebased, profileBeforeDelete, baseline, baselineExists, makeDefault)
		registryAfterDelete, err := agentregistry.Resolve(afterGlobalOverrides, rebased)
		if err != nil {
			return nil, fmt.Errorf("cannot delete profile %s; project %s rebased agent registry does not resolve: %w", profileID, project.ID, err)
		}
		if !reflect.DeepEqual(registryBeforeDelete, registryAfterDelete) {
			return nil, fmt.Errorf("cannot delete profile %s; project %s rebase would change its effective agent registry", profileID, project.ID)
		}
		if reflect.DeepEqual(projectOverrides, rebased) {
			continue
		}
		updates = append(updates, projectAgentProfileOverrideUpdate{project: project, overrides: rebased})
	}
	sort.Slice(updates, func(i, j int) bool {
		return agentProfileProjectLabel(updates[i].project) < agentProfileProjectLabel(updates[j].project)
	})
	return updates, nil
}

func projectAgentProfileOverrideUpdatesByID(updates []projectAgentProfileOverrideUpdate) map[string]agentregistry.Overrides {
	byID := make(map[string]agentregistry.Overrides, len(updates))
	for _, update := range updates {
		byID[update.project.ID] = update.overrides
	}
	return byID
}

func agentProfileProjectLabel(project store.Project) string {
	if strings.TrimSpace(project.Name) != "" {
		return project.Name + " (" + project.ID + ")"
	}
	return project.ID
}

func globalAgentProfileSaveBaseline(overrides agentregistry.Overrides, profileID string) (agentregistry.Profile, bool, error) {
	baselineOverrides := cloneAgentRegistryOverrides(overrides)
	removeProfileOverrideEntry(baselineOverrides.Profiles, profileID)
	clearDefaultProfileOverrideIfMatches(&baselineOverrides, profileID)
	registry, err := agentregistry.Resolve(baselineOverrides, agentregistry.Overrides{})
	if err != nil {
		return agentregistry.Profile{}, false, err
	}
	profile, ok := agentregistry.ProfileByID(registry, profileID)
	return profile, ok, nil
}

func projectAgentProfileSaveBaseline(globalOverrides agentregistry.Overrides, projectOverrides agentregistry.Overrides, profileID string) (agentregistry.Profile, bool, error) {
	baselineProjectOverrides := cloneAgentRegistryOverrides(projectOverrides)
	removeProfileOverrideEntry(baselineProjectOverrides.Profiles, profileID)
	clearDefaultProfileOverrideIfMatches(&baselineProjectOverrides, profileID)
	registry, err := agentregistry.Resolve(globalOverrides, baselineProjectOverrides)
	if err != nil {
		return agentregistry.Profile{}, false, err
	}
	profile, ok := agentregistry.ProfileByID(registry, profileID)
	return profile, ok, nil
}

func agentProfileOverrideIsEmpty(override agentregistry.ProfileOverride) bool {
	return override.FamilyID == nil &&
		override.Name == nil &&
		override.Description == nil &&
		override.Role == nil &&
		override.Headless == nil &&
		override.Prompt == nil &&
		override.DefaultInstructions == nil &&
		override.Model == nil &&
		override.ContextPolicy == nil &&
		override.OutputStyle == nil &&
		override.SuggestedStageTypes == nil
}

func cloneAgentRegistryOverrides(overrides agentregistry.Overrides) agentregistry.Overrides {
	clone := agentregistry.Overrides{
		DefaultProfileID: cloneStringPtr(overrides.DefaultProfileID),
	}
	if overrides.Families != nil {
		clone.Families = make(map[string]agentregistry.FamilyOverride, len(overrides.Families))
		for id, override := range overrides.Families {
			clone.Families[id] = cloneAgentFamilyOverride(override)
		}
	}
	if overrides.Profiles != nil {
		clone.Profiles = make(map[string]agentregistry.ProfileOverride, len(overrides.Profiles))
		for id, override := range overrides.Profiles {
			clone.Profiles[id] = cloneAgentProfileOverride(override)
		}
	}
	return clone
}

func cloneAgentFamilyOverride(override agentregistry.FamilyOverride) agentregistry.FamilyOverride {
	return agentregistry.FamilyOverride{
		Name:        cloneStringPtr(override.Name),
		Description: cloneStringPtr(override.Description),
		Status:      cloneStringPtr(override.Status),
	}
}

func cloneAgentProfileOverride(override agentregistry.ProfileOverride) agentregistry.ProfileOverride {
	return agentregistry.ProfileOverride{
		FamilyID:            cloneStringPtr(override.FamilyID),
		Name:                cloneStringPtr(override.Name),
		Description:         cloneStringPtr(override.Description),
		Role:                cloneStringPtr(override.Role),
		Headless:            cloneBoolPtr(override.Headless),
		Prompt:              cloneStringPtr(override.Prompt),
		DefaultInstructions: cloneStringPtr(override.DefaultInstructions),
		Model:               cloneStringPtr(override.Model),
		ContextPolicy:       cloneStringPtr(override.ContextPolicy),
		OutputStyle:         cloneStringPtr(override.OutputStyle),
		SuggestedStageTypes: cloneStringListPreserveNil(override.SuggestedStageTypes),
	}
}

func cloneStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneStringListPreserveNil(values []string) []string {
	if values == nil {
		return nil
	}
	clone := make([]string, len(values))
	copy(clone, values)
	return clone
}

func removeProfileOverrideEntry(overrides map[string]agentregistry.ProfileOverride, profileID string) {
	profileID = strings.ToLower(strings.TrimSpace(profileID))
	for id := range overrides {
		if strings.ToLower(strings.TrimSpace(id)) == profileID {
			delete(overrides, id)
		}
	}
}

func clearDefaultProfileOverrideIfMatches(overrides *agentregistry.Overrides, profileID string) {
	if overrides.DefaultProfileID == nil {
		return
	}
	if strings.ToLower(strings.TrimSpace(*overrides.DefaultProfileID)) == strings.ToLower(strings.TrimSpace(profileID)) {
		overrides.DefaultProfileID = nil
	}
}

func (s *Server) writeGlobalAgentProfilesFragment(w http.ResponseWriter, r *http.Request, statusCode int, notice, status string) {
	data, err := s.globalAgentProfileEditorData(r, notice, status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeAgentProfilesFragment(w, statusCode, data)
}

func (s *Server) writeGlobalAgentProfilesDeleteConfirmation(w http.ResponseWriter, r *http.Request, profileID string, updates []projectAgentProfileOverrideUpdate) {
	data, err := s.globalAgentProfileEditorData(r, "", "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	projects := make([]string, 0, len(updates))
	for _, update := range updates {
		projects = append(projects, agentProfileProjectLabel(update.project))
	}
	data.DeleteConfirmation = &web.AgentProfileDeleteConfirmationData{
		ProfileID: profileID,
		Projects:  projects,
	}
	s.writeAgentProfilesFragment(w, http.StatusOK, data)
}

func (s *Server) writeProjectAgentProfilesFragment(w http.ResponseWriter, r *http.Request, statusCode int, project store.Project, notice, status string) {
	data, err := s.projectAgentProfileEditorData(r, project, notice, status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeAgentProfilesFragment(w, statusCode, data)
}

func (s *Server) writeAgentProfilesFragment(w http.ResponseWriter, statusCode int, data web.AgentProfileEditorData) {
	fragment, err := s.renderer.ExecutePage("agent_profiles.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(fragment)))
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(fragment))
}
