package managerhttp

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
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

func (s *Server) globalAgentProfileEditorData(r *http.Request, notice, status string) (web.AgentProfileEditorData, error) {
	overrides, err := s.store.GetGlobalAgentRegistryOverrides(r.Context())
	if err != nil {
		return web.AgentProfileEditorData{}, err
	}
	registry, err := s.store.ResolveGlobalAgentRegistry(r.Context())
	if err != nil {
		return web.AgentProfileEditorData{}, err
	}
	return agentProfileEditorData(agentProfileEditorInput{
		Scope:            "global",
		Title:            "Global agent profiles",
		Help:             "View, create, and edit global default profile definitions. TOML agent settings are still readable; this in-app editor is the supported path for ongoing changes.",
		SavePath:         "/settings/agent-profiles",
		Registry:         registry,
		Overrides:        overrides,
		OverrideLabel:    "global override",
		InheritedLabel:   "built-in default",
		Notice:           notice,
		Status:           status,
		CSRF:             csrfFromContext(r.Context()),
		DefaultProfileID: registry.DefaultProfileID,
	}), nil
}

func (s *Server) projectAgentProfileEditorData(r *http.Request, project store.Project, notice, status string) (web.AgentProfileEditorData, error) {
	overrides, err := s.store.GetProjectAgentRegistryOverrides(r.Context(), project.ID)
	if err != nil {
		return web.AgentProfileEditorData{}, err
	}
	registry, err := s.store.ResolveAgentRegistry(r.Context(), project.ID)
	if err != nil {
		return web.AgentProfileEditorData{}, err
	}
	return agentProfileEditorData(agentProfileEditorInput{
		Scope:            "project-" + project.ID,
		Title:            "Project agent profile overrides",
		Help:             "View the resolved profile set and create or edit project-layer overrides without mutating global defaults.",
		SavePath:         "/projects/" + project.ID + "/settings/agent-profiles",
		Registry:         registry,
		Overrides:        overrides,
		OverrideLabel:    "project override",
		InheritedLabel:   "inherited global",
		Notice:           notice,
		Status:           status,
		CSRF:             csrfFromContext(r.Context()),
		DefaultProfileID: registry.DefaultProfileID,
	}), nil
}

type agentProfileEditorInput struct {
	Scope            string
	Title            string
	Help             string
	SavePath         string
	Registry         agentregistry.Registry
	Overrides        agentregistry.Overrides
	OverrideLabel    string
	InheritedLabel   string
	Notice           string
	Status           string
	CSRF             string
	DefaultProfileID string
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
		if input.Overrides.Profiles != nil {
			if _, ok := input.Overrides.Profiles[profile.ID]; ok {
				layer = input.OverrideLabel
			}
		}
		views = append(views, agentProfileFormData(profile, input.DefaultProfileID, layer))
	}
	return web.AgentProfileEditorData{
		Scope:            input.Scope,
		Title:            input.Title,
		Help:             input.Help,
		Profiles:         views,
		Create:           defaultNewAgentProfileForm(),
		SavePath:         input.SavePath,
		DefaultProfileID: input.DefaultProfileID,
		Notice:           input.Notice,
		Status:           input.Status,
		CSRF:             input.CSRF,
	}
}

func agentProfileFormData(profile agentregistry.Profile, defaultProfileID, layer string) web.AgentProfileFormData {
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
		deleteAgentProfileOverride(overrides.Profiles, profile.ID)
	} else {
		if overrides.Profiles == nil {
			overrides.Profiles = map[string]agentregistry.ProfileOverride{}
		}
		overrides.Profiles[profile.ID] = override
	}
	if makeDefault {
		profileID := profile.ID
		overrides.DefaultProfileID = &profileID
	}
}

func globalAgentProfileSaveBaseline(overrides agentregistry.Overrides, profileID string) (agentregistry.Profile, bool, error) {
	baselineOverrides := cloneAgentRegistryOverrides(overrides)
	deleteAgentProfileOverride(baselineOverrides.Profiles, profileID)
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
	deleteAgentProfileOverride(baselineProjectOverrides.Profiles, profileID)
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

func deleteAgentProfileOverride(overrides map[string]agentregistry.ProfileOverride, profileID string) {
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
