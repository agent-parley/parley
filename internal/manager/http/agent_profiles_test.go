package managerhttp

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
	"github.com/agent-parley/parley/internal/manager/store"
)

func TestGlobalAgentProfileSaveDiffsAgainstBuiltInBaseline(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	cookie, csrf := getCSRFToken(t, srv)

	baseline, ok := agentregistry.ProfileByID(agentregistry.Defaults(), agentregistry.ProfilePiHeadlessWorker)
	if !ok {
		t.Fatalf("default profile %q missing", agentregistry.ProfilePiHeadlessWorker)
	}

	noChangeRec := postForm(t, srv, "/settings/agent-profiles", cookie, agentProfileEditForm(csrf, baseline))
	if noChangeRec.Code != http.StatusAccepted {
		t.Fatalf("POST unchanged global profile status = %d want %d body=%s", noChangeRec.Code, http.StatusAccepted, noChangeRec.Body.String())
	}
	overrides, err := st.GetGlobalAgentRegistryOverrides(ctx)
	if err != nil {
		t.Fatalf("get global overrides after no-op save: %v", err)
	}
	if _, ok := overrides.Profiles[baseline.ID]; ok {
		t.Fatalf("unchanged inherited profile wrote override entry: %+v", overrides.Profiles[baseline.ID])
	}

	edited := baseline
	edited.Name = "Custom worker name"
	editRec := postForm(t, srv, "/settings/agent-profiles", cookie, agentProfileEditForm(csrf, edited))
	if editRec.Code != http.StatusAccepted {
		t.Fatalf("POST edited global profile status = %d want %d body=%s", editRec.Code, http.StatusAccepted, editRec.Body.String())
	}
	overrides, err = st.GetGlobalAgentRegistryOverrides(ctx)
	if err != nil {
		t.Fatalf("get global overrides after edit: %v", err)
	}
	override, ok := overrides.Profiles[baseline.ID]
	if !ok {
		t.Fatalf("edited inherited profile did not write override: %+v", overrides.Profiles)
	}
	if override.Name == nil || *override.Name != edited.Name {
		t.Fatalf("name override = %v, want %q", override.Name, edited.Name)
	}
	assertOnlyProfileOverrideFields(t, override, "name")

	changedDefaults := agentregistry.Defaults()
	for i := range changedDefaults.Profiles {
		if changedDefaults.Profiles[i].ID == baseline.ID {
			changedDefaults.Profiles[i].Description = "Changed built-in description"
			changedDefaults.Profiles[i].Model = "changed-built-in-model"
		}
	}
	resolved, err := agentregistry.ApplyOverrides(changedDefaults, "global", overrides)
	if err != nil {
		t.Fatalf("apply overrides to changed defaults: %v", err)
	}
	resolvedProfile, ok := agentregistry.ProfileByID(resolved, baseline.ID)
	if !ok {
		t.Fatalf("resolved profile %q missing", baseline.ID)
	}
	if resolvedProfile.Name != edited.Name {
		t.Fatalf("resolved name = %q, want pinned edit %q", resolvedProfile.Name, edited.Name)
	}
	if resolvedProfile.Description != "Changed built-in description" || resolvedProfile.Model != "changed-built-in-model" {
		t.Fatalf("resolved profile = %+v, want unedited fields inherited from changed defaults", resolvedProfile)
	}
}

func TestGlobalAgentProfileSavePinsDeliberateOptionalClear(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	cookie, csrf := getCSRFToken(t, srv)

	baseline, ok := agentregistry.ProfileByID(agentregistry.Defaults(), agentregistry.ProfilePiHeadlessWorker)
	if !ok {
		t.Fatalf("default profile %q missing", agentregistry.ProfilePiHeadlessWorker)
	}
	if baseline.Description == "" {
		t.Fatal("test baseline needs a non-empty inherited description")
	}
	edited := baseline
	edited.Description = ""

	rec := postForm(t, srv, "/settings/agent-profiles", cookie, agentProfileEditForm(csrf, edited))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST cleared global profile status = %d want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	overrides, err := st.GetGlobalAgentRegistryOverrides(ctx)
	if err != nil {
		t.Fatalf("get global overrides: %v", err)
	}
	override, ok := overrides.Profiles[baseline.ID]
	if !ok {
		t.Fatalf("cleared inherited profile did not write override: %+v", overrides.Profiles)
	}
	if override.Description == nil || *override.Description != "" {
		t.Fatalf("description override = %v, want deliberate empty string", override.Description)
	}
	assertOnlyProfileOverrideFields(t, override, "description")
}

func TestGlobalAgentProfileSaveBaselineAllowsCurrentCustomDefault(t *testing.T) {
	profile := agentregistry.Profile{
		ID:                  "custom_worker",
		FamilyID:            agentregistry.FamilyPi,
		Name:                "Custom worker",
		Role:                "implementation",
		Headless:            true,
		ContextPolicy:       "task_contract_only",
		OutputStyle:         "structured_report",
		SuggestedStageTypes: []string{"implementation"},
	}
	defaultProfileID := profile.ID
	_, exists, err := globalAgentProfileSaveBaseline(agentregistry.Overrides{
		DefaultProfileID: &defaultProfileID,
		Profiles: map[string]agentregistry.ProfileOverride{
			profile.ID: agentregistry.ProfileOverrideFromProfile(profile),
		},
	}, profile.ID)
	if err != nil {
		t.Fatalf("globalAgentProfileSaveBaseline() error = %v", err)
	}
	if exists {
		t.Fatal("globalAgentProfileSaveBaseline() exists = true, want false for layer-local custom profile")
	}
}

func TestProjectAgentProfileSaveDiffsAgainstGlobalBaseline(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	cookie, csrf := getCSRFToken(t, srv)

	initialDescription := "Global description v1"
	initialModel := "global-model-v1"
	if _, err := st.UpdateGlobalAgentRegistryOverrides(ctx, agentregistry.Overrides{Profiles: map[string]agentregistry.ProfileOverride{
		agentregistry.ProfilePiHeadlessWorker: {
			Description: stringPtr(initialDescription),
			Model:       stringPtr(initialModel),
		},
	}}); err != nil {
		t.Fatalf("seed global overrides: %v", err)
	}
	globalRegistry, err := st.ResolveGlobalAgentRegistry(ctx)
	if err != nil {
		t.Fatalf("resolve global registry: %v", err)
	}
	baseline, ok := agentregistry.ProfileByID(globalRegistry, agentregistry.ProfilePiHeadlessWorker)
	if !ok {
		t.Fatalf("global profile %q missing", agentregistry.ProfilePiHeadlessWorker)
	}

	noChangeRec := postForm(t, srv, "/projects/default/settings/agent-profiles", cookie, agentProfileEditForm(csrf, baseline))
	if noChangeRec.Code != http.StatusAccepted {
		t.Fatalf("POST unchanged project profile status = %d want %d body=%s", noChangeRec.Code, http.StatusAccepted, noChangeRec.Body.String())
	}
	projectOverrides, err := st.GetProjectAgentRegistryOverrides(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("get project overrides after no-op save: %v", err)
	}
	if _, ok := projectOverrides.Profiles[baseline.ID]; ok {
		t.Fatalf("unchanged inherited project profile wrote override entry: %+v", projectOverrides.Profiles[baseline.ID])
	}

	edited := baseline
	edited.Name = "Project worker name"
	rec := postForm(t, srv, "/projects/default/settings/agent-profiles", cookie, agentProfileEditForm(csrf, edited))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST edited project profile status = %d want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	projectOverrides, err = st.GetProjectAgentRegistryOverrides(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("get project overrides after edit: %v", err)
	}
	override, ok := projectOverrides.Profiles[baseline.ID]
	if !ok {
		t.Fatalf("edited project profile did not write override: %+v", projectOverrides.Profiles)
	}
	if override.Name == nil || *override.Name != edited.Name {
		t.Fatalf("project name override = %v, want %q", override.Name, edited.Name)
	}
	assertOnlyProfileOverrideFields(t, override, "name")

	updatedDescription := "Global description v2"
	updatedModel := "global-model-v2"
	if _, err := st.UpdateGlobalAgentRegistryOverrides(ctx, agentregistry.Overrides{Profiles: map[string]agentregistry.ProfileOverride{
		agentregistry.ProfilePiHeadlessWorker: {
			Description: stringPtr(updatedDescription),
			Model:       stringPtr(updatedModel),
		},
	}}); err != nil {
		t.Fatalf("update global overrides: %v", err)
	}
	projectRegistry, err := st.ResolveAgentRegistry(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("resolve project registry: %v", err)
	}
	resolvedProfile, ok := agentregistry.ProfileByID(projectRegistry, baseline.ID)
	if !ok {
		t.Fatalf("resolved project profile %q missing", baseline.ID)
	}
	if resolvedProfile.Name != edited.Name {
		t.Fatalf("resolved project name = %q, want pinned edit %q", resolvedProfile.Name, edited.Name)
	}
	if resolvedProfile.Description != updatedDescription || resolvedProfile.Model != updatedModel {
		t.Fatalf("resolved project profile = %+v, want unedited fields inherited from changed global layer", resolvedProfile)
	}
}

func agentProfileEditForm(csrf string, profile agentregistry.Profile) url.Values {
	form := url.Values{
		"_csrf":                 {csrf},
		"profile_id":            {profile.ID},
		"family_id":             {profile.FamilyID},
		"name":                  {profile.Name},
		"description":           {profile.Description},
		"role":                  {profile.Role},
		"model":                 {profile.Model},
		"context_policy":        {profile.ContextPolicy},
		"output_style":          {profile.OutputStyle},
		"prompt":                {profile.Prompt},
		"default_instructions":  {profile.DefaultInstructions},
		"suggested_stage_types": {joinSuggestedStageTypes(profile.SuggestedStageTypes)},
	}
	if profile.Headless {
		form.Set("headless", "1")
	}
	return form
}

func joinSuggestedStageTypes(values []string) string {
	out := ""
	for i, value := range values {
		if i > 0 {
			out += ", "
		}
		out += value
	}
	return out
}

func assertOnlyProfileOverrideFields(t *testing.T, override agentregistry.ProfileOverride, fields ...string) {
	t.Helper()
	allowed := map[string]bool{}
	for _, field := range fields {
		allowed[field] = true
	}
	if !allowed["family_id"] && override.FamilyID != nil {
		t.Fatalf("unexpected family_id override: %+v", override)
	}
	if !allowed["name"] && override.Name != nil {
		t.Fatalf("unexpected name override: %+v", override)
	}
	if !allowed["description"] && override.Description != nil {
		t.Fatalf("unexpected description override: %+v", override)
	}
	if !allowed["role"] && override.Role != nil {
		t.Fatalf("unexpected role override: %+v", override)
	}
	if !allowed["headless"] && override.Headless != nil {
		t.Fatalf("unexpected headless override: %+v", override)
	}
	if !allowed["prompt"] && override.Prompt != nil {
		t.Fatalf("unexpected prompt override: %+v", override)
	}
	if !allowed["default_instructions"] && override.DefaultInstructions != nil {
		t.Fatalf("unexpected default_instructions override: %+v", override)
	}
	if !allowed["model"] && override.Model != nil {
		t.Fatalf("unexpected model override: %+v", override)
	}
	if !allowed["context_policy"] && override.ContextPolicy != nil {
		t.Fatalf("unexpected context_policy override: %+v", override)
	}
	if !allowed["output_style"] && override.OutputStyle != nil {
		t.Fatalf("unexpected output_style override: %+v", override)
	}
	if !allowed["suggested_stage_types"] && override.SuggestedStageTypes != nil {
		t.Fatalf("unexpected suggested_stage_types override: %+v", override)
	}
}

func stringPtr(value string) *string { return &value }
