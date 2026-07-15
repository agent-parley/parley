package store

import (
	"context"
	"reflect"
	"testing"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
)

func TestUpdateAgentRegistryOverridesAtomicallyRollsBackGlobalAndProjectUpdates(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	globalProfile := agentregistry.Profile{
		ID:                  "atomic_builder",
		FamilyID:            agentregistry.FamilyPi,
		Name:                "Global atomic builder",
		Role:                "implementation",
		Headless:            true,
		Prompt:              "global atomic prompt",
		DefaultInstructions: "global atomic instructions",
		Model:               "global-atomic-model",
		ContextPolicy:       "task_contract_only",
		OutputStyle:         "structured_report",
		SuggestedStageTypes: []string{"implementation"},
	}
	globalBefore := agentregistry.Overrides{Profiles: map[string]agentregistry.ProfileOverride{
		globalProfile.ID: agentregistry.ProfileOverrideFromProfile(globalProfile),
	}}
	if _, err := st.UpdateGlobalAgentRegistryOverrides(ctx, globalBefore); err != nil {
		t.Fatalf("seed global overrides: %v", err)
	}
	projectName := "Project atomic builder"
	projectBefore := agentregistry.Overrides{Profiles: map[string]agentregistry.ProfileOverride{
		globalProfile.ID: {Name: &projectName},
	}}
	if _, err := st.UpdateProjectAgentRegistryOverrides(ctx, DefaultProjectID, projectBefore); err != nil {
		t.Fatalf("seed project overrides: %v", err)
	}

	rebasedProfile := globalProfile
	rebasedProfile.Name = projectName
	rebasedProject := agentregistry.Overrides{Profiles: map[string]agentregistry.ProfileOverride{
		globalProfile.ID: agentregistry.ProfileOverrideFromProfile(rebasedProfile),
	}}
	err = st.UpdateAgentRegistryOverridesAtomically(ctx, agentregistry.Overrides{}, map[string]agentregistry.Overrides{
		DefaultProjectID: rebasedProject,
		"zz-missing":    rebasedProject,
	})
	if err == nil {
		t.Fatal("atomic agent registry update succeeded with a missing project")
	}

	globalAfter, err := st.GetGlobalAgentRegistryOverrides(ctx)
	if err != nil {
		t.Fatalf("get global overrides after rollback: %v", err)
	}
	if !reflect.DeepEqual(globalAfter, globalBefore) {
		t.Fatalf("global overrides after rollback = %#v, want %#v", globalAfter, globalBefore)
	}
	projectAfter, err := st.GetProjectAgentRegistryOverrides(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("get project overrides after rollback: %v", err)
	}
	if !reflect.DeepEqual(projectAfter, projectBefore) {
		t.Fatalf("project overrides after rollback = %#v, want %#v", projectAfter, projectBefore)
	}
	registry, err := st.ResolveAgentRegistry(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("resolve project registry after rollback: %v", err)
	}
	profile, ok := agentregistry.ProfileByID(registry, globalProfile.ID)
	if !ok || profile.Name != projectName || profile.Role != globalProfile.Role {
		t.Fatalf("resolved profile after rollback = %+v/%v", profile, ok)
	}
}
