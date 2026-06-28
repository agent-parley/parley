package agentregistry

import (
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/shared/contract"
)

func TestDefaultsRegisterPiAsSoleFamily(t *testing.T) {
	registry := Defaults()
	if err := Validate(registry); err != nil {
		t.Fatalf("Validate(defaults) error = %v", err)
	}
	if len(registry.Families) != 1 || registry.Families[0].ID != FamilyPi {
		t.Fatalf("families = %+v, want only Pi", registry.Families)
	}
	worker, ok := ProfileByID(registry, ProfilePiHeadlessWorker)
	if !ok {
		t.Fatalf("default worker profile %q missing", ProfilePiHeadlessWorker)
	}
	if worker.FamilyID != FamilyPi || !worker.Headless || worker.ContextPolicy != "task_contract_only" {
		t.Fatalf("worker profile = %+v, want Pi headless task-contract metadata", worker)
	}
	for _, profile := range registry.Profiles {
		if profile.FamilyID != FamilyPi {
			t.Fatalf("profile %q family = %q, want %q", profile.ID, profile.FamilyID, FamilyPi)
		}
	}
}

func TestResolveLayersProjectOverridesOverGlobalDefaults(t *testing.T) {
	globalDefault := "global_worker"
	globalName := "Global worker"
	globalModel := "global-model"
	projectDefault := ProfilePiHeadlessWorker
	projectName := "Project worker"
	projectModel := "project-model"
	projectPrompt := "Follow project-specific conventions."
	registry, err := Resolve(
		Overrides{
			DefaultProfileID: &globalDefault,
			Profiles: map[string]ProfileOverride{
				globalDefault: {
					FamilyID:            strPtr(FamilyPi),
					Name:                &globalName,
					Role:                strPtr("implementation"),
					Headless:            boolPtr(true),
					Model:               &globalModel,
					ContextPolicy:       strPtr("task_contract_only"),
					OutputStyle:         strPtr("structured_report"),
					SuggestedStageTypes: []string{contract.StageTypeImplementation},
				},
				ProfilePiHeadlessWorker: {
					Model:               &globalModel,
					SuggestedStageTypes: []string{contract.StageTypeImplementation, contract.StageTypePRCreation},
				},
			},
		},
		Overrides{
			DefaultProfileID: &projectDefault,
			Profiles: map[string]ProfileOverride{
				ProfilePiHeadlessWorker: {
					Name:   &projectName,
					Model:  &projectModel,
					Prompt: &projectPrompt,
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if registry.DefaultProfileID != projectDefault {
		t.Fatalf("default profile = %q, want project override %q", registry.DefaultProfileID, projectDefault)
	}
	worker, ok := ProfileByID(registry, ProfilePiHeadlessWorker)
	if !ok {
		t.Fatalf("profile %q missing", ProfilePiHeadlessWorker)
	}
	if worker.Name != projectName || worker.Model != projectModel || worker.Prompt != projectPrompt {
		t.Fatalf("worker profile = %+v, want project metadata overrides", worker)
	}
	if strings.Join(worker.SuggestedStageTypes, ",") != strings.Join([]string{contract.StageTypeImplementation, contract.StageTypePRCreation}, ",") {
		t.Fatalf("worker suggested stages = %#v, want inherited global stages", worker.SuggestedStageTypes)
	}
	globalWorker, ok := ProfileByID(registry, globalDefault)
	if !ok {
		t.Fatalf("global-added profile %q missing", globalDefault)
	}
	if globalWorker.Name != globalName || globalWorker.FamilyID != FamilyPi {
		t.Fatalf("global profile = %+v, want Pi metadata", globalWorker)
	}
}

func TestDefaultProfileIDForStageTypeUsesSuggestedStageMapping(t *testing.T) {
	registry := Defaults()
	cases := map[string]string{
		contract.StageTypeIdeaRefinement: ProfilePiInteractivePlanner,
		contract.StageTypeImplementation: ProfilePiHeadlessWorker,
		contract.StageTypeMemoryUpdate:   ProfilePiHeadlessWorker,
		contract.StageTypeReview:         ProfilePiFreshReviewer,
	}
	for stageType, want := range cases {
		got, ok := DefaultProfileIDForStageType(registry, stageType)
		if !ok || got != want {
			t.Fatalf("DefaultProfileIDForStageType(%q) = %q/%v, want %q/true", stageType, got, ok, want)
		}
	}
	got, ok := DefaultProfileIDForStageType(registry, contract.StageTypeValidation)
	if !ok || got != registry.DefaultProfileID {
		t.Fatalf("fallback profile = %q/%v, want default %q", got, ok, registry.DefaultProfileID)
	}
}

func TestProjectCanAddPiProfile(t *testing.T) {
	profileID := "project_validator"
	registry, err := Resolve(Overrides{}, Overrides{Profiles: map[string]ProfileOverride{
		profileID: {
			FamilyID:            strPtr(FamilyPi),
			Name:                strPtr("Project validator"),
			Role:                strPtr("validation-support"),
			Headless:            boolPtr(true),
			ContextPolicy:       strPtr("task_contract_only"),
			OutputStyle:         strPtr("structured_report"),
			SuggestedStageTypes: []string{contract.StageTypeValidation},
		},
	}})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	profile, ok := ProfileByID(registry, profileID)
	if !ok {
		t.Fatalf("profile %q missing", profileID)
	}
	if profile.FamilyID != FamilyPi {
		t.Fatalf("profile family = %q, want %q", profile.FamilyID, FamilyPi)
	}
}

func TestResolveRejectsNonPiFamilies(t *testing.T) {
	_, err := Resolve(Overrides{}, Overrides{Families: map[string]FamilyOverride{"docker": {Name: strPtr("Docker")}}})
	if err == nil {
		t.Fatal("Resolve() error = nil, want unsupported family failure")
	}
	if !strings.Contains(err.Error(), "only \"pi\" is supported") {
		t.Fatalf("error = %q, want Pi-only failure", err.Error())
	}
}

func TestResolveRejectsProfileUsingNonPiFamily(t *testing.T) {
	_, err := Resolve(Overrides{}, Overrides{Profiles: map[string]ProfileOverride{
		"docker_worker": {
			FamilyID:      strPtr("docker"),
			Name:          strPtr("Docker worker"),
			Role:          strPtr("implementation"),
			ContextPolicy: strPtr("task_contract_only"),
			OutputStyle:   strPtr("structured_report"),
		},
	}})
	if err == nil {
		t.Fatal("Resolve() error = nil, want unsupported profile family failure")
	}
	if !strings.Contains(err.Error(), "unsupported family \"docker\"") {
		t.Fatalf("error = %q, want profile family failure", err.Error())
	}
}

func strPtr(value string) *string { return &value }

func boolPtr(value bool) *bool { return &value }
