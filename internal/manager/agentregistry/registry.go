// Package agentregistry resolves metadata-only agent family and profile defaults.
package agentregistry

import (
	"fmt"
	"sort"
	"strings"

	"github.com/agent-parley/parley/internal/shared/contract"
)

const (
	SchemaVersion = 1

	FamilyPi = "pi"

	ProfilePiInteractivePlanner = "pi_interactive_planner"
	ProfilePiHeadlessWorker     = "pi_headless_worker"
	ProfilePiFreshReviewer      = "pi_fresh_reviewer"
)

// Registry is the project-resolved agent metadata catalog. It is intentionally
// descriptive only: workflow stages own tool authority and dispatch policy.
type Registry struct {
	SchemaVersion    int       `json:"schema_version"`
	DefaultProfileID string    `json:"default_profile_id"`
	Families         []Family  `json:"families"`
	Profiles         []Profile `json:"profiles"`
}

// Family describes a supported agent family without any executable command,
// credential, or routing detail.
type Family struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Status      string `json:"status"`
}

// Profile describes reusable agent behavior metadata. Profiles may suggest
// stages, prompts, models, context policy, and output style, but they do not
// grant tools or change dispatch behavior.
type Profile struct {
	ID                  string   `json:"id"`
	FamilyID            string   `json:"family_id"`
	Name                string   `json:"name"`
	Description         string   `json:"description"`
	Role                string   `json:"role"`
	Headless            bool     `json:"headless"`
	Prompt              string   `json:"prompt,omitempty"`
	Model               string   `json:"model,omitempty"`
	ContextPolicy       string   `json:"context_policy"`
	OutputStyle         string   `json:"output_style"`
	SuggestedStageTypes []string `json:"suggested_stage_types,omitempty"`
}

// Overrides is the TOML-facing shape used by settings files. Nil fields mean
// "inherit the existing global/built-in value"; project overrides are applied
// after global overrides.
type Overrides struct {
	DefaultProfileID *string                    `toml:"default_profile_id"`
	Families         map[string]FamilyOverride  `toml:"families"`
	Profiles         map[string]ProfileOverride `toml:"profiles"`
}

type FamilyOverride struct {
	Name        *string `toml:"name"`
	Description *string `toml:"description"`
	Status      *string `toml:"status"`
}

type ProfileOverride struct {
	FamilyID            *string  `toml:"family_id"`
	Name                *string  `toml:"name"`
	Description         *string  `toml:"description"`
	Role                *string  `toml:"role"`
	Headless            *bool    `toml:"headless"`
	Prompt              *string  `toml:"prompt"`
	Model               *string  `toml:"model"`
	ContextPolicy       *string  `toml:"context_policy"`
	OutputStyle         *string  `toml:"output_style"`
	SuggestedStageTypes []string `toml:"suggested_stage_types"`
}

func Defaults() Registry {
	return Registry{
		SchemaVersion:    SchemaVersion,
		DefaultProfileID: ProfilePiHeadlessWorker,
		Families: []Family{
			{
				ID:          FamilyPi,
				Name:        "Pi",
				Description: "Pi coding-agent workers managed through Parley's runner adapter boundary.",
				Status:      "available",
			},
		},
		Profiles: []Profile{
			{
				ID:                  ProfilePiInteractivePlanner,
				FamilyID:            FamilyPi,
				Name:                "Pi interactive planner",
				Description:         "Rich planning profile for user-facing idea refinement metadata.",
				Role:                "planning",
				Headless:            false,
				ContextPolicy:       "interactive_session",
				OutputStyle:         "conversational_plan",
				SuggestedStageTypes: []string{contract.StageTypeIdeaRefinement},
			},
			{
				ID:                  ProfilePiHeadlessWorker,
				FamilyID:            FamilyPi,
				Name:                "Pi headless worker",
				Description:         "Minimal, contract-driven worker profile for implementation tasks.",
				Role:                "implementation",
				Headless:            true,
				ContextPolicy:       "task_contract_only",
				OutputStyle:         "structured_report",
				SuggestedStageTypes: []string{contract.StageTypeImplementation, contract.StageTypeMemoryUpdate},
			},
			{
				ID:                  ProfilePiFreshReviewer,
				FamilyID:            FamilyPi,
				Name:                "Pi fresh reviewer",
				Description:         "Fresh reviewer profile for review-stage critique metadata.",
				Role:                "review",
				Headless:            true,
				ContextPolicy:       "fresh_readonly_preferred",
				OutputStyle:         "review_findings",
				SuggestedStageTypes: []string{contract.StageTypeReview},
			},
		},
	}
}

func Resolve(global, project Overrides) (Registry, error) {
	registry, err := ApplyOverrides(Defaults(), "global", global)
	if err != nil {
		return Registry{}, err
	}
	registry, err = ApplyOverrides(registry, "project", project)
	if err != nil {
		return Registry{}, err
	}
	return registry, Validate(registry)
}

func ApplyOverrides(registry Registry, source string, overrides Overrides) (Registry, error) {
	registry = normalizeRegistry(registry)
	if overrides.DefaultProfileID != nil {
		registry.DefaultProfileID = normalizeID(*overrides.DefaultProfileID)
	}
	familiesByID := make(map[string]int, len(registry.Families))
	for i, family := range registry.Families {
		familiesByID[family.ID] = i
	}
	for _, entry := range sortedFamilyOverrideEntries(overrides.Families) {
		id := entry.id
		if id != FamilyPi {
			return Registry{}, fmt.Errorf("%s agent registry: unsupported family %q; only %q is supported", source, id, FamilyPi)
		}
		idx, ok := familiesByID[id]
		if !ok {
			return Registry{}, fmt.Errorf("%s agent registry: family %q is not registered", source, id)
		}
		family := registry.Families[idx]
		override := entry.override
		applyString(&family.Name, override.Name)
		applyString(&family.Description, override.Description)
		applyString(&family.Status, override.Status)
		registry.Families[idx] = family
	}

	profilesByID := make(map[string]int, len(registry.Profiles))
	for i, profile := range registry.Profiles {
		profilesByID[profile.ID] = i
	}
	for _, entry := range sortedProfileOverrideEntries(overrides.Profiles) {
		id := entry.id
		override := entry.override
		idx, exists := profilesByID[id]
		profile := Profile{ID: id, FamilyID: FamilyPi}
		if exists {
			profile = registry.Profiles[idx]
		}
		if override.FamilyID != nil {
			profile.FamilyID = normalizeID(*override.FamilyID)
		}
		if profile.FamilyID != FamilyPi {
			return Registry{}, fmt.Errorf("%s agent registry: profile %q uses unsupported family %q; only %q is supported", source, id, profile.FamilyID, FamilyPi)
		}
		applyString(&profile.Name, override.Name)
		applyString(&profile.Description, override.Description)
		applyString(&profile.Role, override.Role)
		applyBool(&profile.Headless, override.Headless)
		applyString(&profile.Prompt, override.Prompt)
		applyString(&profile.Model, override.Model)
		applyString(&profile.ContextPolicy, override.ContextPolicy)
		applyString(&profile.OutputStyle, override.OutputStyle)
		if override.SuggestedStageTypes != nil {
			profile.SuggestedStageTypes = normalizeList(override.SuggestedStageTypes)
		}
		if err := validateProfile(source, profile); err != nil {
			return Registry{}, err
		}
		if exists {
			registry.Profiles[idx] = profile
			continue
		}
		profilesByID[id] = len(registry.Profiles)
		registry.Profiles = append(registry.Profiles, profile)
	}
	return normalizeRegistry(registry), Validate(registry)
}

func Validate(registry Registry) error {
	registry = normalizeRegistry(registry)
	if registry.SchemaVersion != SchemaVersion {
		return fmt.Errorf("agent registry schema_version = %d, want %d", registry.SchemaVersion, SchemaVersion)
	}
	if len(registry.Families) != 1 || registry.Families[0].ID != FamilyPi {
		return fmt.Errorf("agent registry must contain exactly the %q family", FamilyPi)
	}
	familyIDs := map[string]bool{FamilyPi: true}
	for _, family := range registry.Families {
		if family.Name == "" {
			return fmt.Errorf("agent family %q must have a name", family.ID)
		}
	}
	profileIDs := map[string]bool{}
	for _, profile := range registry.Profiles {
		if err := validateProfile("agent registry", profile); err != nil {
			return err
		}
		if !familyIDs[profile.FamilyID] {
			return fmt.Errorf("agent profile %q references unknown family %q", profile.ID, profile.FamilyID)
		}
		if profileIDs[profile.ID] {
			return fmt.Errorf("agent profile %q is duplicated", profile.ID)
		}
		profileIDs[profile.ID] = true
	}
	if !profileIDs[registry.DefaultProfileID] {
		return fmt.Errorf("agent registry default_profile_id %q is not registered", registry.DefaultProfileID)
	}
	return nil
}

func ProfileByID(registry Registry, id string) (Profile, bool) {
	id = normalizeID(id)
	for _, profile := range registry.Profiles {
		if normalizeID(profile.ID) == id {
			return normalizeProfile(profile), true
		}
	}
	return Profile{}, false
}

func DefaultProfileIDForStageType(registry Registry, stageType string) (string, bool) {
	registry = normalizeRegistry(registry)
	stageType = normalizeID(stageType)
	for _, profile := range registry.Profiles {
		for _, suggested := range profile.SuggestedStageTypes {
			if normalizeID(suggested) == stageType {
				return normalizeID(profile.ID), true
			}
		}
	}
	if _, ok := ProfileByID(registry, registry.DefaultProfileID); ok {
		return registry.DefaultProfileID, true
	}
	return "", false
}

func normalizeRegistry(registry Registry) Registry {
	if registry.SchemaVersion == 0 {
		registry.SchemaVersion = SchemaVersion
	}
	registry.DefaultProfileID = normalizeID(registry.DefaultProfileID)
	for i := range registry.Families {
		registry.Families[i].ID = normalizeID(registry.Families[i].ID)
		registry.Families[i].Name = strings.TrimSpace(registry.Families[i].Name)
		registry.Families[i].Description = strings.TrimSpace(registry.Families[i].Description)
		registry.Families[i].Status = strings.TrimSpace(registry.Families[i].Status)
	}
	for i := range registry.Profiles {
		registry.Profiles[i] = normalizeProfile(registry.Profiles[i])
	}
	return registry
}

func validateProfile(source string, profile Profile) error {
	profile = normalizeProfile(profile)
	if profile.ID == "" {
		return fmt.Errorf("%s: agent profile id must not be empty", source)
	}
	if profile.FamilyID == "" {
		return fmt.Errorf("%s: agent profile %q must declare a family_id", source, profile.ID)
	}
	if profile.Name == "" {
		return fmt.Errorf("%s: agent profile %q must have a name", source, profile.ID)
	}
	if profile.Role == "" {
		return fmt.Errorf("%s: agent profile %q must have a role", source, profile.ID)
	}
	if profile.ContextPolicy == "" {
		return fmt.Errorf("%s: agent profile %q must have a context_policy", source, profile.ID)
	}
	if profile.OutputStyle == "" {
		return fmt.Errorf("%s: agent profile %q must have an output_style", source, profile.ID)
	}
	return nil
}

func normalizeProfile(profile Profile) Profile {
	profile.ID = normalizeID(profile.ID)
	profile.FamilyID = normalizeID(profile.FamilyID)
	profile.Name = strings.TrimSpace(profile.Name)
	profile.Description = strings.TrimSpace(profile.Description)
	profile.Role = normalizeID(profile.Role)
	profile.Prompt = strings.TrimSpace(profile.Prompt)
	profile.Model = strings.TrimSpace(profile.Model)
	profile.ContextPolicy = strings.TrimSpace(profile.ContextPolicy)
	profile.OutputStyle = strings.TrimSpace(profile.OutputStyle)
	profile.SuggestedStageTypes = normalizeList(profile.SuggestedStageTypes)
	return profile
}

func normalizeID(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeList(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func applyString(dst *string, src *string) {
	if src != nil {
		*dst = strings.TrimSpace(*src)
	}
}

func applyBool(dst *bool, src *bool) {
	if src != nil {
		*dst = *src
	}
}

type familyOverrideEntry struct {
	id       string
	override FamilyOverride
}

type profileOverrideEntry struct {
	id       string
	override ProfileOverride
}

func sortedFamilyOverrideEntries(overrides map[string]FamilyOverride) []familyOverrideEntry {
	entries := make([]familyOverrideEntry, 0, len(overrides))
	for id, override := range overrides {
		entries = append(entries, familyOverrideEntry{id: normalizeID(id), override: override})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })
	return entries
}

func sortedProfileOverrideEntries(overrides map[string]ProfileOverride) []profileOverrideEntry {
	entries := make([]profileOverrideEntry, 0, len(overrides))
	for id, override := range overrides {
		entries = append(entries, profileOverrideEntry{id: normalizeID(id), override: override})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })
	return entries
}
