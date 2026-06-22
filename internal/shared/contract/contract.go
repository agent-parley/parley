package contract

import (
	"fmt"
	"strings"
)

// Stage type names are protocol-visible and must remain stable.
const (
	// StageTypeIdeaIntake is the skeleton-era name. New workflow templates use
	// StageTypeIdeaRefinement, but legacy runs and snapshots may still contain it.
	StageTypeIdeaIntake     = "idea_intake"
	StageTypeIdeaRefinement = "idea_refinement"
	StageTypeReview         = "review"
	StageTypeImplementation = "implementation"
	StageTypeValidation     = "validation"
	StageTypeCommit         = "commit"
	StageTypePRCreation     = "pr_creation"
	StageTypePRReady        = "pr_ready"
	StageTypeMemoryUpdate   = "memory_update"
	StageTypeStopReport     = "stop_report"
)

const (
	RefinementLevelDirect   = "direct"
	RefinementLevelStandard = "standard"
	RefinementLevelDeep     = "deep"
)

const (
	AdapterInputModeImplementation = "implementation"
	AdapterInputModePlanning       = "planning"
)

const (
	ReviewRoleCritic  = "critic"
	ReviewRoleArbiter = "arbiter"
)

const (
	ReviewProfileGeneralist  = "generalist"
	ReviewProfileSecurity    = "security"
	ReviewProfileTests       = "tests"
	ReviewProfileAdversarial = "adversarial"
)

const (
	ReviewIntensityLight       = "light"
	ReviewIntensityNormal      = "normal"
	ReviewIntensityStrict      = "strict"
	ReviewIntensityAdversarial = "adversarial"
)

// ReviewerConfig is the user-facing Review-stage configuration. Arbitration is
// intentionally absent: each agent Review stage always runs one critic and one
// hidden arbiter internally.
type ReviewerConfig struct {
	Profile      string `json:"profile"`
	Intensity    string `json:"intensity"`
	Instructions string `json:"instructions,omitempty"`
}

// Dispatch is the Manager -> Runner input for an adapter stage.
type Dispatch struct {
	ProjectID    string         `json:"project_id"`
	RepositoryID string         `json:"repository_id,omitempty"`
	RunID        string         `json:"run_id"`
	TaskID       string         `json:"task_id"`
	AttemptID    string         `json:"attempt_id"`
	StageID      string         `json:"stage_id"`
	StageType    string         `json:"stage_type"`
	Adapter      string         `json:"adapter"`
	Input        map[string]any `json:"input"`
}

// TaskInput is the minimal user-submitted task shape.
type TaskInput struct {
	Idea               string `json:"idea"`
	RefinementLevel    string `json:"refinement_level,omitempty"`
	WorkflowTemplateID string `json:"workflow_template_id,omitempty"`
	ConversationID     string `json:"conversation_id,omitempty"`
}

func NormalizeRefinementLevel(level string) string {
	level = strings.ToLower(strings.TrimSpace(level))
	if level == "" {
		return RefinementLevelStandard
	}
	return level
}

func ValidateRefinementLevel(level string) error {
	switch NormalizeRefinementLevel(level) {
	case RefinementLevelDirect, RefinementLevelStandard, RefinementLevelDeep:
		return nil
	default:
		return fmt.Errorf("refinement_level must be one of %q, %q, or %q", RefinementLevelDirect, RefinementLevelStandard, RefinementLevelDeep)
	}
}

func NormalizeReviewProfile(profile string) string {
	profile = strings.ToLower(strings.TrimSpace(profile))
	if profile == "" {
		return ReviewProfileGeneralist
	}
	return profile
}

func ReviewProfileDefaults() []ReviewerConfig {
	return []ReviewerConfig{
		{Profile: ReviewProfileGeneralist, Intensity: ReviewIntensityNormal},
		{Profile: ReviewProfileSecurity, Intensity: ReviewIntensityStrict},
		{Profile: ReviewProfileTests, Intensity: ReviewIntensityNormal},
		{Profile: ReviewProfileAdversarial, Intensity: ReviewIntensityAdversarial},
	}
}

func DefaultReviewIntensity(profile string) string {
	switch NormalizeReviewProfile(profile) {
	case ReviewProfileSecurity:
		return ReviewIntensityStrict
	case ReviewProfileAdversarial:
		return ReviewIntensityAdversarial
	default:
		return ReviewIntensityNormal
	}
}

func NormalizeReviewIntensity(profile, intensity string) string {
	intensity = strings.ToLower(strings.TrimSpace(intensity))
	if intensity == "" {
		return DefaultReviewIntensity(profile)
	}
	return intensity
}

func NormalizeReviewerConfig(config ReviewerConfig) ReviewerConfig {
	config.Profile = NormalizeReviewProfile(config.Profile)
	config.Intensity = NormalizeReviewIntensity(config.Profile, config.Intensity)
	config.Instructions = strings.TrimSpace(config.Instructions)
	return config
}

func ValidateReviewerConfig(config ReviewerConfig) error {
	config = NormalizeReviewerConfig(config)
	if !ValidReviewProfile(config.Profile) {
		return fmt.Errorf("review profile must be one of %q, %q, %q, or %q", ReviewProfileGeneralist, ReviewProfileSecurity, ReviewProfileTests, ReviewProfileAdversarial)
	}
	if !ValidReviewIntensity(config.Intensity) {
		return fmt.Errorf("review intensity must be one of %q, %q, %q, or %q", ReviewIntensityLight, ReviewIntensityNormal, ReviewIntensityStrict, ReviewIntensityAdversarial)
	}
	return nil
}

func ValidReviewProfile(profile string) bool {
	switch NormalizeReviewProfile(profile) {
	case ReviewProfileGeneralist, ReviewProfileSecurity, ReviewProfileTests, ReviewProfileAdversarial:
		return true
	default:
		return false
	}
}

func ValidReviewIntensity(intensity string) bool {
	switch strings.ToLower(strings.TrimSpace(intensity)) {
	case ReviewIntensityLight, ReviewIntensityNormal, ReviewIntensityStrict, ReviewIntensityAdversarial:
		return true
	default:
		return false
	}
}
