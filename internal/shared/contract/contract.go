package contract

import (
	"fmt"
	"strings"
)

// Stage type names are protocol-visible and must remain stable.
const (
	StageTypeIdeaIntake     = "idea_intake"
	StageTypeImplementation = "implementation"
	StageTypeValidation     = "validation"
	StageTypeCommit         = "commit"
	StageTypePRReady        = "pr_ready"
)

const (
	RefinementLevelDirect   = "direct"
	RefinementLevelStandard = "standard"
	RefinementLevelDeep     = "deep"
)

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
	Idea            string `json:"idea"`
	RefinementLevel string `json:"refinement_level,omitempty"`
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
