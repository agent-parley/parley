package contract

// Stage type names are protocol-visible and must remain stable.
const (
	StageTypeIdeaIntake     = "idea_intake"
	StageTypeImplementation = "implementation"
	StageTypeValidation     = "validation"
	StageTypeCommit         = "commit"
	StageTypePRReady        = "pr_ready"
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
	Idea string `json:"idea"`
}
