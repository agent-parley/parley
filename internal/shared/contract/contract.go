package contract

// Stage type names are protocol-visible and must remain stable.
const (
	StageTypeImplementation = "implementation"
	StageTypeValidation     = "validation"
)

// Dispatch is the Manager -> Runner input for an adapter stage.
type Dispatch struct {
	RunID     string         `json:"run_id"`
	TaskID    string         `json:"task_id"`
	AttemptID string         `json:"attempt_id"`
	StageID   string         `json:"stage_id"`
	StageType string         `json:"stage_type"`
	Adapter   string         `json:"adapter"`
	Input     map[string]any `json:"input"`
}

// TaskInput is the minimal user-submitted task shape for M1.
type TaskInput struct {
	Idea string `json:"idea"`
}
