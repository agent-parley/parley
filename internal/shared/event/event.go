package event

const SchemaVersion = 1

const (
	ActorKindUser             = "user"
	ActorKindHarness          = "harness"
	ActorKindWorkflowEngine   = "workflow_engine"
	ActorKindAdapter          = "adapter"
	ActorKindContainerRuntime = "container_runtime"
	ActorKindGit              = "git"
	ActorKindSecurity         = "security"
)

// Actor identifies the producer of an append-only event.
type Actor struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Event is the durable append-only event envelope. The Manager assigns Sequence.
type Event struct {
	SchemaVersion int            `json:"schema_version"`
	ID            string         `json:"id"`
	Sequence      int64          `json:"sequence"`
	Timestamp     string         `json:"timestamp"`
	RunID         string         `json:"run_id"`
	TaskID        string         `json:"task_id"`
	AttemptID     string         `json:"attempt_id"`
	Type          string         `json:"type"`
	Actor         Actor          `json:"actor"`
	Summary       string         `json:"summary"`
	Data          map[string]any `json:"data"`
}
