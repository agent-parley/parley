package event

import "encoding/json"

const SchemaVersion = 1

const (
	ActorKindUser             = "user"
	ActorKindOperator         = "operator"
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
	ProjectID     string         `json:"project_id"`
	RunID         string         `json:"run_id"`
	TaskID        string         `json:"task_id"`
	AttemptID     string         `json:"attempt_id"`
	Type          string         `json:"type"`
	Actor         Actor          `json:"actor"`
	Summary       string         `json:"summary"`
	Data          map[string]any `json:"data"`
}

// MarshalJSON keeps the Go API simple (empty string means no run/task/attempt)
// while emitting the M5 event envelope with explicit nulls for system-scoped
// runner lifecycle events.
func (e Event) MarshalJSON() ([]byte, error) {
	type envelope struct {
		SchemaVersion int            `json:"schema_version"`
		ID            string         `json:"id"`
		Sequence      int64          `json:"sequence"`
		Timestamp     string         `json:"timestamp"`
		ProjectID     any            `json:"project_id"`
		RunID         any            `json:"run_id"`
		TaskID        any            `json:"task_id"`
		AttemptID     any            `json:"attempt_id"`
		Type          string         `json:"type"`
		Actor         Actor          `json:"actor"`
		Summary       string         `json:"summary"`
		Data          map[string]any `json:"data"`
	}
	return json.Marshal(envelope{
		SchemaVersion: e.SchemaVersion,
		ID:            e.ID,
		Sequence:      e.Sequence,
		Timestamp:     e.Timestamp,
		ProjectID:     nullableString(e.ProjectID),
		RunID:         nullableString(e.RunID),
		TaskID:        nullableString(e.TaskID),
		AttemptID:     nullableString(e.AttemptID),
		Type:          e.Type,
		Actor:         e.Actor,
		Summary:       e.Summary,
		Data:          e.Data,
	})
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
