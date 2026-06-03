package report

import (
	"errors"
	"fmt"

	"github.com/agent-parley/parley/internal/shared/contract"
)

const SchemaVersion = 1

const (
	ActorKindAgent   = "agent"
	ActorKindHarness = "harness"
	ActorKindHuman   = "human"
)

const (
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusNeedsInput = "needs_input"
	StatusInvalid    = "invalid"
)

// Verdict is reserved for judgment stages outside M0/M1.
type Verdict string

// Actor identifies the producer of a report.
type Actor struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Report is the durable stage report envelope. The workflow engine routes on Status.
type Report struct {
	SchemaVersion int            `json:"schema_version"`
	RunID         string         `json:"run_id"`
	TaskID        string         `json:"task_id"`
	AttemptID     string         `json:"attempt_id"`
	StageID       string         `json:"stage_id"`
	StageType     string         `json:"stage_type"`
	Actor         Actor          `json:"actor"`
	Status        string         `json:"status"`
	Verdict       *Verdict       `json:"verdict"`
	Summary       string         `json:"summary"`
	EvidenceRefs  []string       `json:"evidence_refs"`
	Payload       map[string]any `json:"payload"`
	Errors        []string       `json:"errors"`
}

func (r Report) Validate() error {
	var errs []error
	if r.SchemaVersion != SchemaVersion {
		errs = append(errs, fmt.Errorf("schema_version must be %d", SchemaVersion))
	}
	for name, value := range map[string]string{
		"run_id":     r.RunID,
		"task_id":    r.TaskID,
		"attempt_id": r.AttemptID,
		"stage_id":   r.StageID,
		"stage_type": r.StageType,
		"status":     r.Status,
		"summary":    r.Summary,
	} {
		if value == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	if !validStageType(r.StageType) {
		errs = append(errs, fmt.Errorf("stage_type %q is invalid", r.StageType))
	}
	if !validActorKind(r.Actor.Kind) || r.Actor.ID == "" {
		errs = append(errs, fmt.Errorf("actor is invalid"))
	}
	if !validStatus(r.Status) {
		errs = append(errs, fmt.Errorf("status %q is invalid", r.Status))
	}
	if (r.Status == StatusFailed || r.Status == StatusInvalid) && len(r.Errors) == 0 {
		errs = append(errs, fmt.Errorf("errors must be populated when status=%s", r.Status))
	}
	return errors.Join(errs...)
}

func validStageType(v string) bool {
	switch v {
	case contract.StageTypeImplementation, contract.StageTypeValidation:
		return true
	default:
		return false
	}
}

func validActorKind(v string) bool {
	switch v {
	case ActorKindAgent, ActorKindHarness, ActorKindHuman:
		return true
	default:
		return false
	}
}

func validStatus(v string) bool {
	switch v {
	case StatusCompleted, StatusFailed, StatusNeedsInput, StatusInvalid:
		return true
	default:
		return false
	}
}
