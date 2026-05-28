package executor

import (
	"context"
	"time"

	"github.com/agent-parley/parley/internal/models"
)

// Runner is the in-process attempt implementation used by the manager.
// It is distinct from models.Executor, which is the persisted runner registry
// record shown in the UI and referenced by leases/events.
type Runner interface {
	RunAttempt(ctx context.Context, input AttemptInput) (AttemptResult, error)
}

type PreflightRunner interface {
	Preflight(ctx context.Context, input AttemptInput) error
}

type ProgressFunc func(eventType, summary string, data map[string]any)

type AttemptInput struct {
	Project  models.Project
	Run      models.Run
	Task     models.Task
	Attempt  models.Attempt
	Runner      models.Executor
	Lease             models.Lease
	ArtifactDir       string
	ResumeCheckpoints []Checkpoint
	Progress          ProgressFunc
}

func (input AttemptInput) emitProgress(eventType, summary string, data map[string]any) {
	if input.Progress != nil {
		input.Progress(eventType, summary, data)
	}
}

// Checkpoint is compact internal resume context from a prior task step.
type Checkpoint struct {
	Step          string
	Role          string
	Profile       string
	AttemptNumber int
	ArtifactID    string
	Path          string
	Summary       string
	CreatedAt     time.Time
}

type OutputFile struct {
	Name        string
	Kind        string
	Body        string
	Sensitivity string
}

type AttemptResult struct {
	Files   []OutputFile
	Summary string
}
