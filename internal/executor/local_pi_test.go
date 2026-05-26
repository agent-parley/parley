package executor_test

import (
	"context"
	"testing"

	"github.com/agent-parley/parley/internal/adapters/pi"
	"github.com/agent-parley/parley/internal/containers"
	"github.com/agent-parley/parley/internal/executor"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/profiles"
)

type preflightRuntime struct {
	images []string
}

func (r *preflightRuntime) Preflight(ctx context.Context, image string) error {
	r.images = append(r.images, image)
	return nil
}

func (r *preflightRuntime) Run(ctx context.Context, invocation containers.Invocation) (containers.Result, error) {
	return containers.Result{}, nil
}

func TestLocalPiPreflightAcceptsNonDefaultWorkerAdapter(t *testing.T) {
	runtime := &preflightRuntime{}
	runner := executor.NewLocalPiRunner(nil, runtime, pi.Adapter{})
	input := executor.AttemptInput{Task: models.Task{Adapter: "pi-high-context"}}
	if err := runner.Preflight(context.Background(), input); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if len(runtime.images) != 1 || runtime.images[0] != profiles.DefaultImage {
		t.Fatalf("unexpected image checks: %#v", runtime.images)
	}
}

func TestLocalPiPreflightUsesTaskAdapterForWorkerEligibility(t *testing.T) {
	runtime := &preflightRuntime{}
	runner := executor.NewLocalPiRunner(nil, runtime, pi.Adapter{})
	if err := runner.Preflight(context.Background(), executor.AttemptInput{Task: models.Task{Adapter: "pi-reviewer"}}); err == nil {
		t.Fatalf("reviewer-only task adapter must be rejected before runtime preflight")
	}
	if len(runtime.images) != 0 {
		t.Fatalf("runtime preflight should not run for disallowed task adapter: %#v", runtime.images)
	}
}

func TestLocalPiPreflightRejectsUnknownTaskAdapter(t *testing.T) {
	runner := executor.NewLocalPiRunner(nil, &preflightRuntime{}, pi.Adapter{})
	if err := runner.Preflight(context.Background(), executor.AttemptInput{Task: models.Task{Adapter: "missing-profile"}}); err == nil {
		t.Fatalf("expected unknown profile rejection")
	}
}

func TestLocalPiPreflightDefaultsEmptyTaskAdapterToStandardWorker(t *testing.T) {
	runtime := &preflightRuntime{}
	runner := executor.NewLocalPiRunner(nil, runtime, pi.Adapter{})
	if err := runner.Preflight(context.Background(), executor.AttemptInput{}); err != nil {
		t.Fatalf("empty task adapter should default to standard worker: %v", err)
	}
	if len(runtime.images) != 1 || runtime.images[0] != profiles.DefaultImage {
		t.Fatalf("unexpected image checks: %#v", runtime.images)
	}
}
