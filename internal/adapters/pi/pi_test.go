package pi_test

import (
	"context"
	"testing"

	"github.com/agent-parley/parley/internal/adapters/pi"
	"github.com/agent-parley/parley/internal/profiles"
)

func TestPrepareKnownWorkerProfile(t *testing.T) {
	prepared, err := (pi.Adapter{}).Prepare(context.Background(), "worker", "pi-standard", "/input.md")
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Image != profiles.DefaultImage || prepared.Profile != "pi-standard" || prepared.Role != "worker" {
		t.Fatalf("unexpected prepared invocation: %+v", prepared)
	}
	if prepared.Command[len(prepared.Command)-1] != "/input.md" {
		t.Fatalf("input path not substituted: %#v", prepared.Command)
	}
	if prepared.Env["PARLEY_AGENT_ROLE"] != "worker" || prepared.Env["PARLEY_AGENT_PROFILE"] != "pi-standard" {
		t.Fatalf("unexpected env: %#v", prepared.Env)
	}
}

func TestPrepareUnknownProfileFails(t *testing.T) {
	if _, err := (pi.Adapter{}).Prepare(context.Background(), "worker", "unknown", "/input.md"); err == nil {
		t.Fatalf("expected unknown profile failure")
	}
}

func TestPrepareUsesPARLEYPIImageOverride(t *testing.T) {
	t.Setenv("PARLEY_PI_IMAGE", "local/pi:test")
	prepared, err := (pi.Adapter{}).Prepare(context.Background(), "worker", "pi-standard", "/input.md")
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Image != "local/pi:test" {
		t.Fatalf("image override not applied: %+v", prepared)
	}
}
