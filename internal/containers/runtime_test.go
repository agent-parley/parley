package containers_test

import (
	"testing"

	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/containers"
)

func TestNewRuntimeDefaultsToPodman(t *testing.T) {
	if _, ok := containers.NewRuntime("").(containers.PodmanRuntime); !ok {
		t.Fatalf("empty runtime provider should default to Podman")
	}
	if _, ok := containers.NewRuntime(config.RuntimeProviderPodman).(containers.PodmanRuntime); !ok {
		t.Fatalf("podman runtime provider should create Podman runtime")
	}
}

func TestNewRuntimeReturnsUnsupportedRuntimeForDeferredProvider(t *testing.T) {
	if _, ok := containers.NewRuntime(config.RuntimeProviderDocker).(containers.UnsupportedRuntime); !ok {
		t.Fatalf("docker provider should remain unsupported")
	}
}
