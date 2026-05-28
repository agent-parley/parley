package main

import (
	"strings"
	"testing"
)

func TestEnvDefaultReadsRuntimeProviderEnv(t *testing.T) {
	t.Setenv("PARLEY_RUNTIME_PROVIDER", "podman")
	if got := envDefault("PARLEY_RUNTIME_PROVIDER", "fallback"); got != "podman" {
		t.Fatalf("envDefault runtime provider=%q, want podman", got)
	}
}

func TestEnvIntStrictUsesFallbackForEmptyEnv(t *testing.T) {
	t.Setenv("PARLEY_TEST_INT", "")
	got, err := envIntStrict("PARLEY_TEST_INT", 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 7 {
		t.Fatalf("envIntStrict()=%d, want fallback 7", got)
	}
}

func TestEnvIntStrictParsesValidInteger(t *testing.T) {
	t.Setenv("PARLEY_TEST_INT", " 12 ")
	got, err := envIntStrict("PARLEY_TEST_INT", 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 12 {
		t.Fatalf("envIntStrict()=%d, want 12", got)
	}
}

func TestEnvIntStrictRejectsMalformedInteger(t *testing.T) {
	t.Setenv("PARLEY_AGENT_IDLE_RETENTION_MINUTES", "5abc")
	_, err := envIntStrict("PARLEY_AGENT_IDLE_RETENTION_MINUTES", 0)
	if err == nil {
		t.Fatalf("expected malformed env var error")
	}
	if !strings.Contains(err.Error(), "PARLEY_AGENT_IDLE_RETENTION_MINUTES") || !strings.Contains(err.Error(), "must be an integer") {
		t.Fatalf("unexpected error: %v", err)
	}
}
