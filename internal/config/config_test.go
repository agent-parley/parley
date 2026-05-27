package config_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/models"
)

func TestValidateAllowsEmptyModeAndExecutionModeForLocalhost(t *testing.T) {
	for _, bind := range []string{"127.0.0.1:7345", "localhost:7345", "[::1]:7345"} {
		cfg := config.Config{BindAddr: bind, DataRoot: t.TempDir()}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected default mode/execution mode to validate for %s: %v", bind, err)
		}
	}
}

func TestValidateRejectsNonLocalBindAndTrustedLAN(t *testing.T) {
	cases := []config.Config{
		{BindAddr: "0.0.0.0:7345", DataRoot: t.TempDir()},
		{BindAddr: "127.0.0.1:7345", DataRoot: t.TempDir(), TrustedLAN: true},
	}
	for _, cfg := range cases {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected validation failure for %+v", cfg)
		}
	}
}

func TestValidateAllowsWildcardBindOnlyForDryRunAppContainer(t *testing.T) {
	cfg := config.Config{BindAddr: "0.0.0.0:7345", DataRoot: t.TempDir(), AppContainer: true, ExecutionMode: config.ExecutionModeDryRun}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected app-container wildcard bind to validate: %v", err)
	}
	cfg.ExecutionMode = config.ExecutionModeLocalPi
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "app-container") {
		t.Fatalf("expected app-container local-pi rejection, got %v", err)
	}
}

func TestValidateRejectsDisabledManagerExecutorModes(t *testing.T) {
	for _, mode := range []string{models.ModeManager, models.ModeExecutor} {
		cfg := config.Config{BindAddr: "127.0.0.1:7345", DataRoot: t.TempDir(), Mode: mode}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected disabled mode %q to fail", mode)
		}
	}
}

func TestValidateAgentLifecycleSettings(t *testing.T) {
	if config.DefaultAgentIdleRetentionMinutes != 0 || config.DefaultMaxIdleAgents != 1 {
		t.Fatalf("unexpected lifecycle defaults retention=%d max=%d", config.DefaultAgentIdleRetentionMinutes, config.DefaultMaxIdleAgents)
	}
	valid := config.Config{BindAddr: "127.0.0.1:7345", DataRoot: t.TempDir(), AgentIdleRetentionMinutes: 0, MaxIdleAgents: 1}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid lifecycle defaults: %v", err)
	}
	for _, cfg := range []config.Config{
		{BindAddr: "127.0.0.1:7345", DataRoot: t.TempDir(), AgentIdleRetentionMinutes: -1, MaxIdleAgents: 1},
		{BindAddr: "127.0.0.1:7345", DataRoot: t.TempDir(), AgentIdleRetentionMinutes: 0, MaxIdleAgents: -1},
	} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected invalid lifecycle config: %+v", cfg)
		}
	}
}

func TestValidateRejectsUnsupportedExecutionMode(t *testing.T) {
	cfg := config.Config{BindAddr: "127.0.0.1:7345", DataRoot: t.TempDir(), ExecutionMode: "remote"}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected unsupported execution mode failure")
	}
}

func TestValidateRuntimeProviderVocabulary(t *testing.T) {
	valid := config.Config{BindAddr: "127.0.0.1:7345", DataRoot: t.TempDir(), RuntimeProvider: config.RuntimeProviderPodman}
	if err := valid.Validate(); err != nil {
		t.Fatalf("podman provider should validate: %v", err)
	}
	docker := config.Config{BindAddr: "127.0.0.1:7345", DataRoot: t.TempDir(), RuntimeProvider: config.RuntimeProviderDocker}
	if err := docker.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported") || !strings.Contains(err.Error(), config.RuntimeProviderPodman) {
		t.Fatalf("docker provider should fail with clear deferred message, got %v", err)
	}
	missing := config.Config{BindAddr: "127.0.0.1:7345", DataRoot: t.TempDir(), RuntimeProvider: "missing"}
	if err := missing.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported runtime provider") {
		t.Fatalf("unknown provider should fail clearly, got %v", err)
	}
}

func TestValidateLocalPiPlatformGate(t *testing.T) {
	cfg := config.Config{BindAddr: "127.0.0.1:7345", DataRoot: t.TempDir(), ExecutionMode: config.ExecutionModeLocalPi}
	err := cfg.Validate()
	if runtime.GOOS == "linux" {
		if err != nil { t.Fatalf("local-pi should validate on Linux: %v", err) }
	} else {
		if err == nil || !strings.Contains(err.Error(), "Linux") { t.Fatalf("expected Linux-only error, got %v", err) }
	}
}

func TestDefaultDataRootHonorsPARLEYDataRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "custom")
	t.Setenv("PARLEY_DATA_ROOT", root)
	if got := config.DefaultDataRoot(); got != root {
		t.Fatalf("DefaultDataRoot()=%q, want %q", got, root)
	}
}

func TestEnsureDirsCreatesExpectedDirectories(t *testing.T) {
	root := t.TempDir()
	cfg := config.Config{DataRoot: root}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{".", "projects", "managers", "executors", "worktrees"} {
		path := filepath.Join(root, rel)
		info, err := os.Stat(path)
		if err != nil { t.Fatalf("missing %s: %v", rel, err) }
		if !info.IsDir() { t.Fatalf("%s is not a directory", rel) }
	}
}
