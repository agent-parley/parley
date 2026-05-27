package reposettings_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/reposettings"
)

func TestLoadMissingSettingsIsNoop(t *testing.T) {
	settings, found, err := reposettings.Load(t.TempDir())
	if err != nil {
		t.Fatalf("Load() error=%v", err)
	}
	if found {
		t.Fatalf("Load() found settings unexpectedly: %+v", settings)
	}
}

func TestLoadRejectsSymlinkedSettingsFile(t *testing.T) {
	repo := t.TempDir()
	settingsDir := filepath.Join(repo, ".parley")
	if err := os.Mkdir(settingsDir, 0o755); err != nil {
		t.Fatalf("create settings dir: %v", err)
	}
	outside := filepath.Join(t.TempDir(), "settings.toml")
	if err := os.WriteFile(outside, []byte("runtime_provider = \"podman\"\n"), 0o644); err != nil {
		t.Fatalf("write outside settings: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(settingsDir, "settings.toml")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	_, found, err := reposettings.Load(repo)
	if err == nil {
		t.Fatalf("expected symlink settings file rejection")
	}
	if !found {
		t.Fatalf("Load() found=false for symlinked settings file")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsSymlinkedSettingsDirectory(t *testing.T) {
	repo := t.TempDir()
	outsideDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(outsideDir, "settings.toml"), []byte("runtime_provider = \"podman\"\n"), 0o644); err != nil {
		t.Fatalf("write outside settings: %v", err)
	}
	if err := os.Symlink(outsideDir, filepath.Join(repo, ".parley")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	_, found, err := reposettings.Load(repo)
	if err == nil {
		t.Fatalf("expected symlink settings directory rejection")
	}
	if !found {
		t.Fatalf("Load() found=false for symlinked settings directory")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadValidMinimalSettings(t *testing.T) {
	repo := writeSettings(t, `
runtime_provider = "podman"
default_agent_profile = "pi-standard"
workflow_template = "standard"
queue_policy = "manual"
`)
	settings, found, err := reposettings.Load(repo)
	if err != nil {
		t.Fatalf("Load() error=%v", err)
	}
	if !found {
		t.Fatalf("Load() found=false")
	}
	if settings.RuntimeProvider != config.RuntimeProviderPodman {
		t.Fatalf("runtime provider=%q", settings.RuntimeProvider)
	}
	if settings.DefaultAgentProfile != "pi-standard" || settings.WorkflowTemplate != "standard" || settings.QueuePolicy != "manual" {
		t.Fatalf("unexpected settings: %+v", settings)
	}
}

func TestLoadValidReviewProfiles(t *testing.T) {
	repo := writeSettings(t, `review_profiles = ["pi-reviewer"]`)
	settings, found, err := reposettings.Load(repo)
	if err != nil {
		t.Fatalf("Load() error=%v", err)
	}
	if !found || len(settings.ReviewProfiles) != 1 || settings.ReviewProfiles[0] != "pi-reviewer" {
		t.Fatalf("unexpected review profiles: found=%v settings=%+v", found, settings)
	}
}

func TestLoadValidContainerBackendAlias(t *testing.T) {
	repo := writeSettings(t, `container_backend = "podman"`)
	settings, found, err := reposettings.Load(repo)
	if err != nil {
		t.Fatalf("Load() error=%v", err)
	}
	if !found || settings.ContainerBackend != config.RuntimeProviderPodman {
		t.Fatalf("unexpected container backend alias: found=%v settings=%+v", found, settings)
	}
	if snapshot := reposettings.Snapshot(settings); snapshot.RuntimeProvider != config.RuntimeProviderPodman {
		t.Fatalf("snapshot runtime provider=%q", snapshot.RuntimeProvider)
	}
}

func TestLoadRejectsUnsupportedKey(t *testing.T) {
	assertInvalidSettings(t, `runner = "homelab"`, "unsupported repo settings field")
}

func TestLoadRejectsInlineSecretValue(t *testing.T) {
	assertInvalidSettings(t, `workflow_template = "sk-secret"`, "secret-like")
}

func TestLoadRejectsAuthAndCredentials(t *testing.T) {
	assertInvalidSettings(t, `auth.providers = "github"`, "sensitive")
	assertInvalidSettings(t, `credential = "value"`, "sensitive")
}

func TestLoadRejectsSecretFileAndPathSettings(t *testing.T) {
	assertInvalidSettings(t, `secret_file = ".parley/secret"`, "sensitive")
	assertInvalidSettings(t, `cache_path = "../outside"`, "sensitive")
}

func TestLoadRejectsSocketAndContainerNetworkOverrides(t *testing.T) {
	assertInvalidSettings(t, `docker_socket = "/var/run/docker.sock"`, "sensitive")
	assertInvalidSettings(t, `container_network = "host"`, "sensitive")
}

func TestLoadRejectsDockerProvider(t *testing.T) {
	assertInvalidSettings(t, `runtime_provider = "docker"`, "planned but unsupported")
}

func TestLoadRejectsConflictingRuntimeProviderAndContainerBackend(t *testing.T) {
	assertInvalidSettings(t, `
runtime_provider = "podman"
container_backend = "docker"
`, "disagree")
}

func assertInvalidSettings(t *testing.T, body, want string) {
	t.Helper()
	repo := writeSettings(t, body)
	_, found, err := reposettings.Load(repo)
	if err == nil {
		t.Fatalf("expected settings error")
	}
	if !found {
		t.Fatalf("Load() found=false for invalid settings")
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error=%q, want substring %q", err.Error(), want)
	}
}

func writeSettings(t *testing.T, body string) string {
	t.Helper()
	repo := t.TempDir()
	dir := filepath.Join(repo, ".parley")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir settings dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "settings.toml"), []byte(strings.TrimSpace(body)+"\n"), 0o644); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	return repo
}
