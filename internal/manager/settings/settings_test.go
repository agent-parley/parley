package settings

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAbsentFilesUsesDefaults(t *testing.T) {
	loaded, err := Load(LoadOptions{GlobalPath: filepath.Join(t.TempDir(), "global.toml"), ProjectPath: filepath.Join(t.TempDir(), "project.toml")})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Settings.Queue.AutoWhenReady != true || loaded.Settings.Queue.MaxConcurrent != 1 || loaded.Settings.Queue.BacklogCap != 100 {
		t.Fatalf("settings = %+v, want queue defaults", loaded.Settings)
	}
}

func TestLoadProjectOverridesGlobal(t *testing.T) {
	dir := t.TempDir()
	globalPath := filepath.Join(dir, "global.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeFile(t, globalPath, "[queue]\nauto_when_ready = false\nmax_concurrent = 2\nbacklog_cap = 10\n")
	writeFile(t, projectPath, "[queue]\nauto_when_ready = true\nbacklog_cap = 25\n")
	loaded, err := Load(LoadOptions{GlobalPath: globalPath, ProjectPath: projectPath})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	queue := loaded.Settings.Queue
	if queue.AutoWhenReady != true || queue.MaxConcurrent != 2 || queue.BacklogCap != 25 {
		t.Fatalf("queue = %+v, want project overrides layered on global", queue)
	}
}

func TestLoadRejectsSecretLikeValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	writeFile(t, path, "[queue]\nbacklog_cap = 10\napi_token = \"shh\"\n")
	_, err := Load(LoadOptions{ProjectPath: path})
	if err == nil {
		t.Fatal("Load() error = nil, want secret-safety failure")
	}
	if !strings.Contains(err.Error(), "secret-like values") || !strings.Contains(err.Error(), "queue.api_token") {
		t.Fatalf("error = %q, want secret-like path", err.Error())
	}
}

func TestLoadRejectsInvalidQueuePolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	writeFile(t, path, "[queue]\nmax_concurrent = 0\nbacklog_cap = 10\n")
	_, err := Load(LoadOptions{ProjectPath: path})
	if err == nil {
		t.Fatal("Load() error = nil, want validation failure")
	}
	if !strings.Contains(err.Error(), "queue.max_concurrent") {
		t.Fatalf("error = %q, want max_concurrent validation", err.Error())
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	t.Run("queue-level typo", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.toml")
		writeFile(t, path, "[queue]\nbacklog_caps = 10\n")
		_, err := Load(LoadOptions{ProjectPath: path})
		if err == nil {
			t.Fatal("Load() error = nil, want typo to fail loudly instead of falling back to default")
		}
		if !strings.Contains(err.Error(), "parse project settings") {
			t.Fatalf("error = %q, want parse error for unknown key", err.Error())
		}
	})
	t.Run("unknown table", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.toml")
		writeFile(t, path, "[queue]\nbacklog_cap = 10\n\n[unknown]\nfoo = 1\n")
		if _, err := Load(LoadOptions{ProjectPath: path}); err == nil {
			t.Fatal("Load() error = nil, want unknown-table failure")
		}
	})
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
}
