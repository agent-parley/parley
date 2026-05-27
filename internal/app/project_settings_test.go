package app

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/testsupport"
)

func TestProjectRegistrationShowsRepoSettingsValidationError(t *testing.T) {
	st := testsupport.OpenStore(t)
	repo := tempAppRepoWithSettings(t, `runtime_provider = "docker"`)
	s := &Server{cfg: config.Config{DataRoot: t.TempDir()}, store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
	form := url.Values{
		"repo_path":       []string{repo},
		"name":            []string{"Invalid settings"},
		"description":     []string{""},
		"default_branch":  []string{"main"},
		"csrf_token":      []string{"test"},
	}
	req := httptest.NewRequest(http.MethodPost, "/projects", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.projects(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected rendered projects page, got status=%d body=%q", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "repo settings invalid") || !strings.Contains(body, "planned but unsupported") {
		t.Fatalf("expected friendly repo settings error, body=%q", body)
	}
	if projects := st.ListProjects(); len(projects) != 0 {
		t.Fatalf("invalid repo settings should not create project: %+v", projects)
	}
}

func tempAppRepoWithSettings(t *testing.T, body string) string {
	t.Helper()
	repo := t.TempDir()
	if err := os.Mkdir(filepath.Join(repo, ".git"), 0o700); err != nil {
		t.Fatalf("create .git marker: %v", err)
	}
	settingsDir := filepath.Join(repo, ".parley")
	if err := os.Mkdir(settingsDir, 0o755); err != nil {
		t.Fatalf("create settings dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(settingsDir, "settings.toml"), []byte(strings.TrimSpace(body)+"\n"), 0o644); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	return repo
}
