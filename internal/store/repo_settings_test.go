package store_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/reposettings"
	"github.com/agent-parley/parley/internal/store"
)

func TestCreateProjectAppliesRepoLocalSettingsDefaults(t *testing.T) {
	root := t.TempDir()
	st, err := store.Open(root)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repo := tempRepoWithSettings(t, `
runtime_provider = "podman"
default_agent_profile = "pi-high-context"
workflow_template = "highrisk"
queue_policy = "auto_when_ready"
review_profiles = ["pi-reviewer"]
`)
	project, err := st.CreateProject("Repo settings", "", repo, "main")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if project.DefaultAgentProfile != "pi-high-context" {
		t.Fatalf("default agent profile=%q", project.DefaultAgentProfile)
	}
	if !strings.HasSuffix(project.DefaultWorkflowTemplateID, "_highrisk") {
		t.Fatalf("default workflow template=%q", project.DefaultWorkflowTemplateID)
	}
	if project.DefaultExecutorID != models.LocalExecutorID {
		t.Fatalf("default executor changed: %q", project.DefaultExecutorID)
	}
	if project.QueuePolicy != models.QueuePolicyAutoWhenReady {
		t.Fatalf("queue policy=%q", project.QueuePolicy)
	}
	if project.RepoSettings == nil {
		t.Fatalf("repo settings snapshot missing")
	}
	if project.RepoSettings.QueuePolicy != "auto_when_ready" || project.RepoSettings.RuntimeProvider != "podman" {
		t.Fatalf("unexpected repo settings snapshot: %+v", project.RepoSettings)
	}
	if len(project.RepoSettings.Applied) != 3 {
		t.Fatalf("applied fields=%v", project.RepoSettings.Applied)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := store.Open(root)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	persisted, ok := reopened.GetProject(project.ID)
	if !ok {
		t.Fatalf("project not persisted")
	}
	if persisted.RepoSettings == nil || persisted.RepoSettings.DefaultAgentProfile != "pi-high-context" {
		t.Fatalf("repo settings snapshot not persisted: %+v", persisted.RepoSettings)
	}
	if persisted.QueuePolicy != models.QueuePolicyAutoWhenReady {
		t.Fatalf("queue policy not persisted: %q", persisted.QueuePolicy)
	}
}

func TestCreateProjectDefaultsToManualQueuePolicy(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repo := tempRepoWithSettings(t, `runtime_provider = "podman"`)
	project, err := st.CreateProject("Manual queue", "", repo, "main")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if project.QueuePolicy != models.QueuePolicyManual {
		t.Fatalf("queue policy=%q", project.QueuePolicy)
	}
}

func TestCreateProjectRejectsInvalidRepoLocalSettings(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repo := tempRepoWithSettings(t, `runtime_provider = "docker"`)
	_, err = st.CreateProject("Invalid repo settings", "", repo, "main")
	if err == nil {
		t.Fatalf("expected repo settings error")
	}
	if !reposettings.IsError(err) {
		t.Fatalf("expected reposettings error, got %T: %v", err, err)
	}
	if got := st.ListProjects(); len(got) != 0 {
		t.Fatalf("invalid repo settings should not create project: %+v", got)
	}
}

func tempRepoWithSettings(t *testing.T, body string) string {
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
