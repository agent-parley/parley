package testsupport

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/store"
)

func TempGitRepo(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".git"), 0o700); err != nil {
		t.Fatalf("create .git marker: %v", err)
	}
	return dir
}

func OpenStore(t testing.TB) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func ProjectAndTask(t testing.TB, st *store.Store) (models.Project, models.Run, models.Task) {
	t.Helper()
	project, err := st.CreateProject("Test project", "Test context", TempGitRepo(t), "main")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	run, task, err := st.CreateManualRunTask(project, "Test task", "Do meaningful work", "focus", "", "done")
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	return project, run, task
}

func HasEventSummary(events []models.Event, summary string) bool {
	for _, event := range events {
		if event.Summary == summary {
			return true
		}
	}
	return false
}
