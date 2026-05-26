package store_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/store"
	"github.com/agent-parley/parley/internal/testsupport"
	_ "modernc.org/sqlite"
)

func TestOpenCreatesSQLiteDatabaseAndSchemaMigration(t *testing.T) {
	root := t.TempDir()
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "parley.db")); err != nil {
		t.Fatalf("parley.db not created: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "parley.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE version = 1`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected migration version 1, got count %d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE version = 2`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected migration version 2, got count %d", count)
	}
}

func TestExistingV1SQLiteUpgradesToPlannerGenerationTables(t *testing.T) {
	root := t.TempDir()
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	repo := testsupport.TempGitRepo(t)
	project, err := st.CreateProject("Upgrade", "Existing project", repo, "main")
	if err != nil {
		t.Fatal(err)
	}
	session, err := st.CreatePlannerSession(project.ID, "Existing planner session")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(root, "parley.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`DROP TABLE planner_generations`,
		`DROP TABLE planner_diagnostics`,
		`DELETE FROM schema_migrations WHERE version = 2`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.GetPlannerSession(session.ID); !ok {
		t.Fatalf("existing planner session missing after v1 upgrade")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE version = 2`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected migration version 2 after upgrade, got count %d", count)
	}
	for _, table := range []string{"planner_generations", "planner_diagnostics"} {
		if err := db.QueryRow("SELECT COUNT(1) FROM "+table).Scan(&count); err != nil {
			t.Fatalf("expected %s table after upgrade: %v", table, err)
		}
	}
}

func TestLegacyJSONImportedWhenSQLiteEmpty(t *testing.T) {
	root := t.TempDir()
	repo := testsupport.TempGitRepo(t)
	legacy := map[string]any{
		"projects": map[string]models.Project{"legacy-project": {ID: "legacy-project", Name: "Legacy", RepoPath: repo, DefaultBranch: "main", DefaultExecutorID: models.LocalExecutorID, DefaultAgentProfile: "pi-standard", RetryCount: 1}},
		"runs": map[string]models.Run{"legacy-run": {ID: "legacy-run", ProjectID: "legacy-project", Title: "Legacy run", Status: models.RunStatusAwaitingApproval}},
		"tasks": map[string]models.Task{"legacy-task": {ID: "legacy-task", ProjectID: "legacy-project", RunID: "legacy-run", AssignedExecutorID: models.LocalExecutorID, Title: "Legacy task", Status: models.TaskStatusDraft, Adapter: "pi-standard", MaxAttempts: 2}},
		"events": []models.Event{{RunID: "legacy-run", TaskID: "legacy-task", Type: models.EventTaskStateChanged, ActorKind: models.ActorKindManager, ActorID: models.ActorKindManager, Summary: "legacy event"}},
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "state.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.GetTask("legacy-task"); !ok {
		t.Fatalf("legacy task was not imported")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, ok := reopened.GetTask("legacy-task"); !ok {
		t.Fatalf("legacy task was not persisted to SQLite")
	}
}

func TestSQLiteWinsWhenDatabaseIsNotEmpty(t *testing.T) {
	root := t.TempDir()
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	_, _, sqliteTask := testsupport.ProjectAndTask(t, st)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	legacy := map[string]any{"tasks": map[string]models.Task{"json-task": {ID: "json-task", Status: models.TaskStatusDraft}}}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "state.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, ok := reopened.GetTask(sqliteTask.ID); !ok {
		t.Fatalf("SQLite task missing after reopen")
	}
	if _, ok := reopened.GetTask("json-task"); ok {
		t.Fatalf("stale legacy JSON task was imported despite non-empty SQLite")
	}
}
