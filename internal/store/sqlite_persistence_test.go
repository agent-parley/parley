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
	if err := db.QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE version = 3`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected migration version 3, got count %d", count)
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
	for _, table := range []string{"planner_generations", "planner_diagnostics", "planner_generation_events"} {
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

func TestPlannerGenerationEventsPersistInOrder(t *testing.T) {
	root := t.TempDir()
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	repo := testsupport.TempGitRepo(t)
	project, err := st.CreateProject("Events", "Planner activity", repo, "main")
	if err != nil {
		t.Fatal(err)
	}
	session, err := st.CreatePlannerSession(project.ID, "Generate events")
	if err != nil {
		t.Fatal(err)
	}
	generation, startedSession, err := st.BeginPlannerGeneration(session.ID, "dry-run", "pi-planner", "pi-critic")
	if err != nil {
		t.Fatal(err)
	}
	first, err := st.AppendPlannerGenerationEvent(models.PlannerGenerationEvent{ProjectID: project.ID, SessionID: session.ID, GenerationID: generation.ID, Type: models.PlannerGenerationEventStarted, Summary: "first", Data: map[string]any{"mode": "dry-run"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := st.AppendPlannerGenerationEvent(models.PlannerGenerationEvent{ProjectID: project.ID, SessionID: session.ID, GenerationID: generation.ID, Type: models.PlannerGenerationEventPlannerStarted, Summary: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || second.Sequence != 2 {
		t.Fatalf("unexpected sequences: first=%d second=%d", first.Sequence, second.Sequence)
	}
	startedSession.AgentStatus = models.PlannerAgentStatusCompleted
	startedSession.AgentSummary = "done"
	if _, _, _, err := st.CompletePlannerGeneration(generation.ID, startedSession, models.PlannerGenerationStatusCompleted, "done", ""); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	events := reopened.PlannerGenerationEventsForGeneration(generation.ID)
	if len(events) != 2 || events[0].Type != models.PlannerGenerationEventStarted || events[1].Type != models.PlannerGenerationEventPlannerStarted {
		t.Fatalf("unexpected generation events: %+v", events)
	}
	if events[0].Sequence != 1 || events[1].Sequence != 2 || events[0].Data["mode"] != "dry-run" {
		t.Fatalf("event order/data did not persist: %+v", events)
	}
	sessionEvents := reopened.PlannerGenerationEventsForSession(session.ID)
	if len(sessionEvents) != 2 || sessionEvents[0].ID != events[0].ID || sessionEvents[1].ID != events[1].ID {
		t.Fatalf("unexpected session events: %+v", sessionEvents)
	}
}

func TestPrunePlannerDiagnosticsForSessionKeepsLatestGenerations(t *testing.T) {
	st := testsupport.OpenStore(t)
	project, err := st.CreateProject("Planner diagnostics project", "context", testsupport.TempGitRepo(t), "main")
	if err != nil {
		t.Fatal(err)
	}
	session, err := st.CreatePlannerSession(project.ID, "Keep recent planner diagnostics")
	if err != nil {
		t.Fatal(err)
	}
	var oldest models.PlannerDiagnostic
	for i := 0; i < 6; i++ {
		generation, startedSession, err := st.BeginPlannerGeneration(session.ID, "dry-run", "planner", "critic")
		if err != nil {
			t.Fatal(err)
		}
		diagnostic, err := st.SavePlannerDiagnostic(models.PlannerDiagnostic{ProjectID: project.ID, SessionID: session.ID, GenerationID: generation.ID, Kind: models.PlannerDiagnosticKindTrace, Path: filepath.Join(t.TempDir(), "trace.txt")})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			oldest = diagnostic
		}
		startedSession.AgentStatus = models.PlannerAgentStatusCompleted
		startedSession.AgentSummary = "done"
		if _, _, _, err := st.CompletePlannerGeneration(generation.ID, startedSession, models.PlannerGenerationStatusCompleted, "done", ""); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := st.PrunePlannerDiagnosticsForSession(session.ID, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].ID != oldest.ID {
		t.Fatalf("expected oldest diagnostic pruned, got %+v oldest=%+v", removed, oldest)
	}
	remaining := st.PlannerDiagnosticsForSession(session.ID)
	if len(remaining) != 5 {
		t.Fatalf("expected five retained diagnostics, got %+v", remaining)
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
