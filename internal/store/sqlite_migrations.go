package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

func openSQLite(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := applyMigrations(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func applyMigrations(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)`); err != nil {
		return err
	}
	migrations := []struct {
		version int
		sql     string
	}{
		{version: 1, sql: sqliteMigration1},
		{version: 2, sql: sqliteMigration2},
	}
	for _, migration := range migrations {
		var exists int
		if err := db.QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE version = ?`, migration.version).Scan(&exists); err != nil {
			return err
		}
		if exists > 0 {
			continue
		}
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(migration.sql); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply sqlite migration %d: %w", migration.version, err)
		}
		if _, err := tx.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))`, migration.version); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

const sqliteMigration1 = `
CREATE TABLE executors (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE TABLE projects (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE TABLE workflow_templates (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_workflow_templates_project ON workflow_templates(project_id, created_at);
CREATE TABLE planner_sessions (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_planner_sessions_project ON planner_sessions(project_id, created_at);
CREATE TABLE planner_messages (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_planner_messages_session ON planner_messages(session_id, created_at);
CREATE TABLE runs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_runs_project ON runs(project_id, created_at);
CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT '',
  run_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_tasks_run ON tasks(run_id, created_at);
CREATE TABLE attempts (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT '',
  run_id TEXT NOT NULL DEFAULT '',
  task_id TEXT NOT NULL DEFAULT '',
  attempt_number INTEGER NOT NULL DEFAULT 0,
  kind TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_attempts_task ON attempts(task_id, attempt_number);
CREATE TABLE handoffs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT '',
  run_id TEXT NOT NULL DEFAULT '',
  task_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_handoffs_task ON handoffs(task_id, created_at);
CREATE TABLE leases (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL DEFAULT '',
  executor_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  granted_at TEXT NOT NULL DEFAULT '',
  expires_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_leases_task ON leases(task_id, status);
CREATE INDEX idx_leases_executor ON leases(executor_id, status);
CREATE TABLE artifacts (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL DEFAULT '',
  task_id TEXT NOT NULL DEFAULT '',
  attempt_number INTEGER NOT NULL DEFAULT 0,
  kind TEXT NOT NULL DEFAULT '',
  sensitivity TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_artifacts_task ON artifacts(task_id, attempt_number, created_at);
CREATE TABLE events (
  id TEXT PRIMARY KEY,
  event_order INTEGER NOT NULL,
  run_id TEXT NOT NULL DEFAULT '',
  task_id TEXT NOT NULL DEFAULT '',
  executor_id TEXT NOT NULL DEFAULT '',
  lease_id TEXT NOT NULL DEFAULT '',
  sequence INTEGER NOT NULL DEFAULT 0,
  type TEXT NOT NULL DEFAULT '',
  actor_kind TEXT NOT NULL DEFAULT '',
  actor_id TEXT NOT NULL DEFAULT '',
  timestamp TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL,
  UNIQUE(run_id, sequence)
);
CREATE INDEX idx_events_run_sequence ON events(run_id, sequence);
`

const sqliteMigration2 = `
CREATE TABLE planner_generations (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT '',
  session_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_planner_generations_session ON planner_generations(session_id, created_at);
CREATE TABLE planner_diagnostics (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT '',
  session_id TEXT NOT NULL DEFAULT '',
  generation_id TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL DEFAULT '',
  sensitivity TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL
);
CREATE INDEX idx_planner_diagnostics_session ON planner_diagnostics(session_id, created_at);
CREATE INDEX idx_planner_diagnostics_generation ON planner_diagnostics(generation_id, created_at);
`
