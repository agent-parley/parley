package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/ids"
)

//go:embed schema.sql
var schemaFS embed.FS

const (
	RunStatusPending    = "pending"
	RunStatusRunning    = "running"
	RunStatusCompleted  = "completed"
	RunStatusFailed     = "failed"
	RunStatusInvalid    = "invalid"
	RunStatusNeedsInput = "needs_input"
	RunStatusCancelled  = "cancelled"

	StageStatusPending = "pending"
	StageStatusRunning = "running"

	RunnerStatusConnected = "connected"
	RunnerStatusSuspect   = "suspect"
	RunnerStatusDown      = "down"

	RunnerOriginSpawned    = "spawned"
	RunnerOriginRegistered = "registered"
)

type Store struct {
	db          *sql.DB
	artifactDir string
	mu          sync.Mutex
}

type Run struct {
	ID                 string
	Idea               string
	Status             string
	EventLogArtifactID string
	CreatedAt          string
	UpdatedAt          string
}

type Task struct {
	ID        string
	RunID     string
	Idea      string
	Status    string
	CreatedAt string
	UpdatedAt string
}

type Attempt struct {
	ID        string
	RunID     string
	TaskID    string
	Status    string
	CreatedAt string
	UpdatedAt string
}

type Stage struct {
	ID        string
	RunID     string
	TaskID    string
	AttemptID string
	StageType string
	Adapter   string
	Status    string
	CreatedAt string
	UpdatedAt string
}

type Artifact struct {
	ID        string
	RunID     string
	Kind      string
	MediaType string
	Path      string
	CreatedAt string
}

type Runner struct {
	ID               string
	Status           string
	Origin           string
	CapabilitiesJSON string
	MissedHeartbeats int
	ConnectedAt      string
	UpdatedAt        string
}

type SystemEvent struct {
	Cursor int64
	Event  event.Event
}

type SystemEventPage struct {
	Events      []SystemEvent
	OlderCursor int64
	HasOlder    bool
	Limit       int
}

type WorkflowRun struct {
	Run                 Run
	Task                Task
	Attempt             Attempt
	IdeaIntakeStage     Stage
	ImplementationStage Stage
	ValidationStage     Stage
	CommitStage         Stage
	PRReadyStage        Stage
}

type RunBundle struct {
	Run       Run
	Task      Task
	Attempt   Attempt
	Stages    []Stage
	Events    []event.Event
	Artifacts []Artifact
}

func Open(ctx context.Context, dataDir string) (*Store, error) {
	if dataDir == "" {
		dataDir = ".parley-data"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	artifactDir := filepath.Join(dataDir, "artifacts")
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact dir: %w", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "parley.db"))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	st := &Store{db: db, artifactDir: artifactDir}
	if err := st.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate(ctx context.Context) error {
	schema, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, string(schema)); err != nil {
		return fmt.Errorf("migrate sqlite: %w", err)
	}
	if err := s.ensureRunnerRegistrySchema(ctx); err != nil {
		return err
	}
	if err := s.ensureEventsSchema(ctx); err != nil {
		return err
	}
	return nil
}

type sqliteColumn struct {
	Name    string
	NotNull bool
}

func (s *Store) tableColumns(ctx context.Context, table string) (map[string]sqliteColumn, error) {
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return nil, fmt.Errorf("pragma table_info %s: %w", table, err)
	}
	defer rows.Close()
	cols := map[string]sqliteColumn{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &defaultValue, &pk); err != nil {
			return nil, fmt.Errorf("scan table_info %s: %w", table, err)
		}
		cols[name] = sqliteColumn{Name: name, NotNull: notnull != 0}
	}
	return cols, rows.Err()
}

func (s *Store) ensureRunnerRegistrySchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "runner_registry")
	if err != nil {
		return err
	}
	if _, ok := cols["origin"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE runner_registry ADD COLUMN origin TEXT NOT NULL DEFAULT 'registered'`); err != nil {
			return fmt.Errorf("add runner origin column: %w", err)
		}
	}
	if _, ok := cols["missed_heartbeats"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE runner_registry ADD COLUMN missed_heartbeats INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("add runner missed_heartbeats column: %w", err)
		}
	}
	return nil
}

func (s *Store) ensureEventsSchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "events")
	if err != nil {
		return err
	}
	needsRebuild := false
	if _, ok := cols["scope"]; !ok {
		needsRebuild = true
	}
	if runID, ok := cols["run_id"]; ok && runID.NotNull {
		needsRebuild = true
	}
	if needsRebuild {
		if err := s.rebuildEventsTable(ctx); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_events_run_sequence ON events(run_id, sequence)`); err != nil {
		return fmt.Errorf("create events run index: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_events_scope_sequence ON events(scope, sequence)`); err != nil {
		return fmt.Errorf("create events scope index: %w", err)
	}
	return nil
}

func (s *Store) rebuildEventsTable(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin events rebuild: %w", err)
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `ALTER TABLE events RENAME TO events_legacy`); err != nil {
		return fmt.Errorf("rename legacy events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE events (
  id TEXT PRIMARY KEY,
  run_id TEXT REFERENCES runs(id),
  scope TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  timestamp TEXT NOT NULL,
  task_id TEXT,
  attempt_id TEXT,
  type TEXT NOT NULL,
  actor_kind TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  summary TEXT NOT NULL,
  data_json TEXT NOT NULL,
  envelope_json TEXT NOT NULL,
  UNIQUE(scope, sequence)
)`); err != nil {
		return fmt.Errorf("create events table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(id, run_id, scope, sequence, timestamp, task_id, attempt_id, type, actor_kind, actor_id, summary, data_json, envelope_json)
SELECT id, run_id, 'run:' || run_id, sequence, timestamp, task_id, attempt_id, type, actor_kind, actor_id, summary, data_json, envelope_json
FROM events_legacy ORDER BY run_id, sequence`); err != nil {
		return fmt.Errorf("copy legacy events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE events_legacy`); err != nil {
		return fmt.Errorf("drop legacy events: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit events rebuild: %w", err)
	}
	return nil
}

func (s *Store) CreateWorkflowRun(ctx context.Context, idea string) (WorkflowRun, error) {
	now := nowRFC3339()
	wr := WorkflowRun{
		Run: Run{ID: ids.New("run"), Idea: idea, Status: RunStatusPending, EventLogArtifactID: ids.New("artifact"), CreatedAt: now, UpdatedAt: now},
	}
	wr.Task = Task{ID: ids.New("task"), RunID: wr.Run.ID, Idea: idea, Status: RunStatusPending, CreatedAt: now, UpdatedAt: now}
	wr.Attempt = Attempt{ID: ids.New("attempt"), RunID: wr.Run.ID, TaskID: wr.Task.ID, Status: RunStatusPending, CreatedAt: now, UpdatedAt: now}
	wr.IdeaIntakeStage = Stage{ID: ids.New("stage"), RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, StageType: contract.StageTypeIdeaIntake, Adapter: "", Status: StageStatusPending, CreatedAt: now, UpdatedAt: now}
	wr.ImplementationStage = Stage{ID: ids.New("stage"), RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, StageType: contract.StageTypeImplementation, Adapter: "noop", Status: StageStatusPending, CreatedAt: now, UpdatedAt: now}
	wr.ValidationStage = Stage{ID: ids.New("stage"), RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, StageType: contract.StageTypeValidation, Adapter: "validation", Status: StageStatusPending, CreatedAt: now, UpdatedAt: now}
	wr.CommitStage = Stage{ID: ids.New("stage"), RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, StageType: contract.StageTypeCommit, Adapter: "", Status: StageStatusPending, CreatedAt: now, UpdatedAt: now}
	wr.PRReadyStage = Stage{ID: ids.New("stage"), RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, StageType: contract.StageTypePRReady, Adapter: "", Status: StageStatusPending, CreatedAt: now, UpdatedAt: now}

	eventLogPath := s.artifactPath(wr.Run.EventLogArtifactID, ".jsonl")
	if err := os.WriteFile(eventLogPath, nil, 0o600); err != nil {
		return WorkflowRun{}, fmt.Errorf("create event log artifact: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("begin create run: %w", err)
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `INSERT INTO runs(id, idea, status, event_log_artifact_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, wr.Run.ID, wr.Run.Idea, wr.Run.Status, wr.Run.EventLogArtifactID, now, now); err != nil {
		return WorkflowRun{}, fmt.Errorf("insert run: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tasks(id, run_id, idea, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, wr.Task.ID, wr.Task.RunID, wr.Task.Idea, wr.Task.Status, now, now); err != nil {
		return WorkflowRun{}, fmt.Errorf("insert task: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempts(id, run_id, task_id, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, wr.Attempt.ID, wr.Attempt.RunID, wr.Attempt.TaskID, wr.Attempt.Status, now, now); err != nil {
		return WorkflowRun{}, fmt.Errorf("insert attempt: %w", err)
	}
	for _, stage := range []Stage{wr.IdeaIntakeStage, wr.ImplementationStage, wr.ValidationStage, wr.CommitStage, wr.PRReadyStage} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO stages(id, run_id, task_id, attempt_id, stage_type, adapter, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, stage.ID, stage.RunID, stage.TaskID, stage.AttemptID, stage.StageType, stage.Adapter, stage.Status, now, now); err != nil {
			return WorkflowRun{}, fmt.Errorf("insert stage: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifacts(id, run_id, kind, media_type, path, created_at) VALUES (?, ?, ?, ?, ?, ?)`, wr.Run.EventLogArtifactID, wr.Run.ID, "event_log", "application/x-jsonlines", eventLogPath, now); err != nil {
		return WorkflowRun{}, fmt.Errorf("insert event log artifact: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return WorkflowRun{}, fmt.Errorf("commit create run: %w", err)
	}
	return wr, nil
}

func (s *Store) ListRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, idea, status, event_log_artifact_id, created_at, updated_at FROM runs ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	return scanRuns(rows)
}

func (s *Store) ListRunsByStatus(ctx context.Context, status string, limit int) ([]Run, error) {
	query := `SELECT id, idea, status, event_log_artifact_id, created_at, updated_at FROM runs WHERE status = ? ORDER BY created_at ASC`
	args := []any{status}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs by status: %w", err)
	}
	return scanRuns(rows)
}

func (s *Store) CountRunsByStatus(ctx context.Context, status string) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE status = ?`, status).Scan(&count); err != nil {
		return 0, fmt.Errorf("count runs by status: %w", err)
	}
	return count, nil
}

func scanRuns(rows *sql.Rows) ([]Run, error) {
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		var run Run
		if err := rows.Scan(&run.ID, &run.Idea, &run.Status, &run.EventLogArtifactID, &run.CreatedAt, &run.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *Store) GetRun(ctx context.Context, runID string) (Run, error) {
	var run Run
	err := s.db.QueryRowContext(ctx, `SELECT id, idea, status, event_log_artifact_id, created_at, updated_at FROM runs WHERE id = ?`, runID).Scan(&run.ID, &run.Idea, &run.Status, &run.EventLogArtifactID, &run.CreatedAt, &run.UpdatedAt)
	if err != nil {
		return Run{}, fmt.Errorf("get run %s: %w", runID, err)
	}
	return run, nil
}

func (s *Store) GetWorkflowRun(ctx context.Context, runID string) (WorkflowRun, error) {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return WorkflowRun{}, err
	}
	var task Task
	if err := s.db.QueryRowContext(ctx, `SELECT id, run_id, idea, status, created_at, updated_at FROM tasks WHERE run_id = ? ORDER BY created_at DESC, id DESC LIMIT 1`, runID).Scan(&task.ID, &task.RunID, &task.Idea, &task.Status, &task.CreatedAt, &task.UpdatedAt); err != nil {
		return WorkflowRun{}, fmt.Errorf("get task for run %s: %w", runID, err)
	}
	var attempt Attempt
	if err := s.db.QueryRowContext(ctx, `SELECT id, run_id, task_id, status, created_at, updated_at FROM attempts WHERE run_id = ? AND task_id = ? ORDER BY created_at DESC, id DESC LIMIT 1`, runID, task.ID).Scan(&attempt.ID, &attempt.RunID, &attempt.TaskID, &attempt.Status, &attempt.CreatedAt, &attempt.UpdatedAt); err != nil {
		return WorkflowRun{}, fmt.Errorf("get attempt for run %s: %w", runID, err)
	}
	stages, err := s.ListStagesForAttempt(ctx, runID, attempt.ID)
	if err != nil {
		return WorkflowRun{}, err
	}
	wr := WorkflowRun{Run: run, Task: task, Attempt: attempt}
	for _, stage := range stages {
		switch stage.StageType {
		case contract.StageTypeIdeaIntake:
			wr.IdeaIntakeStage = stage
		case contract.StageTypeImplementation:
			wr.ImplementationStage = stage
		case contract.StageTypeValidation:
			wr.ValidationStage = stage
		case contract.StageTypeCommit:
			wr.CommitStage = stage
		case contract.StageTypePRReady:
			wr.PRReadyStage = stage
		}
	}
	return wr, nil
}

func (s *Store) ListStages(ctx context.Context, runID string) ([]Stage, error) {
	return s.listStages(ctx, `SELECT id, run_id, task_id, attempt_id, stage_type, COALESCE(adapter,''), status, created_at, updated_at FROM stages WHERE run_id = ? ORDER BY created_at ASC`, runID)
}

func (s *Store) ListStagesForAttempt(ctx context.Context, runID, attemptID string) ([]Stage, error) {
	return s.listStages(ctx, `SELECT id, run_id, task_id, attempt_id, stage_type, COALESCE(adapter,''), status, created_at, updated_at FROM stages WHERE run_id = ? AND attempt_id = ? ORDER BY created_at ASC`, runID, attemptID)
}

func (s *Store) listStages(ctx context.Context, query string, args ...any) ([]Stage, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list stages: %w", err)
	}
	defer rows.Close()
	var stages []Stage
	for rows.Next() {
		var stage Stage
		if err := rows.Scan(&stage.ID, &stage.RunID, &stage.TaskID, &stage.AttemptID, &stage.StageType, &stage.Adapter, &stage.Status, &stage.CreatedAt, &stage.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan stage: %w", err)
		}
		stages = append(stages, stage)
	}
	return stages, rows.Err()
}

func (s *Store) UpdateRunStatus(ctx context.Context, runID, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET status = ?, updated_at = ? WHERE id = ?`, status, nowRFC3339(), runID)
	if err != nil {
		return fmt.Errorf("update run status: %w", err)
	}
	return nil
}

func (s *Store) UpdateRunStatusIfOpen(ctx context.Context, runID, status string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET status = ?, updated_at = ? WHERE id = ? AND status NOT IN (?, ?, ?, ?, ?)`, status, nowRFC3339(), runID, RunStatusCompleted, RunStatusFailed, RunStatusInvalid, RunStatusNeedsInput, RunStatusCancelled)
	if err != nil {
		return false, fmt.Errorf("update open run status: %w", err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read rows affected: %w", err)
	}
	return changed > 0, nil
}

func (s *Store) UpdateRunStatusFrom(ctx context.Context, runID, fromStatus, toStatus string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET status = ?, updated_at = ? WHERE id = ? AND status = ?`, toStatus, nowRFC3339(), runID, fromStatus)
	if err != nil {
		return false, fmt.Errorf("update run status from %s to %s: %w", fromStatus, toStatus, err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read rows affected: %w", err)
	}
	return changed > 0, nil
}

func RunStatusIsTerminal(status string) bool {
	switch status {
	case RunStatusCompleted, RunStatusFailed, RunStatusInvalid, RunStatusNeedsInput, RunStatusCancelled:
		return true
	default:
		return false
	}
}

func (s *Store) UpdateStageStatus(ctx context.Context, stageID, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE stages SET status = ?, updated_at = ? WHERE id = ?`, status, nowRFC3339(), stageID)
	if err != nil {
		return fmt.Errorf("update stage status: %w", err)
	}
	return nil
}

func (s *Store) UpdateStageAdapter(ctx context.Context, stageID, adapter string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE stages SET adapter = ?, updated_at = ? WHERE id = ?`, adapter, nowRFC3339(), stageID)
	if err != nil {
		return fmt.Errorf("update stage adapter: %w", err)
	}
	return nil
}

func (s *Store) SaveArtifact(ctx context.Context, runID, kind, mediaType string, content []byte, ext string) (Artifact, error) {
	return s.SaveArtifactWithID(ctx, ids.New("artifact"), runID, kind, mediaType, content, ext)
}

func (s *Store) SaveArtifactWithID(ctx context.Context, artifactID, runID, kind, mediaType string, content []byte, ext string) (Artifact, error) {
	if ext == "" {
		ext = ".json"
	}
	artifact := Artifact{ID: artifactID, RunID: runID, Kind: kind, MediaType: mediaType, CreatedAt: nowRFC3339()}
	artifact.Path = s.artifactPath(artifact.ID, ext)
	if err := os.WriteFile(artifact.Path, content, 0o600); err != nil {
		return Artifact{}, fmt.Errorf("write artifact: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO artifacts(id, run_id, kind, media_type, path, created_at) VALUES (?, ?, ?, ?, ?, ?)`, artifact.ID, artifact.RunID, artifact.Kind, artifact.MediaType, artifact.Path, artifact.CreatedAt); err != nil {
		_ = os.Remove(artifact.Path)
		return Artifact{}, fmt.Errorf("insert artifact: %w", err)
	}
	return artifact, nil
}

func (s *Store) GetArtifact(ctx context.Context, artifactID string) (Artifact, []byte, error) {
	var artifact Artifact
	err := s.db.QueryRowContext(ctx, `SELECT id, run_id, kind, media_type, path, created_at FROM artifacts WHERE id = ?`, artifactID).Scan(&artifact.ID, &artifact.RunID, &artifact.Kind, &artifact.MediaType, &artifact.Path, &artifact.CreatedAt)
	if err != nil {
		return Artifact{}, nil, fmt.Errorf("get artifact: %w", err)
	}
	content, err := os.ReadFile(artifact.Path)
	if err != nil {
		return Artifact{}, nil, fmt.Errorf("read artifact: %w", err)
	}
	return artifact, content, nil
}

func (s *Store) AppendEvent(ctx context.Context, ev event.Event) (event.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return event.Event{}, fmt.Errorf("begin append event: %w", err)
	}
	defer rollback(tx)
	ev, err = s.appendEventTx(ctx, tx, ev)
	if err != nil {
		return event.Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return event.Event{}, fmt.Errorf("commit append event: %w", err)
	}
	return ev, nil
}

func (s *Store) UpdateRunStatusFromAndAppendSystemEvent(ctx context.Context, runID, fromStatus, toStatus string, ev event.Event) (event.Event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ev.RunID != "" {
		return event.Event{}, false, fmt.Errorf("system event must not carry run_id")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return event.Event{}, false, fmt.Errorf("begin status/event transaction: %w", err)
	}
	defer rollback(tx)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET status = ?, updated_at = ? WHERE id = ? AND status = ?`, toStatus, nowRFC3339(), runID, fromStatus)
	if err != nil {
		return event.Event{}, false, fmt.Errorf("update run status from %s to %s: %w", fromStatus, toStatus, err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return event.Event{}, false, fmt.Errorf("read rows affected: %w", err)
	}
	if changed == 0 {
		if err := tx.Commit(); err != nil {
			return event.Event{}, false, fmt.Errorf("commit unchanged status/event transaction: %w", err)
		}
		return event.Event{}, false, nil
	}
	ev, err = s.appendEventTx(ctx, tx, ev)
	if err != nil {
		return event.Event{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return event.Event{}, false, fmt.Errorf("commit status/event transaction: %w", err)
	}
	return ev, true, nil
}

func (s *Store) appendEventTx(ctx context.Context, tx *sql.Tx, ev event.Event) (event.Event, error) {
	if ev.SchemaVersion == 0 {
		ev.SchemaVersion = event.SchemaVersion
	}
	if ev.ID == "" {
		ev.ID = ids.New("evt")
	}
	if ev.Timestamp == "" {
		ev.Timestamp = nowRFC3339()
	}
	if ev.Data == nil {
		ev.Data = map[string]any{}
	}
	scope := eventScope(ev)
	var last sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(sequence) FROM events WHERE scope = ?`, scope).Scan(&last); err != nil {
		return event.Event{}, fmt.Errorf("query last event sequence: %w", err)
	}
	if last.Valid {
		ev.Sequence = last.Int64 + 1
	} else {
		ev.Sequence = 1
	}
	dataJSON, err := json.Marshal(ev.Data)
	if err != nil {
		return event.Event{}, fmt.Errorf("marshal event data: %w", err)
	}
	envelopeJSON, err := json.Marshal(ev)
	if err != nil {
		return event.Event{}, fmt.Errorf("marshal event envelope: %w", err)
	}
	var runID any
	if ev.RunID != "" {
		runID = ev.RunID
	}
	var taskID any
	if ev.TaskID != "" {
		taskID = ev.TaskID
	}
	var attemptID any
	if ev.AttemptID != "" {
		attemptID = ev.AttemptID
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(id, run_id, scope, sequence, timestamp, task_id, attempt_id, type, actor_kind, actor_id, summary, data_json, envelope_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, ev.ID, runID, scope, ev.Sequence, ev.Timestamp, taskID, attemptID, ev.Type, ev.Actor.Kind, ev.Actor.ID, ev.Summary, string(dataJSON), string(envelopeJSON)); err != nil {
		return event.Event{}, fmt.Errorf("insert event: %w", err)
	}
	if ev.RunID != "" {
		var eventLogPath string
		if err := tx.QueryRowContext(ctx, `SELECT artifacts.path FROM runs JOIN artifacts ON artifacts.id = runs.event_log_artifact_id WHERE runs.id = ?`, ev.RunID).Scan(&eventLogPath); err != nil {
			return event.Event{}, fmt.Errorf("get event log artifact path: %w", err)
		}
		f, err := os.OpenFile(eventLogPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
		if err != nil {
			return event.Event{}, fmt.Errorf("open event jsonl artifact: %w", err)
		}
		if _, err := f.Write(append(envelopeJSON, '\n')); err != nil {
			_ = f.Close()
			return event.Event{}, fmt.Errorf("append event jsonl artifact: %w", err)
		}
		if err := f.Close(); err != nil {
			return event.Event{}, fmt.Errorf("close event jsonl artifact: %w", err)
		}
	}
	return ev, nil
}

func (s *Store) ListEvents(ctx context.Context, runID string) ([]event.Event, error) {
	return s.ListEventsAfter(ctx, runID, 0)
}

func (s *Store) ListEventsAfter(ctx context.Context, runID string, after int64) ([]event.Event, error) {
	return s.listEventsWhere(ctx, `run_id = ? AND sequence > ?`, []any{runID, after})
}

func (s *Store) ListRunnerEvents(ctx context.Context, runnerID string) ([]event.Event, error) {
	return s.ListRunnerEventsAfter(ctx, runnerID, 0)
}

func (s *Store) ListRunnerEventsAfter(ctx context.Context, runnerID string, after int64) ([]event.Event, error) {
	return s.listEventsWhere(ctx, `scope = ? AND sequence > ?`, []any{"runner:" + runnerID, after})
}

func (s *Store) ListSystemEventsPage(ctx context.Context, before int64, limit int) (SystemEventPage, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	where := `run_id IS NULL`
	args := []any{}
	if before > 0 {
		where += ` AND rowid < ?`
		args = append(args, before)
	}
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, `SELECT rowid, envelope_json FROM events WHERE `+where+` ORDER BY rowid DESC LIMIT ?`, args...)
	if err != nil {
		return SystemEventPage{}, fmt.Errorf("list system events page: %w", err)
	}
	defer rows.Close()
	entries := make([]SystemEvent, 0, limit+1)
	for rows.Next() {
		var entry SystemEvent
		var raw string
		if err := rows.Scan(&entry.Cursor, &raw); err != nil {
			return SystemEventPage{}, fmt.Errorf("scan system event: %w", err)
		}
		if err := json.Unmarshal([]byte(raw), &entry.Event); err != nil {
			return SystemEventPage{}, fmt.Errorf("decode system event envelope: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return SystemEventPage{}, err
	}
	page := SystemEventPage{Limit: limit}
	if len(entries) > limit {
		page.HasOlder = true
		entries = entries[:limit]
	}
	if len(entries) > 0 {
		page.OlderCursor = entries[len(entries)-1].Cursor
	}
	for i := len(entries) - 1; i >= 0; i-- {
		page.Events = append(page.Events, entries[i])
	}
	return page, nil
}

func (s *Store) listEventsWhere(ctx context.Context, where string, args []any) ([]event.Event, error) {
	return s.listEventsWhereOrdered(ctx, where, args, `sequence ASC`)
}

func (s *Store) listEventsWhereOrdered(ctx context.Context, where string, args []any, orderBy string) ([]event.Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT envelope_json FROM events WHERE `+where+` ORDER BY `+orderBy, args...)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	var events []event.Event
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		var ev event.Event
		if err := json.Unmarshal([]byte(raw), &ev); err != nil {
			return nil, fmt.Errorf("decode event envelope: %w", err)
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

func (s *Store) ListArtifacts(ctx context.Context, runID string) ([]Artifact, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, run_id, kind, media_type, path, created_at FROM artifacts WHERE run_id = ? ORDER BY created_at ASC`, runID)
	if err != nil {
		return nil, fmt.Errorf("list artifacts: %w", err)
	}
	defer rows.Close()
	var artifacts []Artifact
	for rows.Next() {
		var artifact Artifact
		if err := rows.Scan(&artifact.ID, &artifact.RunID, &artifact.Kind, &artifact.MediaType, &artifact.Path, &artifact.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan artifact: %w", err)
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

func (s *Store) RunBundle(ctx context.Context, runID string) (RunBundle, error) {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return RunBundle{}, err
	}
	wr, err := s.GetWorkflowRun(ctx, runID)
	if err != nil {
		return RunBundle{}, err
	}
	stages, err := s.ListStages(ctx, runID)
	if err != nil {
		return RunBundle{}, err
	}
	events, err := s.ListEvents(ctx, runID)
	if err != nil {
		return RunBundle{}, err
	}
	artifacts, err := s.ListArtifacts(ctx, runID)
	if err != nil {
		return RunBundle{}, err
	}
	return RunBundle{Run: run, Task: wr.Task, Attempt: wr.Attempt, Stages: stages, Events: events, Artifacts: artifacts}, nil
}

func (s *Store) SaveWorkflowSnapshot(ctx context.Context, runID string, snapshot any) error {
	content, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal workflow snapshot: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO workflow_snapshots(run_id, snapshot_json, created_at) VALUES (?, ?, ?)`, runID, string(content), nowRFC3339())
	if err != nil {
		return fmt.Errorf("insert workflow snapshot: %w", err)
	}
	return nil
}

func (s *Store) UpsertRunner(ctx context.Context, runnerID, status string, capabilities any) error {
	return s.UpsertRunnerWithOrigin(ctx, runnerID, status, RunnerOriginRegistered, capabilities)
}

func (s *Store) UpsertRunnerWithOrigin(ctx context.Context, runnerID, status, origin string, capabilities any) error {
	if origin == "" {
		origin = RunnerOriginRegistered
	}
	capJSON, err := json.Marshal(capabilities)
	if err != nil {
		return fmt.Errorf("marshal runner capabilities: %w", err)
	}
	now := nowRFC3339()
	_, err = s.db.ExecContext(ctx, `INSERT INTO runner_registry(runner_id, status, origin, capabilities_json, missed_heartbeats, connected_at, updated_at) VALUES (?, ?, ?, ?, 0, ?, ?) ON CONFLICT(runner_id) DO UPDATE SET status = excluded.status, origin = excluded.origin, capabilities_json = excluded.capabilities_json, missed_heartbeats = 0, updated_at = excluded.updated_at`, runnerID, status, origin, string(capJSON), now, now)
	if err != nil {
		return fmt.Errorf("upsert runner: %w", err)
	}
	return nil
}

func (s *Store) UpdateRunnerHealth(ctx context.Context, runnerID, status string, missedHeartbeats int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runner_registry SET status = ?, missed_heartbeats = ?, updated_at = ? WHERE runner_id = ?`, status, missedHeartbeats, nowRFC3339(), runnerID)
	if err != nil {
		return fmt.Errorf("update runner health: %w", err)
	}
	return nil
}

func (s *Store) GetRunner(ctx context.Context, runnerID string) (Runner, error) {
	var runner Runner
	err := s.db.QueryRowContext(ctx, `SELECT runner_id, status, origin, capabilities_json, missed_heartbeats, connected_at, updated_at FROM runner_registry WHERE runner_id = ?`, runnerID).Scan(&runner.ID, &runner.Status, &runner.Origin, &runner.CapabilitiesJSON, &runner.MissedHeartbeats, &runner.ConnectedAt, &runner.UpdatedAt)
	if err != nil {
		return Runner{}, fmt.Errorf("get runner %s: %w", runnerID, err)
	}
	return runner, nil
}

func (s *Store) ListRunners(ctx context.Context) ([]Runner, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT runner_id, status, origin, capabilities_json, missed_heartbeats, connected_at, updated_at FROM runner_registry ORDER BY updated_at DESC, runner_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list runners: %w", err)
	}
	defer rows.Close()
	var runners []Runner
	for rows.Next() {
		var runner Runner
		if err := rows.Scan(&runner.ID, &runner.Status, &runner.Origin, &runner.CapabilitiesJSON, &runner.MissedHeartbeats, &runner.ConnectedAt, &runner.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan runner: %w", err)
		}
		runners = append(runners, runner)
	}
	return runners, rows.Err()
}

func eventScope(ev event.Event) string {
	if ev.RunID != "" {
		return "run:" + ev.RunID
	}
	if runnerID, ok := ev.Data["runner_id"].(string); ok && runnerID != "" {
		return "runner:" + runnerID
	}
	if strings.HasPrefix(ev.Type, "runner.") && ev.Actor.ID != "" {
		return "runner:" + ev.Actor.ID
	}
	return "system"
}

func (s *Store) artifactPath(id, ext string) string {
	return filepath.Join(s.artifactDir, id+ext)
}

func rollback(tx *sql.Tx) { _ = tx.Rollback() }

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }
