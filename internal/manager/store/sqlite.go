package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

	StageStatusPending = "pending"
	StageStatusRunning = "running"
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

type WorkflowRun struct {
	Run                 Run
	Task                Task
	Attempt             Attempt
	ImplementationStage Stage
	ValidationStage     Stage
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
	return nil
}

func (s *Store) CreateWorkflowRun(ctx context.Context, idea string) (WorkflowRun, error) {
	now := nowRFC3339()
	wr := WorkflowRun{
		Run: Run{ID: ids.New("run"), Idea: idea, Status: RunStatusPending, EventLogArtifactID: ids.New("artifact"), CreatedAt: now, UpdatedAt: now},
	}
	wr.Task = Task{ID: ids.New("task"), RunID: wr.Run.ID, Idea: idea, Status: RunStatusPending, CreatedAt: now, UpdatedAt: now}
	wr.Attempt = Attempt{ID: ids.New("attempt"), RunID: wr.Run.ID, TaskID: wr.Task.ID, Status: RunStatusPending, CreatedAt: now, UpdatedAt: now}
	wr.ImplementationStage = Stage{ID: ids.New("stage"), RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, StageType: contract.StageTypeImplementation, Adapter: "noop", Status: StageStatusPending, CreatedAt: now, UpdatedAt: now}
	wr.ValidationStage = Stage{ID: ids.New("stage"), RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, StageType: contract.StageTypeValidation, Adapter: "", Status: StageStatusPending, CreatedAt: now, UpdatedAt: now}

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
	for _, stage := range []Stage{wr.ImplementationStage, wr.ValidationStage} {
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
	if err := s.db.QueryRowContext(ctx, `SELECT id, run_id, idea, status, created_at, updated_at FROM tasks WHERE run_id = ? LIMIT 1`, runID).Scan(&task.ID, &task.RunID, &task.Idea, &task.Status, &task.CreatedAt, &task.UpdatedAt); err != nil {
		return WorkflowRun{}, fmt.Errorf("get task for run %s: %w", runID, err)
	}
	var attempt Attempt
	if err := s.db.QueryRowContext(ctx, `SELECT id, run_id, task_id, status, created_at, updated_at FROM attempts WHERE run_id = ? LIMIT 1`, runID).Scan(&attempt.ID, &attempt.RunID, &attempt.TaskID, &attempt.Status, &attempt.CreatedAt, &attempt.UpdatedAt); err != nil {
		return WorkflowRun{}, fmt.Errorf("get attempt for run %s: %w", runID, err)
	}
	stages, err := s.ListStages(ctx, runID)
	if err != nil {
		return WorkflowRun{}, err
	}
	wr := WorkflowRun{Run: run, Task: task, Attempt: attempt}
	for _, stage := range stages {
		switch stage.StageType {
		case contract.StageTypeImplementation:
			wr.ImplementationStage = stage
		case contract.StageTypeValidation:
			wr.ValidationStage = stage
		}
	}
	return wr, nil
}

func (s *Store) ListStages(ctx context.Context, runID string) ([]Stage, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, run_id, task_id, attempt_id, stage_type, COALESCE(adapter,''), status, created_at, updated_at FROM stages WHERE run_id = ? ORDER BY created_at ASC`, runID)
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

func (s *Store) UpdateStageStatus(ctx context.Context, stageID, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE stages SET status = ?, updated_at = ? WHERE id = ?`, status, nowRFC3339(), stageID)
	if err != nil {
		return fmt.Errorf("update stage status: %w", err)
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return event.Event{}, fmt.Errorf("begin append event: %w", err)
	}
	defer rollback(tx)
	var last sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(sequence) FROM events WHERE run_id = ?`, ev.RunID).Scan(&last); err != nil {
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
	var eventLogPath string
	if err := tx.QueryRowContext(ctx, `SELECT artifacts.path FROM runs JOIN artifacts ON artifacts.id = runs.event_log_artifact_id WHERE runs.id = ?`, ev.RunID).Scan(&eventLogPath); err != nil {
		return event.Event{}, fmt.Errorf("get event log artifact path: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(id, run_id, sequence, timestamp, task_id, attempt_id, type, actor_kind, actor_id, summary, data_json, envelope_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, ev.ID, ev.RunID, ev.Sequence, ev.Timestamp, ev.TaskID, ev.AttemptID, ev.Type, ev.Actor.Kind, ev.Actor.ID, ev.Summary, string(dataJSON), string(envelopeJSON)); err != nil {
		return event.Event{}, fmt.Errorf("insert event: %w", err)
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
	if err := tx.Commit(); err != nil {
		return event.Event{}, fmt.Errorf("commit append event: %w", err)
	}
	return ev, nil
}

func (s *Store) ListEvents(ctx context.Context, runID string) ([]event.Event, error) {
	return s.ListEventsAfter(ctx, runID, 0)
}

func (s *Store) ListEventsAfter(ctx context.Context, runID string, after int64) ([]event.Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT envelope_json FROM events WHERE run_id = ? AND sequence > ? ORDER BY sequence ASC`, runID, after)
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
	capJSON, err := json.Marshal(capabilities)
	if err != nil {
		return fmt.Errorf("marshal runner capabilities: %w", err)
	}
	now := nowRFC3339()
	_, err = s.db.ExecContext(ctx, `INSERT INTO runner_registry(runner_id, status, capabilities_json, connected_at, updated_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(runner_id) DO UPDATE SET status = excluded.status, capabilities_json = excluded.capabilities_json, updated_at = excluded.updated_at`, runnerID, status, string(capJSON), now, now)
	if err != nil {
		return fmt.Errorf("upsert runner: %w", err)
	}
	return nil
}

func (s *Store) artifactPath(id, ext string) string {
	return filepath.Join(s.artifactDir, id+ext)
}

func rollback(tx *sql.Tx) { _ = tx.Rollback() }

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }
