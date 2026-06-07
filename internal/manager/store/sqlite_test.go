package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestStorePersistence(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "build a thing")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	persisted, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, Type: "run.created", Actor: event.Actor{Kind: event.ActorKindUser, ID: "test"}, Summary: "created", Data: map[string]any{"ok": true}})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	if persisted.Sequence != 1 || !strings.HasPrefix(persisted.ID, "evt_") {
		t.Fatalf("bad event sequence/id: %+v", persisted)
	}
	rep := report.Report{SchemaVersion: report.SchemaVersion, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, StageID: wr.ImplementationStage.ID, StageType: wr.ImplementationStage.StageType, Actor: report.Actor{Kind: report.ActorKindAgent, ID: "noop"}, Status: report.StatusCompleted, Summary: "done", Payload: map[string]any{}, Errors: []string{}}
	artifact, err := st.SaveReportArtifact(ctx, rep)
	if err != nil {
		t.Fatalf("save report artifact: %v", err)
	}
	_, content, err := st.GetArtifact(ctx, artifact.ID)
	if err != nil {
		t.Fatalf("get artifact: %v", err)
	}
	if !strings.Contains(string(content), "noop") {
		t.Fatalf("artifact content missing report: %s", content)
	}
	bundle, err := st.RunBundle(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("run bundle: %v", err)
	}
	if len(bundle.Stages) != 5 || len(bundle.Events) != 1 || len(bundle.Artifacts) != 2 {
		t.Fatalf("unexpected bundle counts: stages=%d events=%d artifacts=%d", len(bundle.Stages), len(bundle.Events), len(bundle.Artifacts))
	}
}

func TestStageCanReferenceStageBriefArtifact(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "build context")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	artifact, err := st.SaveArtifact(ctx, wr.Run.ID, "stage_brief", "text/markdown", []byte("# Stage brief\n"), ".md")
	if err != nil {
		t.Fatalf("save stage brief: %v", err)
	}
	if err := st.UpdateStageBriefArtifactID(ctx, wr.ImplementationStage.ID, artifact.ID); err != nil {
		t.Fatalf("update stage brief ref: %v", err)
	}
	stages, err := st.ListStages(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list stages: %v", err)
	}
	for _, stage := range stages {
		if stage.ID == wr.ImplementationStage.ID {
			if stage.StageBriefArtifactID != artifact.ID {
				t.Fatalf("stage brief ref = %s, want %s", stage.StageBriefArtifactID, artifact.ID)
			}
			return
		}
	}
	t.Fatal("implementation stage not found")
}

func TestRunPersistsRefinementLevelAndStageCanReferenceTaskPlanArtifact(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "build context", RefinementLevel: contract.RefinementLevelDeep})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if wr.Run.RefinementLevel != contract.RefinementLevelDeep || wr.Task.RefinementLevel != contract.RefinementLevelDeep {
		t.Fatalf("refinement not persisted on create: run=%q task=%q", wr.Run.RefinementLevel, wr.Task.RefinementLevel)
	}
	artifact, err := st.SaveArtifact(ctx, wr.Run.ID, "task_plan", "text/markdown", []byte("# Task Plan\n"), ".md")
	if err != nil {
		t.Fatalf("save task plan: %v", err)
	}
	if err := st.UpdateStageTaskPlanArtifactID(ctx, wr.IdeaIntakeStage.ID, artifact.ID); err != nil {
		t.Fatalf("update task plan ref: %v", err)
	}
	loaded, err := st.GetWorkflowRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("get workflow run: %v", err)
	}
	if loaded.Run.RefinementLevel != contract.RefinementLevelDeep || loaded.Task.RefinementLevel != contract.RefinementLevelDeep {
		t.Fatalf("refinement not loaded: run=%q task=%q", loaded.Run.RefinementLevel, loaded.Task.RefinementLevel)
	}
	if loaded.IdeaIntakeStage.TaskPlanArtifactID != artifact.ID {
		t.Fatalf("task plan ref = %s, want %s", loaded.IdeaIntakeStage.TaskPlanArtifactID, artifact.ID)
	}
}

func TestProjectWorkspaceAndNoOrphanWorkflowRecords(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	project, err := st.GetProject(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("get default project: %v", err)
	}
	for _, child := range []string{"artifacts", "drafts", "memory"} {
		if info, err := os.Stat(filepath.Join(project.WorkspacePath, child)); err != nil || !info.IsDir() {
			t.Fatalf("workspace child %s stat=%v err=%v", child, info, err)
		}
	}
	wr, err := st.CreateWorkflowRun(ctx, "project rooted")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if wr.Project.ID != DefaultProjectID || wr.Task.ProjectID != wr.Project.ID || wr.Run.ProjectID != wr.Project.ID || wr.Run.TaskID != wr.Task.ID || wr.Attempt.ProjectID != wr.Project.ID {
		t.Fatalf("workflow not project-rooted: %+v", wr)
	}
	artifact, err := st.SaveArtifact(ctx, wr.Run.ID, "note", "text/plain", []byte("private"), ".txt")
	if err != nil {
		t.Fatalf("save artifact: %v", err)
	}
	if !strings.HasPrefix(artifact.Path, filepath.Join(project.WorkspacePath, "artifacts")) {
		t.Fatalf("artifact path = %s, want under project workspace %s", artifact.Path, project.WorkspacePath)
	}
	if _, err := st.DB().ExecContext(ctx, `INSERT INTO tasks(id, project_id, idea, status, created_at, updated_at) VALUES ('task_orphan', 'missing_project', 'x', 'pending', 'now', 'now')`); err == nil {
		t.Fatal("insert orphan task succeeded, want foreign-key failure")
	}
	if _, err := st.DB().ExecContext(ctx, `INSERT INTO runs(id, project_id, task_id, idea, status, event_log_artifact_id, created_at, updated_at) VALUES ('run_orphan', ?, 'missing_task', 'x', 'pending', 'artifact_orphan', 'now', 'now')`, wr.Project.ID); err == nil {
		t.Fatal("insert orphan run succeeded, want foreign-key failure")
	}
}

func TestRunlessRunnerEventPersistsWithNullRunIDAndScopedSequence(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	first, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, Type: "runner.registered", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "registered", Data: map[string]any{"runner_id": "runner_a"}})
	if err != nil {
		t.Fatalf("append first runner event: %v", err)
	}
	second, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, Type: "runner.ready", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "ready", Data: map[string]any{"runner_id": "runner_a"}})
	if err != nil {
		t.Fatalf("append second runner event: %v", err)
	}
	other, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, Type: "runner.registered", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "registered", Data: map[string]any{"runner_id": "runner_b"}})
	if err != nil {
		t.Fatalf("append other runner event: %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 2 || other.Sequence != 1 {
		t.Fatalf("sequences = %d,%d,%d; want 1,2,1", first.Sequence, second.Sequence, other.Sequence)
	}
	var nullRunID int
	if err := st.DB().QueryRowContext(ctx, `SELECT run_id IS NULL FROM events WHERE id = ?`, first.ID).Scan(&nullRunID); err != nil {
		t.Fatalf("query null run_id: %v", err)
	}
	if nullRunID != 1 {
		t.Fatal("runner event run_id is not NULL")
	}
	events, err := st.ListRunnerEvents(ctx, "runner_a")
	if err != nil {
		t.Fatalf("list runner events: %v", err)
	}
	if len(events) != 2 || events[0].RunID != "" || events[1].Type != "runner.ready" {
		t.Fatalf("unexpected runner events: %#v", events)
	}
}

func TestSystemEventsUseAppendOrderNotPerScopeSequenceOrID(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	first, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, ID: "evt_z", Timestamp: "2026-06-04T00:00:00Z", Type: "runner.registered", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "registered", Data: map[string]any{"runner_id": "runner_a"}})
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	second, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, ID: "evt_a", Timestamp: "2026-06-04T00:00:00Z", Type: "runner.ready", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "ready", Data: map[string]any{"runner_id": "runner_b"}})
	if err != nil {
		t.Fatalf("append second: %v", err)
	}
	third, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, ID: "evt_m", Timestamp: "2026-06-04T00:00:00Z", Type: "runner.down", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "down", Data: map[string]any{"runner_id": "runner_c"}})
	if err != nil {
		t.Fatalf("append third: %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 1 || third.Sequence != 1 {
		t.Fatalf("per-runner sequences = %d,%d,%d; want all 1", first.Sequence, second.Sequence, third.Sequence)
	}
	page, err := st.ListSystemEventsPage(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list system events: %v", err)
	}
	if len(page.Events) != 3 || page.Events[0].Event.ID != first.ID || page.Events[1].Event.ID != second.ID || page.Events[2].Event.ID != third.ID {
		t.Fatalf("system events = %#v, want append order %s, %s, %s", page.Events, first.ID, second.ID, third.ID)
	}

	latest, err := st.ListSystemEventsPage(ctx, 0, 2)
	if err != nil {
		t.Fatalf("list latest system events: %v", err)
	}
	if !latest.HasOlder || latest.OlderCursor == 0 {
		t.Fatalf("latest page cursor = %d hasOlder=%v, want older cursor", latest.OlderCursor, latest.HasOlder)
	}
	if len(latest.Events) != 2 || latest.Events[0].Event.ID != second.ID || latest.Events[1].Event.ID != third.ID {
		t.Fatalf("latest page = %#v, want %s then %s", latest.Events, second.ID, third.ID)
	}
	older, err := st.ListSystemEventsPage(ctx, latest.OlderCursor, 2)
	if err != nil {
		t.Fatalf("list older system events: %v", err)
	}
	if older.HasOlder || len(older.Events) != 1 || older.Events[0].Event.ID != first.ID {
		t.Fatalf("older page = %#v hasOlder=%v, want only %s", older.Events, older.HasOlder, first.ID)
	}
}

func TestMigrateLegacyEventsBackfillsRunScope(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	artifactDir := filepath.Join(dataDir, "artifacts")
	if err := os.MkdirAll(artifactDir, 0o700); err != nil {
		t.Fatalf("mkdir artifacts: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "parley.db"))
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	legacy := `
CREATE TABLE runs (id TEXT PRIMARY KEY, idea TEXT NOT NULL, status TEXT NOT NULL, event_log_artifact_id TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE tasks (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), idea TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE attempts (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), task_id TEXT NOT NULL REFERENCES tasks(id), status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE stages (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), task_id TEXT NOT NULL REFERENCES tasks(id), attempt_id TEXT NOT NULL REFERENCES attempts(id), stage_type TEXT NOT NULL, adapter TEXT, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE workflow_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL REFERENCES runs(id), snapshot_json TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE artifacts (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), kind TEXT NOT NULL, media_type TEXT NOT NULL, path TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE events (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), sequence INTEGER NOT NULL, timestamp TEXT NOT NULL, task_id TEXT NOT NULL, attempt_id TEXT NOT NULL, type TEXT NOT NULL, actor_kind TEXT NOT NULL, actor_id TEXT NOT NULL, summary TEXT NOT NULL, data_json TEXT NOT NULL, envelope_json TEXT NOT NULL, UNIQUE(run_id, sequence));
CREATE INDEX idx_events_run_sequence ON events(run_id, sequence);
INSERT INTO runs(id, idea, status, event_log_artifact_id, created_at, updated_at) VALUES ('run_legacy', 'idea', 'running', 'artifact_log', '2026-06-04T00:00:00Z', '2026-06-04T00:00:00Z');
INSERT INTO tasks(id, run_id, idea, status, created_at, updated_at) VALUES ('task_legacy', 'run_legacy', 'idea', 'running', '2026-06-04T00:00:00Z', '2026-06-04T00:00:00Z');
INSERT INTO attempts(id, run_id, task_id, status, created_at, updated_at) VALUES ('attempt_legacy', 'run_legacy', 'task_legacy', 'running', '2026-06-04T00:00:00Z', '2026-06-04T00:00:00Z');
INSERT INTO stages(id, run_id, task_id, attempt_id, stage_type, adapter, status, created_at, updated_at) VALUES ('stage_legacy', 'run_legacy', 'task_legacy', 'attempt_legacy', 'implementation', 'noop', 'running', '2026-06-04T00:00:00Z', '2026-06-04T00:00:00Z');
INSERT INTO workflow_snapshots(run_id, snapshot_json, created_at) VALUES ('run_legacy', '{}', '2026-06-04T00:00:00Z');
INSERT INTO artifacts(id, run_id, kind, media_type, path, created_at) VALUES ('artifact_log', 'run_legacy', 'event_log', 'application/x-jsonlines', '` + filepath.Join(artifactDir, "artifact_log.jsonl") + `', '2026-06-04T00:00:00Z');
INSERT INTO events(id, run_id, sequence, timestamp, task_id, attempt_id, type, actor_kind, actor_id, summary, data_json, envelope_json) VALUES ('evt_legacy', 'run_legacy', 1, '2026-06-04T00:00:00Z', 'task_legacy', 'attempt_legacy', 'run.created', 'user', 'test', 'created', '{}', '{"schema_version":1,"id":"evt_legacy","sequence":1,"timestamp":"2026-06-04T00:00:00Z","run_id":"run_legacy","task_id":"task_legacy","attempt_id":"attempt_legacy","type":"run.created","actor":{"kind":"user","id":"test"},"summary":"created","data":{}}');
`
	if _, err := db.ExecContext(ctx, legacy); err != nil {
		_ = db.Close()
		t.Fatalf("seed legacy db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy db: %v", err)
	}

	st, err := Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	defer st.Close()
	var scope string
	var runIDNotNull int
	if err := st.DB().QueryRowContext(ctx, `SELECT scope, run_id IS NOT NULL FROM events WHERE id = 'evt_legacy'`).Scan(&scope, &runIDNotNull); err != nil {
		t.Fatalf("read migrated event row: %v", err)
	}
	if scope != "run:run_legacy" || runIDNotNull != 1 {
		t.Fatalf("migrated scope/run_id = %q/%d, want run:run_legacy/non-null", scope, runIDNotNull)
	}
	persisted, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, RunID: "run_legacy", TaskID: "task_legacy", AttemptID: "attempt_legacy", Type: "run.started", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "started", Data: map[string]any{}})
	if err != nil {
		t.Fatalf("append after migration: %v", err)
	}
	if persisted.Sequence != 2 {
		t.Fatalf("post-migration sequence = %d, want 2", persisted.Sequence)
	}
}

func TestGetWorkflowRunSelectsLatestAttemptStages(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "retry a thing")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	later := "2099-01-01T00:00:00Z"
	attemptID := "attempt_later"
	ideaStageID := "stage_idea_later"
	implStageID := "stage_impl_later"
	validationStageID := "stage_validation_later"
	commitStageID := "stage_commit_later"
	prReadyStageID := "stage_pr_ready_later"
	if _, err := st.DB().ExecContext(ctx, `INSERT INTO attempts(id, project_id, run_id, task_id, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, attemptID, wr.Run.ProjectID, wr.Run.ID, wr.Task.ID, RunStatusPending, later, later); err != nil {
		t.Fatalf("insert later attempt: %v", err)
	}
	for _, stage := range []Stage{
		{ID: ideaStageID, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypeIdeaIntake, Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
		{ID: implStageID, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypeImplementation, Adapter: "noop", Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
		{ID: validationStageID, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypeValidation, Adapter: "validation", Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
		{ID: commitStageID, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypeCommit, Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
		{ID: prReadyStageID, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypePRReady, Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
	} {
		if _, err := st.DB().ExecContext(ctx, `INSERT INTO stages(id, project_id, run_id, task_id, attempt_id, stage_type, adapter, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, stage.ID, stage.ProjectID, stage.RunID, stage.TaskID, stage.AttemptID, stage.StageType, stage.Adapter, stage.Status, stage.CreatedAt, stage.UpdatedAt); err != nil {
			t.Fatalf("insert later stage %s: %v", stage.ID, err)
		}
	}

	got, err := st.GetWorkflowRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("GetWorkflowRun() error = %v", err)
	}
	if got.Attempt.ID != attemptID {
		t.Fatalf("Attempt.ID = %q, want %q", got.Attempt.ID, attemptID)
	}
	if got.IdeaIntakeStage.ID != ideaStageID || got.IdeaIntakeStage.AttemptID != attemptID {
		t.Fatalf("IdeaIntakeStage = %+v, want latest attempt stage", got.IdeaIntakeStage)
	}
	if got.ImplementationStage.ID != implStageID || got.ImplementationStage.AttemptID != attemptID {
		t.Fatalf("ImplementationStage = %+v, want latest attempt stage", got.ImplementationStage)
	}
	if got.ValidationStage.ID != validationStageID || got.ValidationStage.AttemptID != attemptID {
		t.Fatalf("ValidationStage = %+v, want latest attempt stage", got.ValidationStage)
	}
	if got.CommitStage.ID != commitStageID || got.CommitStage.AttemptID != attemptID {
		t.Fatalf("CommitStage = %+v, want latest attempt stage", got.CommitStage)
	}
	if got.PRReadyStage.ID != prReadyStageID || got.PRReadyStage.AttemptID != attemptID {
		t.Fatalf("PRReadyStage = %+v, want latest attempt stage", got.PRReadyStage)
	}
}

func TestUpdateRunStatusFromAndAppendSystemEventIsAtomic(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "dispatch atomically")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	ev, changed, err := st.UpdateRunStatusFromAndAppendSystemEvent(ctx, wr.Run.ID, RunStatusPending, RunStatusRunning, event.Event{Type: "queue.dispatched", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "queued run dispatched", Data: map[string]any{"run_id": wr.Run.ID}})
	if err != nil {
		t.Fatalf("transition with event: %v", err)
	}
	if !changed || ev.Sequence != 1 || ev.RunID != "" {
		t.Fatalf("event=%+v changed=%v, want system event sequence 1 with changed=true", ev, changed)
	}
	run, err := st.GetRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != RunStatusRunning {
		t.Fatalf("status = %s, want running", run.Status)
	}
	_, changed, err = st.UpdateRunStatusFromAndAppendSystemEvent(ctx, wr.Run.ID, RunStatusPending, RunStatusRunning, event.Event{Type: "queue.dispatched", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "should not persist", Data: map[string]any{"run_id": wr.Run.ID}})
	if err != nil {
		t.Fatalf("unchanged transition: %v", err)
	}
	if changed {
		t.Fatal("changed=true for stale pending->running transition")
	}
	page, err := st.ListSystemEventsPage(ctx, 0, 20)
	if err != nil {
		t.Fatalf("list system events: %v", err)
	}
	if len(page.Events) != 1 || page.Events[0].Event.Type != "queue.dispatched" {
		t.Fatalf("system events = %#v, want exactly one queue.dispatched", page.Events)
	}
}
