package store

import (
	"context"
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
	if len(bundle.Stages) != 2 || len(bundle.Events) != 1 || len(bundle.Artifacts) != 2 {
		t.Fatalf("unexpected bundle counts: stages=%d events=%d artifacts=%d", len(bundle.Stages), len(bundle.Events), len(bundle.Artifacts))
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
	implStageID := "stage_impl_later"
	validationStageID := "stage_validation_later"
	if _, err := st.DB().ExecContext(ctx, `INSERT INTO attempts(id, run_id, task_id, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, attemptID, wr.Run.ID, wr.Task.ID, RunStatusPending, later, later); err != nil {
		t.Fatalf("insert later attempt: %v", err)
	}
	for _, stage := range []Stage{
		{ID: implStageID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypeImplementation, Adapter: "noop", Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
		{ID: validationStageID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypeValidation, Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
	} {
		if _, err := st.DB().ExecContext(ctx, `INSERT INTO stages(id, run_id, task_id, attempt_id, stage_type, adapter, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, stage.ID, stage.RunID, stage.TaskID, stage.AttemptID, stage.StageType, stage.Adapter, stage.Status, stage.CreatedAt, stage.UpdatedAt); err != nil {
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
	if got.ImplementationStage.ID != implStageID || got.ImplementationStage.AttemptID != attemptID {
		t.Fatalf("ImplementationStage = %+v, want latest attempt stage", got.ImplementationStage)
	}
	if got.ValidationStage.ID != validationStageID || got.ValidationStage.AttemptID != attemptID {
		t.Fatalf("ValidationStage = %+v, want latest attempt stage", got.ValidationStage)
	}
}
