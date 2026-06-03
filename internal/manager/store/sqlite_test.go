package store

import (
	"context"
	"strings"
	"testing"

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
