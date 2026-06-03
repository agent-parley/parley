package adapter

import (
	"context"
	"fmt"
	"time"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/ids"
	"github.com/agent-parley/parley/internal/shared/report"
)

type Noop struct{}

func (Noop) Name() string { return "noop" }

func (Noop) Run(ctx context.Context, disp contract.Dispatch, sink EventSink) (report.Report, error) {
	if err := sink.Emit(ctx, progressEvent(disp, "noop adapter started", map[string]any{"step": "start"})); err != nil {
		return report.Report{}, err
	}

	delay := 50 * time.Millisecond
	if raw, ok := disp.Input["sleep_ms"]; ok {
		switch v := raw.(type) {
		case float64:
			delay = time.Duration(v) * time.Millisecond
		case int:
			delay = time.Duration(v) * time.Millisecond
		}
	}

	select {
	case <-ctx.Done():
		return report.Report{}, fmt.Errorf("noop canceled: %w", ctx.Err())
	case <-time.After(delay):
	}

	artifactID := ids.New("artifact")
	payload := map[string]any{
		"noop": true,
		"artifact": map[string]any{
			"id":      artifactID,
			"name":    "noop.txt",
			"content": "noop adapter artifact\n",
		},
	}
	if idea, ok := disp.Input["idea"]; ok {
		payload["idea"] = idea
	}

	if err := sink.Emit(ctx, progressEvent(disp, "noop adapter produced artifact metadata", map[string]any{"artifact_id": artifactID})); err != nil {
		return report.Report{}, err
	}

	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: "noop"},
		Status:        report.StatusCompleted,
		Verdict:       nil,
		Summary:       "noop implementation completed",
		EvidenceRefs:  []string{artifactID},
		Payload:       payload,
		Errors:        []string{},
	}, nil
}

func progressEvent(disp contract.Dispatch, summary string, data map[string]any) event.Event {
	return event.Event{
		SchemaVersion: event.SchemaVersion,
		ID:            ids.New("evt"),
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		Type:          "adapter.progress",
		Actor:         event.Actor{Kind: event.ActorKindAdapter, ID: "noop"},
		Summary:       summary,
		Data:          data,
	}
}
