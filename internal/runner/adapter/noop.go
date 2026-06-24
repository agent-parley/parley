package adapter

import (
	"context"
	"fmt"
	"time"

	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/ids"
	"github.com/agent-parley/parley/internal/shared/report"
)

type Noop struct{}

func (Noop) Name() string { return "noop" }

func (Noop) Run(ctx context.Context, disp contract.Dispatch, sink runnerio.Sink) (report.Report, error) {
	if disp.StageType == contract.StageTypeConversation || (disp.Input != nil && disp.Input["input_mode"] == contract.AdapterInputModeConversation) {
		return noopConversationReport(disp), nil
	}
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
	if err := sink.Artifact(ctx, runnerio.Artifact{
		ID:        artifactID,
		Name:      "noop.txt",
		Kind:      "adapter_output",
		MediaType: "text/plain",
		Content:   []byte("noop adapter artifact\n"),
	}); err != nil {
		return report.Report{}, err
	}

	payload := map[string]any{"noop": true}
	if idea, ok := disp.Input["idea"]; ok {
		payload["idea"] = idea
	}
	if disp.Input != nil && disp.Input["input_mode"] == contract.AdapterInputModePlanning {
		payload["task_plan_markdown"] = noopTaskPlanMarkdown(disp)
	}

	if err := sink.Emit(ctx, progressEvent(disp, "noop adapter produced artifact", map[string]any{"artifact_id": artifactID})); err != nil {
		return report.Report{}, err
	}

	summary := "noop implementation completed"
	if disp.Input != nil && disp.Input["input_mode"] == contract.AdapterInputModePlanning {
		summary = "noop planner produced a task plan"
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
		Summary:       summary,
		EvidenceRefs:  []string{artifactID},
		Payload:       payload,
		Errors:        []string{},
	}, nil
}

func noopConversationReport(disp contract.Dispatch) report.Report {
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: "noop"},
		Status:        report.StatusCompleted,
		Summary:       "noop conversational reply",
		Payload: map[string]any{
			"reply_markdown": "I received your message. The noop adapter does not inspect repository content; configure the Pi adapter for repo-aware conversation turns.",
		},
		Errors: []string{},
	}
}

func noopTaskPlanMarkdown(disp contract.Dispatch) string {
	idea := fmt.Sprint(disp.Input["idea"])
	level := fmt.Sprint(disp.Input["refinement_level"])
	if level == "" || level == "<nil>" {
		level = contract.RefinementLevelStandard
	}
	return fmt.Sprintf("# Task Plan\n\n"+
		"Project ID: `%s`\n"+
		"Run ID: `%s`\n"+
		"Task ID: `%s`\n"+
		"Attempt ID: `%s`\n"+
		"Refinement level: `%s`\n\n"+
		"## User Idea\n\n%s\n\n"+
		"## Plan Boundary\n\n"+
		"This artifact is a task plan, not a workflow definition. It does not choose, add, remove, or reorder workflow stages.\n\n"+
		"## Objective\n\n"+
		"Plan the submitted idea while preserving repository behavior outside the requested change.\n\n"+
		"## Repo Evidence Considered\n\n"+
		"- Noop adapter does not inspect repository content; real planner adapters should use the Stage Brief and /project/repo evidence.\n\n"+
		"## Implementation Approach\n\n"+
		"- Inspect the affected code path, make the smallest coherent change, and add or update focused tests.\n\n"+
		"## Assumptions\n\n"+
		"- The submitted idea is the authoritative task statement.\n\n"+
		"## Open Questions\n\n"+
		"- No blocking question was raised during single-shot planning; resolve any project-specific details during plan review.\n\n"+
		"## Validation\n\n"+
		"- Run the narrowest meaningful tests for the changed path.\n", disp.ProjectID, disp.RunID, disp.TaskID, disp.AttemptID, level, idea)
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
