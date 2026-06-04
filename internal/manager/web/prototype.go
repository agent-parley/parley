package web

import (
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
)

type PrototypeData struct {
	Title        string
	Variation    string
	Variations   []PrototypeVariation
	Feedback     string
	Runs         []PrototypeRun
	Runners      []PrototypeRunnerView
	RunnerEvents []PrototypeEventView
	Selected     PrototypeRun
}

type PrototypeVariation struct {
	ID      string
	Name    string
	Summary string
}

type PrototypeRun struct {
	View             RunView
	OutcomeNote      string
	RunnerID         string
	RunnerHealth     string
	TaskPlan         string
	DoneWhen         []string
	CancelEnabled    bool
	StageGroups      []PrototypeStageGroup
	RunEvents        []PrototypeEventView
	EventViews       []PrototypeEventView
	SafetyHighlights []string
}

type PrototypeRunnerView struct {
	Runner       store.Runner
	CurrentRunID string
	Summary      string
}

type PrototypeStageGroup struct {
	Stage     store.Stage
	Label     string
	Performer string
	Events    []PrototypeEventView
}

type PrototypeEventView struct {
	Event      event.Event
	Family     string
	StageType  string
	StageLabel string
	ActorLabel string
}

func NewPrototypeData(variation, runID, mock string) PrototypeData {
	variation = normalizePrototypeVariation(variation)
	runs := prototypeRuns()
	selected := runs[0]
	for _, run := range runs {
		if run.View.Run.ID == runID {
			selected = run
			break
		}
	}
	return PrototypeData{
		Title:        "Parley UI prototype",
		Variation:    variation,
		Variations:   prototypeVariations(),
		Feedback:     prototypeFeedback(mock),
		Runs:         runs,
		Runners:      prototypeRunners(),
		RunnerEvents: prototypeRunnerEvents(),
		Selected:     selected,
	}
}

func normalizePrototypeVariation(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "2", "lanes", "swimlanes":
		return "2"
	case "3", "safety", "review":
		return "3"
	default:
		return "1"
	}
}

func prototypeVariations() []PrototypeVariation {
	return []PrototypeVariation{
		{ID: "1", Name: "Command center", Summary: "Dense operations dashboard: run outcomes, runner health, and the active run stay visible together."},
		{ID: "2", Name: "Stage/event map", Summary: "Timeline-first layout: each stage owns its local events so causality is easier to scan."},
		{ID: "3", Name: "Safety review", Summary: "Review-console layout: diff, outputs, cancellation, and safety constraints are promoted above raw chronology."},
	}
}

func prototypeFeedback(mock string) string {
	switch mock {
	case "create":
		return "Mock only: Create run would freeze the task plan and enqueue a run. No run was started."
	case "cancel":
		return "Mock only: Cancel run would send a user cancel intent to the harness. The seeded run stays running for comparison."
	default:
		return ""
	}
}

func prototypeRuns() []PrototypeRun {
	return []PrototypeRun{
		prototypeRunningRun(),
		prototypeCompletedRun(),
		prototypeFailedRun(),
		prototypeCancelledRun(),
		prototypeAbandonedRun(),
	}
}

func prototypeRunningRun() PrototypeRun {
	runID := "run_proto_running"
	taskID := "task_proto_shipping"
	attemptID := "attempt_proto_001"
	stages := prototypeStages(runID, taskID, attemptID, map[string]string{
		contract.StageTypeIdeaIntake:     "completed",
		contract.StageTypeImplementation: "completed",
		contract.StageTypeValidation:     "running",
		contract.StageTypeCommit:         "pending",
		contract.StageTypePRReady:        "pending",
	})
	events := []event.Event{
		prototypeEvent(1, runID, taskID, attemptID, "run.created", event.ActorKindUser, "operator", "Run created from a task plan", map[string]any{"runner_id": "runner_local_1"}),
		prototypeEvent(2, runID, taskID, attemptID, "run.started", event.ActorKindWorkflowEngine, "manager", "Workflow started", map[string]any{"runner_id": "runner_local_1"}),
		prototypeEvent(3, runID, taskID, attemptID, "stage.started", event.ActorKindWorkflowEngine, "manager", "Idea intake started", map[string]any{"stage_type": contract.StageTypeIdeaIntake}),
		prototypeEvent(4, runID, taskID, attemptID, "harness.completed", event.ActorKindHarness, "idea_intake", "Task plan frozen from the submitted idea", map[string]any{"stage_type": contract.StageTypeIdeaIntake, "task_contract_artifact_id": "artifact_contract_running"}),
		prototypeEvent(5, runID, taskID, attemptID, "stage.completed", event.ActorKindWorkflowEngine, "manager", "Idea intake completed", map[string]any{"stage_type": contract.StageTypeIdeaIntake, "status": "completed"}),
		prototypeEvent(6, runID, taskID, attemptID, "stage.started", event.ActorKindWorkflowEngine, "manager", "Implementation started", map[string]any{"stage_type": contract.StageTypeImplementation}),
		prototypeEvent(7, runID, taskID, attemptID, "adapter.invocation_prepared", event.ActorKindAdapter, "pi", "Pi agent profile received the implementation brief", map[string]any{"stage_type": contract.StageTypeImplementation}),
		prototypeEvent(8, runID, taskID, attemptID, "adapter.started", event.ActorKindAdapter, "pi", "Worker agent started in an isolated worktree", map[string]any{"stage_type": contract.StageTypeImplementation}),
		prototypeEvent(9, runID, taskID, attemptID, "adapter.progress", event.ActorKindAdapter, "pi", "Updated the run detail template and captured a preview artifact", map[string]any{"stage_type": contract.StageTypeImplementation}),
		prototypeEvent(10, runID, taskID, attemptID, "adapter.output", event.ActorKindAdapter, "pi", "Worker streamed implementation notes", map[string]any{"stage_type": contract.StageTypeImplementation, "artifact_id": "artifact_notes_running"}),
		prototypeEvent(11, runID, taskID, attemptID, "artifact.created", event.ActorKindAdapter, "pi", "Implementation notes output created", map[string]any{"stage_type": contract.StageTypeImplementation, "artifact_id": "artifact_notes_running"}),
		prototypeEvent(12, runID, taskID, attemptID, "adapter.completed", event.ActorKindAdapter, "pi", "Implementation completed with files changed", map[string]any{"stage_type": contract.StageTypeImplementation, "report_artifact_id": "artifact_report_running"}),
		prototypeEvent(13, runID, taskID, attemptID, "stage.completed", event.ActorKindWorkflowEngine, "manager", "Implementation completed", map[string]any{"stage_type": contract.StageTypeImplementation, "status": "completed"}),
		prototypeEvent(14, runID, taskID, attemptID, "stage.started", event.ActorKindWorkflowEngine, "manager", "Validation started", map[string]any{"stage_type": contract.StageTypeValidation}),
		prototypeEvent(15, runID, taskID, attemptID, "security.mounts_resolved", event.ActorKindSecurity, "sandbox", "Validation sandbox mounts resolved", map[string]any{"stage_type": contract.StageTypeValidation}),
		prototypeEvent(16, runID, taskID, attemptID, "security.network_policy_applied", event.ActorKindSecurity, "sandbox", "Validation network policy applied", map[string]any{"stage_type": contract.StageTypeValidation, "network": "none"}),
		prototypeEvent(17, runID, taskID, attemptID, "harness.progress", event.ActorKindHarness, "validation", "go build ./... passed; go test ./... still running", map[string]any{"stage_type": contract.StageTypeValidation}),
		prototypeEvent(18, runID, taskID, attemptID, "diff.captured", event.ActorKindGit, "worktree", "diff.patch captured for review", map[string]any{"stage_type": contract.StageTypeValidation, "diff_artifact_id": "artifact_diff_running"}),
	}
	diff := ArtifactView{ID: "artifact_diff_running", Kind: "diff_patch", MediaType: "text/x-diff", Preview: prototypeDiffPatch()}
	artifacts := []ArtifactView{
		diff,
		{ID: "artifact_contract_running", Kind: "task_plan", MediaType: "text/markdown", Preview: "# Task plan\n\nRender a richer operator UI prototype behind `/prototype` with seeded runs, runner health, stage events, and safe output viewers."},
		{ID: "artifact_notes_running", Kind: "agent_output", MediaType: "text/markdown", Preview: "## Worker notes\n\n- Stage/event grouping needs to be visually closer.\n- The diff viewer must stay escaped preformatted text.\n- Raw HTML output is listed as download only."},
		{ID: "artifact_html_running", Kind: "agent_output", MediaType: "text/html", DownloadOnly: true},
		{ID: "artifact_report_running", Kind: "report", MediaType: "application/json", Preview: "{\n  \"status\": \"completed\",\n  \"summary\": \"implementation complete\"\n}"},
	}
	return prototypeRun(runID, taskID, attemptID, "running", "Richer operator UI prototype with stage/event legibility", "Validation is running in a sandbox; the next terminal outcome is not known yet.", "runner_local_1", "connected", stages, events, artifacts, diff, PRReadyView{}, true)
}

func prototypeCompletedRun() PrototypeRun {
	runID := "run_proto_completed"
	taskID := "task_proto_complete"
	attemptID := "attempt_proto_002"
	stages := prototypeStages(runID, taskID, attemptID, map[string]string{
		contract.StageTypeIdeaIntake:     "completed",
		contract.StageTypeImplementation: "completed",
		contract.StageTypeValidation:     "completed",
		contract.StageTypeCommit:         "completed",
		contract.StageTypePRReady:        "completed",
	})
	events := []event.Event{
		prototypeEvent(1, runID, taskID, attemptID, "run.created", event.ActorKindUser, "operator", "Run created", nil),
		prototypeEvent(2, runID, taskID, attemptID, "run.started", event.ActorKindWorkflowEngine, "manager", "Workflow started", nil),
		prototypeEvent(3, runID, taskID, attemptID, "stage.completed", event.ActorKindWorkflowEngine, "manager", "Validation passed", map[string]any{"stage_type": contract.StageTypeValidation}),
		prototypeEvent(4, runID, taskID, attemptID, "harness.completed", event.ActorKindHarness, "commit", "Commit recorded with hooks disabled", map[string]any{"stage_type": contract.StageTypeCommit, "branch": "agent/run_proto_completed/task_proto_complete", "commit_sha": "e4f7c91"}),
		prototypeEvent(5, runID, taskID, attemptID, "stage.completed", event.ActorKindWorkflowEngine, "manager", "PR-ready stop reached", map[string]any{"stage_type": contract.StageTypePRReady, "branch": "agent/run_proto_completed/task_proto_complete", "diff_artifact_id": "artifact_diff_completed"}),
		prototypeEvent(6, runID, taskID, attemptID, "run.completed", event.ActorKindWorkflowEngine, "manager", "Run completed and stopped at PR-ready", map[string]any{"terminal_status": "completed", "branch": "agent/run_proto_completed/task_proto_complete", "commit_sha": "e4f7c91", "diff_artifact_id": "artifact_diff_completed"}),
	}
	diff := ArtifactView{ID: "artifact_diff_completed", Kind: "diff_patch", MediaType: "text/x-diff", Preview: "diff --git a/README.md b/README.md\n+Prototype completed example\n"}
	return prototypeRun(runID, taskID, attemptID, "completed", "Tighten README status copy", "Completed and waiting for a human to take the PR-ready branch forward.", "runner_local_1", "connected", stages, events, []ArtifactView{diff}, diff, PRReadyView{Ready: true, Branch: "agent/run_proto_completed/task_proto_complete", CommitSHA: "e4f7c91", DiffArtifactID: diff.ID}, false)
}

func prototypeFailedRun() PrototypeRun {
	runID := "run_proto_failed"
	taskID := "task_proto_failed"
	attemptID := "attempt_proto_003"
	stages := prototypeStages(runID, taskID, attemptID, map[string]string{
		contract.StageTypeIdeaIntake:     "completed",
		contract.StageTypeImplementation: "completed",
		contract.StageTypeValidation:     "failed",
		contract.StageTypeCommit:         "pending",
		contract.StageTypePRReady:        "pending",
	})
	events := []event.Event{
		prototypeEvent(1, runID, taskID, attemptID, "run.created", event.ActorKindUser, "operator", "Run created", nil),
		prototypeEvent(2, runID, taskID, attemptID, "run.started", event.ActorKindWorkflowEngine, "manager", "Workflow started", nil),
		prototypeEvent(3, runID, taskID, attemptID, "harness.failed", event.ActorKindHarness, "validation", "Validation command exited non-zero", map[string]any{"stage_type": contract.StageTypeValidation, "exit_code": 1}),
		prototypeEvent(4, runID, taskID, attemptID, "stage.failed", event.ActorKindWorkflowEngine, "manager", "Validation failed", map[string]any{"stage_type": contract.StageTypeValidation, "status": "failed"}),
		prototypeEvent(5, runID, taskID, attemptID, "run.failed", event.ActorKindWorkflowEngine, "manager", "Run failed during validation", map[string]any{"terminal_status": "failed"}),
	}
	return prototypeRun(runID, taskID, attemptID, "failed", "Add retry policy examples", "Failed because the deterministic validation gate returned a non-zero exit.", "runner_remote_7", "down", stages, events, nil, ArtifactView{}, PRReadyView{}, false)
}

func prototypeCancelledRun() PrototypeRun {
	runID := "run_proto_cancelled"
	taskID := "task_proto_cancelled"
	attemptID := "attempt_proto_004"
	stages := prototypeStages(runID, taskID, attemptID, map[string]string{
		contract.StageTypeIdeaIntake:     "completed",
		contract.StageTypeImplementation: "failed",
		contract.StageTypeValidation:     "pending",
		contract.StageTypeCommit:         "pending",
		contract.StageTypePRReady:        "pending",
	})
	events := []event.Event{
		prototypeEvent(1, runID, taskID, attemptID, "run.created", event.ActorKindUser, "operator", "Run created", nil),
		prototypeEvent(2, runID, taskID, attemptID, "run.started", event.ActorKindWorkflowEngine, "manager", "Workflow started", nil),
		prototypeEvent(3, runID, taskID, attemptID, "adapter.failed", event.ActorKindAdapter, "pi", "Worker stopped after user cancellation", map[string]any{"stage_type": contract.StageTypeImplementation}),
		prototypeEvent(4, runID, taskID, attemptID, "stage.failed", event.ActorKindWorkflowEngine, "manager", "Implementation interrupted", map[string]any{"stage_type": contract.StageTypeImplementation, "status": "failed"}),
		prototypeEvent(5, runID, taskID, attemptID, "run.cancelled", event.ActorKindWorkflowEngine, "manager", "Run cancelled by the operator", map[string]any{"terminal_status": "cancelled"}),
	}
	return prototypeRun(runID, taskID, attemptID, "cancelled", "Explore alternate onboarding copy", "Cancelled is a deliberate user stop, not a failed report.", "runner_local_1", "connected", stages, events, nil, ArtifactView{}, PRReadyView{}, false)
}

func prototypeAbandonedRun() PrototypeRun {
	runID := "run_proto_abandoned"
	taskID := "task_proto_abandoned"
	attemptID := "attempt_proto_005"
	stages := prototypeStages(runID, taskID, attemptID, map[string]string{
		contract.StageTypeIdeaIntake:     "completed",
		contract.StageTypeImplementation: "completed",
		contract.StageTypeValidation:     "completed",
		contract.StageTypeCommit:         "pending",
		contract.StageTypePRReady:        "pending",
	})
	events := []event.Event{
		prototypeEvent(1, runID, taskID, attemptID, "run.created", event.ActorKindUser, "operator", "Run created", nil),
		prototypeEvent(2, runID, taskID, attemptID, "run.started", event.ActorKindWorkflowEngine, "manager", "Workflow started", nil),
		prototypeEvent(3, runID, taskID, attemptID, "stage.invalid", event.ActorKindWorkflowEngine, "manager", "Report needed input the skeleton cannot resume", map[string]any{"stage_type": contract.StageTypeCommit, "status": "needs_input"}),
		prototypeEvent(4, runID, taskID, attemptID, "run.abandoned", event.ActorKindWorkflowEngine, "manager", "Run abandoned because there is no resume path", map[string]any{"terminal_status": "needs_input"}),
	}
	return prototypeRun(runID, taskID, attemptID, "abandoned", "Draft memory update preview", "Abandoned means no resume path in the current skeleton, distinct from cancel and failure.", "runner_lab_2", "suspect", stages, events, nil, ArtifactView{}, PRReadyView{}, false)
}

func prototypeRun(runID, taskID, attemptID, status, idea, note, runnerID, runnerHealth string, stages []store.Stage, events []event.Event, artifacts []ArtifactView, diff ArtifactView, prReady PRReadyView, cancelEnabled bool) PrototypeRun {
	run := store.Run{ID: runID, Idea: idea, Status: status, EventLogArtifactID: "artifact_event_log_" + strings.TrimPrefix(runID, "run_proto_"), CreatedAt: "2026-06-04T14:00:00Z", UpdatedAt: "2026-06-04T14:35:00Z"}
	task := store.Task{ID: taskID, RunID: runID, Idea: idea, Status: status, CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt}
	attempt := store.Attempt{ID: attemptID, RunID: runID, TaskID: taskID, Status: status, CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt}
	view := RunView{
		RunBundle:     store.RunBundle{Run: run, Task: task, Attempt: attempt, Stages: stages, Events: events},
		ArtifactViews: artifacts,
		DiffPatch:     diff,
		PRReady:       prReady,
	}
	return PrototypeRun{
		View:          view,
		OutcomeNote:   note,
		RunnerID:      runnerID,
		RunnerHealth:  runnerHealth,
		TaskPlan:      "Seeded task plan: " + idea,
		DoneWhen:      []string{"Stage timeline shows each stage outcome", "Event stream maps performer-family events to stages", "diff.patch stays escaped and raw HTML output is download-only"},
		CancelEnabled: cancelEnabled,
		StageGroups:   prototypeStageGroups(stages, events),
		RunEvents:     prototypeRunEvents(events),
		EventViews:    prototypeEventViews(events),
		SafetyHighlights: []string{
			"Outputs are referenced by artifact ID only.",
			"diff.patch is escaped preformatted text, not rendered markup.",
			"Raw HTML output is marked download-only.",
		},
	}
}

func prototypeStages(runID, taskID, attemptID string, statuses map[string]string) []store.Stage {
	stageTypes := []string{contract.StageTypeIdeaIntake, contract.StageTypeImplementation, contract.StageTypeValidation, contract.StageTypeCommit, contract.StageTypePRReady}
	stages := make([]store.Stage, 0, len(stageTypes))
	for i, stageType := range stageTypes {
		status := statuses[stageType]
		if status == "" {
			status = "pending"
		}
		adapter := ""
		if stageType == contract.StageTypeImplementation {
			adapter = "pi"
		}
		stages = append(stages, store.Stage{ID: fmt.Sprintf("stage_%s_%02d", strings.TrimPrefix(runID, "run_proto_"), i+1), RunID: runID, TaskID: taskID, AttemptID: attemptID, StageType: stageType, Adapter: adapter, Status: status, CreatedAt: "2026-06-04T14:00:00Z", UpdatedAt: "2026-06-04T14:35:00Z"})
	}
	return stages
}

func prototypeStageGroups(stages []store.Stage, events []event.Event) []PrototypeStageGroup {
	groups := make([]PrototypeStageGroup, 0, len(stages))
	for _, stage := range stages {
		group := PrototypeStageGroup{Stage: stage, Label: stageLabel(stage.StageType), Performer: prototypePerformer(stage)}
		for _, ev := range events {
			if stageTypeFromEvent(ev) == stage.StageType {
				group.Events = append(group.Events, prototypeEventView(ev))
			}
		}
		groups = append(groups, group)
	}
	return groups
}

func prototypeRunEvents(events []event.Event) []PrototypeEventView {
	views := []PrototypeEventView{}
	for _, ev := range events {
		if strings.HasPrefix(ev.Type, "run.") {
			views = append(views, prototypeEventView(ev))
		}
	}
	return views
}

func prototypeEventViews(events []event.Event) []PrototypeEventView {
	views := make([]PrototypeEventView, 0, len(events))
	for _, ev := range events {
		views = append(views, prototypeEventView(ev))
	}
	return views
}

func prototypeEventView(ev event.Event) PrototypeEventView {
	stageType := stageTypeFromEvent(ev)
	return PrototypeEventView{
		Event:      ev,
		Family:     eventFamily(ev.Type),
		StageType:  stageType,
		StageLabel: stageLabel(stageType),
		ActorLabel: actorLabel(ev.Actor),
	}
}

func prototypePerformer(stage store.Stage) string {
	if stage.Adapter != "" {
		return "Agent profile " + stage.Adapter
	}
	return "Harness"
}

func stageTypeFromEvent(ev event.Event) string {
	if ev.Data == nil {
		return ""
	}
	stageType, _ := ev.Data["stage_type"].(string)
	return stageType
}

func eventFamily(eventType string) string {
	if i := strings.Index(eventType, "."); i > 0 {
		return eventType[:i]
	}
	return eventType
}

func actorLabel(actor event.Actor) string {
	if actor.ID == "" {
		return actor.Kind
	}
	switch actor.Kind {
	case event.ActorKindAdapter:
		return "agent profile/" + actor.ID
	case event.ActorKindWorkflowEngine:
		return "workflow engine/" + actor.ID
	default:
		return actor.Kind + "/" + actor.ID
	}
}

func stageLabel(stageType string) string {
	switch stageType {
	case contract.StageTypeIdeaIntake:
		return "Idea intake"
	case contract.StageTypeImplementation:
		return "Implementation"
	case contract.StageTypeValidation:
		return "Validation"
	case contract.StageTypeCommit:
		return "Commit"
	case contract.StageTypePRReady:
		return "PR-ready stop"
	case "":
		return "Run lifecycle"
	default:
		return stageType
	}
}

func prototypeEvent(sequence int64, runID, taskID, attemptID, eventType, actorKind, actorID, summary string, data map[string]any) event.Event {
	if data == nil {
		data = map[string]any{}
	}
	return event.Event{
		SchemaVersion: event.SchemaVersion,
		ID:            fmt.Sprintf("evt_%s_%02d", strings.TrimPrefix(runID, "run_proto_"), sequence),
		Sequence:      sequence,
		Timestamp:     "2026-06-04T14:00:00Z",
		RunID:         runID,
		TaskID:        taskID,
		AttemptID:     attemptID,
		Type:          eventType,
		Actor:         event.Actor{Kind: actorKind, ID: actorID},
		Summary:       summary,
		Data:          data,
	}
}

func prototypeRunners() []PrototypeRunnerView {
	return []PrototypeRunnerView{
		{Runner: store.Runner{ID: "runner_local_1", Status: store.RunnerStatusConnected, Origin: store.RunnerOriginSpawned, MissedHeartbeats: 0, ConnectedAt: "2026-06-04T13:55:00Z", UpdatedAt: "2026-06-04T14:35:00Z"}, CurrentRunID: "run_proto_running", Summary: "Spawned local runner is connected and owns the mid-flight validation run."},
		{Runner: store.Runner{ID: "runner_lab_2", Status: store.RunnerStatusSuspect, Origin: store.RunnerOriginRegistered, MissedHeartbeats: 2, ConnectedAt: "2026-06-04T13:10:00Z", UpdatedAt: "2026-06-04T14:31:00Z"}, CurrentRunID: "run_proto_abandoned", Summary: "Registered remote runner missed heartbeats; observe-only, never auto-restarted."},
		{Runner: store.Runner{ID: "runner_remote_7", Status: store.RunnerStatusDown, Origin: store.RunnerOriginRegistered, MissedHeartbeats: 5, ConnectedAt: "2026-06-04T12:40:00Z", UpdatedAt: "2026-06-04T14:05:00Z"}, CurrentRunID: "run_proto_failed", Summary: "Down registered runner explains a failed run without implying user cancellation."},
	}
}

func prototypeRunnerEvents() []PrototypeEventView {
	events := []event.Event{
		prototypeRunnerEvent(1, "runner_local_1", "runner.registered", "spawned runner registered", map[string]any{"status": store.RunnerStatusConnected, "origin": store.RunnerOriginSpawned}),
		prototypeRunnerEvent(2, "runner_local_1", "runner.ready", "spawned runner ready", map[string]any{"status": store.RunnerStatusConnected}),
		prototypeRunnerEvent(1, "runner_lab_2", "runner.registered", "registered runner observed", map[string]any{"status": store.RunnerStatusConnected, "origin": store.RunnerOriginRegistered}),
		prototypeRunnerEvent(2, "runner_lab_2", "runner.heartbeat_missed", "runner heartbeat missed", map[string]any{"status": store.RunnerStatusSuspect, "missed_heartbeats": 2}),
		prototypeRunnerEvent(1, "runner_remote_7", "runner.registered", "remote runner registered", map[string]any{"status": store.RunnerStatusConnected, "origin": store.RunnerOriginRegistered}),
		prototypeRunnerEvent(2, "runner_remote_7", "runner.down", "runner down", map[string]any{"status": store.RunnerStatusDown, "reason": "session_done"}),
		prototypeRunnerEvent(3, "runner_local_1", "runner.reconnected", "runner heartbeat recovered", map[string]any{"status": store.RunnerStatusConnected}),
	}
	return prototypeEventViews(events)
}

func prototypeRunnerEvent(sequence int64, runnerID, eventType, summary string, data map[string]any) event.Event {
	if data == nil {
		data = map[string]any{}
	}
	data["runner_id"] = runnerID
	return event.Event{
		SchemaVersion: event.SchemaVersion,
		ID:            fmt.Sprintf("evt_%s_%02d", runnerID, sequence),
		Sequence:      sequence,
		Timestamp:     "2026-06-04T14:00:00Z",
		Type:          eventType,
		Actor:         event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"},
		Summary:       summary,
		Data:          data,
	}
}

func prototypeDiffPatch() string {
	return `diff --git a/internal/manager/web/templates/prototype.html b/internal/manager/web/templates/prototype.html
new file mode 100644
index 0000000..7b7d8a1
--- /dev/null
+++ b/internal/manager/web/templates/prototype.html
@@ -0,0 +1,28 @@
+<section class="prototype-stage-map">
+  <h2>Stage/event map</h2>
+  <p>Events stay grouped by stage while preserving the raw taxonomy labels.</p>
+</section>
+
+<section class="diff-safety">
+  <h2>diff.patch</h2>
+  <pre>{{ escaped diff text only }}</pre>
+</section>
diff --git a/internal/manager/web/prototype.go b/internal/manager/web/prototype.go
new file mode 100644
index 0000000..ac91a44
--- /dev/null
+++ b/internal/manager/web/prototype.go
@@ -0,0 +1,18 @@
+package web
+
+// Throwaway seed data for /prototype; never wired to the store or runner.
+func NewPrototypeData() PrototypeData {
+  return PrototypeData{Title: "Parley UI prototype"}
+}
+
+// Deliberately escaped by html/template in the viewer:
+const rawHTMLExample = "<section class=\"malicious\"><script>alert('x')</script></section>"
`
}
