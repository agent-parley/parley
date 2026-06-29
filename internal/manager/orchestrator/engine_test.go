package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestStartProjectRunWithWorkflowExecutesCustomizedSnapshotWithoutTemplateMutation(t *testing.T) {
	ctx := context.Background()
	runner := &capturingRunner{}
	env := newTestEnv(t, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}})
	custom := runLocalWorkflowTemplate()

	runID, err := env.engine.StartProjectRunWithWorkflow(ctx, store.DefaultProjectID, contract.TaskInput{Idea: "ship custom workflow", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: workflow.BalancedPRDeliveryID}, custom)
	if err != nil {
		t.Fatalf("StartProjectRunWithWorkflow() error = %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCompleted)

	snapshot, err := env.store.LatestWorkflowTemplateSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("latest workflow snapshot: %v", err)
	}
	if snapshot.Predefined || !snapshot.Editable {
		t.Fatalf("snapshot flags predefined=%v editable=%v, want run-local editable", snapshot.Predefined, snapshot.Editable)
	}
	if len(snapshot.Stages) != 3 {
		t.Fatalf("snapshot stages = %d, want 3: %+v", len(snapshot.Stages), snapshot.Stages)
	}
	if snapshot.Stages[1].ID != "implementation" || snapshot.Stages[1].Instructions != "Use focused edits." {
		t.Fatalf("implementation snapshot stage = %+v", snapshot.Stages[1])
	}
	if len(runner.disps) != 1 {
		t.Fatalf("dispatch count = %d, want implementation only", len(runner.disps))
	}
	if got := runner.disps[0].Input["workflow_stage_instructions"]; got != "Use focused edits." {
		t.Fatalf("dispatch instructions = %#v", got)
	}
	events, err := env.store.ListEvents(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for _, ev := range events {
		if strings.Contains(ev.Type, "workflow_adjustment") {
			t.Fatalf("unexpected workflow adjustment event: %s", ev.Type)
		}
	}
	source, err := env.store.GetWorkflowTemplate(ctx, workflow.BalancedPRDeliveryID)
	if err != nil {
		t.Fatalf("get source template: %v", err)
	}
	if !source.Predefined || source.Editable {
		t.Fatalf("source template mutated flags predefined=%v editable=%v", source.Predefined, source.Editable)
	}
	if len(source.Stages) == len(snapshot.Stages) {
		t.Fatalf("source template appears replaced by run-local snapshot: %+v", source.Stages)
	}
}

func TestStartProjectRunInputKeepsPlainTemplateSnapshotUnchanged(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, &capturingRunner{}, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100}})

	runID, err := env.engine.StartProjectRunInput(ctx, store.DefaultProjectID, contract.TaskInput{Idea: "plain workflow", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: workflow.QuickFixDeliveryID})
	if err != nil {
		t.Fatalf("StartProjectRunInput() error = %v", err)
	}
	run, err := env.store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != store.RunStatusPending {
		t.Fatalf("plain run status = %s, want pending", run.Status)
	}
	snapshot, err := env.store.LatestWorkflowTemplateSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("latest workflow snapshot: %v", err)
	}
	source, err := env.store.GetWorkflowTemplate(ctx, workflow.QuickFixDeliveryID)
	if err != nil {
		t.Fatalf("get source template: %v", err)
	}
	if snapshot.ID != source.ID || snapshot.Name != source.Name || snapshot.Predefined != source.Predefined || snapshot.Editable != source.Editable || len(snapshot.Stages) != len(source.Stages) {
		t.Fatalf("plain snapshot diverged from source: snapshot=%+v source=%+v", snapshot, source)
	}
	for i := range source.Stages {
		if snapshot.Stages[i].ID != source.Stages[i].ID || snapshot.Stages[i].Type != source.Stages[i].Type {
			t.Fatalf("plain snapshot stage %d = %+v, want %+v", i, snapshot.Stages[i], source.Stages[i])
		}
	}
}

func runLocalWorkflowTemplate() workflow.Template {
	template := workflow.Template{
		SchemaVersion: workflow.SchemaVersion,
		ID:            workflow.BalancedPRDeliveryID,
		Name:          "Run-local Balanced",
		Description:   "Customized for one run.",
		Predefined:    false,
		Editable:      true,
		Stages: []workflow.StageTemplate{
			{ID: "idea_refinement", Type: workflow.StageTypeIdeaRefinement, Label: "Idea refinement", Actor: workflow.ActorHarness},
			{ID: "implementation", Type: workflow.StageTypeImplementation, Label: "Implementation", Actor: workflow.ActorAgent, ProfileID: agentregistry.ProfilePiHeadlessWorker, Instructions: "Use focused edits."},
			{ID: "stop_report", Type: workflow.StageTypeStopReport, Label: "Stop/report", Actor: workflow.ActorHarness},
		},
		Settings: map[string]any{"pr_behavior": "none"},
	}
	template.Edges = workflow.DeriveTemplateEdges(template)
	return workflow.NormalizeTemplate(template)
}

func TestCompletionEventType(t *testing.T) {
	cases := []struct {
		name   string
		actor  string
		status string
		want   string
	}{
		{"agent completed", report.ActorKindAgent, report.StatusCompleted, "adapter.completed"},
		{"agent failed", report.ActorKindAgent, report.StatusFailed, "adapter.failed"},
		{"agent invalid", report.ActorKindAgent, report.StatusInvalid, "adapter.failed"},
		{"harness completed", report.ActorKindHarness, report.StatusCompleted, "harness.completed"},
		{"harness failed", report.ActorKindHarness, report.StatusFailed, "harness.failed"},
		{"harness invalid", report.ActorKindHarness, report.StatusInvalid, "harness.failed"},
		{"human completed", report.ActorKindHuman, report.StatusCompleted, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := report.Report{Actor: report.Actor{Kind: tc.actor, ID: "x"}, Status: tc.status}
			if got := completionEventType(rep); got != tc.want {
				t.Fatalf("completionEventType(%s,%s) = %q, want %q", tc.actor, tc.status, got, tc.want)
			}
		})
	}
}

func TestNotificationDispatcherPersistsSubscribedEventsAndContinuesOnSinkError(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRun(ctx, "ship notification center")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	sink := &capturingNotificationSink{err: errors.New("sse down")}
	engine := newRecordingEngine(t, st, nil, EngineOptions{NotificationSinks: []NotificationSink{sink}})

	if _, err := engine.emit(ctx, reviewAwaitingEvent(wr)); err != nil {
		t.Fatalf("emit awaiting human: %v", err)
	}
	if _, err := engine.emit(ctx, runEvent(wr, "run.completed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "done", map[string]any{"terminal_status": store.RunStatusCompleted})); err != nil {
		t.Fatalf("emit completed: %v", err)
	}

	items, err := st.ListNotifications(ctx, 10)
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("notifications = %+v, want 2", items)
	}
	sink.waitFor(t, 2)
	if items[0].RunID != wr.Run.ID || items[0].ProjectID != wr.Project.ID {
		t.Fatalf("notification anchors = %+v", items[0])
	}
}

func TestNotificationDeliveryIsAsyncAndDoesNotBlockEmit(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRun(ctx, "external endpoint hangs")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	sink := newBlockingNotificationSink()
	engine := newRecordingEngine(t, st, nil, EngineOptions{NotificationSinks: []NotificationSink{sink}})

	done := make(chan error, 1)
	go func() {
		_, err := engine.emit(ctx, reviewAwaitingEvent(wr))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("emit awaiting human: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("emit blocked behind notification delivery")
	}
	items, err := st.ListNotifications(ctx, 10)
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("notifications = %+v, want persisted notification before async delivery finishes", items)
	}
	select {
	case <-sink.started:
	case <-time.After(testWaitTimeout):
		t.Fatal("blocking sink was not invoked")
	}
	close(sink.release)
}

func TestNotificationDispatcherRespectsTogglesAndExcludesNonSubscribedEvents(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRun(ctx, "avoid notification spam")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	sink := &capturingNotificationSink{}
	engine := newRecordingEngine(t, st, nil, EngineOptions{NotificationSinks: []NotificationSink{sink}})

	if _, err := engine.emit(ctx, nonReviewAwaitingEvent(wr)); err != nil {
		t.Fatalf("emit non-review awaiting human: %v", err)
	}
	if _, err := st.UpdateProjectNotificationPreferences(ctx, wr.Project.ID, store.ProjectNotificationPreferences{OnlyWhenNeeded: false, WhenFinished: true}); err != nil {
		t.Fatalf("disable needed notifications: %v", err)
	}
	if _, err := engine.emit(ctx, reviewAwaitingEvent(wr)); err != nil {
		t.Fatalf("emit awaiting human: %v", err)
	}
	if _, err := engine.emit(ctx, stageEvent(wr, wr.ImplementationStage, "stage.completed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "stage done", nil)); err != nil {
		t.Fatalf("emit stage completed: %v", err)
	}
	if _, err := engine.emit(ctx, runEvent(wr, "run.cancelled", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "cancelled", map[string]any{"terminal_status": store.RunStatusCancelled})); err != nil {
		t.Fatalf("emit cancelled: %v", err)
	}
	if _, err := engine.emit(ctx, runEvent(wr, "run.abandoned", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "needs input", map[string]any{"terminal_status": store.RunStatusNeedsInput})); err != nil {
		t.Fatalf("emit needs input: %v", err)
	}
	if _, err := engine.emit(ctx, runEvent(wr, "run.failed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "invalid", map[string]any{"terminal_status": store.RunStatusInvalid})); err != nil {
		t.Fatalf("emit invalid: %v", err)
	}

	items, err := st.ListNotifications(ctx, 10)
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("notifications = %+v, want only invalid run.failed", items)
	}
	if items[0].Class != store.NotificationClassFinished || !strings.Contains(items[0].Title, "Run invalid:") {
		t.Fatalf("notification = %+v, want finished invalid", items[0])
	}
	sink.waitFor(t, 1)
}

type blockingNotificationSink struct {
	started chan struct{}
	release chan struct{}
}

func newBlockingNotificationSink() *blockingNotificationSink {
	return &blockingNotificationSink{started: make(chan struct{}), release: make(chan struct{})}
}

func (s *blockingNotificationSink) Notify(ctx context.Context, _ store.Notification) error {
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func reviewAwaitingEvent(wr store.WorkflowRun) event.Event {
	return event.Event{SchemaVersion: event.SchemaVersion, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, Type: "stage.awaiting_human", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "awaiting verdict", Data: map[string]any{"stage_type": contract.StageTypeReview}}
}

func nonReviewAwaitingEvent(wr store.WorkflowRun) event.Event {
	return event.Event{SchemaVersion: event.SchemaVersion, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, Type: "stage.awaiting_human", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "non-review stage awaiting human input", Data: map[string]any{"stage_type": contract.StageTypeMemoryUpdate}}
}

func TestDispatchStagePersistsStageBriefAndPassesItToRunner(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRun(ctx, "build a bounded brief")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	runner := &capturingRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{})
	rep, err := engine.dispatchStage(ctx, wr, wr.ImplementationStage, "capture", contract.StageTypeImplementation, implementationInput(wr, report.Report{}))
	if err != nil {
		t.Fatalf("dispatchStage() error = %v", err)
	}
	if rep.Status != report.StatusCompleted {
		t.Fatalf("status = %s", rep.Status)
	}
	briefID, _ := runner.disp.Input["stage_brief_artifact_id"].(string)
	if briefID == "" {
		t.Fatalf("dispatch input missing stage brief artifact id: %+v", runner.disp.Input)
	}
	briefText, _ := runner.disp.Input["stage_brief_markdown"].(string)
	if !strings.Contains(briefText, "# Stage brief") || !strings.Contains(briefText, "## Source: workflow_snapshot") {
		t.Fatalf("dispatch input missing source-labeled Stage brief:\n%s", briefText)
	}
	if _, ok := runner.disp.Input["stage_brief"]; ok {
		t.Fatalf("dispatch input embeds stage_brief struct: %+v", runner.disp.Input)
	}
	if _, ok := runner.disp.Input["curated_context"]; ok {
		t.Fatalf("dispatch input embeds duplicate curated_context: %+v", runner.disp.Input)
	}
	briefContentKeys := 0
	for key, value := range runner.disp.Input {
		text, ok := value.(string)
		if !ok || !strings.Contains(text, "# Stage brief") {
			continue
		}
		briefContentKeys++
		if key != "stage_brief_markdown" {
			t.Fatalf("dispatch input carries Stage brief content in %q", key)
		}
	}
	if briefContentKeys != 1 {
		t.Fatalf("dispatch input carries Stage brief content %d times, want once: %+v", briefContentKeys, runner.disp.Input)
	}
	stages, err := st.ListStages(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list stages: %v", err)
	}
	for _, stage := range stages {
		if stage.ID == wr.ImplementationStage.ID {
			if stage.StageBriefArtifactID != briefID {
				t.Fatalf("stage brief ref = %s, want %s", stage.StageBriefArtifactID, briefID)
			}
			return
		}
	}
	t.Fatal("implementation stage not found")
}

func TestDispatchStageBriefIncludesCuratedProjectMemory(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "include curated memory", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	sourceReport := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       wr.IdeaIntakeStage.ID,
		StageType:     wr.IdeaIntakeStage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: "noop"},
		Status:        report.StatusCompleted,
		Summary:       "captured a reusable memory entry",
		Payload:       map[string]any{},
		Errors:        []string{},
	}
	sourceArtifact, err := st.SaveReportArtifact(ctx, sourceReport)
	if err != nil {
		t.Fatalf("save source report: %v", err)
	}
	if _, err := st.ApplyProjectMemoryUpdate(ctx, store.ProjectMemoryUpdate{ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, CuratorStageID: wr.MemoryUpdateStage.ID, Entries: []store.ProjectMemoryInput{
		{Kind: store.ProjectMemoryKindGotcha, Title: "Validation image needs git", Body: "validation_image: git\nValidation image used git before checking worktree snapshots.", SourceStageID: wr.IdeaIntakeStage.ID, SourceArtifactID: sourceArtifact.ID, SourceSummary: "idea intake report"},
	}}); err != nil {
		t.Fatalf("apply memory update: %v", err)
	}

	runner := &capturingRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{})
	if _, err := engine.dispatchStage(ctx, wr, wr.ImplementationStage, "capture", contract.StageTypeImplementation, implementationInput(wr, report.Report{})); err != nil {
		t.Fatalf("dispatchStage() error = %v", err)
	}
	briefText, _ := runner.disp.Input["stage_brief_markdown"].(string)
	for _, want := range []string{"## Source: project_memory", "Validation image needs git", "Source artifact: `" + sourceArtifact.ID + "`", "Curated project memory is precedence rank 7"} {
		if !strings.Contains(briefText, want) {
			t.Fatalf("stage brief missing %q:\n%s", want, briefText)
		}
	}
}

func TestDispatchStageRepairsMalformedReportBeforeCompletion(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRun(ctx, "repair a malformed implementation report")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	runner := &malformedThenRepairedRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{})
	rep, err := engine.dispatchStage(ctx, wr, wr.ImplementationStage, "repairable", contract.StageTypeImplementation, implementationInput(wr, report.Report{}))
	if err != nil {
		t.Fatalf("dispatchStage() error = %v", err)
	}
	if rep.Status != report.StatusCompleted {
		t.Fatalf("status = %s, want completed", rep.Status)
	}
	if len(runner.disps) != 2 {
		t.Fatalf("dispatch count = %d, want initial + repair", len(runner.disps))
	}
	if runner.disps[0].AttemptID != runner.disps[1].AttemptID {
		t.Fatalf("repair attempt id = %s, want shared attempt %s", runner.disps[1].AttemptID, runner.disps[0].AttemptID)
	}
	repairInput := runner.disps[1].Input
	if repairInput["report_repair"] != true || repairInput["report_repair_attempt"] != 1 {
		t.Fatalf("repair input missing bounded repair markers: %+v", repairInput)
	}
	if _, ok := repairInput["invalid_report"].(map[string]any); !ok {
		t.Fatalf("repair input missing invalid report: %+v", repairInput)
	}
	if _, ok := repairInput["expected_report_schema"].(map[string]any); !ok {
		t.Fatalf("repair input missing expected schema: %+v", repairInput)
	}
	contractText, _ := repairInput["contract_markdown"].(string)
	for _, want := range []string{"Repair the malformed stage report", "Invalid candidate report", "Expected report envelope", "Do not modify `/project/repo`"} {
		if !strings.Contains(contractText, want) {
			t.Fatalf("repair contract missing %q:\n%s", want, contractText)
		}
	}
	events, err := st.ListEvents(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if !hasEventType(events, "report.invalid") {
		t.Fatalf("missing report.invalid event: %#v", eventTypes(events))
	}
}

func TestValidationReportRepairExhaustionUsesHarnessActor(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRun(ctx, "malformed validation report")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	runner := &alwaysMalformedRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{})
	rep, err := engine.dispatchStage(ctx, wr, wr.ValidationStage, "validation", contract.StageTypeValidation, map[string]any{"idea": wr.Run.Idea})
	if err != nil {
		t.Fatalf("dispatchStage() error = %v", err)
	}
	if rep.Status != report.StatusInvalid || rep.Actor.Kind != report.ActorKindHarness {
		t.Fatalf("validation invalid report status/actor = %s/%s", rep.Status, rep.Actor.Kind)
	}
	if _, err := report.ValidationOutputFromPayload(rep.Payload); err != nil {
		t.Fatalf("invalid validation report missing typed payload: %v", err)
	}
	events, err := st.ListEvents(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if !hasEventType(events, "harness.failed") {
		t.Fatalf("missing harness.failed event: %#v", eventTypes(events))
	}
	if hasEventType(events, "adapter.failed") {
		t.Fatalf("validation report repair emitted adapter.failed: %#v", eventTypes(events))
	}
}

func TestReviewArbiterMissingVerdictIsRepaired(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "review malformed arbiter output", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	runner := &arbiterRepairRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	runtime, err := engine.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		t.Fatalf("load runtime workflow: %v", err)
	}
	reviewStage := runtime.ByID["change_review_agent"]
	rep, err := engine.runWorkflowStage(ctx, wr, runtime, reviewStage, report.Report{}, report.Report{}, report.Report{}, workerSnapshot{}, nil)
	if err != nil {
		t.Fatalf("run review stage: %v", err)
	}
	if rep.Status != report.StatusCompleted || verdictString(rep.Verdict) != string(report.ReviewVerdictPass) {
		t.Fatalf("review status/verdict = %s/%s, want completed/pass", rep.Status, verdictString(rep.Verdict))
	}
	if len(runner.disps) != 3 {
		t.Fatalf("dispatch count = %d, want critic + arbiter + arbiter repair", len(runner.disps))
	}
	repair := runner.disps[2]
	if repair.Input["review_role"] != contract.ReviewRoleArbiter || repair.Input["report_repair"] != true {
		t.Fatalf("arbiter repair input = %+v", repair.Input)
	}
	if repair.AttemptID != wr.Attempt.ID {
		t.Fatalf("repair attempt id = %s, want %s", repair.AttemptID, wr.Attempt.ID)
	}
	events, err := st.ListEvents(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if !hasEventType(events, "report.invalid") {
		t.Fatalf("missing report.invalid event: %#v", eventTypes(events))
	}
}

func TestMalformedReportRepairExhaustionRoutesInvalidWithoutCrash(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	runner := &alwaysMalformedRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "malformed adapter report should not pass or crash", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRun() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusInvalid)
	events, err := st.ListEvents(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for _, want := range []string{"report.invalid", "adapter.failed", "stage.invalid", "run.failed"} {
		if !hasEventType(events, want) {
			t.Fatalf("missing %s event: %#v", want, eventTypes(events))
		}
	}
	if len(runner.disps) != 2 {
		t.Fatalf("dispatch count = %d, want initial + bounded repair", len(runner.disps))
	}
}

func TestStageBriefRepoEvidenceUsesConfiguredGitExecutable(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoPath := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoPath, "README.md"), []byte("# fake repo\n"), 0o600); err != nil {
		t.Fatalf("write README: %v", err)
	}
	spec := store.DefaultProjectSpec(dataDir)
	spec.RepositoryPath = repoPath
	if _, err := st.EnsureProject(ctx, spec); err != nil {
		t.Fatalf("ensure project repo: %v", err)
	}
	wr, err := st.CreateWorkflowRun(ctx, "use configured git for repo evidence")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	fakeGit := filepath.Join(t.TempDir(), "fake-git")
	gitLog := filepath.Join(t.TempDir(), "git.log")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> '" + gitLog + "'\n" +
		"case \"$1\" in\n" +
		"  status) echo '## fake-main' ;;\n" +
		"  diff) echo 'fake diff' ;;\n" +
		"  log) echo 'fake log' ;;\n" +
		"  *) echo 'fake git' ;;\n" +
		"esac\n"
	if err := os.WriteFile(fakeGit, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake git: %v", err)
	}
	runner := &capturingRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{GitExecutable: fakeGit})
	if _, err := engine.dispatchStage(ctx, wr, wr.ImplementationStage, "capture", contract.StageTypeImplementation, implementationInput(wr, report.Report{})); err != nil {
		t.Fatalf("dispatchStage() error = %v", err)
	}
	logContent, err := os.ReadFile(gitLog)
	if err != nil {
		t.Fatalf("read fake git log: %v", err)
	}
	for _, want := range []string{"status --short --branch", "diff --stat", "diff --no-ext-diff", "log --oneline -n 5"} {
		if !strings.Contains(string(logContent), want) {
			t.Fatalf("fake git log missing %q:\n%s", want, logContent)
		}
	}
}

func TestRuntimeGraphUsesFrozenTemplateEdges(t *testing.T) {
	template := workflow.DefaultTemplate()
	for _, candidate := range workflow.PredefinedTemplates() {
		if candidate.ID == workflow.AutonomousPRDeliveryID {
			template = candidate
			break
		}
	}
	graph, err := newRuntimeGraph(template)
	if err != nil {
		t.Fatalf("newRuntimeGraph() error = %v", err)
	}
	if next, ok := graph.Next("change_review_agent", report.StatusChangesRequested); !ok || next != "implementation" {
		t.Fatalf("changes_requested next = %q ok=%v, want implementation", next, ok)
	}
	if next, ok := graph.Next("validation", report.StatusFailed); !ok || next != "implementation" {
		t.Fatalf("validation failed next = %q ok=%v, want implementation fix loop", next, ok)
	}
	if next, ok := graph.Next("change_review_agent", report.StatusApproved); !ok || next != "commit_feature_branch" {
		t.Fatalf("approved next = %q ok=%v, want commit_feature_branch", next, ok)
	}
}

func TestSelectedTemplateCreatesRuntimeStages(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "ship direct", WorkflowTemplateID: workflow.DirectCommitID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	stages, err := st.ListStages(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list stages: %v", err)
	}
	if len(stages) != len(workflow.DefaultTemplate().Stages)-1 {
		t.Fatalf("direct stage count = %d, want no PR creation stage", len(stages))
	}
	for _, stage := range stages {
		if stage.StageType == workflow.StageTypePRCreation {
			t.Fatalf("direct template persisted PR creation stage: %+v", stage)
		}
		if stage.StageType == workflow.StageTypeCommit && stage.WorkflowStageID != "commit_target_branch" {
			t.Fatalf("direct commit workflow stage id = %s, want commit_target_branch", stage.WorkflowStageID)
		}
	}
}

func TestAgentStageDispatchReceivesTemplateActorTargetSettings(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	runner := &capturingRunner{}
	policy := QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}
	engine := newRecordingEngine(t, st, runner, EngineOptions{QueuePolicy: &policy})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "review my changes", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	wr, err := st.GetWorkflowRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	runtime, err := engine.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		t.Fatalf("load runtime workflow: %v", err)
	}
	review := runtime.ByID["change_review_agent"]
	if _, err := engine.runWorkflowStage(ctx, wr, runtime, review, report.Report{}, report.Report{}, report.Report{}, workerSnapshot{}, nil); err != nil {
		t.Fatalf("run review stage: %v", err)
	}
	if len(runner.disps) != 2 {
		t.Fatalf("review dispatch count = %d, want critic + arbiter", len(runner.disps))
	}
	if runner.disps[0].Input["review_role"] != contract.ReviewRoleCritic || runner.disps[1].Input["review_role"] != contract.ReviewRoleArbiter {
		t.Fatalf("review roles = %v, %v", runner.disps[0].Input["review_role"], runner.disps[1].Input["review_role"])
	}
	if runner.disp.StageType != workflow.StageTypeReview {
		t.Fatalf("dispatch stage type = %s, want review", runner.disp.StageType)
	}
	if runner.disp.Input["workflow_stage_actor"] != workflow.ActorAgent || runner.disp.Input["workflow_stage_target"] != workflow.TargetCodeChanges {
		t.Fatalf("dispatch input missing actor/target: %+v", runner.disp.Input)
	}
	settings, ok := runner.disp.Input["workflow_stage_settings"].(map[string]any)
	if !ok || settings["profile"] != "generalist" || settings["intensity"] != "normal" {
		t.Fatalf("dispatch input settings = %#v", runner.disp.Input["workflow_stage_settings"])
	}
	briefText, _ := runner.disp.Input["stage_brief_markdown"].(string)
	for _, want := range []string{"workflow_stage_settings", "profile: generalist", "intensity: normal"} {
		if !strings.Contains(briefText, want) {
			t.Fatalf("stage brief missing %q:\n%s", want, briefText)
		}
	}
}

func TestMemoryUpdateStageDispatchesAgentCuratorAndAppliesGuardrailedCandidates(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "remember safe lessons", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	runner := &capturingRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	runtime, err := engine.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		t.Fatalf("load runtime workflow: %v", err)
	}
	implementation := runtime.ByID["implementation"]
	sourceReport := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       implementation.Stage.ID,
		StageType:     implementation.Stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: "impl"},
		Status:        report.StatusCompleted,
		Summary:       "implementation emitted memory candidates",
		Payload: map[string]any{"memory_candidates": []any{
			map[string]any{"kind": "gotcha", "title": "Validation needs git", "body": "Validation containers need git installed before worktree inspection succeeds.", "source_summary": "implementation report"},
			map[string]any{"kind": "lesson", "title": "Bad secret", "body": "access token ghp_example should not be stored"},
		}},
		Errors: []string{},
	}
	if err := engine.completeStage(ctx, wr, implementation.Stage, sourceReport); err != nil {
		t.Fatalf("complete source stage: %v", err)
	}
	memoryStage := runtime.ByID["memory_update"]
	rep, err := engine.runWorkflowStage(ctx, wr, runtime, memoryStage, sourceReport, report.Report{}, report.Report{}, workerSnapshot{}, nil)
	if err != nil {
		t.Fatalf("run memory update stage: %v", err)
	}
	if rep.Status != report.StatusCompleted {
		t.Fatalf("memory update status = %s", rep.Status)
	}
	if len(runner.disps) != 1 || runner.disps[0].StageType != contract.StageTypeMemoryUpdate {
		t.Fatalf("memory update dispatches = %#v, want one memory_update curator dispatch", runner.disps)
	}
	if rep.Payload["applied_count"] != 1 || rep.Payload["rejected_count"] != 1 || rep.Payload["writes_private_sqlite_only"] != true {
		t.Fatalf("memory update payload = %#v", rep.Payload)
	}
	memoryOutput, err := report.MemoryUpdateOutputFromPayload(rep.Payload)
	if err != nil {
		t.Fatalf("memory_update_output invalid: %v", err)
	}
	if len(memoryOutput.Applied) != 1 || len(memoryOutput.Rejected) != 1 || len(memoryOutput.MemoryChanges) != 1 {
		t.Fatalf("memory_update_output = %#v", memoryOutput)
	}
	if memoryOutput.Applied[0].EntryID == "" || len(memoryOutput.Applied[0].SourceArtifactRefs) == 0 || memoryOutput.Applied[0].Freshness.UpdatedAt == "" {
		t.Fatalf("applied decision missing entry/source/freshness metadata: %#v", memoryOutput.Applied[0])
	}
	entries, err := st.ListProjectMemoryEntries(ctx, wr.Run.ProjectID)
	if err != nil {
		t.Fatalf("list project memory: %v", err)
	}
	if len(entries) != 1 || entries[0].Title != "Validation needs git" || entries[0].CuratorStageID != memoryStage.Stage.ID {
		t.Fatalf("project memory entries = %#v", entries)
	}
}

func TestMemoryUpdateAgentCuratorAuditsOmittedCandidatesAsDeferred(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "audit omitted memory candidates", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	runner := &omittingMemoryCuratorRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	runtime, err := engine.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		t.Fatalf("load runtime workflow: %v", err)
	}
	implementation := runtime.ByID["implementation"]
	sourceReport := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       implementation.Stage.ID,
		StageType:     implementation.Stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: "impl"},
		Status:        report.StatusCompleted,
		Summary:       "implementation emitted memory candidates",
		Payload: map[string]any{"memory_candidates": []any{
			map[string]any{"kind": "lesson", "title": "Useful lesson", "body": "Apply this durable lesson in future runs.", "source_summary": "implementation report"},
			map[string]any{"kind": "gotcha", "title": "Needs audit", "body": "This candidate is omitted by the curator and must remain visible.", "source_summary": "implementation report"},
		}},
		Errors: []string{},
	}
	if err := engine.completeStage(ctx, wr, implementation.Stage, sourceReport); err != nil {
		t.Fatalf("complete source stage: %v", err)
	}
	memoryStage := runtime.ByID["memory_update"]
	rep, err := engine.runWorkflowStage(ctx, wr, runtime, memoryStage, sourceReport, report.Report{}, report.Report{}, workerSnapshot{}, nil)
	if err != nil {
		t.Fatalf("run memory update stage: %v", err)
	}
	output, err := report.MemoryUpdateOutputFromPayload(rep.Payload)
	if err != nil {
		t.Fatalf("memory_update_output invalid: %v", err)
	}
	if len(output.Applied) != 1 || len(output.Deferred) != 1 {
		t.Fatalf("memory_update_output states = applied:%d deferred:%d output=%#v", len(output.Applied), len(output.Deferred), output)
	}
	if output.InboxSummary.CandidatesGenerated != 2 || output.InboxSummary.CandidatesCurated != 2 {
		t.Fatalf("memory curation counts = generated:%d curated:%d output=%#v", output.InboxSummary.CandidatesGenerated, output.InboxSummary.CandidatesCurated, output)
	}
	deferred := output.Deferred[0]
	if deferred.CandidateID != "candidate-002" || !strings.Contains(deferred.Rationale, "omitted") || len(deferred.SourceArtifactRefs) == 0 {
		t.Fatalf("omitted candidate was not audited as source-linked deferred decision: %#v", deferred)
	}
	if rep.Payload["deferred_count"] != 1 {
		t.Fatalf("memory update payload = %#v", rep.Payload)
	}
	entries, err := st.ListProjectMemoryEntries(ctx, wr.Run.ProjectID)
	if err != nil {
		t.Fatalf("list project memory: %v", err)
	}
	if len(entries) != 1 || entries[0].Title != "Useful lesson" {
		t.Fatalf("project memory entries = %#v", entries)
	}
}

func TestMemoryUpdateAgentCuratorSupportsEditedMergedDeferredOutput(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "curate memory states", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	runner := &scriptedMemoryCuratorRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	runtime, err := engine.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		t.Fatalf("load runtime workflow: %v", err)
	}
	implementation := runtime.ByID["implementation"]
	sourceReport := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       implementation.Stage.ID,
		StageType:     implementation.Stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: "impl"},
		Status:        report.StatusCompleted,
		Summary:       "implementation emitted mergeable memory candidates",
		Payload: map[string]any{"memory_candidates": []any{
			map[string]any{"kind": "lesson", "title": "Noisy title", "body": "Candidate needs a concise edited form for future runs.", "source_summary": "edit source"},
			map[string]any{"kind": "gotcha", "title": "Duplicate A", "body": "Validation containers need git before inspecting worktrees.", "source_summary": "merge source A"},
			map[string]any{"kind": "gotcha", "title": "Duplicate B", "body": "Install git in validation images before worktree diff inspection.", "source_summary": "merge source B"},
			map[string]any{"kind": "lesson", "title": "Needs human", "body": "This may be useful but needs more evidence.", "source_summary": "defer source"},
		}},
		Errors: []string{},
	}
	if err := engine.completeStage(ctx, wr, implementation.Stage, sourceReport); err != nil {
		t.Fatalf("complete source stage: %v", err)
	}
	memoryStage := runtime.ByID["memory_update"]
	rep, err := engine.runWorkflowStage(ctx, wr, runtime, memoryStage, sourceReport, report.Report{}, report.Report{}, workerSnapshot{}, nil)
	if err != nil {
		t.Fatalf("run memory update stage: %v", err)
	}
	if rep.Status != report.StatusCompleted {
		t.Fatalf("memory update status = %s payload=%#v", rep.Status, rep.Payload)
	}
	output, err := report.MemoryUpdateOutputFromPayload(rep.Payload)
	if err != nil {
		t.Fatalf("memory_update_output invalid: %v", err)
	}
	if len(output.Edited) != 1 || len(output.Merged) != 1 || len(output.Deferred) != 1 || len(output.MemoryChanges) != 2 {
		t.Fatalf("memory_update_output states = edited:%d merged:%d deferred:%d changes:%d output=%#v", len(output.Edited), len(output.Merged), len(output.Deferred), len(output.MemoryChanges), output)
	}
	if output.Edited[0].EntryID == "" || output.Merged[0].EntryID == "" || len(output.Merged[0].CandidateIDs) != 2 {
		t.Fatalf("edited/merged decisions missing entry IDs or candidate IDs: %#v %#v", output.Edited[0], output.Merged[0])
	}
	if output.InboxSummary.CandidatesGenerated != 4 || output.InboxSummary.CandidatesCurated != 4 {
		t.Fatalf("memory curation counts = generated:%d curated:%d output=%#v", output.InboxSummary.CandidatesGenerated, output.InboxSummary.CandidatesCurated, output)
	}
	entries, err := st.ListProjectMemoryEntries(ctx, wr.Run.ProjectID)
	if err != nil {
		t.Fatalf("list project memory: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("project memory entries = %d, want 2: %#v", len(entries), entries)
	}
}

func TestMemoryProducerLoopPersistsAgentLearningOpportunities(t *testing.T) {
	ctx := context.Background()
	runner := &memoryLearningRunner{}
	env := newTestEnv(t, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	templateID := "memory_producer_with_update"
	createMemoryProducerTemplate(t, env.store, templateID, true)

	runID, err := env.engine.StartRunInput(ctx, contract.TaskInput{Idea: "learn safe implementation and review lessons", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: templateID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	if err := env.engine.StartQueuedRun(ctx, runID); err != nil {
		t.Fatalf("StartQueuedRun() error = %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCompleted)

	var sawImplementationCapture, sawReviewCapture bool
	for _, disp := range runner.disps {
		if disp.StageType == contract.StageTypeImplementation && memoryCaptureInputEnabled(disp.Input) {
			sawImplementationCapture = true
		}
		if disp.StageType == contract.StageTypeReview && disp.Input["review_role"] == contract.ReviewRoleArbiter && memoryCaptureInputEnabled(disp.Input) {
			sawReviewCapture = true
		}
	}
	if !sawImplementationCapture || !sawReviewCapture {
		t.Fatalf("memory capture dispatch flags implementation=%v review=%v dispatches=%+v", sawImplementationCapture, sawReviewCapture, runner.disps)
	}

	memoryReport := reportForWorkflowStage(t, env.store, runID, "memory_update")
	if got := reportPayloadInt(memoryReport.Payload, "candidate_count"); got != 2 {
		t.Fatalf("memory candidate_count = %d, want 2; payload=%#v", got, memoryReport.Payload)
	}
	if got := reportPayloadInt(memoryReport.Payload, "applied_count"); got != 2 {
		t.Fatalf("memory applied_count = %d, want 2; payload=%#v", got, memoryReport.Payload)
	}
	entries, err := env.store.ListProjectMemoryEntries(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("list project memory entries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("project memory entry count = %d, want 2: %#v", len(entries), entries)
	}
	titles := map[string]bool{}
	for _, entry := range entries {
		titles[entry.Title] = true
		if entry.SourceArtifactID == "" || entry.SourceStageID == "" || entry.CuratorStageID == "" {
			t.Fatalf("memory entry missing source/curator links: %+v", entry)
		}
	}
	for _, want := range []string{"Implementation fixture layout", "Review source-linking lesson"} {
		if !titles[want] {
			t.Fatalf("project memory titles = %#v, missing %q", titles, want)
		}
	}
}

func TestHumanMemoryUpdateSuspendsApprovesEditsRejectsDefersAndResumesOnce(t *testing.T) {
	ctx := context.Background()
	runner := &humanMemoryApprovalRunner{}
	env := newTestEnv(t, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	templateID := "human_memory_approval"
	createMemoryProducerTemplateWithActor(t, env.store, templateID, true, workflow.ActorHuman)

	runID, err := env.engine.StartRunInput(ctx, contract.TaskInput{Idea: "approve memory candidates", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: templateID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	if err := env.engine.StartQueuedRun(ctx, runID); err != nil {
		t.Fatalf("StartQueuedRun() error = %v", err)
	}
	waitForWorkflowStageAwaiting(t, env.store, runID, "memory_update")
	queue, err := env.engine.QueueState(ctx)
	if err != nil {
		t.Fatalf("queue state: %v", err)
	}
	if queue.Running != 0 {
		t.Fatalf("running slots = %d, want 0 after memory approval suspend", queue.Running)
	}

	awaiting := eventByWorkflowStage(t, env.store, runID, "stage.awaiting_human", "memory_update")
	packetID := payloadString(awaiting.Data, "memory_approval_packet_id")
	if packetID == "" {
		t.Fatalf("awaiting event missing memory_approval_packet_id: %#v", awaiting.Data)
	}
	_, packetContent, err := env.store.GetArtifact(ctx, packetID)
	if err != nil {
		t.Fatalf("get memory approval packet: %v", err)
	}
	var packet memoryApprovalPacket
	if err := json.Unmarshal(packetContent, &packet); err != nil {
		t.Fatalf("decode memory approval packet: %v", err)
	}
	if len(packet.Candidates) != 4 || packet.InboxSummary["candidate_count"] == nil {
		t.Fatalf("memory approval packet = %#v", packet)
	}

	memoryStage := stageByWorkflowID(t, env.store, runID, "memory_update")
	rep, err := env.engine.SubmitHumanReview(ctx, runID, memoryStage.ID, HumanReviewSubmission{
		ActorID: "alice",
		Summary: "memory decisions approved",
		MemoryDecisions: []HumanMemoryDecision{
			{CandidateID: "candidate-1", Action: store.ProjectMemoryDecisionApprove},
			{CandidateID: "candidate-2", Action: store.ProjectMemoryDecisionEdit, Kind: store.ProjectMemoryKindLesson, Title: "Edited memory lesson", Body: "Edited memory body stays source-linked and useful.", SourceSummary: "edited by human"},
			{CandidateID: "candidate-3", Action: store.ProjectMemoryDecisionReject, Reason: "not durable"},
			{CandidateID: "candidate-4", Action: store.ProjectMemoryDecisionDefer, Reason: "needs more evidence"},
		},
	})
	if err != nil {
		t.Fatalf("SubmitHumanReview(memory) error = %v", err)
	}
	if rep.Status != report.StatusCompleted || rep.Actor.Kind != report.ActorKindHuman || rep.Actor.ID != "alice" {
		t.Fatalf("human memory report = %#v", rep)
	}
	if rep.Summary != "memory decisions approved" {
		t.Fatalf("human memory report summary = %q, want submitted summary", rep.Summary)
	}
	if got := reportPayloadInt(rep.Payload, "applied_count"); got != 2 {
		t.Fatalf("applied_count = %d; payload=%#v", got, rep.Payload)
	}
	if got := reportPayloadInt(rep.Payload, "human_rejected_count"); got != 1 {
		t.Fatalf("human_rejected_count = %d; payload=%#v", got, rep.Payload)
	}
	if got := reportPayloadInt(rep.Payload, "deferred_count"); got != 1 {
		t.Fatalf("deferred_count = %d; payload=%#v", got, rep.Payload)
	}
	if _, err := env.engine.SubmitHumanReview(ctx, runID, memoryStage.ID, HumanReviewSubmission{Summary: "late", MemoryDecisions: []HumanMemoryDecision{{CandidateID: "candidate-1", Action: store.ProjectMemoryDecisionApprove}}}); err == nil {
		t.Fatal("double memory approval submit succeeded")
	}

	entries, err := env.store.ListProjectMemoryEntries(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("list project memory: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("project memory entries = %#v, want 2", entries)
	}
	titles := map[string]bool{}
	for _, entry := range entries {
		titles[entry.Title] = true
		if entry.CuratorStageID != memoryStage.ID {
			t.Fatalf("entry curator stage = %s, want %s", entry.CuratorStageID, memoryStage.ID)
		}
	}
	for _, want := range []string{"Approve this memory", "Edited memory lesson"} {
		if !titles[want] {
			t.Fatalf("project memory titles = %#v, missing %q", titles, want)
		}
	}
	for _, unwanted := range []string{"Reject this memory", "Defer this memory"} {
		if titles[unwanted] {
			t.Fatalf("project memory titles include rejected/deferred %q: %#v", unwanted, titles)
		}
	}
	decisions, err := env.store.ListProjectMemoryDecisions(ctx, runID)
	if err != nil {
		t.Fatalf("list memory decisions: %v", err)
	}
	if len(decisions) != 4 {
		t.Fatalf("memory decisions = %#v, want 4", decisions)
	}
	outcomes := map[string]string{}
	actions := map[string]string{}
	for _, decision := range decisions {
		outcomes[decision.CandidateID] = decision.Outcome
		actions[decision.CandidateID] = decision.Action
		if (decision.CandidateID == "candidate-1" || decision.CandidateID == "candidate-2") && decision.EntryID == "" {
			t.Fatalf("applied decision missing entry_id: %+v", decision)
		}
	}
	if outcomes["candidate-1"] != store.ProjectMemoryDecisionOutcomeApplied || outcomes["candidate-2"] != store.ProjectMemoryDecisionOutcomeApplied || outcomes["candidate-3"] != store.ProjectMemoryDecisionOutcomeRejected || outcomes["candidate-4"] != store.ProjectMemoryDecisionOutcomeDeferred {
		t.Fatalf("memory decision outcomes = %#v actions=%#v", outcomes, actions)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCompleted)
}

func TestMemoryCaptureDisabledWithoutMemoryUpdateStage(t *testing.T) {
	ctx := context.Background()
	runner := &memoryLearningRunner{emitEvenWhenDisabled: true}
	env := newTestEnv(t, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	templateID := "memory_producer_without_update"
	createMemoryProducerTemplate(t, env.store, templateID, false)

	runID, err := env.engine.StartRunInput(ctx, contract.TaskInput{Idea: "do not collect memory", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: templateID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	if err := env.engine.StartQueuedRun(ctx, runID); err != nil {
		t.Fatalf("StartQueuedRun() error = %v", err)
	}
	waitForRunStatus(t, env.store, runID, store.RunStatusCompleted)

	for _, disp := range runner.disps {
		if memoryCaptureInputEnabled(disp.Input) {
			t.Fatalf("dispatch unexpectedly enabled memory capture without memory_update stage: %+v", disp)
		}
	}
	for _, workflowStageID := range []string{"implementation", "change_review_agent"} {
		rep := reportForWorkflowStage(t, env.store, runID, workflowStageID)
		if _, ok := firstPayloadValue(rep.Payload, "memory_candidates", "project_memory_candidates", memoryCapturePayloadKey); ok {
			t.Fatalf("%s report retained memory candidates without memory_update stage: %#v", workflowStageID, rep.Payload)
		}
	}
	entries, err := env.store.ListProjectMemoryEntries(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("list project memory entries: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("project memory entries = %#v, want none", entries)
	}
}

func TestReviewChangesRequestedCreatesFixLoopAttempt(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "review loop", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	runner := &reviewVerdictRunner{verdict: report.ReviewVerdictChangesRequested}
	engine := newRecordingEngine(t, st, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	runtime, err := engine.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		t.Fatalf("load runtime workflow: %v", err)
	}
	reviewStage := runtime.ByID["change_review_agent"]
	rep, err := engine.runWorkflowStage(ctx, wr, runtime, reviewStage, report.Report{}, report.Report{}, report.Report{}, workerSnapshot{}, nil)
	if err != nil {
		t.Fatalf("run review stage: %v", err)
	}
	if rep.Status != report.StatusChangesRequested || verdictString(rep.Verdict) != string(report.ReviewVerdictChangesRequested) {
		t.Fatalf("review report status/verdict = %s/%s", rep.Status, verdictString(rep.Verdict))
	}
	if got := len(acceptedFindings(rep.Payload)); got != 1 {
		t.Fatalf("accepted findings = %d, want 1; payload=%#v", got, rep.Payload)
	}
	nextID, ok := runtime.Graph.Next(reviewStage.Template.ID, rep.Status)
	if !ok || nextID != "implementation" {
		t.Fatalf("next = %s ok=%v, want implementation", nextID, ok)
	}
	newWR, newRuntime, err := engine.startFixLoopAttempt(ctx, wr, runtime, reviewStage, rep, nextID)
	if err != nil {
		t.Fatalf("start fix loop attempt: %v", err)
	}
	if newWR.Attempt.ID == wr.Attempt.ID {
		t.Fatalf("attempt did not advance: %s", newWR.Attempt.ID)
	}
	if got := newRuntime.ByID["implementation"].Stage.AttemptID; got != newWR.Attempt.ID {
		t.Fatalf("implementation attempt id = %s, want %s", got, newWR.Attempt.ID)
	}
}

func TestValidationReportWithMalformedOutputCompletesAsInvalid(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "validation schema", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	engine := newRecordingEngine(t, st, nil, EngineOptions{})
	runtime, err := engine.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		t.Fatalf("load runtime workflow: %v", err)
	}
	validationStage := runtime.ByID["validation"].Stage
	rep := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       validationStage.ID,
		StageType:     contract.StageTypeValidation,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "validation"},
		Status:        report.StatusFailed,
		Summary:       "validation failed",
		Payload: map[string]any{report.ValidationOutputPayloadKey: map[string]any{
			"result":                "failed",
			"checks_run":            []any{map[string]any{"name": "go test", "status": "failed"}},
			"confidence":            "high",
			"suggested_next_action": "fix tests",
		}},
		Errors: []string{"legacy error"},
	}
	if err := engine.completeStage(ctx, wr, validationStage, rep); err != nil {
		t.Fatalf("complete validation stage: %v", err)
	}
	updated := stageByWorkflowID(t, st, wr.Run.ID, "validation")
	if updated.Status != report.StatusInvalid {
		t.Fatalf("validation stage status = %s, want invalid", updated.Status)
	}
}

func TestValidationFailureFixLoopContextUsesTypedFailures(t *testing.T) {
	wr := store.WorkflowRun{Run: store.Run{Idea: "fix typed validation failure"}}
	previous := report.Report{
		StageID:   "stage_validation",
		StageType: contract.StageTypeValidation,
		Status:    report.StatusFailed,
		Summary:   "validation failed",
		Payload: map[string]any{report.ValidationOutputPayloadKey: report.ValidationOutput{
			Result: report.ValidationResultFailed,
			ChecksRun: []report.ValidationCheck{{
				Name:    "go test ./...",
				Status:  report.ValidationCheckFailed,
				Command: "go test ./...",
			}},
			Outputs: []report.ValidationOutputRef{{ID: "artifact_log", Name: "test.log", Kind: "log"}},
			Failures: []report.ValidationFailure{{
				Check:      "go test ./...",
				Message:    "TestWidget failed",
				Severity:   "error",
				OutputRefs: []string{"artifact_log"},
			}},
			Skipped:             []report.ValidationSkippedCheck{},
			EnvNotes:            []string{"network=none"},
			Confidence:          report.ValidationConfidenceMedium,
			SuggestedNextAction: "fix the failing test",
		}},
		Errors: []string{"legacy conventional error should not drive the fix loop"},
	}
	input := implementationInput(wr, previous)
	fixContext, ok := input["fix_loop_context"].(map[string]any)
	if !ok {
		t.Fatalf("missing fix loop context: %#v", input)
	}
	failures, ok := fixContext["failures"].([]report.ValidationFailure)
	if !ok || len(failures) != 1 || failures[0].Message != "TestWidget failed" {
		t.Fatalf("typed failures = %#v", fixContext["failures"])
	}
	if _, ok := fixContext["errors"]; ok {
		t.Fatalf("fix loop context used conventional errors: %#v", fixContext)
	}
	if fixContext["suggested_next_action"] != "fix the failing test" {
		t.Fatalf("suggested_next_action = %#v", fixContext["suggested_next_action"])
	}
}

func TestReviewFixLoopExhaustionBlocksThroughNeedsInput(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "review loop cap", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	runner := &reviewVerdictRunner{verdict: report.ReviewVerdictChangesRequested}
	engine := newRecordingEngine(t, st, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	runtime, err := engine.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		t.Fatalf("load runtime workflow: %v", err)
	}
	runtime.Template.Settings["max_fix_loops"] = 0
	reviewStage := runtime.ByID["change_review_agent"]
	rep, err := engine.runWorkflowStage(ctx, wr, runtime, reviewStage, report.Report{}, report.Report{}, report.Report{}, workerSnapshot{}, nil)
	if err != nil {
		t.Fatalf("run review stage: %v", err)
	}
	if rep.Status != report.StatusNeedsInput || verdictString(rep.Verdict) != string(report.ReviewVerdictBlocked) {
		t.Fatalf("review exhaustion status/verdict = %s/%s", rep.Status, verdictString(rep.Verdict))
	}
	if rep.Payload["fix_loop_exhausted"] != true {
		t.Fatalf("fix_loop_exhausted payload missing: %#v", rep.Payload)
	}
	if next, ok := runtime.Graph.Next(reviewStage.Template.ID, rep.Status); !ok || next != "stop_report" {
		t.Fatalf("exhausted next = %s ok=%v, want stop_report", next, ok)
	}
}

func TestStartRunFreezesSelectedWorkflowTemplateSnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	copied, err := st.CopyWorkflowTemplate(ctx, workflow.BalancedPRDeliveryID, "team_template", "Team Template")
	if err != nil {
		t.Fatalf("copy template: %v", err)
	}
	engine := newRecordingEngine(t, st, nil, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "use team template", WorkflowTemplateID: copied.ID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	var rawSnapshot string
	if err := st.DB().QueryRowContext(ctx, `SELECT snapshot_json FROM workflow_snapshots WHERE run_id = ? ORDER BY id ASC LIMIT 1`, runID).Scan(&rawSnapshot); err != nil {
		t.Fatalf("read start snapshot: %v", err)
	}
	var snapshot map[string]any
	if err := json.Unmarshal([]byte(rawSnapshot), &snapshot); err != nil {
		t.Fatalf("decode start snapshot: %v", err)
	}
	if snapshot["frozen"] != true {
		t.Fatalf("snapshot frozen = %v, want true", snapshot["frozen"])
	}
	if snapshot["workflow_template_id"] != copied.ID {
		t.Fatalf("snapshot template id = %v, want %s", snapshot["workflow_template_id"], copied.ID)
	}
	if snapshot["workflow_template_frozen"] != true {
		t.Fatalf("workflow_template_frozen = %v, want true", snapshot["workflow_template_frozen"])
	}
	templateSnapshot, ok := snapshot["workflow_template_snapshot"].(map[string]any)
	if !ok || templateSnapshot["id"] != copied.ID {
		t.Fatalf("workflow template snapshot = %+v", snapshot["workflow_template_snapshot"])
	}
}

func createMemoryProducerTemplate(t *testing.T, st *store.Store, templateID string, includeMemoryUpdate bool) {
	t.Helper()
	createMemoryProducerTemplateWithActor(t, st, templateID, includeMemoryUpdate, workflow.ActorAgent)
}

func createMemoryProducerTemplateWithActor(t *testing.T, st *store.Store, templateID string, includeMemoryUpdate bool, memoryActor string) {
	t.Helper()
	stages := []workflow.StageTemplate{
		{ID: "idea_refinement", Type: workflow.StageTypeIdeaRefinement, Label: "Idea refinement", Actor: workflow.ActorHarness},
		{ID: "implementation", Type: workflow.StageTypeImplementation, Label: "Implementation", Actor: workflow.ActorAgent},
		{ID: "change_review_agent", Type: workflow.StageTypeReview, Label: "Code review", Actor: workflow.ActorAgent, Target: workflow.TargetCodeChanges},
	}
	edges := []workflow.Edge{
		{From: "idea_refinement", To: "implementation", On: workflow.OnCompleted},
		{From: "implementation", To: "change_review_agent", On: workflow.OnCompleted},
	}
	reviewNext := "stop_report"
	if includeMemoryUpdate {
		stages = append(stages, workflow.StageTemplate{ID: "memory_update", Type: workflow.StageTypeMemoryUpdate, Label: "Memory update", Actor: memoryActor})
		reviewNext = "memory_update"
		edges = append(edges, workflow.Edge{From: "memory_update", To: "stop_report", On: workflow.OnCompleted})
	}
	stages = append(stages, workflow.StageTemplate{ID: "stop_report", Type: workflow.StageTypeStopReport, Label: "Stop/report", Actor: workflow.ActorHarness})
	edges = append(edges, workflow.Edge{From: "change_review_agent", To: reviewNext, On: workflow.OnCompleted})
	template := workflow.Template{
		SchemaVersion: workflow.SchemaVersion,
		ID:            templateID,
		Name:          templateID,
		Description:   "test memory producer workflow",
		Editable:      true,
		Stages:        stages,
		Edges:         edges,
	}
	if err := st.CreateWorkflowTemplate(context.Background(), template); err != nil {
		t.Fatalf("create workflow template: %v", err)
	}
}

func reportForWorkflowStage(t *testing.T, st *store.Store, runID, workflowStageID string) report.Report {
	t.Helper()
	events, err := st.ListEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for _, ev := range events {
		if ev.Type != "stage.completed" || payloadString(ev.Data, "workflow_stage_id") != workflowStageID {
			continue
		}
		artifactID := payloadString(ev.Data, "report_artifact_id")
		if artifactID == "" {
			t.Fatalf("stage.completed for %s missing report_artifact_id: %#v", workflowStageID, ev)
		}
		_, content, err := st.GetArtifact(context.Background(), artifactID)
		if err != nil {
			t.Fatalf("get report artifact %s: %v", artifactID, err)
		}
		var rep report.Report
		if err := json.Unmarshal(content, &rep); err != nil {
			t.Fatalf("decode report artifact %s: %v", artifactID, err)
		}
		return rep
	}
	t.Fatalf("stage.completed report for workflow stage %s not found", workflowStageID)
	return report.Report{}
}

func reportPayloadInt(payload map[string]any, key string) int {
	switch value := payload[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}

func memoryUpdateOutputEmpty(disp contract.Dispatch) report.MemoryUpdateOutput {
	candidates := memoryInboxCandidatesForTest(disp)
	return report.MemoryUpdateOutput{
		InboxSummary: report.MemoryInboxSummary{
			LearningOpportunities: len(candidates),
			CandidatesGenerated:   len(candidates),
			CandidatesCurated:     0,
			SourceArtifactRefs:    memoryInboxSourceRefsForTest(candidates),
		},
		Applied:       []report.MemoryCandidateDecision{},
		Rejected:      []report.MemoryCandidateDecision{},
		Edited:        []report.MemoryCandidateDecision{},
		Merged:        []report.MemoryCandidateDecision{},
		Deferred:      []report.MemoryCandidateDecision{},
		MemoryChanges: []report.MemoryChange{},
		ActorAuthority: report.MemoryActorAuthority{
			Kind:      report.ActorKindAgent,
			ID:        disp.Adapter,
			Authority: "test memory curator returned no approved writes",
		},
		SafetyNotes:       []string{},
		StopReportSummary: "project memory curation complete",
	}
}

func memoryUpdateOutputApplyAll(disp contract.Dispatch) report.MemoryUpdateOutput {
	out := memoryUpdateOutputEmpty(disp)
	candidates := memoryInboxCandidatesForTest(disp)
	out.Applied = make([]report.MemoryCandidateDecision, 0, len(candidates))
	for _, candidate := range candidates {
		id := candidate["candidate_id"]
		ref := candidate["source_artifact_id"]
		out.Applied = append(out.Applied, report.MemoryCandidateDecision{
			CandidateID:        id,
			CandidateIDs:       []string{id},
			State:              report.MemoryCandidateApplied,
			Kind:               candidate["kind"],
			Title:              candidate["title"],
			Body:               candidate["body"],
			Rationale:          "test curator approved candidate",
			SourceArtifactRefs: []string{ref},
			Freshness:          report.MemoryFreshness{SourceArtifactRefs: []string{ref}},
		})
	}
	out.InboxSummary.CandidatesCurated = len(out.Applied)
	return out
}

func memoryInboxCandidatesForTest(disp contract.Dispatch) []map[string]string {
	inbox, _ := disp.Input["project_memory_inbox"].(map[string]any)
	raw, _ := inbox["candidates"].([]map[string]any)
	out := make([]map[string]string, 0, len(raw))
	for _, item := range raw {
		candidate := map[string]string{}
		for _, key := range []string{"candidate_id", "kind", "title", "body", "source_stage_id", "source_artifact_id", "source_summary"} {
			candidate[key] = fmt.Sprint(item[key])
		}
		out = append(out, candidate)
	}
	return out
}

func memoryInboxSourceRefsForTest(candidates []map[string]string) []string {
	refs := make([]string, 0, len(candidates))
	seen := map[string]bool{}
	for _, candidate := range candidates {
		ref := strings.TrimSpace(candidate["source_artifact_id"])
		if ref == "" || seen[ref] {
			continue
		}
		seen[ref] = true
		refs = append(refs, ref)
	}
	return refs
}

type reviewVerdictRunner struct {
	verdict report.Verdict
	disps   []contract.Dispatch
}

func (r *reviewVerdictRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.disps = append(r.disps, disp)
	rep := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        report.StatusCompleted,
		Summary:       "review dispatch",
		Payload:       map[string]any{},
		Errors:        []string{},
	}
	if disp.Input["review_role"] == contract.ReviewRoleCritic {
		rep.Payload = map[string]any{"raw_findings": []any{map[string]any{"id": "finding-1", "title": "fix me"}}}
		return rep, nil
	}
	rep.Verdict = &r.verdict
	rep.Payload = map[string]any{
		"raw_findings": disp.Input["raw_findings"],
		"arbitration_decisions": []any{map[string]any{
			"finding_id":     "finding-1",
			"classification": report.ReviewFindingAccepted,
			"rationale":      "real issue",
			"severity":       "medium",
			"priority":       "p1",
		}},
		"residual_risk": "medium",
		"confidence":    "normal",
	}
	return rep, nil
}

func (r *reviewVerdictRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

type capturingRunner struct {
	disp  contract.Dispatch
	disps []contract.Dispatch
}

func (r *capturingRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.disp = disp
	r.disps = append(r.disps, disp)
	rep := validAdapterReport(disp, "captured dispatch")
	if disp.StageType == contract.StageTypeReview {
		switch disp.Input["review_role"] {
		case contract.ReviewRoleCritic:
			rep.Payload = map[string]any{"raw_findings": []any{map[string]any{"id": "finding-1", "title": "tighten test"}}}
		case contract.ReviewRoleArbiter:
			verdict := report.ReviewVerdictPass
			rep.Verdict = &verdict
			rep.Payload = map[string]any{
				"raw_findings":          disp.Input["raw_findings"],
				"arbitration_decisions": []any{map[string]any{"finding_id": "finding-1", "classification": report.ReviewFindingRejected, "rationale": "not required"}},
				"residual_risk":         "low",
				"confidence":            "high",
			}
		}
	}
	if disp.StageType == contract.StageTypeMemoryUpdate {
		rep.Payload = map[string]any{report.MemoryUpdateOutputPayloadKey: memoryUpdateOutputApplyAll(disp)}
	}
	return rep, nil
}

func (r *capturingRunner) CancelAttempt(context.Context, string, string, string) error { return nil }

type omittingMemoryCuratorRunner struct {
	disps []contract.Dispatch
}

func (r *omittingMemoryCuratorRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.disps = append(r.disps, disp)
	rep := validAdapterReport(disp, "omitting memory curator")
	if disp.StageType != contract.StageTypeMemoryUpdate {
		return rep, nil
	}
	candidates := memoryInboxCandidatesForTest(disp)
	output := memoryUpdateOutputEmpty(disp)
	if len(candidates) > 0 {
		candidate := candidates[0]
		output.Applied = []report.MemoryCandidateDecision{{
			CandidateID:        candidate["candidate_id"],
			CandidateIDs:       []string{candidate["candidate_id"]},
			State:              report.MemoryCandidateApplied,
			Kind:               candidate["kind"],
			Title:              candidate["title"],
			Body:               candidate["body"],
			Rationale:          "test curator approved only the first candidate",
			SourceArtifactRefs: []string{candidate["source_artifact_id"]},
			Freshness:          report.MemoryFreshness{SourceArtifactRefs: []string{candidate["source_artifact_id"]}},
		}}
		output.InboxSummary.CandidatesCurated = 1
	}
	rep.Payload = map[string]any{report.MemoryUpdateOutputPayloadKey: output}
	return rep, nil
}

func (r *omittingMemoryCuratorRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

type scriptedMemoryCuratorRunner struct {
	disps []contract.Dispatch
}

func (r *scriptedMemoryCuratorRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.disps = append(r.disps, disp)
	rep := validAdapterReport(disp, "scripted memory curator")
	if disp.StageType != contract.StageTypeMemoryUpdate {
		return rep, nil
	}
	candidates := memoryInboxCandidatesForTest(disp)
	if len(candidates) < 4 {
		rep.Status = report.StatusFailed
		rep.Summary = "missing memory candidates"
		rep.Errors = []string{"scripted runner expected four candidates"}
		return rep, nil
	}
	refs := memoryInboxSourceRefsForTest(candidates)
	output := memoryUpdateOutputEmpty(disp)
	output.InboxSummary.CandidatesCurated = 4
	output.Edited = []report.MemoryCandidateDecision{{
		CandidateID:        candidates[0]["candidate_id"],
		CandidateIDs:       []string{candidates[0]["candidate_id"]},
		State:              report.MemoryCandidateEdited,
		Kind:               store.ProjectMemoryKindLesson,
		Title:              "Edited concise lesson",
		Body:               "Memory candidates should be edited into concise reusable lessons before persistence.",
		Rationale:          "candidate was useful but noisy",
		SourceArtifactRefs: []string{candidates[0]["source_artifact_id"]},
		Freshness:          report.MemoryFreshness{SourceArtifactRefs: []string{candidates[0]["source_artifact_id"]}},
	}}
	output.Merged = []report.MemoryCandidateDecision{{
		CandidateIDs:       []string{candidates[1]["candidate_id"], candidates[2]["candidate_id"]},
		State:              report.MemoryCandidateMerged,
		Kind:               store.ProjectMemoryKindGotcha,
		Title:              "Validation image needs git",
		Body:               "Validation containers need git installed before worktree diff inspection succeeds.",
		Rationale:          "two candidates described the same validation-image gotcha",
		SourceArtifactRefs: refs,
		Freshness:          report.MemoryFreshness{SourceArtifactRefs: refs},
	}}
	output.Deferred = []report.MemoryCandidateDecision{{
		CandidateID:        candidates[3]["candidate_id"],
		CandidateIDs:       []string{candidates[3]["candidate_id"]},
		State:              report.MemoryCandidateDeferred,
		Title:              candidates[3]["title"],
		Rationale:          "candidate needs more evidence before becoming durable memory",
		SourceArtifactRefs: []string{candidates[3]["source_artifact_id"]},
		Freshness:          report.MemoryFreshness{SourceArtifactRefs: []string{candidates[3]["source_artifact_id"]}},
	}}
	rep.Payload = map[string]any{report.MemoryUpdateOutputPayloadKey: output}
	return rep, nil
}

func (r *scriptedMemoryCuratorRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

type memoryLearningRunner struct {
	emitEvenWhenDisabled bool
	disps                []contract.Dispatch
}

func (r *memoryLearningRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.disps = append(r.disps, disp)
	rep := validAdapterReport(disp, "memory learning dispatch")
	switch disp.StageType {
	case contract.StageTypeImplementation:
		r.addLearning(&rep, disp, store.ProjectMemoryKindGotcha, "Implementation fixture layout", "Implementation discovered that generated fixtures should be kept under internal testdata for this repo.", "implementation report")
	case contract.StageTypeMemoryUpdate:
		rep.Payload = map[string]any{report.MemoryUpdateOutputPayloadKey: memoryUpdateOutputApplyAll(disp)}
	case contract.StageTypeReview:
		switch disp.Input["review_role"] {
		case contract.ReviewRoleCritic:
			rep.Summary = "critic reviewed memory capture"
			rep.Payload = map[string]any{"raw_findings": []any{}}
			r.addLearning(&rep, disp, store.ProjectMemoryKindLesson, "Critic transient lesson", "Critic observed a transient learning that the final arbiter may consider.", "critic report")
		case contract.ReviewRoleArbiter:
			verdict := report.ReviewVerdictPass
			rep.Verdict = &verdict
			rep.Summary = "arbiter approved memory capture"
			rep.Payload = map[string]any{
				"raw_findings":          disp.Input["raw_findings"],
				"arbitration_decisions": []any{},
				"residual_risk":         "low",
				"confidence":            "high",
			}
			r.addLearning(&rep, disp, store.ProjectMemoryKindLesson, "Review source-linking lesson", "Review confirmed that memory candidates can stay source-linked through report artifacts until curation.", "review arbiter report")
		}
	}
	return rep, nil
}

func (r *memoryLearningRunner) addLearning(rep *report.Report, disp contract.Dispatch, kind, title, body, sourceSummary string) {
	if !memoryCaptureInputEnabled(disp.Input) && !r.emitEvenWhenDisabled {
		return
	}
	if rep.Payload == nil {
		rep.Payload = map[string]any{}
	}
	rep.Payload[memoryCapturePayloadKey] = []any{map[string]any{
		"kind":           kind,
		"title":          title,
		"body":           body,
		"source_summary": sourceSummary,
	}}
}

func (r *memoryLearningRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

type humanMemoryApprovalRunner struct {
	disps []contract.Dispatch
}

func (r *humanMemoryApprovalRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.disps = append(r.disps, disp)
	rep := validAdapterReport(disp, "human memory approval dispatch")
	if !memoryCaptureInputEnabled(disp.Input) {
		return rep, nil
	}
	switch disp.StageType {
	case contract.StageTypeImplementation:
		rep.Payload[memoryCapturePayloadKey] = []any{
			map[string]any{"kind": store.ProjectMemoryKindGotcha, "title": "Approve this memory", "body": "Approved memory should be written after human approval.", "source_summary": "implementation approval candidate"},
			map[string]any{"kind": store.ProjectMemoryKindLesson, "title": "Edit this memory", "body": "Original body that the human should edit before writing.", "source_summary": "implementation edit candidate"},
		}
	case contract.StageTypeReview:
		switch disp.Input["review_role"] {
		case contract.ReviewRoleCritic:
			rep.Payload = map[string]any{"raw_findings": []any{}}
		case contract.ReviewRoleArbiter:
			verdict := report.ReviewVerdictPass
			rep.Verdict = &verdict
			rep.Payload = map[string]any{
				"raw_findings":          disp.Input["raw_findings"],
				"arbitration_decisions": []any{},
				"residual_risk":         "low",
				"confidence":            "high",
				memoryCapturePayloadKey: []any{
					map[string]any{"kind": store.ProjectMemoryKindRepoFact, "title": "Reject this memory", "body": "Rejected memory should be recorded but not written.", "source_summary": "review rejection candidate"},
					map[string]any{"kind": store.ProjectMemoryKindPriorResult, "title": "Defer this memory", "body": "Deferred memory should be recorded without writing.", "source_summary": "review deferred candidate"},
				},
			}
		}
	}
	return rep, nil
}

func (r *humanMemoryApprovalRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

type malformedThenRepairedRunner struct {
	disps []contract.Dispatch
}

func (r *malformedThenRepairedRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.disps = append(r.disps, disp)
	if disp.Input["report_repair"] == true {
		return validAdapterReport(disp, "repaired report"), nil
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        "surprised",
		Summary:       "not a valid status",
		Payload:       map[string]any{},
		Errors:        []string{},
	}, nil
}

func (r *malformedThenRepairedRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

type arbiterRepairRunner struct {
	disps       []contract.Dispatch
	arbiterSeen int
}

func (r *arbiterRepairRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.disps = append(r.disps, disp)
	rep := validAdapterReport(disp, "review dispatch")
	switch disp.Input["review_role"] {
	case contract.ReviewRoleCritic:
		rep.Payload = map[string]any{"raw_findings": []any{map[string]any{"id": "finding-1", "title": "tighten report repair test"}}}
	case contract.ReviewRoleArbiter:
		r.arbiterSeen++
		rep.Payload = map[string]any{
			"raw_findings": disp.Input["raw_findings"],
			"arbitration_decisions": []any{map[string]any{
				"finding_id":     "finding-1",
				"classification": report.ReviewFindingRejected,
				"rationale":      "not required",
				"severity":       "low",
				"priority":       "p3",
			}},
			"residual_risk": "low",
			"confidence":    "high",
		}
		if disp.Input["report_repair"] == true {
			verdict := report.ReviewVerdictPass
			rep.Verdict = &verdict
		}
	}
	return rep, nil
}

func (r *arbiterRepairRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

type alwaysMalformedRunner struct {
	disps []contract.Dispatch
}

func (r *alwaysMalformedRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.disps = append(r.disps, disp)
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        "surprised",
		Summary:       "still malformed",
		Payload:       map[string]any{},
		Errors:        []string{},
	}, nil
}

func (r *alwaysMalformedRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func validAdapterReport(disp contract.Dispatch, summary string) report.Report {
	payload := map[string]any{}
	actor := report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter}
	if disp.StageType == contract.StageTypeValidation {
		actor = report.Actor{Kind: report.ActorKindHarness, ID: disp.Adapter}
		payload[report.ValidationOutputPayloadKey] = report.ValidationOutput{
			Result: report.ValidationResultPassed,
			ChecksRun: []report.ValidationCheck{{
				Name:    "test validation",
				Status:  report.ValidationCheckPassed,
				Summary: "validation passed",
			}},
			Outputs:             []report.ValidationOutputRef{},
			Failures:            []report.ValidationFailure{},
			Skipped:             []report.ValidationSkippedCheck{},
			EnvNotes:            []string{},
			Confidence:          report.ValidationConfidenceHigh,
			SuggestedNextAction: "continue",
		}
	}
	if disp.StageType == contract.StageTypeMemoryUpdate {
		payload[report.MemoryUpdateOutputPayloadKey] = memoryUpdateOutputEmpty(disp)
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         actor,
		Status:        report.StatusCompleted,
		Summary:       summary,
		Payload:       payload,
		Errors:        []string{},
	}
}
