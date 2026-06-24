package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

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
	if len(sink.seen) != 2 {
		t.Fatalf("sink saw %d notifications, want 2", len(sink.seen))
	}
	if items[0].RunID != wr.Run.ID || items[0].ProjectID != wr.Project.ID {
		t.Fatalf("notification anchors = %+v", items[0])
	}
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
	if len(sink.seen) != 1 {
		t.Fatalf("sink saw %d notifications, want 1", len(sink.seen))
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
	rep, err := engine.runWorkflowStage(ctx, wr, runtime, reviewStage, report.Report{}, report.Report{}, workerSnapshot{}, nil)
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
	if _, err := engine.runWorkflowStage(ctx, wr, runtime, review, report.Report{}, report.Report{}, workerSnapshot{}, nil); err != nil {
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
}

func TestMemoryUpdateStageAppliesCuratedCandidatesWithoutDispatchingWorkflowAgent(t *testing.T) {
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
	rep, err := engine.runWorkflowStage(ctx, wr, runtime, memoryStage, sourceReport, report.Report{}, workerSnapshot{}, nil)
	if err != nil {
		t.Fatalf("run memory update stage: %v", err)
	}
	if rep.Status != report.StatusCompleted {
		t.Fatalf("memory update status = %s", rep.Status)
	}
	if len(runner.disps) != 0 {
		t.Fatalf("memory update dispatched arbitrary workflow agent %d time(s)", len(runner.disps))
	}
	if rep.Payload["applied_count"] != 1 || rep.Payload["rejected_count"] != 1 || rep.Payload["writes_private_sqlite_only"] != true {
		t.Fatalf("memory update payload = %#v", rep.Payload)
	}
	entries, err := st.ListProjectMemoryEntries(ctx, wr.Run.ProjectID)
	if err != nil {
		t.Fatalf("list project memory: %v", err)
	}
	if len(entries) != 1 || entries[0].Title != "Validation needs git" || entries[0].CuratorStageID != memoryStage.Stage.ID {
		t.Fatalf("project memory entries = %#v", entries)
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
	rep, err := engine.runWorkflowStage(ctx, wr, runtime, reviewStage, report.Report{}, report.Report{}, workerSnapshot{}, nil)
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
	rep, err := engine.runWorkflowStage(ctx, wr, runtime, reviewStage, report.Report{}, report.Report{}, workerSnapshot{}, nil)
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
	rep := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        report.StatusCompleted,
		Summary:       "captured dispatch",
		Payload:       map[string]any{},
		Errors:        []string{},
	}
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
	return rep, nil
}

func (r *capturingRunner) CancelAttempt(context.Context, string, string, string) error { return nil }

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
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        report.StatusCompleted,
		Summary:       summary,
		Payload:       map[string]any{},
		Errors:        []string{},
	}
}
