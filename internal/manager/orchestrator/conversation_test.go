package orchestrator

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestSubmitConversationMessageDispatchesFreshAgentTurnAndPersistsReply(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoPath := t.TempDir()
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: repoPath})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	conversation, err := st.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	if _, err := st.AddMessage(ctx, conversation.ID, store.MessageRoleUser, "What does this project do?"); err != nil {
		t.Fatalf("seed user message: %v", err)
	}
	if _, err := st.AddMessage(ctx, conversation.ID, store.MessageRoleAssistant, "It orchestrates agent work."); err != nil {
		t.Fatalf("seed assistant message: %v", err)
	}

	runner := &conversationTestRunner{dispatches: make(chan contract.Dispatch, 1), reply: "Auth lives under `internal/auth`."}
	broadcast := &capturingConversationBroadcaster{events: make(chan event.Event, 4)}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, broadcast, EngineOptions{ConversationAdapter: "chat-agent"})
	registerEngineTeardown(t, engine, st)

	if _, err := engine.SubmitConversationMessage(ctx, project.ID, "Where is auth handled?"); err != nil {
		t.Fatalf("SubmitConversationMessage() error = %v", err)
	}
	disp := receiveDispatch(t, runner.dispatches)
	if disp.StageType != contract.StageTypeConversation {
		t.Fatalf("stage_type = %q, want conversation", disp.StageType)
	}
	if disp.Adapter != "chat-agent" {
		t.Fatalf("adapter = %q, want chat-agent", disp.Adapter)
	}
	if got := disp.Input["input_mode"]; got != contract.AdapterInputModeConversation {
		t.Fatalf("input_mode = %v, want conversation", got)
	}
	if actions, ok := disp.Input["allowed_actions"].([]string); !ok || len(actions) != 1 || actions[0] != conversationActionCreateTask {
		t.Fatalf("allowed_actions = %#v, want create-Task allow-list", disp.Input["allowed_actions"])
	}
	repository, ok := disp.Input["repository"].(map[string]any)
	if !ok || repository["mode"] != "read_only" || repository["mount_path"] != "/project/repo" {
		t.Fatalf("repository input = %#v, want read-only /project/repo", disp.Input["repository"])
	}
	workspace, ok := disp.Input["workspace"].(map[string]any)
	if !ok || workspace["mode"] != "read_write" || workspace["mount_path"] != "/project/workspace" {
		t.Fatalf("workspace input = %#v, want read-write /project/workspace", disp.Input["workspace"])
	}
	history, ok := disp.Input["messages"].([]map[string]any)
	if !ok {
		t.Fatalf("messages = %#v, want message history", disp.Input["messages"])
	}
	if len(history) != 3 || history[0]["body"] != "What does this project do?" || history[1]["role"] != store.MessageRoleAssistant || history[2]["body"] != "Where is auth handled?" {
		t.Fatalf("history = %#v, want persisted history through trigger message", history)
	}

	messages := waitForConversationMessages(t, st, conversation.ID, 4)
	last := messages[len(messages)-1]
	if last.Role != store.MessageRoleAssistant || last.Body != "Auth lives under `internal/auth`." {
		t.Fatalf("last message = %#v, want assistant reply", last)
	}
	runs, err := st.ListRunsForProject(ctx, project.ID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %#v, want no Task/Run from conversation tracer", runs)
	}
	if !sawProjectConversationBroadcast(t, broadcast.events, project.ID) {
		t.Fatalf("did not observe project-scoped conversation broadcast")
	}
}

func TestConversationTurnsForOneConversationAreSerializedFIFO(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: t.TempDir()})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	runner := &managedConversationRunner{dispatches: make(chan contract.Dispatch, 4), releases: make(chan struct{}, 4), replies: []string{"reply one", "reply two"}}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, newEventRecorder(), EngineOptions{ConversationAdapter: "chat-agent"})
	registerEngineTeardown(t, engine, st)

	if _, err := engine.SubmitConversationMessage(ctx, project.ID, "first"); err != nil {
		t.Fatalf("submit first: %v", err)
	}
	if _, err := engine.SubmitConversationMessage(ctx, project.ID, "second"); err != nil {
		t.Fatalf("submit second: %v", err)
	}
	first := receiveDispatch(t, runner.dispatches)
	if first.WarmSessionKey == "" {
		t.Fatalf("first warm session key empty")
	}
	assertNoConversationDispatch(t, runner.dispatches, 75*time.Millisecond)
	runner.releases <- struct{}{}
	second := receiveDispatch(t, runner.dispatches)
	if second.WarmSessionKey != first.WarmSessionKey {
		t.Fatalf("warm session key = %q, want %q", second.WarmSessionKey, first.WarmSessionKey)
	}
	runner.releases <- struct{}{}

	conversation, err := st.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	messages := waitForConversationMessages(t, st, conversation.ID, 4)
	if messages[2].Role != store.MessageRoleAssistant || messages[2].Body != "reply one" || messages[3].Role != store.MessageRoleAssistant || messages[3].Body != "reply two" {
		t.Fatalf("messages = %#v, want ordered assistant replies", messages)
	}
}

func TestConversationBudgetQueuesDifferentConversationUntilActiveTurnFinishes(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	projectA, err := st.EnsureProject(ctx, store.ProjectSpec{ID: "project-a", Name: "Project A", RepositoryPath: t.TempDir()})
	if err != nil {
		t.Fatalf("ensure project A: %v", err)
	}
	projectB, err := st.EnsureProject(ctx, store.ProjectSpec{ID: "project-b", Name: "Project B", RepositoryPath: t.TempDir()})
	if err != nil {
		t.Fatalf("ensure project B: %v", err)
	}
	runner := &managedConversationRunner{dispatches: make(chan contract.Dispatch, 4), releases: make(chan struct{}, 4), replies: []string{"reply A", "reply B"}, evictions: make(chan string, 2)}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, newEventRecorder(), EngineOptions{ConversationAdapter: "chat-agent", ConversationBudget: 1})
	registerEngineTeardown(t, engine, st)

	if _, err := engine.SubmitConversationMessage(ctx, projectA.ID, "message A"); err != nil {
		t.Fatalf("submit A: %v", err)
	}
	first := receiveDispatch(t, runner.dispatches)
	if _, err := engine.SubmitConversationMessage(ctx, projectB.ID, "message B"); err != nil {
		t.Fatalf("submit B: %v", err)
	}
	assertNoConversationDispatch(t, runner.dispatches, 75*time.Millisecond)
	runner.releases <- struct{}{}
	second := receiveDispatch(t, runner.dispatches)
	if second.WarmSessionKey == first.WarmSessionKey {
		t.Fatalf("second warm session key = %q, want different conversation", second.WarmSessionKey)
	}
	if evicted := receiveEviction(t, runner.evictions); evicted != first.WarmSessionKey {
		t.Fatalf("evicted warm session = %q, want %q", evicted, first.WarmSessionKey)
	}
	runner.releases <- struct{}{}
}

func TestConversationWarmSessionReusedAcrossTurnsUntilIdleTTLEvicts(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: t.TempDir()})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	runner := &managedConversationRunner{dispatches: make(chan contract.Dispatch, 4), replies: []string{"first reply", "second reply"}, evictions: make(chan string, 2)}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, newEventRecorder(), EngineOptions{ConversationAdapter: "chat-agent", ConversationBudget: 1, ConversationIdleTTL: 25 * time.Millisecond})
	registerEngineTeardown(t, engine, st)

	if _, err := engine.SubmitConversationMessage(ctx, project.ID, "first"); err != nil {
		t.Fatalf("submit first: %v", err)
	}
	first := receiveDispatch(t, runner.dispatches)
	if first.WarmSessionKey == "" {
		t.Fatalf("first warm session key empty")
	}
	conversation, err := st.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	waitForConversationMessages(t, st, conversation.ID, 2)
	if evicted := receiveEviction(t, runner.evictions); evicted != first.WarmSessionKey {
		t.Fatalf("evicted warm session = %q, want %q", evicted, first.WarmSessionKey)
	}

	if _, err := engine.SubmitConversationMessage(ctx, project.ID, "second"); err != nil {
		t.Fatalf("submit second: %v", err)
	}
	second := receiveDispatch(t, runner.dispatches)
	if second.WarmSessionKey != first.WarmSessionKey {
		t.Fatalf("warm session key = %q, want %q", second.WarmSessionKey, first.WarmSessionKey)
	}
	history, ok := second.Input["messages"].([]map[string]any)
	if !ok {
		t.Fatalf("messages = %#v, want history", second.Input["messages"])
	}
	if len(history) != 3 || history[0]["body"] != "first" || history[1]["body"] != "first reply" || history[2]["body"] != "second" {
		t.Fatalf("history = %#v, want cold resume from persisted transcript", history)
	}
}

func TestConversationEvictionRaceColdStartsWithoutLosingQueuedTurn(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: t.TempDir()})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	runner := &managedConversationRunner{dispatches: make(chan contract.Dispatch, 4), replies: []string{"first reply", "second reply"}, evictStarted: make(chan string, 1), evictRelease: make(chan struct{}), evictions: make(chan string, 2)}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, newEventRecorder(), EngineOptions{ConversationAdapter: "chat-agent", ConversationBudget: 1, ConversationIdleTTL: 20 * time.Millisecond})
	registerEngineTeardown(t, engine, st)

	if _, err := engine.SubmitConversationMessage(ctx, project.ID, "first"); err != nil {
		t.Fatalf("submit first: %v", err)
	}
	first := receiveDispatch(t, runner.dispatches)
	conversation, err := st.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	waitForConversationMessages(t, st, conversation.ID, 2)
	if started := receiveEviction(t, runner.evictStarted); started != first.WarmSessionKey {
		t.Fatalf("eviction started for %q, want %q", started, first.WarmSessionKey)
	}
	if _, err := engine.SubmitConversationMessage(ctx, project.ID, "second"); err != nil {
		t.Fatalf("submit second: %v", err)
	}
	assertNoConversationDispatch(t, runner.dispatches, 75*time.Millisecond)
	close(runner.evictRelease)
	second := receiveDispatch(t, runner.dispatches)
	if second.WarmSessionKey != first.WarmSessionKey {
		t.Fatalf("warm session key = %q, want %q", second.WarmSessionKey, first.WarmSessionKey)
	}
	messages := waitForConversationMessages(t, st, conversation.ID, 4)
	if messages[3].Body != "second reply" {
		t.Fatalf("messages = %#v, want queued second turn reply", messages)
	}
}

func TestConversationShutdownCancelsInFlightTurn(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: t.TempDir()})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	runner := &cancellableConversationRunner{dispatches: make(chan contract.Dispatch, 1), cancelled: make(chan struct{})}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, newEventRecorder(), EngineOptions{ConversationAdapter: "chat-agent"})
	defer st.Close()

	if _, err := engine.SubmitConversationMessage(ctx, project.ID, "please think"); err != nil {
		t.Fatalf("submit: %v", err)
	}
	_ = receiveDispatch(t, runner.dispatches)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := engine.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-runner.cancelled:
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for conversation dispatch cancellation")
	}
}

func TestConversationDispatchInputIncludesReadOnlyOrchestrationState(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoPath := t.TempDir()
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: repoPath})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	conversation, err := st.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "fix flaky review gate", RefinementLevel: contract.RefinementLevelStandard, ConversationID: conversation.ID})
	if err != nil {
		t.Fatalf("create linked workflow run: %v", err)
	}
	stages, err := st.ListStages(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list stages: %v", err)
	}
	reviewStage := firstStageByType(t, stages, contract.StageTypeReview)
	verdict := report.ReviewVerdictChangesRequested
	reviewReport := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       reviewStage.ID,
		StageType:     contract.StageTypeReview,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: "reviewer"},
		Status:        report.StatusChangesRequested,
		Verdict:       &verdict,
		Summary:       "review rejected stale cache handling",
		Payload: map[string]any{
			"arbitration_decisions": []any{map[string]any{"finding_id": "finding-1", "classification": report.ReviewFindingAccepted, "rationale": "cache invalidation was missing"}},
			"residual_risk":         "retry behavior still needs coverage",
		},
		Errors: []string{},
	}
	if _, err := st.SaveReportArtifact(ctx, reviewReport); err != nil {
		t.Fatalf("save review report: %v", err)
	}
	if err := st.UpdateStageStatus(ctx, reviewStage.ID, report.StatusChangesRequested); err != nil {
		t.Fatalf("update review stage status: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusFailed); err != nil {
		t.Fatalf("update run status: %v", err)
	}
	trigger, err := st.AddMessage(ctx, conversation.ID, store.MessageRoleUser, "Why did review reject the last run?")
	if err != nil {
		t.Fatalf("add trigger message: %v", err)
	}

	engine := NewEngineWithOptions(st, nil, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{ConversationAdapter: "chat-agent"})
	input, err := engine.conversationDispatchInput(ctx, project, conversation, trigger.ID)
	if err != nil {
		t.Fatalf("conversationDispatchInput() error = %v", err)
	}
	if actions, ok := input["allowed_actions"].([]string); !ok || len(actions) != 1 || actions[0] != conversationActionCreateTask {
		t.Fatalf("allowed_actions = %#v, want create-Task allow-list", input["allowed_actions"])
	}
	state, ok := input["orchestration_state"].(conversationOrchestrationState)
	if !ok {
		t.Fatalf("orchestration_state = %#v, want structured state", input["orchestration_state"])
	}
	if len(state.ConversationTasks) != 1 || state.ConversationTasks[0].ID != wr.Task.ID || state.ConversationTasks[0].ConversationID != conversation.ID {
		t.Fatalf("conversation tasks = %#v, want linked task %s", state.ConversationTasks, wr.Task.ID)
	}
	if len(state.Runs) != 1 || state.Runs[0].ID != wr.Run.ID || state.Runs[0].TaskConversationID != conversation.ID {
		t.Fatalf("runs = %#v, want linked run %s", state.Runs, wr.Run.ID)
	}
	var foundReview bool
	for _, stage := range state.Runs[0].Stages {
		if stage.ID != reviewStage.ID {
			continue
		}
		foundReview = true
		if stage.Status != report.StatusChangesRequested || stage.Verdict != string(report.ReviewVerdictChangesRequested) || len(stage.Reports) != 1 {
			t.Fatalf("review stage state = %#v, want rejected verdict and one report", stage)
		}
		if stage.Reports[0].Summary != "review rejected stale cache handling" || stage.Reports[0].Payload["residual_risk"] != "retry behavior still needs coverage" {
			t.Fatalf("review report state = %#v, want report summary/payload", stage.Reports[0])
		}
	}
	if !foundReview {
		t.Fatalf("review stage %s not found in state: %#v", reviewStage.ID, state.Runs[0].Stages)
	}
	summary, _ := input["orchestration_state_summary"].(string)
	if !strings.Contains(summary, wr.Run.ID) || !strings.Contains(summary, "changes_requested") || !strings.Contains(summary, "review rejected stale cache handling") {
		t.Fatalf("summary missing run verdict/report: %q", summary)
	}
	markdown, _ := input["orchestration_state_markdown"].(string)
	for _, want := range []string{wr.Run.ID, reviewStage.ID, "review rejected stale cache handling", "cache invalidation was missing"} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("orchestration markdown missing %q:\n%s", want, markdown)
		}
	}
}

func TestConversationReadOrchestrationNoActionReplyDoesNotCreateWork(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoPath := t.TempDir()
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: repoPath})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	conversation, err := st.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "finished work", ConversationID: conversation.ID})
	if err != nil {
		t.Fatalf("create linked workflow run: %v", err)
	}
	beforeRuns, err := st.ListRunsForProject(ctx, project.ID)
	if err != nil {
		t.Fatalf("list runs before: %v", err)
	}
	beforeTasks, err := st.ListTasksForConversation(ctx, conversation.ID)
	if err != nil {
		t.Fatalf("list tasks before: %v", err)
	}
	runner := &conversationTestRunner{dispatches: make(chan contract.Dispatch, 1), reply: "Run `" + wr.Run.ID + "` is visible from orchestration state."}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{ConversationAdapter: "chat-agent"})

	if _, err := engine.SubmitConversationMessage(ctx, project.ID, "How did the last run go?"); err != nil {
		t.Fatalf("SubmitConversationMessage() error = %v", err)
	}
	disp := receiveDispatch(t, runner.dispatches)
	if actions, ok := disp.Input["allowed_actions"].([]string); !ok || len(actions) != 1 || actions[0] != conversationActionCreateTask {
		t.Fatalf("allowed_actions = %#v, want create-Task allow-list", disp.Input["allowed_actions"])
	}
	if _, ok := disp.Input["orchestration_state"].(conversationOrchestrationState); !ok {
		t.Fatalf("orchestration_state = %#v, want structured state", disp.Input["orchestration_state"])
	}
	messages := waitForConversationMessages(t, st, conversation.ID, 2)
	last := messages[len(messages)-1]
	if last.Role != store.MessageRoleAssistant || !strings.Contains(last.Body, wr.Run.ID) {
		t.Fatalf("last message = %#v, want orchestration answer", last)
	}
	afterRuns, err := st.ListRunsForProject(ctx, project.ID)
	if err != nil {
		t.Fatalf("list runs after: %v", err)
	}
	if len(afterRuns) != len(beforeRuns) {
		t.Fatalf("runs after = %#v, want same count as before %#v", afterRuns, beforeRuns)
	}
	afterTasks, err := st.ListTasksForConversation(ctx, conversation.ID)
	if err != nil {
		t.Fatalf("list tasks after: %v", err)
	}
	if len(afterTasks) != len(beforeTasks) {
		t.Fatalf("conversation tasks after = %#v, want same count as before %#v", afterTasks, beforeTasks)
	}
}

func TestValidateConversationActionsAcceptsCreateTask(t *testing.T) {
	brief := sectionedConversationBrief()
	action, err := validateConversationActions(map[string]any{
		"reply_markdown": "Creating a task.",
		"actions":        []any{map[string]any{"type": conversationActionCreateTask, "idea": brief}},
	}, []string{conversationActionCreateTask})
	if err != nil {
		t.Fatalf("validateConversationActions() error = %v", err)
	}
	if action == nil || action.Type != conversationActionCreateTask || action.Idea != brief {
		t.Fatalf("action = %#v, want create-Task with verbatim brief", action)
	}

	singular, err := validateConversationActions(map[string]any{
		"action": map[string]any{"type": conversationActionCreateTask, "idea": brief},
	}, []string{conversationActionCreateTask})
	if err != nil {
		t.Fatalf("validateConversationActions() singular error = %v", err)
	}
	if singular == nil || singular.Idea != brief {
		t.Fatalf("singular action = %#v, want verbatim brief", singular)
	}
}

func TestValidateConversationBriefAllowsMarkdownInsideSections(t *testing.T) {
	cases := []struct {
		name  string
		brief string
	}{
		{
			name: "fenced code block with hash lines",
			brief: conversationBriefSections(
				[2]string{"Goal", "Ship the conversation-created task path.\n\n```bash\n# install deps\n#!/bin/bash\nmake test\n```"},
				[2]string{"In scope", "- Validate a create-Task action."},
				[2]string{"Out of scope", "- Direct commits from chat."},
				[2]string{"Key decisions", "- Use Direct refinement."},
				[2]string{"Open assumptions", "- The human will approve the plan before code runs."},
			),
		},
		{
			name: "sub-heading in section body",
			brief: conversationBriefSections(
				[2]string{"Goal", "Ship the conversation-created task path."},
				[2]string{"In scope", "### Validation cases\n- Accept ordinary Markdown inside sections."},
				[2]string{"Out of scope", "- Direct commits from chat."},
				[2]string{"Key decisions", "- Use Direct refinement."},
				[2]string{"Open assumptions", "- The human will approve the plan before code runs."},
			),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateConversationBrief(tc.brief); err != nil {
				t.Fatalf("validateConversationBrief() error = %v", err)
			}
		})
	}
}

func TestValidateConversationBriefRejectsRequiredSectionViolations(t *testing.T) {
	cases := []struct {
		name  string
		brief string
	}{
		{
			name: "missing section",
			brief: conversationBriefSections(
				[2]string{"Goal", "Ship the conversation-created task path."},
				[2]string{"In scope", "- Validate a create-Task action."},
				[2]string{"Out of scope", "- Direct commits from chat."},
				[2]string{"Key decisions", "- Use Direct refinement."},
			),
		},
		{
			name: "out of order section",
			brief: conversationBriefSections(
				[2]string{"Goal", "Ship the conversation-created task path."},
				[2]string{"Out of scope", "- Direct commits from chat."},
				[2]string{"In scope", "- Validate a create-Task action."},
				[2]string{"Key decisions", "- Use Direct refinement."},
				[2]string{"Open assumptions", "- The human will approve the plan before code runs."},
			),
		},
		{
			name: "duplicated section",
			brief: conversationBriefSections(
				[2]string{"Goal", "Ship the conversation-created task path."},
				[2]string{"Goal", "Duplicate goal."},
				[2]string{"In scope", "- Validate a create-Task action."},
				[2]string{"Out of scope", "- Direct commits from chat."},
				[2]string{"Key decisions", "- Use Direct refinement."},
				[2]string{"Open assumptions", "- The human will approve the plan before code runs."},
			),
		},
		{
			name: "empty section",
			brief: conversationBriefSections(
				[2]string{"Goal", "Ship the conversation-created task path."},
				[2]string{"In scope", ""},
				[2]string{"Out of scope", "- Direct commits from chat."},
				[2]string{"Key decisions", "- Use Direct refinement."},
				[2]string{"Open assumptions", "- The human will approve the plan before code runs."},
			),
		},
		{
			name: "extra top-level section",
			brief: conversationBriefSections(
				[2]string{"Goal", "Ship the conversation-created task path."},
				[2]string{"In scope", "- Validate a create-Task action."},
				[2]string{"Out of scope", "- Direct commits from chat."},
				[2]string{"Key decisions", "- Use Direct refinement."},
				[2]string{"Open assumptions", "- The human will approve the plan before code runs."},
				[2]string{"Notes", "- Extra top-level sections are not allowed."},
			),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateConversationBrief(tc.brief); err == nil {
				t.Fatalf("validateConversationBrief() nil err; want rejection")
			}
		})
	}
}

func TestValidateConversationActionsRejectsNonAllowListedAndMalformed(t *testing.T) {
	brief := sectionedConversationBrief()
	allowed := []string{conversationActionCreateTask}
	cases := []struct {
		name    string
		payload map[string]any
	}{
		{name: "unknown action", payload: map[string]any{"actions": []any{map[string]any{"type": "rerun-stage", "idea": brief}}}},
		{name: "missing idea", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionCreateTask}}}},
		{name: "too many actions", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionCreateTask, "idea": brief}, map[string]any{"type": conversationActionCreateTask, "idea": brief}}}},
		{name: "unsupported field", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionCreateTask, "idea": brief, "workflow_template_id": workflow.DirectCommitID}}}},
		{name: "non-sectioned brief", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionCreateTask, "idea": "Just build it."}}}},
		{name: "malformed actions", payload: map[string]any{"actions": "create-Task"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if action, err := validateConversationActions(tc.payload, allowed); err == nil {
				t.Fatalf("validateConversationActions() = %#v, nil err; want rejection", action)
			}
		})
	}
}

func TestConversationCreateTaskActionCreatesDirectBalancedRunLinkedToConversation(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	brief := sectionedConversationBriefWithMarkdown()
	runner := &conversationTestRunner{dispatches: make(chan contract.Dispatch, 1), payload: map[string]any{
		"reply_markdown": "I have enough to create a gated Task.",
		"actions":        []any{map[string]any{"type": conversationActionCreateTask, "idea": brief}},
	}}
	engine := newRecordingEngine(t, st, runner, EngineOptions{ConversationAdapter: "chat-agent"})
	conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	if _, err := engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "Start the work"); err != nil {
		t.Fatalf("SubmitConversationMessage() error = %v", err)
	}
	_ = receiveDispatch(t, runner.dispatches)
	messages := waitForConversationMessages(t, st, conversation.ID, 3)
	if messages[len(messages)-2].Body != "I have enough to create a gated Task." {
		t.Fatalf("agent reply = %#v", messages[len(messages)-2])
	}
	last := messages[len(messages)-1]
	if last.Role != store.MessageRoleAssistant || !strings.Contains(last.Body, "awaiting `plan_review_human`") {
		t.Fatalf("last message = %#v, want task-created plan-review note", last)
	}
	runs, err := st.ListRunsForProject(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("runs = %#v, want one created run", runs)
	}
	waitForWorkflowStageAwaiting(t, st, runs[0].ID, "plan_review_human")
	wr, err := st.GetWorkflowRun(ctx, runs[0].ID)
	if err != nil {
		t.Fatalf("get workflow run: %v", err)
	}
	if wr.Run.Idea != brief || wr.Task.Idea != brief {
		t.Fatalf("idea = run:%q task:%q, want verbatim brief %q", wr.Run.Idea, wr.Task.Idea, brief)
	}
	if wr.Run.RefinementLevel != contract.RefinementLevelDirect || wr.Task.RefinementLevel != contract.RefinementLevelDirect {
		t.Fatalf("refinement = run:%q task:%q, want direct", wr.Run.RefinementLevel, wr.Task.RefinementLevel)
	}
	if wr.Run.WorkflowTemplateID != workflow.BalancedPRDeliveryID {
		t.Fatalf("workflow template = %q, want %q", wr.Run.WorkflowTemplateID, workflow.BalancedPRDeliveryID)
	}
	if wr.Task.ConversationID != conversation.ID {
		t.Fatalf("task conversation_id = %q, want %q", wr.Task.ConversationID, conversation.ID)
	}
}

func TestConversationRejectsInvalidActionAndCreatesNothing(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
	}{
		{name: "unknown", payload: map[string]any{"reply_markdown": "Creating a task.", "actions": []any{map[string]any{"type": "rerun-stage", "idea": sectionedConversationBrief()}}}},
		{name: "malformed", payload: map[string]any{"reply_markdown": "Creating a task.", "actions": []any{map[string]any{"type": conversationActionCreateTask}}}},
		{name: "non-sectioned", payload: map[string]any{"reply_markdown": "Creating a task.", "actions": []any{map[string]any{"type": conversationActionCreateTask, "idea": "Just build it."}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, t.TempDir())
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			runner := &conversationTestRunner{dispatches: make(chan contract.Dispatch, 1), payload: tc.payload}
			engine := newRecordingEngine(t, st, runner, EngineOptions{ConversationAdapter: "chat-agent"})
			conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
			if err != nil {
				t.Fatalf("ensure conversation: %v", err)
			}
			if _, err := engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "Start the work"); err != nil {
				t.Fatalf("SubmitConversationMessage() error = %v", err)
			}
			_ = receiveDispatch(t, runner.dispatches)
			messages := waitForConversationMessages(t, st, conversation.ID, 2)
			last := messages[len(messages)-1]
			if last.Role != store.MessageRoleAssistant || !strings.Contains(last.Body, "could not complete this turn") {
				t.Fatalf("last message = %#v, want rejected action notice", last)
			}
			runs, err := st.ListRunsForProject(ctx, store.DefaultProjectID)
			if err != nil {
				t.Fatalf("list runs: %v", err)
			}
			if len(runs) != 0 {
				t.Fatalf("runs = %#v, want no created run", runs)
			}
		})
	}
}

func sectionedConversationBrief() string {
	return conversationBriefSections(
		[2]string{"Goal", "Ship the conversation-created task path."},
		[2]string{"In scope", "- Validate a create-Task action.\n- Start a gated Task."},
		[2]string{"Out of scope", "- Direct commits from chat."},
		[2]string{"Key decisions", "- Use Direct refinement.\n- Use the Balanced plan-gated template."},
		[2]string{"Open assumptions", "- The human will approve the plan before code runs."},
	)
}

func sectionedConversationBriefWithMarkdown() string {
	return conversationBriefSections(
		[2]string{"Goal", "Ship the conversation-created task path."},
		[2]string{"In scope", "### Validation cases\n- Validate a create-Task action.\n- Start a gated Task.\n\n```bash\n# install deps\n#!/bin/bash\nmake test\n```"},
		[2]string{"Out of scope", "- Direct commits from chat."},
		[2]string{"Key decisions", "- Use Direct refinement.\n- Use the Balanced plan-gated template."},
		[2]string{"Open assumptions", "- The human will approve the plan before code runs."},
	)
}

func conversationBriefSections(sections ...[2]string) string {
	parts := make([]string, 0, len(sections))
	for _, section := range sections {
		parts = append(parts, "## "+section[0]+"\n"+section[1])
	}
	return strings.Join(parts, "\n\n")
}

type managedConversationRunner struct {
	dispatches   chan contract.Dispatch
	releases     chan struct{}
	replies      []string
	evictions    chan string
	evictStarted chan string
	evictRelease chan struct{}

	mu    sync.Mutex
	count int
}

func (r *managedConversationRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	r.mu.Lock()
	idx := r.count
	r.count++
	r.mu.Unlock()
	select {
	case r.dispatches <- disp:
	case <-ctx.Done():
		return report.Report{}, ctx.Err()
	}
	if r.releases != nil {
		select {
		case <-r.releases:
		case <-ctx.Done():
			return report.Report{}, ctx.Err()
		}
	}
	reply := "conversation reply"
	if idx < len(r.replies) {
		reply = r.replies[idx]
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        report.StatusCompleted,
		Summary:       "conversation reply",
		Payload:       map[string]any{"reply_markdown": reply},
		Errors:        []string{},
	}, nil
}

func (r *managedConversationRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func (r *managedConversationRunner) EvictWarmSession(ctx context.Context, warmSessionKey string) error {
	if r.evictStarted != nil {
		select {
		case r.evictStarted <- warmSessionKey:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r.evictRelease != nil {
		select {
		case <-r.evictRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r.evictions != nil {
		select {
		case r.evictions <- warmSessionKey:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type cancellableConversationRunner struct {
	dispatches chan contract.Dispatch
	cancelled  chan struct{}
	once       sync.Once
}

func (r *cancellableConversationRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	select {
	case r.dispatches <- disp:
	case <-ctx.Done():
		r.once.Do(func() { close(r.cancelled) })
		return report.Report{}, ctx.Err()
	}
	<-ctx.Done()
	r.once.Do(func() { close(r.cancelled) })
	return report.Report{}, ctx.Err()
}

func (r *cancellableConversationRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

type conversationTestRunner struct {
	dispatches chan contract.Dispatch
	reply      string
	payload    map[string]any
}

func (r *conversationTestRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	select {
	case r.dispatches <- disp:
	case <-ctx.Done():
		return report.Report{}, ctx.Err()
	}
	payload := r.payload
	if payload == nil {
		payload = map[string]any{"reply_markdown": r.reply}
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        report.StatusCompleted,
		Summary:       "conversation reply",
		Payload:       payload,
		Errors:        []string{},
	}, nil
}

func (r *conversationTestRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

type capturingConversationBroadcaster struct {
	events chan event.Event
}

func (b *capturingConversationBroadcaster) Broadcast(_ string, ev event.Event, _ string) {
	select {
	case b.events <- ev:
	default:
	}
}

func firstStageByType(t *testing.T, stages []store.Stage, stageType string) store.Stage {
	t.Helper()
	for _, stage := range stages {
		if stage.StageType == stageType {
			return stage
		}
	}
	t.Fatalf("stage type %s not found in %#v", stageType, stages)
	return store.Stage{}
}

func receiveDispatch(t *testing.T, ch <-chan contract.Dispatch) contract.Dispatch {
	t.Helper()
	select {
	case disp := <-ch:
		return disp
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for conversation dispatch")
		return contract.Dispatch{}
	}
}

func assertNoConversationDispatch(t *testing.T, ch <-chan contract.Dispatch, d time.Duration) {
	t.Helper()
	select {
	case disp := <-ch:
		t.Fatalf("unexpected conversation dispatch: %#v", disp)
	case <-time.After(d):
	}
}

func receiveEviction(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case warmSessionKey := <-ch:
		return warmSessionKey
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for warm session eviction")
		return ""
	}
}

func waitForConversationMessages(t *testing.T, st *store.Store, conversationID string, want int) []store.Message {
	t.Helper()
	var messages []store.Message
	pred := func() bool {
		var err error
		messages, err = st.ListMessagesForConversation(context.Background(), conversationID)
		if err != nil {
			t.Fatalf("list messages: %v", err)
		}
		return len(messages) >= want
	}
	// Event-driven when the engine broadcasts to a recorder; the custom-broadcaster test
	// has no recorder, so fall back to a generous poll (a safety net, not a tight deadline).
	if rec, ok := lookupRecorder(st); ok {
		rec.waitUntil(t, pred)
		return messages
	}
	deadline := time.Now().Add(testWaitTimeout)
	for time.Now().Before(deadline) {
		if pred() {
			return messages
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d messages, got %#v", want, messages)
	return nil
}

func sawProjectConversationBroadcast(t *testing.T, ch <-chan event.Event, projectID string) bool {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case ev := <-ch:
			if ev.ProjectID == projectID && ev.RunID == "" && ev.Type == "conversation.agent_replied" {
				return true
			}
		case <-deadline:
			return false
		}
	}
}
