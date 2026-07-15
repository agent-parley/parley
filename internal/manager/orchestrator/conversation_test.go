package orchestrator

import (
	"context"
	"errors"
	"fmt"
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
	assertAllowedConversationActions(t, disp.Input["allowed_actions"])
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

func TestConversationDispatchInputAdvertisesFloorMeetingTemplates(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: t.TempDir()})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	if _, err := st.UpdateProjectWorkflowTemplatePolicy(ctx, project.ID, store.ProjectWorkflowTemplatePolicy{DefaultTemplateID: workflow.CarefulReviewID, SmallFixTemplateID: workflow.QuickFixDeliveryID}); err != nil {
		t.Fatalf("update workflow template policy: %v", err)
	}
	conversation, err := st.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	message, err := st.AddMessage(ctx, conversation.ID, store.MessageRoleUser, "Fix a typo")
	if err != nil {
		t.Fatalf("add message: %v", err)
	}
	engine := NewEngineWithOptions(st, nil, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{ConversationAdapter: "chat-agent"})
	registerEngineTeardown(t, engine, st)

	input, err := engine.conversationDispatchInput(ctx, project, conversation, message.ID)
	if err != nil {
		t.Fatalf("conversationDispatchInput() error = %v", err)
	}
	selection, ok := input["workflow_template_selection"].(map[string]any)
	if !ok {
		t.Fatalf("workflow_template_selection = %#v, want object", input["workflow_template_selection"])
	}
	if selection["default_template_id"] != workflow.CarefulReviewID || selection["small_fix_template_id"] != workflow.QuickFixDeliveryID {
		t.Fatalf("workflow template selection policy = %#v", selection)
	}
	selectable, ok := selection["selectable_templates"].([]map[string]any)
	if !ok {
		t.Fatalf("selectable_templates = %#v, want []map", selection["selectable_templates"])
	}
	seen := map[string][]string{}
	for _, item := range selectable {
		id, _ := item["id"].(string)
		sources, _ := item["sources"].([]string)
		seen[id] = sources
	}
	for _, id := range []string{workflow.BalancedPRDeliveryID, workflow.CarefulReviewID, workflow.QuickFixDeliveryID} {
		if _, ok := seen[id]; !ok {
			t.Fatalf("selectable templates missing %s: %#v", id, selectable)
		}
	}
	for _, id := range []string{workflow.DirectCommitID, workflow.AutonomousPRDeliveryID} {
		if _, ok := seen[id]; ok {
			t.Fatalf("non-floor template %s was advertised: %#v", id, selectable)
		}
	}
	if !stringSliceContains(seen[workflow.CarefulReviewID], "default") || !stringSliceContains(seen[workflow.QuickFixDeliveryID], "small_fix") {
		t.Fatalf("selection sources = %#v, want default Careful and small-fix Quick Fix", seen)
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

func TestConversationTurnDeadlineTimesOutEvictsAndColdStartsQueuedFollowUp(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: t.TempDir()})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	const deadline = 200 * time.Millisecond
	runner := &managedConversationRunner{
		dispatches:    make(chan contract.Dispatch, 2),
		releases:      make(chan struct{}, 2),
		replies:       []string{"unused timed-out reply", "follow-up reply"},
		evictions:     make(chan string, 1),
		cancellations: make(chan contract.Dispatch, 1),
		hasDeadlines:  make(chan bool, 2),
	}
	recorder := newEventRecorder()
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, recorder, EngineOptions{
		ConversationAdapter:      "chat-agent",
		ConversationTurnDeadline: deadline,
	})
	registerEngineTeardown(t, engine, st)

	firstMessage, err := engine.SubmitConversationMessage(ctx, project.ID, "hang forever")
	if err != nil {
		t.Fatalf("submit first: %v", err)
	}
	firstDispatch := receiveDispatch(t, runner.dispatches)
	if !receiveConversationDeadlineState(t, runner.hasDeadlines) {
		t.Fatal("first dispatch context has no deadline")
	}
	secondMessage, err := engine.SubmitConversationMessage(ctx, project.ID, "try the next turn")
	if err != nil {
		t.Fatalf("submit queued follow-up: %v", err)
	}
	if cancelled := receiveDispatch(t, runner.cancellations); cancelled.AttemptID != firstDispatch.AttemptID {
		t.Fatalf("cancelled attempt = %q, want %q", cancelled.AttemptID, firstDispatch.AttemptID)
	}

	timedOut := waitForConversationEvent(t, recorder, "conversation.turn_timed_out", firstMessage.ID)
	if timedOut.Data["deadline_seconds"] != deadline.Seconds() || timedOut.Data["queued"] != 1 || timedOut.Data["cold_start"] != true {
		t.Fatalf("timed-out event data = %#v, want deadline/queue/cold-start metadata", timedOut.Data)
	}
	conversation, err := st.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	messages := waitForConversationMessages(t, st, conversation.ID, 3)
	if messages[0].Body != "hang forever" || messages[1].Body != "try the next turn" || messages[2].Role != store.MessageRoleAssistant || messages[2].Body != conversationTurnDeadlineMessage(deadline) {
		t.Fatalf("messages after timeout = %#v, want intact transcript plus timeout assistant message", messages)
	}
	if evicted := receiveEviction(t, runner.evictions); evicted != firstDispatch.WarmSessionKey {
		t.Fatalf("evicted warm session = %q, want %q", evicted, firstDispatch.WarmSessionKey)
	}
	evicted := waitForConversationEvent(t, recorder, "conversation.session_evicted", "")
	if evicted.Data["reason"] != "turn_deadline" {
		t.Fatalf("eviction event data = %#v, want turn_deadline reason", evicted.Data)
	}

	secondDispatch := receiveDispatch(t, runner.dispatches)
	if secondDispatch.WarmSessionKey != firstDispatch.WarmSessionKey {
		t.Fatalf("follow-up warm session key = %q, want conversation key %q", secondDispatch.WarmSessionKey, firstDispatch.WarmSessionKey)
	}
	if !receiveConversationDeadlineState(t, runner.hasDeadlines) {
		t.Fatal("queued follow-up dispatch context has no fresh deadline")
	}
	runner.releases <- struct{}{}
	messages = waitForConversationMessages(t, st, conversation.ID, 4)
	if messages[3].Role != store.MessageRoleAssistant || messages[3].Body != "follow-up reply" {
		t.Fatalf("messages after follow-up = %#v, want completed assistant reply", messages)
	}
	completed := waitForConversationEvent(t, recorder, "conversation.turn_completed", secondMessage.ID)
	if completed.Data["cold_start"] != true {
		t.Fatalf("follow-up completion data = %#v, want cold start after timeout eviction", completed.Data)
	}
	assertNoConversationEventType(t, recorder, "conversation.turn_cancelled")
}

func TestConversationUserCancelDuringDeadlinedTurnIsUnchanged(t *testing.T) {
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
	recorder := newEventRecorder()
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, recorder, EngineOptions{
		ConversationAdapter:      "chat-agent",
		ConversationTurnDeadline: 5 * time.Second,
	})
	registerEngineTeardown(t, engine, st)

	message, err := engine.SubmitConversationMessage(ctx, project.ID, "cancel this")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	_ = receiveDispatch(t, runner.dispatches)
	conversation, err := st.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	if err := engine.CancelConversationTurn(ctx, conversation.ID); err != nil {
		t.Fatalf("cancel conversation turn: %v", err)
	}
	select {
	case <-runner.cancelled:
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for user cancellation to reach dispatch")
	}
	_ = waitForConversationEvent(t, recorder, "conversation.turn_cancelled", message.ID)
	messages := waitForConversationMessages(t, st, conversation.ID, 1)
	if len(messages) != 1 || messages[0].Role != store.MessageRoleUser {
		t.Fatalf("messages after user cancel = %#v, want only original user message", messages)
	}
	assertNoConversationEventType(t, recorder, "conversation.turn_timed_out")
	assertNoConversationEvictionReason(t, recorder, "turn_deadline")
}

func TestConversationTurnDeadlineZeroDoesNotArmTimer(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: t.TempDir()})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	runner := &managedConversationRunner{
		dispatches:   make(chan contract.Dispatch, 1),
		releases:     make(chan struct{}, 1),
		replies:      []string{"completed without a deadline"},
		hasDeadlines: make(chan bool, 1),
	}
	recorder := newEventRecorder()
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, recorder, EngineOptions{
		ConversationAdapter:      "chat-agent",
		ConversationTurnDeadline: 0,
	})
	registerEngineTeardown(t, engine, st)

	message, err := engine.SubmitConversationMessage(ctx, project.ID, "no timer")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	_ = receiveDispatch(t, runner.dispatches)
	if receiveConversationDeadlineState(t, runner.hasDeadlines) {
		t.Fatal("dispatch context has a deadline when conversation turn deadline is disabled")
	}
	runner.releases <- struct{}{}
	_ = waitForConversationEvent(t, recorder, "conversation.turn_completed", message.ID)
	assertNoConversationEventType(t, recorder, "conversation.turn_timed_out")
}

func TestNewEngineDefaultsConversationTurnDeadlineOn(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	engine := NewEngine(st, nil, fakeFragmentRenderer{}, newEventRecorder())
	registerEngineTeardown(t, engine, st)
	if engine.conversationTurnDeadline != 15*time.Minute {
		t.Fatalf("conversation turn deadline = %s, want 15m", engine.conversationTurnDeadline)
	}
}

func TestConversationTurnDeadlineMessageUsesConfiguredMinutes(t *testing.T) {
	const want = "This turn hit the 15-minute execution deadline and was stopped. The conversation is intact - send a message to try again."
	if got := conversationTurnDeadlineMessage(15 * time.Minute); got != want {
		t.Fatalf("deadline message = %q, want %q", got, want)
	}
}

func TestConversationTurnCompletesBeforeDeadlineAndReleasesTimer(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: store.DefaultProjectID, Name: "Default project", RepositoryPath: t.TempDir()})
	if err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	const deadline = 500 * time.Millisecond
	runner := &managedConversationRunner{
		dispatches:   make(chan contract.Dispatch, 1),
		releases:     make(chan struct{}, 1),
		replies:      []string{"finished in time"},
		hasDeadlines: make(chan bool, 1),
		contexts:     make(chan context.Context, 1),
	}
	recorder := newEventRecorder()
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, recorder, EngineOptions{
		ConversationAdapter:      "chat-agent",
		ConversationTurnDeadline: deadline,
	})
	registerEngineTeardown(t, engine, st)

	message, err := engine.SubmitConversationMessage(ctx, project.ID, "finish near the deadline")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	_ = receiveDispatch(t, runner.dispatches)
	if !receiveConversationDeadlineState(t, runner.hasDeadlines) {
		t.Fatal("dispatch context has no configured deadline")
	}
	turnCtx := receiveConversationContext(t, runner.contexts)
	runner.releases <- struct{}{}
	_ = waitForConversationEvent(t, recorder, "conversation.turn_completed", message.ID)
	select {
	case <-turnCtx.Done():
	case <-time.After(testWaitTimeout):
		t.Fatal("completed turn context was not cancelled to release its deadline timer")
	}
	if cause := context.Cause(turnCtx); !errors.Is(cause, context.Canceled) {
		t.Fatalf("completed turn context cause = %v, want context.Canceled", cause)
	}
	assertNoConversationEventType(t, recorder, "conversation.turn_timed_out")
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

func TestConversationSessionsReturnToBaselineAfterIdleEvictionChurn(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	const conversations = 8
	runner := &managedConversationRunner{dispatches: make(chan contract.Dispatch, conversations), evictions: make(chan string, conversations)}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, newEventRecorder(), EngineOptions{ConversationAdapter: "chat-agent", ConversationBudget: conversations, ConversationIdleTTL: 20 * time.Millisecond})
	registerEngineTeardown(t, engine, st)

	for i := range conversations {
		projectID := fmt.Sprintf("project_%d", i)
		project, err := st.EnsureProject(ctx, store.ProjectSpec{ID: projectID, Name: fmt.Sprintf("Project %d", i), RepositoryPath: t.TempDir()})
		if err != nil {
			t.Fatalf("ensure project %d: %v", i, err)
		}
		if _, err := engine.SubmitConversationMessage(ctx, project.ID, fmt.Sprintf("message %d", i)); err != nil {
			t.Fatalf("submit conversation %d: %v", i, err)
		}
		disp := receiveDispatch(t, runner.dispatches)
		if disp.WarmSessionKey == "" {
			t.Fatalf("warm session key for conversation %d empty", i)
		}
		conversation, err := st.EnsureProjectConversation(ctx, project.ID)
		if err != nil {
			t.Fatalf("ensure conversation %d: %v", i, err)
		}
		waitForConversationMessages(t, st, conversation.ID, 2)
	}
	for range conversations {
		_ = receiveEviction(t, runner.evictions)
	}
	waitForConversationSessionCount(t, engine, 0)
}

func TestPruneDormantConversationSessionRequiresFullyDormant(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*Engine, *conversationSession)
		wantPruned bool
	}{
		{name: "dormant", wantPruned: true},
		{name: "running", mutate: func(_ *Engine, session *conversationSession) { session.running = true }},
		{name: "queued", mutate: func(_ *Engine, session *conversationSession) {
			session.queue = []conversationTurn{{conversationID: session.conversationID}}
		}},
		{name: "warm", mutate: func(_ *Engine, session *conversationSession) { session.warm = true }},
		{name: "ready flag", mutate: func(_ *Engine, session *conversationSession) { session.ready = true }},
		{name: "ready list", mutate: func(e *Engine, session *conversationSession) { e.conversationReady = []string{session.conversationID} }},
		{name: "stale pointer", mutate: func(e *Engine, session *conversationSession) {
			e.conversationSessions[session.conversationID] = &conversationSession{conversationID: session.conversationID, projectID: session.projectID}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engine := &Engine{conversationSessions: map[string]*conversationSession{}}
			session := &conversationSession{conversationID: "conversation-1", projectID: "project-1"}
			engine.conversationSessions[session.conversationID] = session
			if tc.mutate != nil {
				tc.mutate(engine, session)
			}

			engine.conversationMu.Lock()
			pruned := engine.pruneDormantConversationSessionLocked(session)
			_, exists := engine.conversationSessions[session.conversationID]
			engine.conversationMu.Unlock()

			if pruned != tc.wantPruned {
				t.Fatalf("pruned = %v, want %v", pruned, tc.wantPruned)
			}
			if exists == tc.wantPruned {
				t.Fatalf("conversationSessions exists = %v, want %v", exists, !tc.wantPruned)
			}
		})
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
	recorder := newEventRecorder()
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, recorder, EngineOptions{
		ConversationAdapter:      "chat-agent",
		ConversationTurnDeadline: 5 * time.Second,
	})
	defer st.Close()

	message, err := engine.SubmitConversationMessage(ctx, project.ID, "please think")
	if err != nil {
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
	_ = waitForConversationEvent(t, recorder, "conversation.turn_cancelled", message.ID)
	assertNoConversationEventType(t, recorder, "conversation.turn_timed_out")
	assertNoConversationEvictionReason(t, recorder, "turn_deadline")
	conversation, err := st.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	messages, err := st.ListMessagesForConversation(ctx, conversation.ID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Role != store.MessageRoleUser {
		t.Fatalf("messages after shutdown = %#v, want no timeout assistant message", messages)
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
	assertAllowedConversationActions(t, input["allowed_actions"])
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
	registerEngineTeardown(t, engine, st)

	if _, err := engine.SubmitConversationMessage(ctx, project.ID, "How did the last run go?"); err != nil {
		t.Fatalf("SubmitConversationMessage() error = %v", err)
	}
	disp := receiveDispatch(t, runner.dispatches)
	assertAllowedConversationActions(t, disp.Input["allowed_actions"])
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
		"action": map[string]any{"type": conversationActionCreateTask, "idea": brief, "template": workflow.QuickFixDeliveryID},
	}, []string{conversationActionCreateTask})
	if err != nil {
		t.Fatalf("validateConversationActions() singular error = %v", err)
	}
	if singular == nil || singular.Idea != brief || singular.Template != workflow.QuickFixDeliveryID {
		t.Fatalf("singular action = %#v, want verbatim brief and template", singular)
	}
}

func TestValidateConversationActionsAcceptsReRunStage(t *testing.T) {
	action, err := validateConversationActions(map[string]any{
		"reply_markdown": "Re-running validation.",
		"actions":        []any{map[string]any{"type": conversationActionReRunStage, "run_id": " run_123 ", "stage": " validation "}},
	}, []string{conversationActionCreateTask, conversationActionReRunStage})
	if err != nil {
		t.Fatalf("validateConversationActions() error = %v", err)
	}
	if action == nil || action.Type != conversationActionReRunStage || action.RunID != "run_123" || action.Stage != "validation" {
		t.Fatalf("action = %#v, want re-run-stage with trimmed run/stage", action)
	}

	singular, err := validateConversationActions(map[string]any{
		"action": map[string]any{"type": conversationActionReRunStage, "run_id": "run_456", "stage": "change_review_agent"},
	}, []string{conversationActionReRunStage})
	if err != nil {
		t.Fatalf("validateConversationActions() singular error = %v", err)
	}
	if singular == nil || singular.RunID != "run_456" || singular.Stage != "change_review_agent" {
		t.Fatalf("singular action = %#v, want re-run-stage run/stage", singular)
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
	allowed := []string{conversationActionCreateTask, conversationActionReRunStage}
	cases := []struct {
		name    string
		payload map[string]any
	}{
		{name: "unknown action", payload: map[string]any{"actions": []any{map[string]any{"type": "rerun-stage", "idea": brief}}}},
		{name: "missing idea", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionCreateTask}}}},
		{name: "too many actions", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionCreateTask, "idea": brief}, map[string]any{"type": conversationActionCreateTask, "idea": brief}}}},
		{name: "unsupported create field", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionCreateTask, "idea": brief, "workflow_template_id": workflow.DirectCommitID}}}},
		{name: "non-string template", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionCreateTask, "idea": brief, "template": 42}}}},
		{name: "non-sectioned brief", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionCreateTask, "idea": "Just build it."}}}},
		{name: "missing rerun run", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionReRunStage, "stage": "validation"}}}},
		{name: "missing rerun stage", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionReRunStage, "run_id": "run_123"}}}},
		{name: "non-string rerun run", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionReRunStage, "run_id": 123, "stage": "validation"}}}},
		{name: "unsupported rerun field", payload: map[string]any{"actions": []any{map[string]any{"type": conversationActionReRunStage, "run_id": "run_123", "stage": "validation", "idea": brief}}}},
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
	if last.Role != store.MessageRoleAssistant || !strings.Contains(last.Body, "workflow template `balanced_pr_delivery`") {
		t.Fatalf("last message = %#v, want task-created template note", last)
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

func TestConversationCreateTaskActionHonorsFloorMeetingTemplateChoice(t *testing.T) {
	for _, templateID := range []string{workflow.BalancedPRDeliveryID, workflow.CarefulReviewID, workflow.QuickFixDeliveryID} {
		t.Run(templateID, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, t.TempDir())
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			engine := NewEngineWithOptions(st, nil, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{
				ConversationAdapter: "chat-agent",
				QueuePolicy:         &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100},
			})
			registerEngineTeardown(t, engine, st)
			conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
			if err != nil {
				t.Fatalf("ensure conversation: %v", err)
			}

			wr, err := engine.executeConversationAction(ctx, store.DefaultProjectID, conversation.ID, conversationAction{Type: conversationActionCreateTask, Idea: sectionedConversationBrief(), Template: templateID})
			if err != nil {
				t.Fatalf("executeConversationAction() error = %v", err)
			}
			if wr.Run.WorkflowTemplateID != templateID {
				t.Fatalf("workflow template = %q, want %q", wr.Run.WorkflowTemplateID, templateID)
			}
			if wr.Task.ConversationID != conversation.ID {
				t.Fatalf("task conversation_id = %q, want %q", wr.Task.ConversationID, conversation.ID)
			}
		})
	}
}

func TestConversationCreateTaskActionRejectsNonFloorTemplateChoice(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	engine := NewEngineWithOptions(st, nil, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{
		ConversationAdapter: "chat-agent",
		QueuePolicy:         &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100},
	})
	registerEngineTeardown(t, engine, st)
	conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}

	_, err = engine.executeConversationAction(ctx, store.DefaultProjectID, conversation.ID, conversationAction{Type: conversationActionCreateTask, Idea: sectionedConversationBrief(), Template: workflow.DirectCommitID})
	if err == nil || !strings.Contains(err.Error(), "lacks a human gate before the target branch") {
		t.Fatalf("executeConversationAction() error = %v, want human-gate rejection", err)
	}
	runs, err := st.ListRunsForProject(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %#v, want no created run", runs)
	}
}

func TestConversationCreateTaskActionUsesConfiguredDefaultTemplate(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.UpdateProjectWorkflowTemplatePolicy(ctx, store.DefaultProjectID, store.ProjectWorkflowTemplatePolicy{DefaultTemplateID: workflow.CarefulReviewID}); err != nil {
		t.Fatalf("update workflow template policy: %v", err)
	}
	engine := NewEngineWithOptions(st, nil, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{
		ConversationAdapter: "chat-agent",
		QueuePolicy:         &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100},
	})
	registerEngineTeardown(t, engine, st)
	conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}

	wr, err := engine.executeConversationAction(ctx, store.DefaultProjectID, conversation.ID, conversationAction{Type: conversationActionCreateTask, Idea: sectionedConversationBrief()})
	if err != nil {
		t.Fatalf("executeConversationAction() error = %v", err)
	}
	if wr.Run.WorkflowTemplateID != workflow.CarefulReviewID {
		t.Fatalf("workflow template = %q, want configured default %q", wr.Run.WorkflowTemplateID, workflow.CarefulReviewID)
	}
}

func TestConversationCreateTaskActionRejectsConfiguredNonFloorDefault(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `UPDATE projects SET workflow_template_default_id = ? WHERE id = ?`, workflow.DirectCommitID, store.DefaultProjectID); err != nil {
		t.Fatalf("seed non-floor workflow template policy directly: %v", err)
	}
	engine := NewEngineWithOptions(st, nil, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{
		ConversationAdapter: "chat-agent",
		QueuePolicy:         &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100},
	})
	registerEngineTeardown(t, engine, st)
	conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}

	_, err = engine.executeConversationAction(ctx, store.DefaultProjectID, conversation.ID, conversationAction{Type: conversationActionCreateTask, Idea: sectionedConversationBrief()})
	if err == nil || !strings.Contains(err.Error(), "not selectable") {
		t.Fatalf("executeConversationAction() error = %v, want configured default rejection", err)
	}
}

func TestConversationCreateTaskActionFromAgentSmallFixTemplateStartsQuickFixRun(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.UpdateProjectWorkflowTemplatePolicy(ctx, store.DefaultProjectID, store.ProjectWorkflowTemplatePolicy{SmallFixTemplateID: workflow.QuickFixDeliveryID}); err != nil {
		t.Fatalf("update workflow template policy: %v", err)
	}
	brief := sectionedConversationBriefWithMarkdown()
	runner := &conversationTestRunner{dispatches: make(chan contract.Dispatch, 1), payload: map[string]any{
		"reply_markdown": "I have enough to create a small-fix Task.",
		"actions":        []any{map[string]any{"type": conversationActionCreateTask, "idea": brief, "template": workflow.QuickFixDeliveryID}},
	}}
	engine := newRecordingEngine(t, st, runner, EngineOptions{
		ConversationAdapter: "chat-agent",
		QueuePolicy:         &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100},
	})
	conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	if _, err := engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "Please make a trivial fix"); err != nil {
		t.Fatalf("SubmitConversationMessage() error = %v", err)
	}
	_ = receiveDispatch(t, runner.dispatches)
	messages := waitForConversationMessages(t, st, conversation.ID, 3)
	last := messages[len(messages)-1]
	if last.Role != store.MessageRoleAssistant || !strings.Contains(last.Body, "workflow template `quick_fix_delivery`") {
		t.Fatalf("last message = %#v, want quick-fix task-created note", last)
	}
	runs, err := st.ListRunsForProject(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 || runs[0].WorkflowTemplateID != workflow.QuickFixDeliveryID {
		t.Fatalf("runs = %#v, want one Quick Fix run", runs)
	}
}

func TestConversationReRunStageActionFromAgentStartsNewAttempt(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	template := stageRerunTemplate("conversation_rerun", false)
	if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create template: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "rerun from chat", WorkflowTemplateID: template.ID, ConversationID: conversation.ID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.SaveWorkflowSnapshot(ctx, wr.Run.ID, map[string]any{"workflow_template_snapshot": template}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	beforeAttempts, err := st.CountAttemptsForRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("count attempts before: %v", err)
	}
	runner := &conversationStageReRunRunner{dispatches: make(chan contract.Dispatch, 8), payload: map[string]any{
		"reply_markdown": "Re-running implementation for that run.",
		"actions":        []any{map[string]any{"type": conversationActionReRunStage, "run_id": wr.Run.ID, "stage": "implementation"}},
	}}
	engine := newRecordingEngine(t, st, runner, EngineOptions{
		ConversationAdapter: "chat-agent",
		QueuePolicy:         &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100},
	})

	if _, err := engine.SubmitConversationMessage(ctx, store.DefaultProjectID, "Please re-run implementation for the last run"); err != nil {
		t.Fatalf("SubmitConversationMessage() error = %v", err)
	}
	disp := receiveDispatch(t, runner.dispatches)
	if disp.StageType != contract.StageTypeConversation {
		t.Fatalf("first dispatch stage type = %q, want conversation", disp.StageType)
	}
	messages := waitForConversationMessages(t, st, conversation.ID, 3)
	if messages[len(messages)-2].Body != "Re-running implementation for that run." {
		t.Fatalf("agent reply = %#v", messages[len(messages)-2])
	}
	last := messages[len(messages)-1]
	if last.Role != store.MessageRoleAssistant || !strings.Contains(last.Body, "Started re-running Run `"+wr.Run.ID+"`") || !strings.Contains(last.Body, "frozen workflow graph") {
		t.Fatalf("last message = %#v, want re-run-started note", last)
	}
	waitForRunStatus(t, st, wr.Run.ID, store.RunStatusCompleted)
	afterAttempts, err := st.CountAttemptsForRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("count attempts after: %v", err)
	}
	if afterAttempts != beforeAttempts+1 {
		t.Fatalf("attempt count = %d, want %d", afterAttempts, beforeAttempts+1)
	}
	events, err := st.ListEvents(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	rerunEvent := latestEventOfType(events, "run.stage_rerun_started")
	if rerunEvent.Type == "" {
		t.Fatalf("missing run.stage_rerun_started in events %#v", eventTypes(events))
	}
	if rerunEvent.Actor != (event.Actor{Kind: event.ActorKindAdapter, ID: "chat-agent"}) {
		t.Fatalf("rerun actor = %#v, want chat adapter", rerunEvent.Actor)
	}
	if !runner.sawStageType(contract.StageTypeImplementation) || !runner.sawStageType(contract.StageTypeValidation) {
		t.Fatalf("dispatch stage types = %#v, want implementation and validation", runner.stageTypes())
	}
}

func TestConversationReRunStageRejectsRunOutsideProjectScope(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	otherProjectID := "other_project"
	if _, err := st.EnsureProject(ctx, store.ProjectSpec{ID: otherProjectID, Name: "Other project", RepositoryPath: t.TempDir()}); err != nil {
		t.Fatalf("ensure other project: %v", err)
	}
	template := stageRerunTemplate("conversation_rerun_scope", false)
	if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create template: %v", err)
	}
	otherWR, err := st.CreateWorkflowRunForProjectInput(ctx, otherProjectID, contract.TaskInput{Idea: "other project rerun", WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("create other project run: %v", err)
	}
	if err := st.SaveWorkflowSnapshot(ctx, otherWR.Run.ID, map[string]any{"workflow_template_snapshot": template}); err != nil {
		t.Fatalf("save other project snapshot: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, otherWR.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("complete other project run: %v", err)
	}
	engine := newRecordingEngine(t, st, &recordingRerunRunner{}, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100}})

	beforeAttempts, beforeStatus, beforeEvents := rerunMutationSnapshot(t, st, otherWR.Run.ID)
	_, err = engine.executeConversationAction(ctx, store.DefaultProjectID, conversation.ID, conversationAction{Type: conversationActionReRunStage, RunID: otherWR.Run.ID, Stage: "implementation"})
	if !errors.Is(err, ErrConversationRunNotReadable) {
		t.Fatalf("executeConversationAction() error = %v, want ErrConversationRunNotReadable", err)
	}
	for _, sentinel := range []error{ErrStageReRunRunNotTerminal, ErrStageReRunInvalidTarget, ErrStageReRunPrerequisiteGap} {
		if errors.Is(err, sentinel) {
			t.Fatalf("executeConversationAction() error = %v, want read-scope rejection, not %v", err, sentinel)
		}
	}
	assertNoRerunMutation(t, st, otherWR.Run.ID, beforeAttempts, beforeStatus, beforeEvents)
}

func TestConversationReRunStageRejectsConversationOutsideProjectScope(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	otherProjectID := "other_project"
	if _, err := st.EnsureProject(ctx, store.ProjectSpec{ID: otherProjectID, Name: "Other project", RepositoryPath: t.TempDir()}); err != nil {
		t.Fatalf("ensure other project: %v", err)
	}
	otherConversation, err := st.EnsureProjectConversation(ctx, otherProjectID)
	if err != nil {
		t.Fatalf("ensure other project conversation: %v", err)
	}
	template := stageRerunTemplate("conversation_rerun_conversation_scope", false)
	if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create template: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "default project rerun", WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("create default project run: %v", err)
	}
	if err := st.SaveWorkflowSnapshot(ctx, wr.Run.ID, map[string]any{"workflow_template_snapshot": template}); err != nil {
		t.Fatalf("save default project snapshot: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("complete default project run: %v", err)
	}
	engine := newRecordingEngine(t, st, &recordingRerunRunner{}, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100}})

	beforeAttempts, beforeStatus, beforeEvents := rerunMutationSnapshot(t, st, wr.Run.ID)
	_, err = engine.executeConversationAction(ctx, store.DefaultProjectID, otherConversation.ID, conversationAction{Type: conversationActionReRunStage, RunID: wr.Run.ID, Stage: "implementation"})
	if !errors.Is(err, ErrConversationRunNotReadable) {
		t.Fatalf("executeConversationAction() error = %v, want ErrConversationRunNotReadable", err)
	}
	assertNoRerunMutation(t, st, wr.Run.ID, beforeAttempts, beforeStatus, beforeEvents)
}

func TestConversationReRunStageActionReusesEngineValidationFailClosed(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	template := stageRerunTemplate("conversation_rerun_invalid", false)
	if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create template: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "invalid rerun", WorkflowTemplateID: template.ID, ConversationID: conversation.ID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.SaveWorkflowSnapshot(ctx, wr.Run.ID, map[string]any{"workflow_template_snapshot": template}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	engine := newRecordingEngine(t, st, &recordingRerunRunner{}, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 100}})

	beforeAttempts, beforeStatus, beforeEvents := rerunMutationSnapshot(t, st, wr.Run.ID)
	_, err = engine.executeConversationAction(ctx, store.DefaultProjectID, conversation.ID, conversationAction{Type: conversationActionReRunStage, RunID: wr.Run.ID, Stage: "implementation"})
	if !errors.Is(err, ErrStageReRunRunNotTerminal) {
		t.Fatalf("executeConversationAction() error = %v, want ErrStageReRunRunNotTerminal", err)
	}
	assertNoRerunMutation(t, st, wr.Run.ID, beforeAttempts, beforeStatus, beforeEvents)

	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	beforeAttempts, beforeStatus, beforeEvents = rerunMutationSnapshot(t, st, wr.Run.ID)
	_, err = engine.executeConversationAction(ctx, store.DefaultProjectID, conversation.ID, conversationAction{Type: conversationActionReRunStage, RunID: wr.Run.ID, Stage: "stop_report"})
	if !errors.Is(err, ErrStageReRunInvalidTarget) {
		t.Fatalf("executeConversationAction() error = %v, want ErrStageReRunInvalidTarget", err)
	}
	assertNoRerunMutation(t, st, wr.Run.ID, beforeAttempts, beforeStatus, beforeEvents)
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

func assertAllowedConversationActions(t *testing.T, raw any) {
	t.Helper()
	actions, ok := raw.([]string)
	if !ok {
		t.Fatalf("allowed_actions = %#v, want string allow-list", raw)
	}
	want := []string{conversationActionCreateTask, conversationActionReRunStage}
	if len(actions) != len(want) {
		t.Fatalf("allowed_actions = %#v, want %#v", actions, want)
	}
	for i := range want {
		if actions[i] != want[i] {
			t.Fatalf("allowed_actions = %#v, want %#v", actions, want)
		}
	}
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

type managedConversationRunner struct {
	dispatches    chan contract.Dispatch
	releases      chan struct{}
	replies       []string
	evictions     chan string
	cancellations chan contract.Dispatch
	hasDeadlines  chan bool
	contexts      chan context.Context
	evictStarted  chan string
	evictRelease  chan struct{}

	mu    sync.Mutex
	count int
}

func (r *managedConversationRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	r.mu.Lock()
	idx := r.count
	r.count++
	r.mu.Unlock()
	if r.hasDeadlines != nil {
		_, hasDeadline := ctx.Deadline()
		r.hasDeadlines <- hasDeadline
	}
	if r.contexts != nil {
		r.contexts <- ctx
	}
	select {
	case r.dispatches <- disp:
	case <-ctx.Done():
		r.recordCancellation(disp)
		return report.Report{}, ctx.Err()
	}
	if r.releases != nil {
		select {
		case <-r.releases:
		case <-ctx.Done():
			r.recordCancellation(disp)
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

func (r *managedConversationRunner) recordCancellation(disp contract.Dispatch) {
	if r.cancellations == nil {
		return
	}
	select {
	case r.cancellations <- disp:
	default:
	}
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

type conversationStageReRunRunner struct {
	dispatches chan contract.Dispatch
	payload    map[string]any

	mu    sync.Mutex
	disps []contract.Dispatch
}

func (r *conversationStageReRunRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	r.mu.Lock()
	r.disps = append(r.disps, disp)
	r.mu.Unlock()
	select {
	case r.dispatches <- disp:
	case <-ctx.Done():
		return report.Report{}, ctx.Err()
	}
	if disp.StageType == contract.StageTypeConversation {
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
			Payload:       r.payload,
			Errors:        []string{},
		}, nil
	}
	return validAdapterReport(disp, "conversation re-run dispatch completed"), nil
}

func (r *conversationStageReRunRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func (r *conversationStageReRunRunner) stageTypes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.disps))
	for i, disp := range r.disps {
		out[i] = disp.StageType
	}
	return out
}

func (r *conversationStageReRunRunner) sawStageType(stageType string) bool {
	for _, got := range r.stageTypes() {
		if got == stageType {
			return true
		}
	}
	return false
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

func receiveConversationContext(t *testing.T, ch <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-ch:
		return ctx
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for conversation context")
		return nil
	}
}

func receiveConversationDeadlineState(t *testing.T, ch <-chan bool) bool {
	t.Helper()
	select {
	case hasDeadline := <-ch:
		return hasDeadline
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for conversation context deadline state")
		return false
	}
}

func waitForConversationEvent(t *testing.T, recorder *eventRecorder, typ, triggerMessageID string) event.Event {
	t.Helper()
	var found event.Event
	recorder.waitUntil(t, func() bool {
		for _, ev := range recorder.snapshot() {
			if ev.Type != typ {
				continue
			}
			if triggerMessageID != "" && ev.Data["trigger_message_id"] != triggerMessageID {
				continue
			}
			found = ev
			return true
		}
		return false
	})
	return found
}

func assertNoConversationEventType(t *testing.T, recorder *eventRecorder, typ string) {
	t.Helper()
	for _, ev := range recorder.snapshot() {
		if ev.Type == typ {
			t.Fatalf("unexpected %s event: %#v", typ, ev)
		}
	}
}

func assertNoConversationEvictionReason(t *testing.T, recorder *eventRecorder, reason string) {
	t.Helper()
	for _, ev := range recorder.snapshot() {
		if ev.Type == "conversation.session_evicted" && ev.Data["reason"] == reason {
			t.Fatalf("unexpected conversation session eviction with reason %q: %#v", reason, ev)
		}
	}
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

func waitForConversationSessionCount(t *testing.T, engine *Engine, want int) {
	t.Helper()
	deadline := time.Now().Add(testWaitTimeout)
	for {
		engine.conversationMu.Lock()
		got := len(engine.conversationSessions)
		engine.conversationMu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("conversationSessions size = %d, want %d", got, want)
		}
		time.Sleep(5 * time.Millisecond)
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
