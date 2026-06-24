package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
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
	if actions, ok := disp.Input["allowed_actions"].([]string); !ok || len(actions) != 0 {
		t.Fatalf("allowed_actions = %#v, want empty allow-list", disp.Input["allowed_actions"])
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

func TestConversationAgentActionsAreRejectedInTracerSlice(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	runner := &conversationTestRunner{dispatches: make(chan contract.Dispatch, 1), reply: "", payload: map[string]any{"reply_markdown": "Creating a task.", "actions": []any{map[string]any{"type": "create-Task"}}}}
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
	if last.Role != store.MessageRoleAssistant || last.Body == "Creating a task." {
		t.Fatalf("last message = %#v, want rejected action notice", last)
	}
	runs, err := st.ListRunsForProject(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %#v, want no created run when action is present", runs)
	}
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
