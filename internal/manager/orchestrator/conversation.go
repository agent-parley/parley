package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/ids"
	"github.com/agent-parley/parley/internal/shared/report"
)

const conversationalPlanningAgentRole = "conversational_planning_agent"

// SubmitConversationMessage persists one user message and starts a fresh agent
// turn for that message. The turn is intentionally not a workflow run: it is a
// per-message AgentAdapter dispatch that rehydrates from persisted Messages and
// may only return an assistant reply in this tracer slice.
func (e *Engine) SubmitConversationMessage(ctx context.Context, projectID, body string) (store.Message, error) {
	body = strings.TrimSpace(body)
	if body == "" {
		return store.Message{}, fmt.Errorf("message is required")
	}
	if projectID == "" {
		projectID = e.projectID
	}
	project, err := e.store.GetProject(ctx, projectID)
	if err != nil {
		return store.Message{}, err
	}
	conversation, err := e.store.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		return store.Message{}, err
	}
	message, err := e.store.AddMessage(ctx, conversation.ID, store.MessageRoleUser, body)
	if err != nil {
		return store.Message{}, err
	}
	if _, err := e.emit(ctx, conversationMessageEvent(project.ID, conversation.ID, message, "conversation.message.created", event.Actor{Kind: event.ActorKindUser, ID: "local"}, "conversation message created")); err != nil {
		return store.Message{}, err
	}

	dispatchInput, err := e.conversationDispatchInput(ctx, project, conversation, message.ID)
	if err != nil {
		return store.Message{}, err
	}
	go e.dispatchConversationReply(context.Background(), project.ID, conversation.ID, message.ID, dispatchInput)
	return message, nil
}

func (e *Engine) conversationDispatchInput(ctx context.Context, project store.Project, conversation store.Conversation, triggerMessageID string) (map[string]any, error) {
	messages, err := e.store.ListMessagesForConversation(ctx, conversation.ID)
	if err != nil {
		return nil, err
	}
	history := conversationHistoryThrough(messages, triggerMessageID)
	repositoryID, err := e.store.DefaultRepositoryID(ctx, project.ID)
	if err != nil {
		return nil, err
	}
	orchestrationState, orchestrationSummary, orchestrationMarkdown, err := e.conversationOrchestrationEvidence(ctx, project.ID, conversation.ID)
	if err != nil {
		return nil, err
	}
	input := map[string]any{
		"input_mode":                   contract.AdapterInputModeConversation,
		"agent_role":                   conversationalPlanningAgentRole,
		"conversation_id":              conversation.ID,
		"conversation_title":           conversation.Title,
		"trigger_message_id":           triggerMessageID,
		"messages":                     history,
		"orchestration_state":          orchestrationState,
		"orchestration_state_summary":  orchestrationSummary,
		"orchestration_state_markdown": orchestrationMarkdown,
		"project": map[string]any{
			"id":          project.ID,
			"name":        project.Name,
			"description": project.Description,
		},
		"repository": map[string]any{
			"id":         repositoryID,
			"mount_path": "/project/repo",
			"mode":       "read_only",
			"state":      "canonical_committed_default_branch_head",
		},
		"workspace": map[string]any{
			"mount_path": "/project/workspace",
			"mode":       "read_write",
		},
		"tool_policy": map[string]any{
			"repository": []string{"read", "list", "grep"},
			"workspace":  []string{"read", "write"},
		},
		"allowed_actions": []string{},
	}
	if strings.TrimSpace(project.ProjectRules) != "" {
		input["project_rules"] = project.ProjectRules
	}
	if strings.TrimSpace(project.ProjectPreferences) != "" {
		input["project_preferences"] = project.ProjectPreferences
	}
	return input, nil
}

func conversationHistoryThrough(messages []store.Message, triggerMessageID string) []map[string]any {
	history := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		history = append(history, map[string]any{
			"id":         message.ID,
			"role":       message.Role,
			"body":       message.Body,
			"created_at": message.CreatedAt,
		})
		if message.ID == triggerMessageID {
			break
		}
	}
	return history
}

func (e *Engine) dispatchConversationReply(ctx context.Context, projectID, conversationID, triggerMessageID string, input map[string]any) {
	reply, err := e.runConversationAgentTurn(ctx, projectID, input)
	if err != nil {
		reply = "The conversational agent could not complete this turn: " + err.Error()
	}
	message, addErr := e.store.AddMessage(ctx, conversationID, store.MessageRoleAssistant, reply)
	if addErr != nil {
		_, _ = e.emit(ctx, event.Event{
			SchemaVersion: event.SchemaVersion,
			ProjectID:     projectID,
			Type:          "conversation.agent_failed",
			Actor:         event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"},
			Summary:       "conversation agent reply could not be persisted",
			Data: map[string]any{
				"conversation_id":    conversationID,
				"trigger_message_id": triggerMessageID,
				"error":              addErr.Error(),
			},
		})
		return
	}
	_, _ = e.emit(ctx, conversationMessageEvent(projectID, conversationID, message, "conversation.agent_replied", event.Actor{Kind: event.ActorKindAdapter, ID: e.conversationAdapter}, "conversation agent replied"))
}

func (e *Engine) runConversationAgentTurn(ctx context.Context, projectID string, input map[string]any) (string, error) {
	if e.runner == nil {
		return "", errors.New("runner unavailable")
	}
	disp := contract.Dispatch{
		ProjectID:    projectID,
		RepositoryID: inputRepositoryID(input),
		RunID:        ids.New("convrun"),
		TaskID:       ids.New("convturn"),
		AttemptID:    ids.New("attempt"),
		StageID:      ids.New("convstage"),
		StageType:    contract.StageTypeConversation,
		Adapter:      e.conversationAdapter,
		Input:        input,
	}
	rep, err := e.runner.Dispatch(ctx, disp)
	if err != nil {
		return "", err
	}
	if rep.Status != report.StatusCompleted {
		if len(rep.Errors) > 0 {
			return "", fmt.Errorf("agent returned %s: %s", rep.Status, strings.Join(rep.Errors, "; "))
		}
		return "", fmt.Errorf("agent returned %s", rep.Status)
	}
	if err := rejectConversationActions(rep.Payload); err != nil {
		return "", err
	}
	reply := conversationReplyFromReport(rep)
	if reply == "" {
		return "", fmt.Errorf("agent report missing payload.reply_markdown")
	}
	return reply, nil
}

func inputRepositoryID(input map[string]any) string {
	repository, _ := input["repository"].(map[string]any)
	id, _ := repository["id"].(string)
	return id
}

func rejectConversationActions(payload map[string]any) error {
	if payload == nil {
		return nil
	}
	for _, key := range []string{"action", "actions"} {
		raw, ok := payload[key]
		if !ok || actionValueEmpty(raw) {
			continue
		}
		return fmt.Errorf("conversation actions are not enabled in this slice")
	}
	return nil
}

func actionValueEmpty(raw any) bool {
	switch v := raw.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(v) == ""
	case []any:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	default:
		return false
	}
}

func conversationReplyFromReport(rep report.Report) string {
	if rep.Payload != nil {
		for _, key := range []string{"reply_markdown", "reply", "message"} {
			if value, ok := rep.Payload[key].(string); ok && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
	}
	return strings.TrimSpace(rep.Summary)
}

func conversationMessageEvent(projectID, conversationID string, message store.Message, typ string, actor event.Actor, summary string) event.Event {
	return event.Event{
		SchemaVersion: event.SchemaVersion,
		ProjectID:     projectID,
		Type:          typ,
		Actor:         actor,
		Summary:       summary,
		Data: map[string]any{
			"conversation_id": conversationID,
			"message_id":      message.ID,
			"message_role":    message.Role,
		},
	}
}
