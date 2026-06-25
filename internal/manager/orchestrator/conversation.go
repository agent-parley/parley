package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/ids"
	"github.com/agent-parley/parley/internal/shared/report"
)

const (
	conversationalPlanningAgentRole = "conversational_planning_agent"
	conversationActionCreateTask    = "create-Task"
)

// SubmitConversationMessage persists one user message and starts a fresh agent
// turn for that message. The turn is intentionally not a workflow run: it is a
// per-message AgentAdapter dispatch that rehydrates from persisted Messages and
// may only return an assistant reply plus allow-listed orchestration actions.
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
	e.spawn(func() {
		e.dispatchConversationReply(e.rootCtx, project.ID, conversation.ID, message.ID, dispatchInput)
	})
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
		"allowed_actions": []string{conversationActionCreateTask},
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
	result, err := e.runConversationAgentTurn(ctx, projectID, input)
	if err != nil {
		_, _ = e.persistConversationAssistantMessage(ctx, projectID, conversationID, triggerMessageID, "The conversational agent could not complete this turn: "+err.Error(), event.Actor{Kind: event.ActorKindAdapter, ID: e.conversationAdapter}, "conversation agent failed")
		return
	}
	if _, err := e.persistConversationAssistantMessage(ctx, projectID, conversationID, triggerMessageID, result.Reply, event.Actor{Kind: event.ActorKindAdapter, ID: e.conversationAdapter}, "conversation agent replied"); err != nil {
		return
	}
	if result.Action == nil {
		return
	}
	wr, err := e.executeConversationAction(ctx, projectID, conversationID, *result.Action)
	if err != nil {
		_, _ = e.persistConversationAssistantMessage(ctx, projectID, conversationID, triggerMessageID, "The conversational agent could not create a Task: "+err.Error(), event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "conversation action failed")
		return
	}
	_, _ = e.persistConversationAssistantMessage(ctx, projectID, conversationID, triggerMessageID, conversationTaskCreatedMessage(wr), event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "conversation task created")
}

func (e *Engine) persistConversationAssistantMessage(ctx context.Context, projectID, conversationID, triggerMessageID, body string, actor event.Actor, summary string) (store.Message, error) {
	message, addErr := e.store.AddMessage(ctx, conversationID, store.MessageRoleAssistant, body)
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
		return store.Message{}, addErr
	}
	_, _ = e.emit(ctx, conversationMessageEvent(projectID, conversationID, message, "conversation.agent_replied", actor, summary))
	return message, nil
}

func (e *Engine) executeConversationAction(ctx context.Context, projectID, conversationID string, action conversationAction) (store.WorkflowRun, error) {
	switch action.Type {
	case conversationActionCreateTask:
		runID, err := e.StartProjectRunInput(ctx, projectID, contract.TaskInput{
			Idea:               action.Idea,
			RefinementLevel:    contract.RefinementLevelDirect,
			WorkflowTemplateID: workflow.BalancedPRDeliveryID,
			ConversationID:     conversationID,
		})
		if err != nil {
			return store.WorkflowRun{}, err
		}
		return e.store.GetWorkflowRun(ctx, runID)
	default:
		return store.WorkflowRun{}, fmt.Errorf("unsupported conversation action %q", action.Type)
	}
}

type conversationTurnResult struct {
	Reply  string
	Action *conversationAction
}

type conversationAction struct {
	Type string
	Idea string
}

func (e *Engine) runConversationAgentTurn(ctx context.Context, projectID string, input map[string]any) (conversationTurnResult, error) {
	if e.runner == nil {
		return conversationTurnResult{}, errors.New("runner unavailable")
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
		return conversationTurnResult{}, err
	}
	if rep.Status != report.StatusCompleted {
		if len(rep.Errors) > 0 {
			return conversationTurnResult{}, fmt.Errorf("agent returned %s: %s", rep.Status, strings.Join(rep.Errors, "; "))
		}
		return conversationTurnResult{}, fmt.Errorf("agent returned %s", rep.Status)
	}
	action, err := validateConversationActions(rep.Payload, allowedConversationActions(input))
	if err != nil {
		return conversationTurnResult{}, err
	}
	reply := conversationReplyFromReport(rep)
	if reply == "" {
		return conversationTurnResult{}, fmt.Errorf("agent report missing payload.reply_markdown")
	}
	return conversationTurnResult{Reply: reply, Action: action}, nil
}

func inputRepositoryID(input map[string]any) string {
	repository, _ := input["repository"].(map[string]any)
	id, _ := repository["id"].(string)
	return id
}

func allowedConversationActions(input map[string]any) []string {
	raw, ok := input["allowed_actions"].([]string)
	if ok {
		out := make([]string, 0, len(raw))
		for _, action := range raw {
			if strings.TrimSpace(action) != "" {
				out = append(out, strings.TrimSpace(action))
			}
		}
		return out
	}
	rawAny, _ := input["allowed_actions"].([]any)
	out := make([]string, 0, len(rawAny))
	for _, item := range rawAny {
		if action, ok := item.(string); ok && strings.TrimSpace(action) != "" {
			out = append(out, strings.TrimSpace(action))
		}
	}
	return out
}

func validateConversationActions(payload map[string]any, allowed []string) (*conversationAction, error) {
	if payload == nil {
		return nil, nil
	}
	singular, hasSingular := payload["action"]
	plural, hasPlural := payload["actions"]
	singularPresent := hasSingular && conversationActionFieldPresent(singular)
	pluralPresent := hasPlural && conversationActionFieldPresent(plural)
	if singularPresent && pluralPresent {
		return nil, fmt.Errorf("conversation report must use either payload.action or payload.actions, not both")
	}
	if singularPresent {
		action, err := parseConversationAction(singular, allowed)
		if err != nil {
			return nil, err
		}
		return &action, nil
	}
	if !pluralPresent {
		return nil, nil
	}
	actions, ok := plural.([]any)
	if !ok {
		return nil, fmt.Errorf("payload.actions must be an array")
	}
	if len(actions) == 0 {
		return nil, nil
	}
	if len(actions) > 1 {
		return nil, fmt.Errorf("conversation report may contain at most one action")
	}
	action, err := parseConversationAction(actions[0], allowed)
	if err != nil {
		return nil, err
	}
	return &action, nil
}

func conversationActionFieldPresent(raw any) bool {
	switch v := raw.(type) {
	case nil:
		return false
	case []any:
		return len(v) > 0
	default:
		return true
	}
}

func parseConversationAction(raw any, allowed []string) (conversationAction, error) {
	envelope, ok := raw.(map[string]any)
	if !ok {
		return conversationAction{}, fmt.Errorf("conversation action envelope must be an object")
	}
	for key := range envelope {
		if key != "type" && key != "idea" {
			return conversationAction{}, fmt.Errorf("conversation action %q contains unsupported field %q", actionType(envelope), key)
		}
	}
	typ := actionType(envelope)
	if typ == "" {
		return conversationAction{}, fmt.Errorf("conversation action missing type")
	}
	if !conversationActionAllowed(typ, allowed) {
		return conversationAction{}, fmt.Errorf("conversation action %q is not allowed", typ)
	}
	switch typ {
	case conversationActionCreateTask:
		idea, ok := envelope["idea"].(string)
		if !ok || strings.TrimSpace(idea) == "" {
			return conversationAction{}, fmt.Errorf("create-Task action requires non-empty idea")
		}
		if err := validateConversationBrief(idea); err != nil {
			return conversationAction{}, err
		}
		return conversationAction{Type: typ, Idea: idea}, nil
	default:
		return conversationAction{}, fmt.Errorf("unsupported conversation action %q", typ)
	}
}

func validateConversationBrief(brief string) error {
	required := []string{"Goal", "In scope", "Out of scope", "Key decisions", "Open assumptions"}
	section := 0
	activeSection := -1
	seenContent := make([]bool, len(required))
	inFence := false
	for _, line := range strings.Split(brief, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "```") {
			if activeSection >= 0 {
				seenContent[activeSection] = true
			}
			inFence = !inFence
			continue
		}
		if !inFence {
			if name, ok := conversationBriefSectionHeading(trimmed); ok {
				if section >= len(required) {
					return fmt.Errorf("create-Task idea must contain exactly these Markdown sections: %s", strings.Join(required, ", "))
				}
				if name != required[section] {
					return fmt.Errorf("create-Task idea section %d must be %q", section+1, required[section])
				}
				activeSection = section
				section++
				continue
			}
		}
		if activeSection >= 0 {
			seenContent[activeSection] = true
		}
	}
	if section != len(required) {
		return fmt.Errorf("create-Task idea must contain exactly these Markdown sections: %s", strings.Join(required, ", "))
	}
	for i, ok := range seenContent {
		if !ok {
			return fmt.Errorf("create-Task idea section %q must not be empty", required[i])
		}
	}
	return nil
}

func conversationBriefSectionHeading(trimmed string) (string, bool) {
	if !strings.HasPrefix(trimmed, "## ") {
		return "", false
	}
	name := strings.TrimSpace(strings.TrimPrefix(trimmed, "## "))
	if i := strings.LastIndexFunc(name, func(r rune) bool { return r != '#' }); i >= 0 && i < len(name)-1 {
		if name[i] == ' ' || name[i] == '\t' {
			name = strings.TrimSpace(name[:i])
		}
	}
	return name, true
}

func actionType(envelope map[string]any) string {
	typ, _ := envelope["type"].(string)
	return strings.TrimSpace(typ)
}

func conversationActionAllowed(action string, allowed []string) bool {
	for _, allowedAction := range allowed {
		if action == allowedAction {
			return true
		}
	}
	return false
}

func conversationTaskCreatedMessage(wr store.WorkflowRun) string {
	return fmt.Sprintf("Created Task `%s` / Run `%s` from this conversation. It is awaiting `plan_review_human` for human confirmation before any code runs.", wr.Task.ID, wr.Run.ID)
}

func conversationReplyFromReport(rep report.Report) string {
	if rep.Payload == nil {
		return ""
	}
	value, ok := rep.Payload["reply_markdown"].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
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
