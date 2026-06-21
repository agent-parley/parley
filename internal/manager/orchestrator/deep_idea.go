package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

const (
	defaultDeepIdeaMaxQuestionRounds = 3
	deepIdeaQuestionsArtifactKind    = "idea_refinement_questions"
	deepIdeaAnswersArtifactKind      = "idea_refinement_answers"
)

var (
	ErrDeepIdeaNotAwaiting   = errors.New("run is not awaiting that idea refinement stage")
	ErrInvalidDeepIdeaAnswer = errors.New("invalid idea refinement answer submission")
)

type DeepIdeaAnswersSubmission struct {
	ActorID      string           `json:"actor_id"`
	AnswerText   string           `json:"answer_text,omitempty"`
	Answers      []DeepIdeaAnswer `json:"answers,omitempty"`
	EvidenceRefs []string         `json:"evidence_refs,omitempty"`
}

type DeepIdeaAnswer struct {
	Question string `json:"question,omitempty"`
	Answer   string `json:"answer"`
}

type DeepIdeaAnswerReceipt struct {
	RunID      string `json:"run_id"`
	StageID    string `json:"stage_id"`
	ArtifactID string `json:"artifact_id"`
	Round      int    `json:"round"`
}

type deepIdeaQuestionPacket struct {
	SchemaVersion          int            `json:"schema_version"`
	RunID                  string         `json:"run_id"`
	TaskID                 string         `json:"task_id"`
	AttemptID              string         `json:"attempt_id"`
	StageID                string         `json:"stage_id"`
	StageType              string         `json:"stage_type"`
	WorkflowStageID        string         `json:"workflow_stage_id"`
	RefinementLevel        string         `json:"refinement_level"`
	Round                  int            `json:"round"`
	MaxRounds              int            `json:"max_rounds"`
	Questions              []string       `json:"questions"`
	Summary                string         `json:"summary"`
	StageBriefArtifactID   string         `json:"stage_brief_artifact_id"`
	TaskContractArtifactID string         `json:"task_contract_artifact_id"`
	PlannerReport          map[string]any `json:"planner_report,omitempty"`
	SubmissionEndpointHint string         `json:"submission_endpoint_hint"`
}

type deepIdeaAnswerPacket struct {
	SchemaVersion      int              `json:"schema_version"`
	RunID              string           `json:"run_id"`
	TaskID             string           `json:"task_id"`
	AttemptID          string           `json:"attempt_id"`
	StageID            string           `json:"stage_id"`
	StageType          string           `json:"stage_type"`
	WorkflowStageID    string           `json:"workflow_stage_id"`
	RefinementLevel    string           `json:"refinement_level"`
	Round              int              `json:"round"`
	ActorID            string           `json:"actor_id"`
	Questions          []string         `json:"questions"`
	AnswerText         string           `json:"answer_text,omitempty"`
	Answers            []DeepIdeaAnswer `json:"answers,omitempty"`
	EvidenceRefs       []string         `json:"evidence_refs,omitempty"`
	QuestionArtifactID string           `json:"question_artifact_id"`
}

type deepIdeaRound struct {
	Round              int
	StageID            string
	WorkflowStageID    string
	Questions          []string
	QuestionArtifactID string
	AnswerArtifactID   string
	AnswerText         string
	Answers            []DeepIdeaAnswer
	ActorID            string
}

type deepIdeaConversation struct {
	Rounds []deepIdeaRound
}

func (c deepIdeaConversation) answeredRounds() int {
	count := 0
	for _, round := range c.Rounds {
		if round.AnswerArtifactID != "" {
			count++
		}
	}
	return count
}

func (c deepIdeaConversation) pendingRound() (deepIdeaRound, bool) {
	for i := len(c.Rounds) - 1; i >= 0; i-- {
		round := c.Rounds[i]
		if round.QuestionArtifactID != "" && round.AnswerArtifactID == "" {
			return round, true
		}
	}
	return deepIdeaRound{}, false
}

func (c deepIdeaConversation) dispatchHistory() []map[string]any {
	history := make([]map[string]any, 0, len(c.Rounds))
	for _, round := range c.Rounds {
		entry := map[string]any{
			"round":     round.Round,
			"questions": append([]string{}, round.Questions...),
		}
		if round.AnswerArtifactID != "" {
			entry["answer_artifact_id"] = round.AnswerArtifactID
			entry["answer_text"] = round.AnswerText
			entry["answers"] = append([]DeepIdeaAnswer{}, round.Answers...)
			entry["actor_id"] = round.ActorID
		} else {
			entry["pending"] = true
		}
		history = append(history, entry)
	}
	return history
}

func (c deepIdeaConversation) markdown() string {
	var b strings.Builder
	for _, round := range c.Rounds {
		fmt.Fprintf(&b, "### Round %d\n\n", round.Round)
		if len(round.Questions) > 0 {
			b.WriteString("Questions:\n")
			for _, question := range round.Questions {
				fmt.Fprintf(&b, "- %s\n", question)
			}
			b.WriteString("\n")
		}
		if round.AnswerText != "" {
			b.WriteString("Answer summary:\n\n")
			b.WriteString(round.AnswerText)
			b.WriteString("\n\n")
		}
		if len(round.Answers) > 0 {
			b.WriteString("Question-specific answers:\n")
			for _, answer := range round.Answers {
				if answer.Question != "" {
					fmt.Fprintf(&b, "- %s — %s\n", answer.Question, answer.Answer)
				} else {
					fmt.Fprintf(&b, "- %s\n", answer.Answer)
				}
			}
			b.WriteString("\n")
		}
	}
	return strings.TrimSpace(b.String())
}

func (e *Engine) SubmitDeepIdeaAnswers(ctx context.Context, runID, stageID string, submission DeepIdeaAnswersSubmission) (DeepIdeaAnswerReceipt, error) {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return DeepIdeaAnswerReceipt{}, err
	}
	if wr.Run.Status != store.RunStatusAwaitingHuman || contract.NormalizeRefinementLevel(wr.Run.RefinementLevel) != contract.RefinementLevelDeep {
		return DeepIdeaAnswerReceipt{}, ErrDeepIdeaNotAwaiting
	}
	runtime, err := e.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		return DeepIdeaAnswerReceipt{}, err
	}
	runtimeStage, ok := runtimeStageByStageID(runtime, stageID)
	if !ok || runtimeStage.Stage.Status != store.StageStatusRunning || !isIdeaRefinementStage(runtimeStage.Template.Type) {
		return DeepIdeaAnswerReceipt{}, ErrDeepIdeaNotAwaiting
	}
	conversation, err := e.deepIdeaConversation(ctx, runID)
	if err != nil {
		return DeepIdeaAnswerReceipt{}, err
	}
	pending, ok := conversation.pendingRound()
	if !ok || pending.StageID != stageID {
		return DeepIdeaAnswerReceipt{}, ErrDeepIdeaNotAwaiting
	}
	cleaned, err := normalizeDeepIdeaSubmission(submission)
	if err != nil {
		return DeepIdeaAnswerReceipt{}, err
	}
	changed, err := e.store.UpdateRunStatusFrom(ctx, wr.Run.ID, store.RunStatusAwaitingHuman, store.RunStatusRunning)
	if err != nil {
		return DeepIdeaAnswerReceipt{}, err
	}
	if !changed {
		return DeepIdeaAnswerReceipt{}, ErrDeepIdeaNotAwaiting
	}
	packet := deepIdeaAnswerPacket{
		SchemaVersion:      report.SchemaVersion,
		RunID:              wr.Run.ID,
		TaskID:             wr.Task.ID,
		AttemptID:          wr.Attempt.ID,
		StageID:            runtimeStage.Stage.ID,
		StageType:          runtimeStage.Stage.StageType,
		WorkflowStageID:    runtimeStage.Template.ID,
		RefinementLevel:    contract.RefinementLevelDeep,
		Round:              pending.Round,
		ActorID:            cleaned.ActorID,
		Questions:          append([]string{}, pending.Questions...),
		AnswerText:         cleaned.AnswerText,
		Answers:            append([]DeepIdeaAnswer{}, cleaned.Answers...),
		EvidenceRefs:       append([]string{}, cleaned.EvidenceRefs...),
		QuestionArtifactID: pending.QuestionArtifactID,
	}
	content, err := json.MarshalIndent(packet, "", "  ")
	if err != nil {
		_, _ = e.store.UpdateRunStatusFrom(context.Background(), wr.Run.ID, store.RunStatusRunning, store.RunStatusAwaitingHuman)
		return DeepIdeaAnswerReceipt{}, err
	}
	artifact, err := e.store.SaveArtifact(ctx, wr.Run.ID, deepIdeaAnswersArtifactKind, "application/json", content, ".json")
	if err != nil {
		_, _ = e.store.UpdateRunStatusFrom(context.Background(), wr.Run.ID, store.RunStatusRunning, store.RunStatusAwaitingHuman)
		return DeepIdeaAnswerReceipt{}, err
	}
	if _, err := e.emit(ctx, runEvent(wr, "run.resumed", event.Actor{Kind: event.ActorKindUser, ID: cleaned.ActorID}, "run resumed from deep idea refinement answers", map[string]any{
		"status":             store.RunStatusRunning,
		"stage_id":           runtimeStage.Stage.ID,
		"workflow_stage_id":  runtimeStage.Template.ID,
		"question_round":     pending.Round,
		"answer_artifact_id": artifact.ID,
		"refinement_level":   contract.RefinementLevelDeep,
	})); err != nil {
		return DeepIdeaAnswerReceipt{}, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	e.registerActiveRun(wr.Run.ID, cancel)
	go e.executeDeepIdeaResumeAfterAnswers(runCtx, wr.Run.ID, runtimeStage.Stage.ID)
	return DeepIdeaAnswerReceipt{RunID: wr.Run.ID, StageID: runtimeStage.Stage.ID, ArtifactID: artifact.ID, Round: pending.Round}, nil
}

func (e *Engine) executeDeepIdeaResumeAfterAnswers(ctx context.Context, runID, stageID string) {
	e.executeRunWithCleanup(ctx, runID, func() error {
		return e.executeDeepIdeaResumeFromStage(ctx, runID, stageID)
	})
}

func (e *Engine) executeDeepIdeaResumeFromStage(ctx context.Context, runID, stageID string) error {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	if wr.Run.Status != store.RunStatusRunning {
		return nil
	}
	runtime, err := e.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		return err
	}
	runtimeStage, ok := runtimeStageByStageID(runtime, stageID)
	if !ok || runtimeStage.Stage.Status != store.StageStatusRunning || !isIdeaRefinementStage(runtimeStage.Template.Type) {
		return ErrDeepIdeaNotAwaiting
	}
	contractMarkdown, contractArtifact, err := e.taskContractArtifact(ctx, wr)
	if err != nil {
		return err
	}
	briefMarkdown, briefArtifactID, stage, err := e.stageBriefForDeepIdeaResume(ctx, wr, runtimeStage.Stage)
	if err != nil {
		return err
	}
	if _, err := e.runDeepIdeaPlanningStage(ctx, wr, stage, runtime.Template, contractMarkdown, contractArtifact.ID, briefMarkdown, briefArtifactID, false); err != nil {
		return err
	}
	return e.executeRunFrom(ctx, runID, runtimeStage.Template.ID)
}

func (e *Engine) stageBriefForDeepIdeaResume(ctx context.Context, wr store.WorkflowRun, stage store.Stage) (string, string, store.Stage, error) {
	if stage.StageBriefArtifactID != "" {
		_, content, err := e.store.GetArtifact(ctx, stage.StageBriefArtifactID)
		if err != nil {
			return "", "", store.Stage{}, err
		}
		return string(content), stage.StageBriefArtifactID, stage, nil
	}
	markdown, artifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return "", "", store.Stage{}, err
	}
	stage.StageBriefArtifactID = artifact.ID
	return markdown, artifact.ID, stage, nil
}

func normalizeDeepIdeaSubmission(submission DeepIdeaAnswersSubmission) (DeepIdeaAnswersSubmission, error) {
	submission.ActorID = strings.TrimSpace(submission.ActorID)
	if submission.ActorID == "" {
		submission.ActorID = "operator"
	}
	submission.AnswerText = strings.TrimSpace(submission.AnswerText)
	answers := make([]DeepIdeaAnswer, 0, len(submission.Answers))
	for _, answer := range submission.Answers {
		answer.Question = strings.TrimSpace(answer.Question)
		answer.Answer = strings.TrimSpace(answer.Answer)
		if answer.Answer != "" {
			answers = append(answers, answer)
		}
	}
	submission.Answers = answers
	refs := make([]string, 0, len(submission.EvidenceRefs))
	for _, ref := range submission.EvidenceRefs {
		ref = strings.TrimSpace(ref)
		if ref != "" {
			refs = append(refs, ref)
		}
	}
	submission.EvidenceRefs = refs
	if submission.AnswerText == "" && len(submission.Answers) == 0 {
		return DeepIdeaAnswersSubmission{}, fmt.Errorf("%w: answer_text or answers is required", ErrInvalidDeepIdeaAnswer)
	}
	return submission, nil
}

func (e *Engine) deepIdeaConversation(ctx context.Context, runID string) (deepIdeaConversation, error) {
	artifacts, err := e.store.ListArtifacts(ctx, runID)
	if err != nil {
		return deepIdeaConversation{}, err
	}
	rounds := map[int]*deepIdeaRound{}
	for _, artifact := range artifacts {
		if artifact.Kind != deepIdeaQuestionsArtifactKind && artifact.Kind != deepIdeaAnswersArtifactKind {
			continue
		}
		_, content, err := e.store.GetArtifact(ctx, artifact.ID)
		if err != nil {
			return deepIdeaConversation{}, err
		}
		switch artifact.Kind {
		case deepIdeaQuestionsArtifactKind:
			var packet deepIdeaQuestionPacket
			if err := json.Unmarshal(content, &packet); err != nil {
				return deepIdeaConversation{}, fmt.Errorf("decode deep idea questions %s: %w", artifact.ID, err)
			}
			if packet.Round <= 0 {
				continue
			}
			round := rounds[packet.Round]
			if round == nil {
				round = &deepIdeaRound{Round: packet.Round}
				rounds[packet.Round] = round
			}
			round.StageID = packet.StageID
			round.WorkflowStageID = packet.WorkflowStageID
			round.Questions = append([]string{}, packet.Questions...)
			round.QuestionArtifactID = artifact.ID
		case deepIdeaAnswersArtifactKind:
			var packet deepIdeaAnswerPacket
			if err := json.Unmarshal(content, &packet); err != nil {
				return deepIdeaConversation{}, fmt.Errorf("decode deep idea answers %s: %w", artifact.ID, err)
			}
			if packet.Round <= 0 {
				continue
			}
			round := rounds[packet.Round]
			if round == nil {
				round = &deepIdeaRound{Round: packet.Round}
				rounds[packet.Round] = round
			}
			round.StageID = packet.StageID
			round.WorkflowStageID = packet.WorkflowStageID
			round.AnswerArtifactID = artifact.ID
			round.AnswerText = packet.AnswerText
			round.Answers = append([]DeepIdeaAnswer{}, packet.Answers...)
			round.ActorID = packet.ActorID
		}
	}
	ordered := make([]deepIdeaRound, 0, len(rounds))
	for _, round := range rounds {
		ordered = append(ordered, *round)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Round < ordered[j].Round })
	return deepIdeaConversation{Rounds: ordered}, nil
}

func (e *Engine) suspendForDeepIdeaAnswers(ctx context.Context, wr store.WorkflowRun, stage store.Stage, templateStage workflow.StageTemplate, plannerReport report.Report, questions []string, contractArtifactID, briefArtifactID string, round, maxRounds int) error {
	packet := deepIdeaQuestionPacket{
		SchemaVersion:          report.SchemaVersion,
		RunID:                  wr.Run.ID,
		TaskID:                 wr.Task.ID,
		AttemptID:              wr.Attempt.ID,
		StageID:                stage.ID,
		StageType:              stage.StageType,
		WorkflowStageID:        templateStage.ID,
		RefinementLevel:        contract.RefinementLevelDeep,
		Round:                  round,
		MaxRounds:              maxRounds,
		Questions:              append([]string{}, questions...),
		Summary:                strings.TrimSpace(plannerReport.Summary),
		StageBriefArtifactID:   briefArtifactID,
		TaskContractArtifactID: contractArtifactID,
		PlannerReport:          reportForRepairInput(plannerReport),
		SubmissionEndpointHint: fmt.Sprintf("/runs/%s/idea-stages/%s/answers", wr.Run.ID, stage.ID),
	}
	content, err := json.MarshalIndent(packet, "", "  ")
	if err != nil {
		return err
	}
	artifact, err := e.store.SaveArtifact(ctx, wr.Run.ID, deepIdeaQuestionsArtifactKind, "application/json", content, ".json")
	if err != nil {
		return err
	}
	changed, err := e.store.UpdateRunStatusFrom(ctx, wr.Run.ID, store.RunStatusRunning, store.RunStatusAwaitingHuman)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	_, err = e.emit(ctx, stageEvent(wr, stage, "stage.awaiting_human", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "deep idea refinement awaiting answers", map[string]any{
		"status":                    store.RunStatusAwaitingHuman,
		"pending_stage_id":          stage.ID,
		"workflow_stage_id":         templateStage.ID,
		"refinement_level":          contract.RefinementLevelDeep,
		"question_round":            round,
		"max_question_rounds":       maxRounds,
		"questions_artifact_id":     artifact.ID,
		"stage_brief_artifact_id":   briefArtifactID,
		"task_contract_artifact_id": contractArtifactID,
		"runner_slot_released":      true,
		"submission_endpoint_hint":  fmt.Sprintf("/runs/%s/idea-stages/%s/answers", wr.Run.ID, stage.ID),
	}))
	return err
}

func deepPlannerQuestions(payload map[string]any) []string {
	for _, key := range []string{"questions", "clarifying_questions"} {
		if raw, ok := payload[key]; ok {
			questions := stringSlice(raw)
			if len(questions) > 0 {
				return questions
			}
		}
	}
	for _, key := range []string{"question_markdown", "questions_markdown", "prompt"} {
		if text := strings.TrimSpace(payloadString(payload, key)); text != "" {
			return []string{text}
		}
	}
	return nil
}

func stringSlice(raw any) []string {
	switch v := raw.(type) {
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			text := strings.TrimSpace(fmt.Sprint(item))
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return nil
		}
		return []string{text}
	default:
		return nil
	}
}

func isIdeaRefinementStage(stageType string) bool {
	return stageType == workflow.StageTypeIdeaRefinement || stageType == contract.StageTypeIdeaIntake || stageType == contract.StageTypeIdeaRefinement
}

func exhaustedDeepTaskPlan(wr store.WorkflowRun, conversation deepIdeaConversation, requested []string) string {
	var b strings.Builder
	b.WriteString("# Task Plan\n\n")
	fmt.Fprintf(&b, "Project ID: `%s`\n", wr.Run.ProjectID)
	fmt.Fprintf(&b, "Run ID: `%s`\n", wr.Run.ID)
	fmt.Fprintf(&b, "Task ID: `%s`\n", wr.Task.ID)
	fmt.Fprintf(&b, "Attempt ID: `%s`\n", wr.Attempt.ID)
	b.WriteString("Refinement level: `deep`\n\n")
	b.WriteString("## User Idea\n\n")
	b.WriteString(wr.Run.Idea)
	b.WriteString("\n\n## Plan Boundary\n\n")
	b.WriteString("This artifact is a task plan, not a workflow definition. It does not choose, add, remove, or reorder workflow stages.\n\n")
	b.WriteString("## Objective\n\n")
	b.WriteString("Proceed with the submitted idea using the clarifying answers collected during Deep refinement.\n\n")
	b.WriteString("## Clarifying Exchange\n\n")
	if exchange := conversation.markdown(); exchange != "" {
		b.WriteString(exchange)
		b.WriteString("\n\n")
	} else {
		b.WriteString("No completed clarifying answers were recorded before the question budget was exhausted.\n\n")
	}
	b.WriteString("## Repo Evidence Considered\n\n")
	b.WriteString("- Repository evidence should be inspected during implementation using the frozen stage brief and task contract.\n\n")
	b.WriteString("## Implementation Approach\n\n")
	b.WriteString("- Use the answered constraints as scope, inspect the affected code path, make the smallest coherent change, and preserve unrelated behavior.\n\n")
	b.WriteString("## Assumptions\n\n")
	b.WriteString("- Deep refinement reached the configured question-round cap, so any unanswered clarifying requests are treated as non-blocking assumptions.\n")
	for _, question := range requested {
		fmt.Fprintf(&b, "- Unanswered planner question treated as non-blocking: %s\n", question)
	}
	b.WriteString("\n## Open Questions\n\n")
	if len(requested) == 0 {
		b.WriteString("- No additional planner questions were available after the configured Deep refinement budget was exhausted.\n\n")
	} else {
		for _, question := range requested {
			fmt.Fprintf(&b, "- %s\n", question)
		}
		b.WriteString("\n")
	}
	b.WriteString("## Validation\n\n")
	b.WriteString("- Run the narrowest meaningful validation for the implementation path.\n")
	return b.String()
}
