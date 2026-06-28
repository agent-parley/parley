package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func (e *Engine) runMemoryUpdateStage(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, runtimeStage runtimeStage, lastReport report.Report) (report.Report, error) {
	stage := runtimeStage.Stage
	_, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	if err := e.startStage(ctx, wr, stage, stage.StageType+" stage started"); err != nil {
		return report.Report{}, err
	}

	candidates, extractionRejections, err := e.collectProjectMemoryCandidates(ctx, wr, stage.ID)
	if err != nil {
		return report.Report{}, err
	}
	if runtimeStage.Template.Actor == workflow.ActorHuman {
		if err := e.suspendForHumanMemoryApproval(context.Background(), wr, stage, runtimeStage.Template, briefArtifact, candidates, extractionRejections); err != nil {
			return report.Report{}, err
		}
		return report.Report{}, errRunAwaitingHuman
	}
	result, err := e.store.ApplyProjectMemoryUpdate(ctx, store.ProjectMemoryUpdate{
		ProjectID:      wr.Run.ProjectID,
		RunID:          wr.Run.ID,
		TaskID:         wr.Task.ID,
		CuratorStageID: stage.ID,
		Entries:        candidates,
	})
	if err != nil {
		return report.Report{}, err
	}
	result.Rejections = append(extractionRejections, result.Rejections...)

	entryIDs := make([]string, 0, len(result.Entries))
	evidenceRefs := []string{briefArtifact.ID}
	for _, entry := range result.Entries {
		entryIDs = append(entryIDs, entry.ID)
		evidenceRefs = append(evidenceRefs, entry.SourceArtifactID)
	}
	for _, rejection := range result.Rejections {
		if rejection.SourceArtifactID != "" {
			evidenceRefs = append(evidenceRefs, rejection.SourceArtifactID)
		}
	}
	evidenceRefs = uniqueStrings(evidenceRefs)

	summary := fmt.Sprintf("project memory update completed: %d applied, %d rejected", len(result.Entries), len(result.Rejections))
	if len(candidates) == 0 && len(result.Rejections) == 0 {
		summary = "project memory update completed: no candidates"
	}
	rep := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "memory_curator"},
		Status:        report.StatusCompleted,
		Summary:       summary,
		EvidenceRefs:  evidenceRefs,
		Payload: map[string]any{
			"workflow_template_id":       runtime.Template.ID,
			"workflow_stage_id":          runtimeStage.Template.ID,
			"workflow_stage_label":       runtimeStage.Template.Label,
			"workflow_stage_actor":       runtimeStage.Template.Actor,
			"candidate_count":            len(candidates) + len(extractionRejections),
			"applied_count":              len(result.Entries),
			"rejected_count":             len(result.Rejections),
			"project_memory_entry_ids":   entryIDs,
			"project_memory_rejections":  result.Rejections,
			"curator":                    "memory_update_gatekeeper",
			"writes_private_sqlite_only": true,
			"repo_export_performed":      false,
		},
		Errors: []string{},
	}
	if lastReport.StageID != "" {
		rep.Payload["previous_stage_id"] = lastReport.StageID
		rep.Payload["previous_stage_type"] = lastReport.StageType
		rep.Payload["previous_status"] = lastReport.Status
	}
	if err := e.completeStage(context.Background(), wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

type memoryApprovalPacket struct {
	SchemaVersion          int                            `json:"schema_version"`
	RunID                  string                         `json:"run_id"`
	TaskID                 string                         `json:"task_id"`
	AttemptID              string                         `json:"attempt_id"`
	StageID                string                         `json:"stage_id"`
	StageType              string                         `json:"stage_type"`
	WorkflowStageID        string                         `json:"workflow_stage_id"`
	WorkflowStageLabel     string                         `json:"workflow_stage_label"`
	StageBriefArtifactID   string                         `json:"stage_brief_artifact_id"`
	PacketType             string                         `json:"packet_type"`
	InboxSummary           map[string]any                 `json:"inbox_summary"`
	Candidates             []memoryApprovalCandidate      `json:"candidates"`
	ExtractionRejections   []store.ProjectMemoryRejection `json:"extraction_rejections,omitempty"`
	AllowedActions         []string                       `json:"allowed_actions"`
	HumanIsAuthoritative   bool                           `json:"human_is_authoritative"`
	CuratorEnactsDecisions bool                           `json:"curator_enacts_decisions"`
	RepairLoop             bool                           `json:"repair_loop"`
	Timeout                any                            `json:"timeout"`
	SubmissionEndpointHint string                         `json:"submission_endpoint_hint"`
}

type memoryApprovalCandidate struct {
	CandidateID      string `json:"candidate_id"`
	Kind             string `json:"kind"`
	Title            string `json:"title"`
	Body             string `json:"body"`
	SourceStageID    string `json:"source_stage_id"`
	SourceArtifactID string `json:"source_artifact_id"`
	SourceSummary    string `json:"source_summary"`
}

func (e *Engine) suspendForHumanMemoryApproval(ctx context.Context, wr store.WorkflowRun, stage store.Stage, templateStage workflow.StageTemplate, briefArtifact store.Artifact, candidates []store.ProjectMemoryInput, extractionRejections []store.ProjectMemoryRejection) error {
	packetCandidates := memoryApprovalCandidates(candidates)
	packet := memoryApprovalPacket{
		SchemaVersion:        report.SchemaVersion,
		RunID:                wr.Run.ID,
		TaskID:               wr.Task.ID,
		AttemptID:            wr.Attempt.ID,
		StageID:              stage.ID,
		StageType:            stage.StageType,
		WorkflowStageID:      templateStage.ID,
		WorkflowStageLabel:   templateStage.Label,
		StageBriefArtifactID: briefArtifact.ID,
		PacketType:           "memory_approval",
		InboxSummary: map[string]any{
			"candidate_count":            len(packetCandidates),
			"extraction_rejection_count": len(extractionRejections),
			"source_artifact_count":      memoryApprovalSourceArtifactCount(packetCandidates, extractionRejections),
			"writes_private_sqlite_only": true,
			"repo_export_performed":      false,
		},
		Candidates:             packetCandidates,
		ExtractionRejections:   extractionRejections,
		AllowedActions:         []string{store.ProjectMemoryDecisionApprove, store.ProjectMemoryDecisionEdit, store.ProjectMemoryDecisionReject, store.ProjectMemoryDecisionDefer},
		HumanIsAuthoritative:   true,
		CuratorEnactsDecisions: true,
		RepairLoop:             false,
		Timeout:                nil,
		SubmissionEndpointHint: fmt.Sprintf("/runs/%s/human-stages/%s/verdict", wr.Run.ID, stage.ID),
	}
	content, err := json.MarshalIndent(packet, "", "  ")
	if err != nil {
		return err
	}
	artifact, err := e.store.SaveArtifact(ctx, wr.Run.ID, "memory_approval_packet", "application/json", content, ".json")
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
	eventData := map[string]any{
		"status":                     store.RunStatusAwaitingHuman,
		"pending_stage_id":           stage.ID,
		"workflow_stage_id":          templateStage.ID,
		"memory_approval_packet_id":  artifact.ID,
		"stage_brief_artifact_id":    briefArtifact.ID,
		"allowed_actions":            packet.AllowedActions,
		"candidate_count":            len(packetCandidates),
		"extraction_rejection_count": len(extractionRejections),
		"human_is_authoritative":     true,
		"curator_enacts_decisions":   true,
		"runner_slot_released":       true,
		"submission_endpoint_hint":   fmt.Sprintf("/runs/%s/human-stages/%s/verdict", wr.Run.ID, stage.ID),
	}
	_, err = e.emit(ctx, stageEvent(wr, stage, "stage.awaiting_human", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "memory update awaiting human approval", eventData))
	return err
}

func memoryApprovalCandidates(inputs []store.ProjectMemoryInput) []memoryApprovalCandidate {
	out := make([]memoryApprovalCandidate, 0, len(inputs))
	for i, input := range inputs {
		candidateID := strings.TrimSpace(input.CandidateID)
		if candidateID == "" {
			candidateID = fmt.Sprintf("candidate-%d", i+1)
		}
		out = append(out, memoryApprovalCandidate{
			CandidateID:      candidateID,
			Kind:             strings.TrimSpace(input.Kind),
			Title:            strings.TrimSpace(input.Title),
			Body:             strings.TrimSpace(input.Body),
			SourceStageID:    strings.TrimSpace(input.SourceStageID),
			SourceArtifactID: strings.TrimSpace(input.SourceArtifactID),
			SourceSummary:    strings.TrimSpace(input.SourceSummary),
		})
	}
	return out
}

func memoryApprovalSourceArtifactCount(candidates []memoryApprovalCandidate, rejections []store.ProjectMemoryRejection) int {
	seen := map[string]bool{}
	for _, candidate := range candidates {
		if candidate.SourceArtifactID != "" {
			seen[candidate.SourceArtifactID] = true
		}
	}
	for _, rejection := range rejections {
		if rejection.SourceArtifactID != "" {
			seen[rejection.SourceArtifactID] = true
		}
	}
	return len(seen)
}

func (candidate memoryApprovalCandidate) projectMemoryInput() store.ProjectMemoryInput {
	return store.ProjectMemoryInput{
		CandidateID:      candidate.CandidateID,
		Kind:             candidate.Kind,
		Title:            candidate.Title,
		Body:             candidate.Body,
		SourceStageID:    candidate.SourceStageID,
		SourceArtifactID: candidate.SourceArtifactID,
		SourceSummary:    candidate.SourceSummary,
	}
}

func (e *Engine) collectProjectMemoryCandidates(ctx context.Context, wr store.WorkflowRun, memoryStageID string) ([]store.ProjectMemoryInput, []store.ProjectMemoryRejection, error) {
	events, err := e.store.ListEvents(ctx, wr.Run.ID)
	if err != nil {
		return nil, nil, err
	}
	var candidates []store.ProjectMemoryInput
	var rejections []store.ProjectMemoryRejection
	seenArtifacts := map[string]bool{}
	for _, ev := range events {
		if ev.Type != "stage.completed" {
			continue
		}
		artifactID := payloadString(ev.Data, "report_artifact_id")
		if artifactID == "" || seenArtifacts[artifactID] {
			continue
		}
		seenArtifacts[artifactID] = true
		stageType := payloadString(ev.Data, "stage_type")
		if stageType == workflow.StageTypeMemoryUpdate {
			continue
		}
		artifact, content, err := e.store.GetArtifact(ctx, artifactID)
		if err != nil {
			rejections = append(rejections, store.ProjectMemoryRejection{Title: "unreadable memory source", Reason: err.Error(), SourceArtifactID: artifactID})
			continue
		}
		var rep report.Report
		if err := json.Unmarshal(content, &rep); err != nil {
			rejections = append(rejections, store.ProjectMemoryRejection{Title: "invalid memory source report", Reason: err.Error(), SourceArtifactID: artifactID})
			continue
		}
		if rep.StageID == memoryStageID || rep.StageType == workflow.StageTypeMemoryUpdate {
			continue
		}
		if rep.RunID != wr.Run.ID || rep.TaskID != wr.Task.ID {
			continue
		}
		extracted, rejected := projectMemoryCandidatesFromReport(rep, artifact.ID)
		candidates = append(candidates, extracted...)
		rejections = append(rejections, rejected...)
	}
	return candidates, rejections, nil
}

func projectMemoryCandidatesFromReport(rep report.Report, artifactID string) ([]store.ProjectMemoryInput, []store.ProjectMemoryRejection) {
	raw, ok := firstPayloadValue(rep.Payload, "learning_opportunities", "memory_candidates", "project_memory_candidates")
	if !ok || raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, []store.ProjectMemoryRejection{{Title: "invalid memory candidates", Reason: "memory candidates must be a list", SourceStageID: rep.StageID, SourceArtifactID: artifactID}}
	}
	candidates := make([]store.ProjectMemoryInput, 0, len(items))
	rejections := []store.ProjectMemoryRejection{}
	for _, item := range items {
		candidate, err := projectMemoryCandidateFromValue(item, rep, artifactID)
		if err != nil {
			rejections = append(rejections, store.ProjectMemoryRejection{Title: "invalid memory candidate", Reason: err.Error(), SourceStageID: rep.StageID, SourceArtifactID: artifactID})
			continue
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rejections
}

func projectMemoryCandidateFromValue(value any, rep report.Report, artifactID string) (store.ProjectMemoryInput, error) {
	candidate := store.ProjectMemoryInput{SourceStageID: rep.StageID, SourceArtifactID: artifactID, SourceSummary: rep.Summary}
	switch v := value.(type) {
	case string:
		body := strings.TrimSpace(v)
		if body == "" {
			return store.ProjectMemoryInput{}, fmt.Errorf("memory candidate string is empty")
		}
		candidate.Kind = store.ProjectMemoryKindLesson
		candidate.Title = memoryTitleFromBody(body)
		candidate.Body = body
	case map[string]any:
		candidate.Kind = memoryMapString(v, "kind", "category", "type")
		candidate.Title = memoryMapString(v, "title", "name")
		candidate.Body = memoryMapString(v, "body", "content", "lesson", "summary")
		if source := memoryMapString(v, "source_summary", "source", "rationale"); source != "" {
			candidate.SourceSummary = source
		}
		if candidate.Title == "" && candidate.Body != "" {
			candidate.Title = memoryTitleFromBody(candidate.Body)
		}
	case map[string]string:
		candidate.Kind = firstStringMapString(v, "kind", "category", "type")
		candidate.Title = firstStringMapString(v, "title", "name")
		candidate.Body = firstStringMapString(v, "body", "content", "lesson", "summary")
		if source := firstStringMapString(v, "source_summary", "source", "rationale"); source != "" {
			candidate.SourceSummary = source
		}
		if candidate.Title == "" && candidate.Body != "" {
			candidate.Title = memoryTitleFromBody(candidate.Body)
		}
	default:
		return store.ProjectMemoryInput{}, fmt.Errorf("memory candidate must be an object or string")
	}
	return candidate, nil
}

func firstPayloadValue(payload map[string]any, keys ...string) (any, bool) {
	if payload == nil {
		return nil, false
	}
	for _, key := range keys {
		value, ok := payload[key]
		if ok {
			return value, true
		}
	}
	return nil, false
}

func memoryMapString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key]; ok && value != nil {
			text := strings.TrimSpace(fmt.Sprint(value))
			if text != "" {
				return text
			}
		}
	}
	return ""
}

func firstStringMapString(values map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func memoryTitleFromBody(body string) string {
	body = strings.TrimSpace(strings.ReplaceAll(body, "\n", " "))
	if body == "" {
		return "untitled memory candidate"
	}
	if i := strings.IndexAny(body, ".:;—-"); i > 0 && i < 80 {
		body = body[:i]
	}
	return truncateRunes(body, 80)
}

func truncateRunes(value string, max int) string {
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max])
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
