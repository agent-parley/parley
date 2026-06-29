package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

type projectMemoryInboxCandidate struct {
	ID    string
	Input store.ProjectMemoryInput
}

type projectMemoryApplyDecision struct {
	State              report.MemoryCandidateState
	Decision           report.MemoryCandidateDecision
	Input              store.ProjectMemoryInput
	CandidateIDs       []string
	SourceArtifactRefs []string
}

func (e *Engine) runMemoryUpdateStage(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, runtimeStage runtimeStage, lastReport report.Report) (report.Report, error) {
	if runtimeStage.Template.Actor == workflow.ActorHuman {
		return e.runHumanMemoryUpdateStage(ctx, wr, runtime, runtimeStage, lastReport)
	}
	if runtimeStage.Template.Actor == workflow.ActorAgent {
		return e.runAgentMemoryUpdateStage(ctx, wr, runtime, runtimeStage, lastReport)
	}
	return e.runHarnessMemoryUpdateStage(ctx, wr, runtime, runtimeStage, lastReport)
}

// runHumanMemoryUpdateStage suspends the run for human approval of the project
// memory candidates instead of writing them. The operator's approve/edit/reject/
// defer decisions are enacted later via SubmitHumanReview.
func (e *Engine) runHumanMemoryUpdateStage(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, runtimeStage runtimeStage, lastReport report.Report) (report.Report, error) {
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
	if err := e.suspendForHumanMemoryApproval(context.Background(), wr, stage, runtimeStage.Template, briefArtifact, candidates, extractionRejections); err != nil {
		return report.Report{}, err
	}
	return report.Report{}, errRunAwaitingHuman
}

func (e *Engine) runAgentMemoryUpdateStage(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, runtimeStage runtimeStage, lastReport report.Report) (report.Report, error) {
	stage := runtimeStage.Stage
	briefMarkdown, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	e.setActiveStage(wr.Run.ID, stage)
	defer e.clearActiveStage(wr.Run.ID, stage.ID)
	if err := e.startStage(ctx, wr, stage, stage.StageType+" stage started"); err != nil {
		return report.Report{}, err
	}

	candidates, extractionRejections, err := e.collectProjectMemoryCandidateInbox(ctx, wr, stage.ID)
	if err != nil {
		return report.Report{}, err
	}
	existingEntries, err := e.store.ListProjectMemoryEntries(ctx, wr.Run.ProjectID)
	if err != nil {
		return report.Report{}, err
	}

	input := e.stageDispatchInput(runtime, runtimeStage.Template, map[string]any{
		"memory_update_mode":              "agent_curator",
		"memory_update_policy":            projectMemoryPolicyPayload(),
		"project_memory_inbox":            projectMemoryInboxPayload(candidates, extractionRejections),
		"project_memory_existing_entries": projectMemoryEntriesPayload(existingEntries),
	})
	if lastReport.StageID != "" {
		input["previous_stage_id"] = lastReport.StageID
		input["previous_stage_type"] = lastReport.StageType
		input["previous_status"] = lastReport.Status
	}
	input = withStageBriefInput(input, briefMarkdown, briefArtifact.ID)
	adapterName := stage.Adapter
	if strings.TrimSpace(adapterName) == "" {
		adapterName = e.implementationAdapter
	}
	disp := contract.Dispatch{
		ProjectID:    wr.Run.ProjectID,
		RepositoryID: wr.Task.RepositoryID,
		RunID:        wr.Run.ID,
		TaskID:       wr.Task.ID,
		AttemptID:    wr.Attempt.ID,
		StageID:      stage.ID,
		StageType:    stage.StageType,
		Adapter:      adapterName,
		Input:        input,
	}
	curatorReport, err := e.dispatchWithReportRepair(ctx, wr, stage, disp, reportRepairOptions{
		AdapterName:   adapterName,
		StageType:     stage.StageType,
		EmitLifecycle: adapterName != "",
		LifecycleData: map[string]any{"adapter": adapterName, "curator": "memory_update_agent"},
		Validator:     memoryCuratorReportValidator(wr, stage, stage.StageType, adapterName),
	})
	if err != nil {
		return report.Report{}, err
	}
	if curatorReport.Status != report.StatusCompleted {
		if err := e.completeStage(context.Background(), wr, stage, curatorReport); err != nil {
			return report.Report{}, err
		}
		return curatorReport, nil
	}
	proposal, err := report.MemoryUpdateOutputFromPayload(curatorReport.Payload)
	if err != nil {
		invalid := invalidAdapterReport(wr, stage, adapterName, "memory curator returned invalid structured output", curatorReport, err)
		if err := e.completeStage(context.Background(), wr, stage, invalid); err != nil {
			return report.Report{}, err
		}
		return invalid, nil
	}
	result, output, err := e.applyMemoryCuratorProposal(ctx, wr, stage, candidates, extractionRejections, proposal, curatorReport.Actor)
	if err != nil {
		return report.Report{}, err
	}

	finalReport := curatorReport
	finalReport.Summary = output.StopReportSummary
	finalReport.EvidenceRefs = memoryUpdateEvidenceRefs(briefArtifact.ID, output)
	finalReport.Payload = memoryUpdatePayload(runtime, runtimeStage, output, result, "agent_memory_curator")
	if lastReport.StageID != "" {
		finalReport.Payload["previous_stage_id"] = lastReport.StageID
		finalReport.Payload["previous_stage_type"] = lastReport.StageType
		finalReport.Payload["previous_status"] = lastReport.Status
	}
	if err := e.completeStage(context.Background(), wr, stage, finalReport); err != nil {
		return report.Report{}, err
	}
	return finalReport, nil
}

func (e *Engine) runHarnessMemoryUpdateStage(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, runtimeStage runtimeStage, lastReport report.Report) (report.Report, error) {
	stage := runtimeStage.Stage
	_, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	if err := e.startStage(ctx, wr, stage, stage.StageType+" stage started"); err != nil {
		return report.Report{}, err
	}

	inbox, extractionRejections, err := e.collectProjectMemoryCandidateInbox(ctx, wr, stage.ID)
	if err != nil {
		return report.Report{}, err
	}
	candidates := make([]store.ProjectMemoryInput, 0, len(inbox))
	for _, candidate := range inbox {
		candidates = append(candidates, candidate.Input)
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
	output := harnessMemoryUpdateOutput(wr, inbox, extractionRejections, result)
	result.Rejections = append(extractionRejections, result.Rejections...)
	payload := memoryUpdatePayload(runtime, runtimeStage, output, result, "memory_update_gatekeeper")
	if lastReport.StageID != "" {
		payload["previous_stage_id"] = lastReport.StageID
		payload["previous_stage_type"] = lastReport.StageType
		payload["previous_status"] = lastReport.Status
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
		Summary:       output.StopReportSummary,
		EvidenceRefs:  memoryUpdateEvidenceRefs(briefArtifact.ID, output),
		Payload:       payload,
		Errors:        []string{},
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

func memoryCuratorReportValidator(wr store.WorkflowRun, stage store.Stage, stageType, adapterName string) reportRepairValidator {
	base := baseReportValidator(wr, stage, stageType, adapterName)
	return func(rep report.Report) (report.Report, error) {
		stamped, err := base(rep)
		if err != nil {
			return stamped, err
		}
		if stamped.Status != report.StatusCompleted {
			return stamped, nil
		}
		if _, err := report.MemoryUpdateOutputFromPayload(stamped.Payload); err != nil {
			return invalidAdapterReport(wr, stage, adapterName, "memory curator returned invalid structured output", stamped, err), err
		}
		return stamped, nil
	}
}

func (e *Engine) applyMemoryCuratorProposal(ctx context.Context, wr store.WorkflowRun, stage store.Stage, inbox []projectMemoryInboxCandidate, extractionRejections []store.ProjectMemoryRejection, proposal report.MemoryUpdateOutput, actor report.Actor) (store.ProjectMemoryUpdateResult, report.MemoryUpdateOutput, error) {
	actions, preflightRejected := projectMemoryApplyDecisions(wr, inbox, proposal)
	entries := make([]store.ProjectMemoryInput, 0, len(actions))
	for _, action := range actions {
		entries = append(entries, action.Input)
	}
	result, err := e.store.ApplyProjectMemoryUpdate(ctx, store.ProjectMemoryUpdate{
		ProjectID:      wr.Run.ProjectID,
		RunID:          wr.Run.ID,
		TaskID:         wr.Task.ID,
		CuratorStageID: stage.ID,
		Entries:        entries,
	})
	if err != nil {
		return store.ProjectMemoryUpdateResult{}, report.MemoryUpdateOutput{}, err
	}
	output := baseMemoryUpdateOutput(wr, inbox, extractionRejections, actor, "agent curator approved automatically and store guardrails applied the approved writes")
	output.SafetyNotes = append(output.SafetyNotes, proposal.SafetyNotes...)
	output.Rejected = append(output.Rejected, rejectedDecisionsFromExtraction(wr, extractionRejections)...)
	output.Rejected = append(output.Rejected, normalizeMemoryDecisions(wr, inbox, proposal.Rejected, report.MemoryCandidateRejected)...)
	output.Rejected = append(output.Rejected, preflightRejected...)
	output.Deferred = append(output.Deferred, normalizeMemoryDecisions(wr, inbox, proposal.Deferred, report.MemoryCandidateDeferred)...)
	omitted := omittedMemoryCandidateDecisions(wr, inbox, proposal)
	if len(omitted) > 0 {
		output.Deferred = append(output.Deferred, omitted...)
		output.SafetyNotes = append(output.SafetyNotes, fmt.Sprintf("curator omitted %d candidate(s); recorded as deferred without writing project memory", len(omitted)))
	}

	for i, action := range actions {
		if i >= len(result.Outcomes) {
			decision := action.Decision
			decision.State = report.MemoryCandidateRejected
			decision.Rationale = appendRationale(decision.Rationale, "memory guardrail outcome was not returned")
			decision.SourceArtifactRefs = action.SourceArtifactRefs
			decision.Freshness = freshnessFromRefs(wr, action.Input.SourceStageID, action.SourceArtifactRefs, "")
			output.Rejected = append(output.Rejected, decision)
			continue
		}
		outcome := result.Outcomes[i]
		if outcome.Entry != nil {
			decision := appliedMemoryDecision(wr, action, *outcome.Entry)
			switch action.State {
			case report.MemoryCandidateEdited:
				output.Edited = append(output.Edited, decision)
			case report.MemoryCandidateMerged:
				output.Merged = append(output.Merged, decision)
			default:
				output.Applied = append(output.Applied, decision)
			}
			output.MemoryChanges = append(output.MemoryChanges, memoryChangeFromEntry(wr, action, *outcome.Entry))
			continue
		}
		if outcome.Rejection != nil {
			output.Rejected = append(output.Rejected, rejectedDecisionFromGuardrail(wr, action, *outcome.Rejection))
			continue
		}
	}
	output.InboxSummary.CandidatesCurated = countMemoryCuratedCandidates(output)
	output.StopReportSummary = memoryUpdateSummary(output)
	storeRejectionCount := len(result.Rejections)
	if storeRejectionCount > 0 {
		output.SafetyNotes = append(output.SafetyNotes, fmt.Sprintf("store guardrails rejected %d curator-approved candidate(s)", storeRejectionCount))
	}
	result.Rejections = append(extractionRejections, result.Rejections...)
	return result, output, nil
}

func (e *Engine) collectProjectMemoryCandidates(ctx context.Context, wr store.WorkflowRun, memoryStageID string) ([]store.ProjectMemoryInput, []store.ProjectMemoryRejection, error) {
	inbox, rejections, err := e.collectProjectMemoryCandidateInbox(ctx, wr, memoryStageID)
	if err != nil {
		return nil, nil, err
	}
	candidates := make([]store.ProjectMemoryInput, 0, len(inbox))
	for _, candidate := range inbox {
		candidates = append(candidates, candidate.Input)
	}
	return candidates, rejections, nil
}

func (e *Engine) collectProjectMemoryCandidateInbox(ctx context.Context, wr store.WorkflowRun, memoryStageID string) ([]projectMemoryInboxCandidate, []store.ProjectMemoryRejection, error) {
	events, err := e.store.ListEvents(ctx, wr.Run.ID)
	if err != nil {
		return nil, nil, err
	}
	var candidates []projectMemoryInboxCandidate
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
		for _, candidate := range extracted {
			candidates = append(candidates, projectMemoryInboxCandidate{ID: fmt.Sprintf("candidate-%03d", len(candidates)+1), Input: candidate})
		}
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

func projectMemoryApplyDecisions(wr store.WorkflowRun, inbox []projectMemoryInboxCandidate, proposal report.MemoryUpdateOutput) ([]projectMemoryApplyDecision, []report.MemoryCandidateDecision) {
	byID := map[string]projectMemoryInboxCandidate{}
	for _, candidate := range inbox {
		byID[candidate.ID] = candidate
	}
	var actions []projectMemoryApplyDecision
	var rejected []report.MemoryCandidateDecision
	for _, item := range []struct {
		state     report.MemoryCandidateState
		decisions []report.MemoryCandidateDecision
	}{
		{report.MemoryCandidateApplied, proposal.Applied},
		{report.MemoryCandidateEdited, proposal.Edited},
		{report.MemoryCandidateMerged, proposal.Merged},
	} {
		for _, decision := range item.decisions {
			decision.State = item.state
			ids := memoryDecisionCandidateIDs(decision)
			known := make([]projectMemoryInboxCandidate, 0, len(ids))
			missing := []string{}
			for _, id := range ids {
				candidate, ok := byID[id]
				if !ok {
					missing = append(missing, id)
					continue
				}
				known = append(known, candidate)
			}
			if len(known) == 0 || len(missing) > 0 {
				decision.State = report.MemoryCandidateRejected
				decision.Body = ""
				reason := "curator decision did not reference a known candidate id"
				if len(missing) > 0 {
					reason = "curator decision references unknown candidate id(s): " + strings.Join(missing, ", ")
				}
				decision.Rationale = appendRationale(decision.Rationale, reason)
				sourceRefs := append([]string{}, decision.SourceArtifactRefs...)
				stageID := ""
				for _, candidate := range known {
					sourceRefs = append(sourceRefs, candidate.Input.SourceArtifactID)
					if stageID == "" {
						stageID = candidate.Input.SourceStageID
					}
				}
				decision.SourceArtifactRefs = uniqueStrings(sourceRefs)
				decision.Freshness = freshnessFromRefs(wr, stageID, decision.SourceArtifactRefs, "")
				rejected = append(rejected, decision)
				continue
			}
			primary := known[0].Input
			input := store.ProjectMemoryInput{
				Kind:             firstNonEmpty(decision.Kind, primary.Kind),
				Title:            firstNonEmpty(decision.Title, primary.Title),
				Body:             firstNonEmpty(decision.Body, primary.Body),
				SourceStageID:    primary.SourceStageID,
				SourceArtifactID: primary.SourceArtifactID,
				SourceSummary:    firstNonEmpty(decision.Rationale, primary.SourceSummary),
			}
			sourceRefs := append([]string{}, decision.SourceArtifactRefs...)
			for _, candidate := range known {
				sourceRefs = append(sourceRefs, candidate.Input.SourceArtifactID)
			}
			sourceRefs = uniqueStrings(sourceRefs)
			actions = append(actions, projectMemoryApplyDecision{State: item.state, Decision: decision, Input: input, CandidateIDs: ids, SourceArtifactRefs: sourceRefs})
		}
	}
	return actions, rejected
}

func omittedMemoryCandidateDecisions(wr store.WorkflowRun, inbox []projectMemoryInboxCandidate, proposal report.MemoryUpdateOutput) []report.MemoryCandidateDecision {
	referenced := map[string]bool{}
	for _, decisions := range [][]report.MemoryCandidateDecision{proposal.Applied, proposal.Rejected, proposal.Edited, proposal.Merged, proposal.Deferred} {
		for _, decision := range decisions {
			for _, id := range memoryDecisionCandidateIDs(decision) {
				referenced[id] = true
			}
		}
	}
	out := make([]report.MemoryCandidateDecision, 0)
	for _, candidate := range inbox {
		if referenced[candidate.ID] {
			continue
		}
		refs := uniqueStrings([]string{candidate.Input.SourceArtifactID})
		out = append(out, report.MemoryCandidateDecision{
			CandidateID:        candidate.ID,
			CandidateIDs:       []string{candidate.ID},
			State:              report.MemoryCandidateDeferred,
			Kind:               candidate.Input.Kind,
			Title:              candidate.Input.Title,
			Rationale:          "curator omitted candidate from memory_update_output; recorded as deferred without writing project memory",
			SourceArtifactRefs: refs,
			Freshness:          freshnessFromRefs(wr, candidate.Input.SourceStageID, refs, ""),
		})
	}
	return out
}

func harnessMemoryUpdateOutput(wr store.WorkflowRun, inbox []projectMemoryInboxCandidate, extractionRejections []store.ProjectMemoryRejection, result store.ProjectMemoryUpdateResult) report.MemoryUpdateOutput {
	output := baseMemoryUpdateOutput(wr, inbox, extractionRejections, report.Actor{Kind: report.ActorKindHarness, ID: "memory_curator"}, "harness gatekeeper applied deterministic curation and store guardrails")
	output.Rejected = append(output.Rejected, rejectedDecisionsFromExtraction(wr, extractionRejections)...)
	for i, candidate := range inbox {
		decision := report.MemoryCandidateDecision{
			CandidateID:        candidate.ID,
			CandidateIDs:       []string{candidate.ID},
			State:              report.MemoryCandidateApplied,
			Kind:               candidate.Input.Kind,
			Title:              candidate.Input.Title,
			Body:               candidate.Input.Body,
			Rationale:          "deterministic memory gatekeeper accepted candidate",
			SourceArtifactRefs: []string{candidate.Input.SourceArtifactID},
			Freshness:          freshnessFromRefs(wr, candidate.Input.SourceStageID, []string{candidate.Input.SourceArtifactID}, ""),
		}
		if i < len(result.Outcomes) && result.Outcomes[i].Entry != nil {
			decision = appliedMemoryDecision(wr, projectMemoryApplyDecision{State: report.MemoryCandidateApplied, Decision: decision, Input: candidate.Input, CandidateIDs: []string{candidate.ID}, SourceArtifactRefs: []string{candidate.Input.SourceArtifactID}}, *result.Outcomes[i].Entry)
			output.Applied = append(output.Applied, decision)
			output.MemoryChanges = append(output.MemoryChanges, memoryChangeFromEntry(wr, projectMemoryApplyDecision{State: report.MemoryCandidateApplied, Decision: decision, Input: candidate.Input, CandidateIDs: []string{candidate.ID}, SourceArtifactRefs: []string{candidate.Input.SourceArtifactID}}, *result.Outcomes[i].Entry))
			continue
		}
		if i < len(result.Outcomes) && result.Outcomes[i].Rejection != nil {
			output.Rejected = append(output.Rejected, rejectedDecisionFromGuardrail(wr, projectMemoryApplyDecision{State: report.MemoryCandidateApplied, Decision: decision, Input: candidate.Input, CandidateIDs: []string{candidate.ID}, SourceArtifactRefs: []string{candidate.Input.SourceArtifactID}}, *result.Outcomes[i].Rejection))
		}
	}
	output.InboxSummary.CandidatesCurated = countMemoryCuratedCandidates(output)
	output.StopReportSummary = memoryUpdateSummary(output)
	return output
}

func baseMemoryUpdateOutput(wr store.WorkflowRun, inbox []projectMemoryInboxCandidate, extractionRejections []store.ProjectMemoryRejection, actor report.Actor, authority string) report.MemoryUpdateOutput {
	if actor.Kind == "" {
		actor.Kind = report.ActorKindHarness
	}
	if actor.ID == "" {
		actor.ID = "memory_curator"
	}
	sourceRefs := memoryInboxSourceRefs(inbox, extractionRejections)
	return report.MemoryUpdateOutput{
		InboxSummary: report.MemoryInboxSummary{
			LearningOpportunities: len(inbox) + len(extractionRejections),
			CandidatesGenerated:   len(inbox) + len(extractionRejections),
			CandidatesCurated:     0,
			SourceArtifactRefs:    sourceRefs,
		},
		Applied:       []report.MemoryCandidateDecision{},
		Rejected:      []report.MemoryCandidateDecision{},
		Edited:        []report.MemoryCandidateDecision{},
		Merged:        []report.MemoryCandidateDecision{},
		Deferred:      []report.MemoryCandidateDecision{},
		MemoryChanges: []report.MemoryChange{},
		ActorAuthority: report.MemoryActorAuthority{
			Kind:      actor.Kind,
			ID:        actor.ID,
			Authority: authority,
		},
		SafetyNotes:       []string{},
		StopReportSummary: "project memory update completed",
	}
}

func memoryUpdatePayload(runtime runtimeWorkflow, runtimeStage runtimeStage, output report.MemoryUpdateOutput, result store.ProjectMemoryUpdateResult, curator string) map[string]any {
	entryIDs := make([]string, 0, len(result.Entries))
	for _, entry := range result.Entries {
		entryIDs = append(entryIDs, entry.ID)
	}
	return map[string]any{
		"workflow_template_id":              runtime.Template.ID,
		"workflow_stage_id":                 runtimeStage.Template.ID,
		"workflow_stage_label":              runtimeStage.Template.Label,
		"workflow_stage_actor":              runtimeStage.Template.Actor,
		"candidate_count":                   output.InboxSummary.CandidatesGenerated,
		"applied_count":                     len(result.Entries),
		"rejected_count":                    len(output.Rejected),
		"edited_count":                      len(output.Edited),
		"merged_count":                      len(output.Merged),
		"deferred_count":                    len(output.Deferred),
		"project_memory_entry_ids":          uniqueStrings(entryIDs),
		"project_memory_rejections":         result.Rejections,
		"curator":                           curator,
		"writes_private_sqlite_only":        true,
		"repo_export_performed":             false,
		report.MemoryUpdateOutputPayloadKey: output,
	}
}

func projectMemoryInboxPayload(candidates []projectMemoryInboxCandidate, rejections []store.ProjectMemoryRejection) map[string]any {
	items := make([]map[string]any, 0, len(candidates))
	for _, candidate := range candidates {
		items = append(items, map[string]any{
			"candidate_id":       candidate.ID,
			"kind":               candidate.Input.Kind,
			"title":              candidate.Input.Title,
			"body":               candidate.Input.Body,
			"source_stage_id":    candidate.Input.SourceStageID,
			"source_artifact_id": candidate.Input.SourceArtifactID,
			"source_summary":     candidate.Input.SourceSummary,
		})
	}
	rejected := make([]store.ProjectMemoryRejection, len(rejections))
	copy(rejected, rejections)
	return map[string]any{
		"candidates":            items,
		"extraction_rejections": rejected,
		"candidate_count":       len(candidates) + len(rejections),
	}
}

func projectMemoryEntriesPayload(entries []store.ProjectMemoryEntry) []map[string]any {
	out := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		out = append(out, map[string]any{
			"id":                 entry.ID,
			"kind":               entry.Kind,
			"title":              entry.Title,
			"body":               entry.Body,
			"source_run_id":      entry.SourceRunID,
			"source_task_id":     entry.SourceTaskID,
			"source_stage_id":    entry.SourceStageID,
			"source_artifact_id": entry.SourceArtifactID,
			"curator_stage_id":   entry.CuratorStageID,
			"source_summary":     entry.SourceSummary,
			"created_at":         entry.CreatedAt,
			"updated_at":         entry.UpdatedAt,
		})
	}
	return out
}

func projectMemoryPolicyPayload() map[string]any {
	return map[string]any{
		"candidate_kinds": projectMemoryCandidateKinds(),
		"allowed_states": []string{
			string(report.MemoryCandidateApplied),
			string(report.MemoryCandidateRejected),
			string(report.MemoryCandidateEdited),
			string(report.MemoryCandidateMerged),
			string(report.MemoryCandidateDeferred),
		},
		"max_applied_entries": store.ProjectMemoryMaxEntriesPerUpdate,
		"source_linking":      "every applied, edited, or merged decision must reference candidate_id or candidate_ids from project_memory_inbox.candidates; the manager chooses the source stage/artifact from those candidates",
		"durable_writes":      "do not write project memory directly; return memory_update_output and the manager writes approved entries through deterministic guardrails",
		"forbidden_content":   []string{"secrets or credentials", "standing instructions", "raw logs or transcripts", "speculative plans", "current code truth"},
	}
}

func appliedMemoryDecision(wr store.WorkflowRun, action projectMemoryApplyDecision, entry store.ProjectMemoryEntry) report.MemoryCandidateDecision {
	decision := action.Decision
	decision.State = action.State
	decision.CandidateIDs = action.CandidateIDs
	if len(action.CandidateIDs) == 1 {
		decision.CandidateID = action.CandidateIDs[0]
	}
	decision.Kind = entry.Kind
	decision.Title = entry.Title
	decision.Body = entry.Body
	decision.EntryID = entry.ID
	decision.EntryIDs = uniqueStrings(append(decision.EntryIDs, entry.ID))
	decision.SourceArtifactRefs = uniqueStrings(action.SourceArtifactRefs)
	decision.Freshness = report.MemoryFreshness{
		SourceRunID:        entry.SourceRunID,
		SourceTaskID:       entry.SourceTaskID,
		SourceStageID:      entry.SourceStageID,
		SourceArtifactRefs: decision.SourceArtifactRefs,
		VerifiedAt:         entry.UpdatedAt,
		UpdatedAt:          entry.UpdatedAt,
	}
	return decision
}

func memoryChangeFromEntry(wr store.WorkflowRun, action projectMemoryApplyDecision, entry store.ProjectMemoryEntry) report.MemoryChange {
	refs := uniqueStrings(action.SourceArtifactRefs)
	if len(refs) == 0 {
		refs = uniqueStrings([]string{entry.SourceArtifactID})
	}
	return report.MemoryChange{
		Action:             report.MemoryChangeApplied,
		EntryID:            entry.ID,
		CandidateIDs:       action.CandidateIDs,
		Kind:               entry.Kind,
		Title:              entry.Title,
		SourceArtifactRefs: refs,
		Freshness: report.MemoryFreshness{
			SourceRunID:        entry.SourceRunID,
			SourceTaskID:       entry.SourceTaskID,
			SourceStageID:      entry.SourceStageID,
			SourceArtifactRefs: refs,
			VerifiedAt:         entry.UpdatedAt,
			UpdatedAt:          entry.UpdatedAt,
		},
	}
}

func rejectedDecisionFromGuardrail(wr store.WorkflowRun, action projectMemoryApplyDecision, rejection store.ProjectMemoryRejection) report.MemoryCandidateDecision {
	return report.MemoryCandidateDecision{
		CandidateID:        firstCandidateID(action.CandidateIDs),
		CandidateIDs:       action.CandidateIDs,
		State:              report.MemoryCandidateRejected,
		Kind:               action.Input.Kind,
		Title:              firstNonEmpty(rejection.Title, action.Input.Title),
		Rationale:          "deterministic guardrail rejected curator-approved candidate: " + rejection.Reason,
		SourceArtifactRefs: uniqueStrings(action.SourceArtifactRefs),
		Freshness:          freshnessFromRefs(wr, action.Input.SourceStageID, action.SourceArtifactRefs, ""),
	}
}

func rejectedDecisionsFromExtraction(wr store.WorkflowRun, rejections []store.ProjectMemoryRejection) []report.MemoryCandidateDecision {
	out := make([]report.MemoryCandidateDecision, 0, len(rejections))
	for i, rejection := range rejections {
		refs := []string{}
		if rejection.SourceArtifactID != "" {
			refs = append(refs, rejection.SourceArtifactID)
		}
		out = append(out, report.MemoryCandidateDecision{
			CandidateID:        fmt.Sprintf("extraction-rejection-%03d", i+1),
			CandidateIDs:       []string{fmt.Sprintf("extraction-rejection-%03d", i+1)},
			State:              report.MemoryCandidateRejected,
			Title:              rejection.Title,
			Rationale:          "candidate extraction rejected source payload: " + rejection.Reason,
			SourceArtifactRefs: refs,
			Freshness:          freshnessFromRefs(wr, rejection.SourceStageID, refs, ""),
		})
	}
	return out
}

func normalizeMemoryDecisions(wr store.WorkflowRun, inbox []projectMemoryInboxCandidate, decisions []report.MemoryCandidateDecision, state report.MemoryCandidateState) []report.MemoryCandidateDecision {
	byID := map[string]projectMemoryInboxCandidate{}
	for _, candidate := range inbox {
		byID[candidate.ID] = candidate
	}
	out := make([]report.MemoryCandidateDecision, 0, len(decisions))
	for _, decision := range decisions {
		decision.State = state
		ids := memoryDecisionCandidateIDs(decision)
		decision.CandidateIDs = ids
		if len(ids) == 1 {
			decision.CandidateID = ids[0]
		}
		refs := append([]string{}, decision.SourceArtifactRefs...)
		stageID := ""
		for _, id := range ids {
			candidate, ok := byID[id]
			if !ok {
				continue
			}
			refs = append(refs, candidate.Input.SourceArtifactID)
			if stageID == "" {
				stageID = candidate.Input.SourceStageID
			}
			if decision.Title == "" {
				decision.Title = candidate.Input.Title
			}
			if decision.Kind == "" {
				decision.Kind = candidate.Input.Kind
			}
		}
		decision.SourceArtifactRefs = uniqueStrings(refs)
		if state == report.MemoryCandidateRejected || state == report.MemoryCandidateDeferred {
			decision.Body = ""
		}
		decision.Freshness = freshnessFromRefs(wr, stageID, decision.SourceArtifactRefs, "")
		out = append(out, decision)
	}
	return out
}

func memoryDecisionCandidateIDs(decision report.MemoryCandidateDecision) []string {
	ids := append([]string{}, decision.CandidateIDs...)
	if decision.CandidateID != "" {
		ids = append(ids, decision.CandidateID)
	}
	return uniqueStrings(ids)
}

func firstCandidateID(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
}

func memoryInboxSourceRefs(inbox []projectMemoryInboxCandidate, rejections []store.ProjectMemoryRejection) []string {
	refs := []string{}
	for _, candidate := range inbox {
		refs = append(refs, candidate.Input.SourceArtifactID)
	}
	for _, rejection := range rejections {
		refs = append(refs, rejection.SourceArtifactID)
	}
	return uniqueStrings(refs)
}

func memoryUpdateEvidenceRefs(briefArtifactID string, output report.MemoryUpdateOutput) []string {
	refs := []string{briefArtifactID}
	refs = append(refs, output.InboxSummary.SourceArtifactRefs...)
	for _, decision := range append(append(append(append(output.Applied, output.Edited...), output.Merged...), output.Rejected...), output.Deferred...) {
		refs = append(refs, decision.SourceArtifactRefs...)
	}
	for _, change := range output.MemoryChanges {
		refs = append(refs, change.SourceArtifactRefs...)
	}
	return uniqueStrings(refs)
}

func freshnessFromRefs(wr store.WorkflowRun, sourceStageID string, refs []string, updatedAt string) report.MemoryFreshness {
	return report.MemoryFreshness{
		SourceRunID:        wr.Run.ID,
		SourceTaskID:       wr.Task.ID,
		SourceStageID:      sourceStageID,
		SourceArtifactRefs: uniqueStrings(refs),
		VerifiedAt:         updatedAt,
		UpdatedAt:          updatedAt,
	}
}

func countMemoryCuratedCandidates(output report.MemoryUpdateOutput) int {
	seen := map[string]bool{}
	count := 0
	for _, decisions := range [][]report.MemoryCandidateDecision{output.Applied, output.Rejected, output.Edited, output.Merged, output.Deferred} {
		for _, decision := range decisions {
			ids := memoryDecisionCandidateIDs(decision)
			if len(ids) == 0 {
				count++
				continue
			}
			for _, id := range ids {
				if seen[id] {
					continue
				}
				seen[id] = true
				count++
			}
		}
	}
	return count
}

func memoryUpdateSummary(output report.MemoryUpdateOutput) string {
	applied := len(output.Applied) + len(output.Edited) + len(output.Merged)
	if output.InboxSummary.CandidatesGenerated == 0 && applied == 0 && len(output.Rejected) == 0 && len(output.Deferred) == 0 {
		return "project memory update completed: no candidates"
	}
	parts := []string{fmt.Sprintf("%d applied", applied)}
	if len(output.Edited) > 0 {
		parts = append(parts, fmt.Sprintf("%d edited", len(output.Edited)))
	}
	if len(output.Merged) > 0 {
		parts = append(parts, fmt.Sprintf("%d merged", len(output.Merged)))
	}
	parts = append(parts, fmt.Sprintf("%d rejected", len(output.Rejected)))
	if len(output.Deferred) > 0 {
		parts = append(parts, fmt.Sprintf("%d deferred", len(output.Deferred)))
	}
	return "project memory update completed: " + strings.Join(parts, ", ")
}

func appendRationale(existing, addition string) string {
	existing = strings.TrimSpace(existing)
	addition = strings.TrimSpace(addition)
	if existing == "" {
		return addition
	}
	if addition == "" {
		return existing
	}
	return existing + "; " + addition
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
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
