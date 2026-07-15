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

type humanMemoryApprovalPlan struct {
	packet          memoryApprovalPacket
	approvedEntries []store.ProjectMemoryInput
	decisions       []store.ProjectMemoryDecisionInput
	approvedCount   int
	editedCount     int
	rejectedCount   int
	deferredCount   int
	evidenceRefs    []string
}

func (e *Engine) submitHumanMemoryApproval(ctx context.Context, wr store.WorkflowRun, runtimeStage runtimeStage, submission HumanReviewSubmission) (report.Report, error) {
	plan, err := e.humanMemoryApprovalPlan(ctx, wr.Run.ID, runtimeStage.Stage.ID, submission)
	if err != nil {
		return report.Report{}, err
	}
	if err := e.reserveRunAdmission(ctx); err != nil {
		return report.Report{}, err
	}
	reservationTransferred := false
	defer func() {
		if !reservationTransferred {
			e.releaseGlobalRun()
		}
	}()
	changed, err := e.store.UpdateRunStatusFrom(ctx, wr.Run.ID, store.RunStatusAwaitingHuman, store.RunStatusRunning)
	if err != nil {
		return report.Report{}, err
	}
	if !changed {
		return report.Report{}, ErrHumanReviewNotAwaiting
	}
	wr.Run.Status = store.RunStatusRunning
	rollbackResume := func() {
		changed, err := e.store.UpdateRunStatusFrom(
			context.Background(),
			wr.Run.ID,
			store.RunStatusRunning,
			store.RunStatusAwaitingHuman,
		)
		if err != nil || !changed {
			return
		}
		wr.Run.Status = store.RunStatusAwaitingHuman
		_ = e.store.UpdateStageStatus(context.Background(), runtimeStage.Stage.ID, store.StageStatusRunning)
	}
	var rep report.Report
	result, err := e.store.ApplyProjectMemoryUpdateGuarded(ctx, store.ProjectMemoryUpdate{
		ProjectID:      wr.Run.ProjectID,
		RunID:          wr.Run.ID,
		TaskID:         wr.Task.ID,
		CuratorStageID: runtimeStage.Stage.ID,
		Entries:        plan.approvedEntries,
		Decisions:      plan.decisions,
	}, func(result store.ProjectMemoryUpdateResult) error {
		var err error
		rep, err = humanMemoryApprovalReport(wr, runtimeStage.Stage, runtimeStage.Template, submission, plan, result)
		if err != nil {
			return err
		}
		if err := rep.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidHumanReview, err)
		}
		return nil
	})
	if err != nil {
		rollbackResume()
		return report.Report{}, err
	}
	rollbackAppliedUpdate := func() {
		outcome, _ := e.store.RollbackProjectMemoryUpdate(context.Background(), result.Revert)
		e.emitProjectMemoryRollbackSkipped(context.Background(), wr, outcome)
		rollbackResume()
	}
	if err := e.completeStage(ctx, wr, runtimeStage.Stage, rep); err != nil {
		rollbackAppliedUpdate()
		return report.Report{}, err
	}
	if _, err := e.emit(ctx, runEvent(wr, "run.resumed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run resumed from human memory approval", map[string]any{
		"status":              store.RunStatusRunning,
		"stage_id":            runtimeStage.Stage.ID,
		"workflow_stage_id":   runtimeStage.Template.ID,
		"applied_count":       len(result.Entries),
		"rejected_count":      plan.rejectedCount + len(result.Rejections),
		"deferred_count":      plan.deferredCount,
		"human_authoritative": true,
	})); err != nil {
		rollbackAppliedUpdate()
		return report.Report{}, err
	}
	reservationTransferred = e.resumeRunAfterHumanStage(wr.Run.ID, runtimeStage.Template.ID)
	return rep, nil
}

func (e *Engine) emitProjectMemoryRollbackSkipped(ctx context.Context, wr store.WorkflowRun, outcome store.ProjectMemoryRollbackOutcome) {
	if len(outcome.Skipped) == 0 {
		return
	}
	_, _ = e.emit(ctx, runEvent(wr, "memory.rollback_skipped", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "project memory rollback skipped concurrently modified rows", map[string]any{
		"skipped_count":   len(outcome.Skipped),
		"skipped_reverts": outcome.Skipped,
	}))
}

func (e *Engine) humanMemoryApprovalPlan(ctx context.Context, runID, stageID string, submission HumanReviewSubmission) (humanMemoryApprovalPlan, error) {
	if strings.TrimSpace(submission.Summary) == "" {
		return humanMemoryApprovalPlan{}, fmt.Errorf("%w: summary is required", ErrInvalidHumanReview)
	}
	packet, _, err := e.memoryApprovalPacketForAwaiting(ctx, runID, stageID)
	if err != nil {
		return humanMemoryApprovalPlan{}, err
	}
	candidates := map[string]memoryApprovalCandidate{}
	for _, candidate := range packet.Candidates {
		candidate.CandidateID = strings.TrimSpace(candidate.CandidateID)
		if candidate.CandidateID == "" {
			return humanMemoryApprovalPlan{}, fmt.Errorf("%w: memory approval packet has a candidate without candidate_id", ErrInvalidHumanReview)
		}
		candidates[candidate.CandidateID] = candidate
	}
	seen := map[string]HumanMemoryDecision{}
	for _, decision := range submission.MemoryDecisions {
		decision.CandidateID = strings.TrimSpace(decision.CandidateID)
		if decision.CandidateID == "" {
			return humanMemoryApprovalPlan{}, fmt.Errorf("%w: memory decision candidate_id is required", ErrInvalidHumanReview)
		}
		if _, ok := candidates[decision.CandidateID]; !ok {
			return humanMemoryApprovalPlan{}, fmt.Errorf("%w: memory decision references unknown candidate %q", ErrInvalidHumanReview, decision.CandidateID)
		}
		if _, ok := seen[decision.CandidateID]; ok {
			return humanMemoryApprovalPlan{}, fmt.Errorf("%w: memory decision for candidate %q is duplicated", ErrInvalidHumanReview, decision.CandidateID)
		}
		seen[decision.CandidateID] = decision
	}
	plan := humanMemoryApprovalPlan{packet: packet, evidenceRefs: []string{packet.StageBriefArtifactID}}
	for _, candidate := range packet.Candidates {
		decision, ok := seen[candidate.CandidateID]
		if !ok {
			return humanMemoryApprovalPlan{}, fmt.Errorf("%w: memory candidate %q has no human decision", ErrInvalidHumanReview, candidate.CandidateID)
		}
		action, err := normalizeHumanMemoryAction(decision.Action)
		if err != nil {
			return humanMemoryApprovalPlan{}, err
		}
		entry := candidate.projectMemoryInput()
		record := store.ProjectMemoryDecisionInput{
			CandidateID:      candidate.CandidateID,
			Action:           action,
			Kind:             candidate.Kind,
			Title:            candidate.Title,
			Body:             candidate.Body,
			Reason:           strings.TrimSpace(decision.Reason),
			SourceStageID:    candidate.SourceStageID,
			SourceArtifactID: candidate.SourceArtifactID,
			SourceSummary:    candidate.SourceSummary,
		}
		switch action {
		case store.ProjectMemoryDecisionApprove:
			plan.approvedEntries = append(plan.approvedEntries, entry)
			plan.approvedCount++
		case store.ProjectMemoryDecisionEdit:
			if value := strings.TrimSpace(decision.Kind); value != "" {
				entry.Kind = value
			}
			if value := strings.TrimSpace(decision.Title); value != "" {
				entry.Title = value
			}
			if value := strings.TrimSpace(decision.Body); value != "" {
				entry.Body = value
			}
			if value := strings.TrimSpace(decision.SourceSummary); value != "" {
				entry.SourceSummary = value
			}
			record.Kind = entry.Kind
			record.Title = entry.Title
			record.Body = entry.Body
			record.SourceSummary = entry.SourceSummary
			plan.approvedEntries = append(plan.approvedEntries, entry)
			plan.approvedCount++
			plan.editedCount++
		case store.ProjectMemoryDecisionReject:
			plan.rejectedCount++
		case store.ProjectMemoryDecisionDefer:
			plan.deferredCount++
		}
		plan.decisions = append(plan.decisions, record)
		plan.evidenceRefs = append(plan.evidenceRefs, candidate.SourceArtifactID)
	}
	for _, rejection := range packet.ExtractionRejections {
		plan.evidenceRefs = append(plan.evidenceRefs, rejection.SourceArtifactID)
	}
	plan.evidenceRefs = uniqueStrings(plan.evidenceRefs)
	return plan, nil
}

func (e *Engine) memoryApprovalPacketForAwaiting(ctx context.Context, runID, stageID string) (memoryApprovalPacket, store.Artifact, error) {
	events, err := e.store.ListEvents(ctx, runID)
	if err != nil {
		return memoryApprovalPacket{}, store.Artifact{}, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if ev.Type != "stage.awaiting_human" {
			continue
		}
		pendingStageID := payloadString(ev.Data, "pending_stage_id")
		if pendingStageID == "" {
			pendingStageID = payloadString(ev.Data, "stage_id")
		}
		if pendingStageID != stageID {
			continue
		}
		packetID := payloadString(ev.Data, "memory_approval_packet_id")
		if packetID == "" {
			continue
		}
		artifact, content, err := e.store.GetArtifact(ctx, packetID)
		if err != nil {
			return memoryApprovalPacket{}, store.Artifact{}, err
		}
		var packet memoryApprovalPacket
		if err := json.Unmarshal(content, &packet); err != nil {
			return memoryApprovalPacket{}, store.Artifact{}, fmt.Errorf("decode memory approval packet %s: %w", packetID, err)
		}
		if packet.StageID != stageID || packet.RunID != runID {
			return memoryApprovalPacket{}, store.Artifact{}, fmt.Errorf("%w: memory approval packet does not match run/stage", ErrInvalidHumanReview)
		}
		return packet, artifact, nil
	}
	return memoryApprovalPacket{}, store.Artifact{}, ErrHumanReviewNotAwaiting
}

func normalizeHumanMemoryAction(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case store.ProjectMemoryDecisionApprove:
		return store.ProjectMemoryDecisionApprove, nil
	case store.ProjectMemoryDecisionEdit:
		return store.ProjectMemoryDecisionEdit, nil
	case store.ProjectMemoryDecisionReject:
		return store.ProjectMemoryDecisionReject, nil
	case store.ProjectMemoryDecisionDefer:
		return store.ProjectMemoryDecisionDefer, nil
	default:
		return "", fmt.Errorf("%w: memory decision action must be approve, edit, reject, or defer", ErrInvalidHumanReview)
	}
}

func humanMemoryApprovalReport(wr store.WorkflowRun, stage store.Stage, templateStage workflow.StageTemplate, submission HumanReviewSubmission, plan humanMemoryApprovalPlan, result store.ProjectMemoryUpdateResult) (report.Report, error) {
	actorID := strings.TrimSpace(submission.ActorID)
	if actorID == "" {
		actorID = "operator"
	}
	summary := strings.TrimSpace(submission.Summary)
	if summary == "" {
		return report.Report{}, fmt.Errorf("%w: summary is required", ErrInvalidHumanReview)
	}
	allRejections := append([]store.ProjectMemoryRejection{}, plan.packet.ExtractionRejections...)
	allRejections = append(allRejections, result.Rejections...)
	entryIDs := make([]string, 0, len(result.Entries))
	for _, entry := range result.Entries {
		entryIDs = append(entryIDs, entry.ID)
	}
	payload := map[string]any{
		"workflow_stage_id":          templateStage.ID,
		"workflow_stage_label":       templateStage.Label,
		"workflow_stage_actor":       templateStage.Actor,
		"candidate_count":            len(plan.packet.Candidates) + len(plan.packet.ExtractionRejections),
		"approval_candidate_count":   len(plan.packet.Candidates),
		"approved_count":             plan.approvedCount,
		"edited_count":               plan.editedCount,
		"applied_count":              len(result.Entries),
		"rejected_count":             plan.rejectedCount + len(result.Rejections) + len(plan.packet.ExtractionRejections),
		"human_rejected_count":       plan.rejectedCount,
		"curator_rejected_count":     len(result.Rejections),
		"extraction_rejected_count":  len(plan.packet.ExtractionRejections),
		"deferred_count":             plan.deferredCount,
		"project_memory_entry_ids":   entryIDs,
		"project_memory_rejections":  allRejections,
		"project_memory_decisions":   result.Decisions,
		"curator":                    "memory_update_gatekeeper",
		"human_authoritative":        true,
		"curator_enacts_decisions":   true,
		"writes_private_sqlite_only": true,
		"repo_export_performed":      false,
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHuman, ID: actorID},
		Status:        report.StatusCompleted,
		Summary:       summary,
		EvidenceRefs:  plan.evidenceRefs,
		Payload:       payload,
		Errors:        []string{},
	}, nil
}
