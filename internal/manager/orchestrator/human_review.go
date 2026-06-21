package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

var (
	errRunAwaitingHuman       = errors.New("run awaiting human input")
	ErrHumanReviewNotAwaiting = errors.New("run is not awaiting that human stage")
	ErrInvalidHumanReview     = errors.New("invalid human review submission")
)

type HumanReviewSubmission struct {
	ActorID      string         `json:"actor_id"`
	Verdict      string         `json:"verdict"`
	Summary      string         `json:"summary"`
	Findings     []HumanFinding `json:"findings,omitempty"`
	EvidenceRefs []string       `json:"evidence_refs,omitempty"`
}

type HumanFinding struct {
	ID        string `json:"id,omitempty"`
	Summary   string `json:"summary"`
	Rationale string `json:"rationale,omitempty"`
}

func (e *Engine) SubmitHumanReview(ctx context.Context, runID, stageID string, submission HumanReviewSubmission) (report.Report, error) {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return report.Report{}, err
	}
	if wr.Run.Status != store.RunStatusAwaitingHuman {
		return report.Report{}, ErrHumanReviewNotAwaiting
	}
	runtime, err := e.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		return report.Report{}, err
	}
	runtimeStage, ok := runtimeStageByStageID(runtime, stageID)
	if !ok || runtimeStage.Stage.Status != store.StageStatusRunning || runtimeStage.Template.Actor != workflow.ActorHuman || runtimeStage.Template.Type != workflow.StageTypeReview {
		return report.Report{}, ErrHumanReviewNotAwaiting
	}
	rep, err := humanReviewReport(wr, runtimeStage.Stage, runtimeStage.Template, submission)
	if err != nil {
		return report.Report{}, err
	}
	if err := rep.Validate(); err != nil {
		return report.Report{}, fmt.Errorf("%w: %v", ErrInvalidHumanReview, err)
	}
	changed, err := e.store.UpdateRunStatusFrom(ctx, wr.Run.ID, store.RunStatusAwaitingHuman, store.RunStatusRunning)
	if err != nil {
		return report.Report{}, err
	}
	if !changed {
		return report.Report{}, ErrHumanReviewNotAwaiting
	}
	wr.Run.Status = store.RunStatusRunning
	if err := e.completeStage(ctx, wr, runtimeStage.Stage, rep); err != nil {
		return report.Report{}, err
	}
	if _, err := e.emit(ctx, runEvent(wr, "run.resumed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run resumed from human review", map[string]any{
		"status":            store.RunStatusRunning,
		"stage_id":          runtimeStage.Stage.ID,
		"workflow_stage_id": runtimeStage.Template.ID,
		"verdict":           verdictString(rep.Verdict),
	})); err != nil {
		return report.Report{}, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	e.registerActiveRun(wr.Run.ID, cancel)
	go e.executeRunAfter(runCtx, wr.Run.ID, runtimeStage.Template.ID)
	return rep, nil
}

func humanReviewReport(wr store.WorkflowRun, stage store.Stage, templateStage workflow.StageTemplate, submission HumanReviewSubmission) (report.Report, error) {
	actorID := strings.TrimSpace(submission.ActorID)
	if actorID == "" {
		actorID = "operator"
	}
	summary := strings.TrimSpace(submission.Summary)
	if summary == "" {
		return report.Report{}, fmt.Errorf("%w: summary is required", ErrInvalidHumanReview)
	}
	verdict, status, err := normalizeHumanVerdict(submission.Verdict)
	if err != nil {
		return report.Report{}, err
	}
	payload := map[string]any{
		"workflow_stage_id":       templateStage.ID,
		"workflow_stage_label":    templateStage.Label,
		"workflow_stage_actor":    templateStage.Actor,
		"workflow_stage_target":   templateStage.Target,
		"workflow_stage_settings": templateStage.Settings,
		"accepted_findings":       humanFindingsPayload(submission.Findings),
		"human_arbiter":           true,
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHuman, ID: actorID},
		Status:        status,
		Verdict:       &verdict,
		Summary:       summary,
		EvidenceRefs:  cleanStrings(submission.EvidenceRefs),
		Payload:       payload,
		Errors:        []string{},
	}, nil
}

func normalizeHumanVerdict(value string) (report.Verdict, string, error) {
	switch report.Verdict(strings.TrimSpace(value)) {
	case report.ReviewVerdictPass:
		return report.ReviewVerdictPass, report.StatusCompleted, nil
	case report.ReviewVerdictChangesRequested:
		return report.ReviewVerdictChangesRequested, report.StatusChangesRequested, nil
	case report.ReviewVerdictBlocked:
		return report.ReviewVerdictBlocked, report.StatusNeedsInput, nil
	case report.ReviewVerdictEscalate:
		return report.ReviewVerdictBlocked, report.StatusNeedsInput, nil
	default:
		return "", "", fmt.Errorf("%w: verdict must be pass, changes_requested, or blocked", ErrInvalidHumanReview)
	}
}

func humanFindingsPayload(findings []HumanFinding) []any {
	out := make([]any, 0, len(findings))
	for i, finding := range findings {
		summary := strings.TrimSpace(finding.Summary)
		if summary == "" {
			continue
		}
		id := strings.TrimSpace(finding.ID)
		if id == "" {
			id = fmt.Sprintf("human-finding-%d", i+1)
		}
		out = append(out, map[string]any{
			"finding_id":     id,
			"summary":        summary,
			"rationale":      strings.TrimSpace(finding.Rationale),
			"classification": report.ReviewFindingAccepted,
		})
	}
	return out
}

func cleanStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func runtimeStageByStageID(runtime runtimeWorkflow, stageID string) (runtimeStage, bool) {
	for _, stage := range runtime.Stages {
		if stage.Stage.ID == stageID {
			return stage, true
		}
	}
	return runtimeStage{}, false
}

func routingOutcome(stage workflow.StageTemplate, rep report.Report) string {
	if stage.Type == workflow.StageTypeReview && rep.Verdict != nil {
		switch *rep.Verdict {
		case report.ReviewVerdictPass:
			return workflow.OnCompleted
		case report.ReviewVerdictChangesRequested:
			return workflow.OnChangesRequested
		case report.ReviewVerdictBlocked:
			return workflow.OnBlocked
		case report.ReviewVerdictEscalate:
			return workflow.OnEscalate
		}
	}
	return rep.Status
}

func (e *Engine) suspendForHumanReview(ctx context.Context, wr store.WorkflowRun, stage store.Stage, templateStage workflow.StageTemplate, briefArtifact store.Artifact) error {
	packet := map[string]any{
		"schema_version":           report.SchemaVersion,
		"run_id":                   wr.Run.ID,
		"task_id":                  wr.Task.ID,
		"attempt_id":               wr.Attempt.ID,
		"stage_id":                 stage.ID,
		"stage_type":               stage.StageType,
		"workflow_stage_id":        templateStage.ID,
		"workflow_stage_label":     templateStage.Label,
		"workflow_stage_target":    templateStage.Target,
		"stage_brief_artifact_id":  briefArtifact.ID,
		"allowed_verdicts":         []string{string(report.ReviewVerdictPass), string(report.ReviewVerdictChangesRequested), string(report.ReviewVerdictBlocked)},
		"human_is_arbiter":         true,
		"repair_loop":              false,
		"timeout":                  nil,
		"human_fix_loops_counted":  false,
		"submission_endpoint_hint": fmt.Sprintf("/runs/%s/human-stages/%s/verdict", wr.Run.ID, stage.ID),
	}
	content, err := json.MarshalIndent(packet, "", "  ")
	if err != nil {
		return err
	}
	artifact, err := e.store.SaveArtifact(ctx, wr.Run.ID, "human_review_packet", "application/json", content, ".json")
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
	_, err = e.emit(ctx, stageEvent(wr, stage, "stage.awaiting_human", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "human review stage awaiting verdict", map[string]any{
		"status":                   store.RunStatusAwaitingHuman,
		"pending_stage_id":         stage.ID,
		"workflow_stage_id":        templateStage.ID,
		"human_review_packet_id":   artifact.ID,
		"stage_brief_artifact_id":  briefArtifact.ID,
		"allowed_verdicts":         []string{string(report.ReviewVerdictPass), string(report.ReviewVerdictChangesRequested), string(report.ReviewVerdictBlocked)},
		"human_fix_loops_counted":  false,
		"runner_slot_released":     true,
		"submission_endpoint_hint": fmt.Sprintf("/runs/%s/human-stages/%s/verdict", wr.Run.ID, stage.ID),
	}))
	return err
}
