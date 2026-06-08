package orchestrator

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

const defaultMaxFixLoops = 3

func (e *Engine) runReviewStage(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, runtimeStage runtimeStage, lastReport, lastValidationReport report.Report, snapshot workerSnapshot, snapshotErr error) (report.Report, error) {
	stage := runtimeStage.Stage
	templateStage := runtimeStage.Template
	brief, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	if err := e.startStage(ctx, wr, stage, stage.StageType+" stage started"); err != nil {
		return report.Report{}, err
	}

	baseInput := e.stageDispatchInput(runtime, templateStage, reviewBaseInput(wr, templateStage, lastReport, lastValidationReport, snapshot, snapshotErr))
	baseInput = withStageBriefInput(baseInput, brief, briefArtifact.ID)

	criticInput := cloneInput(baseInput)
	criticInput["review_role"] = contract.ReviewRoleCritic
	criticInput["adapter_execution_id"] = "review_" + contract.ReviewRoleCritic
	criticInput["review_dispatch_sequence"] = 1
	criticInput["critic_count"] = 1
	criticInput["arbiter_count"] = 1
	criticInput["arbitration_internal"] = true
	criticRep, err := e.dispatchReviewPass(ctx, wr, stage, stage.Adapter, contract.ReviewRoleCritic, criticInput, func(rep report.Report) (report.Report, error) {
		return normalizeIntermediateReviewReport(wr, stage, rep)
	})
	if err != nil {
		return report.Report{}, err
	}
	if criticRep.Status != report.StatusCompleted {
		if err := e.completeStage(context.Background(), wr, stage, criticRep); err != nil {
			return report.Report{}, err
		}
		return criticRep, nil
	}
	criticArtifact, err := e.store.SaveReportArtifact(ctx, criticRep)
	if err != nil {
		return report.Report{}, err
	}
	if _, err := e.emit(ctx, stageEvent(wr, stage, "review.critic.completed", reportActor(criticRep.Actor, stage), "review critic completed", map[string]any{
		"adapter":            stage.Adapter,
		"review_role":        contract.ReviewRoleCritic,
		"report_artifact_id": criticArtifact.ID,
	})); err != nil {
		return report.Report{}, err
	}

	rawFindings := reviewRawFindings(criticRep.Payload)
	arbiterInput := cloneInput(baseInput)
	arbiterInput["review_role"] = contract.ReviewRoleArbiter
	arbiterInput["adapter_execution_id"] = "review_" + contract.ReviewRoleArbiter
	arbiterInput["review_dispatch_sequence"] = 2
	arbiterInput["critic_count"] = 1
	arbiterInput["arbiter_count"] = 1
	arbiterInput["arbitration_internal"] = true
	arbiterInput["raw_findings"] = rawFindings
	arbiterInput["critic_report"] = reportInput(criticRep)
	arbiterInput["critic_report_artifact_id"] = criticArtifact.ID
	finalRep, err := e.dispatchReviewPass(ctx, wr, stage, stage.Adapter, contract.ReviewRoleArbiter, arbiterInput, func(rep report.Report) (report.Report, error) {
		return normalizeArbiterReviewReport(wr, stage, runtime.Template, templateStage, rep, criticRep, criticArtifact.ID)
	})
	if err != nil {
		return report.Report{}, err
	}
	if finalRep.Status == report.StatusChangesRequested {
		exhausted, err := e.fixLoopExhausted(context.Background(), wr, runtime.Template, templateStage)
		if err != nil {
			return report.Report{}, err
		}
		if exhausted {
			finalRep = exhaustedReviewReport(finalRep, maxFixLoops(runtime.Template, templateStage))
		}
	}
	if err := e.completeStage(context.Background(), wr, stage, finalRep); err != nil {
		return report.Report{}, err
	}
	return finalRep, nil
}

func (e *Engine) dispatchReviewPass(ctx context.Context, wr store.WorkflowRun, stage store.Stage, adapterName, role string, input map[string]any, validator reportRepairValidator) (report.Report, error) {
	data := map[string]any{"adapter": adapterName, "review_role": role, "critic_count": 1, "arbiter_count": 1}
	disp := contract.Dispatch{
		ProjectID:    wr.Run.ProjectID,
		RepositoryID: wr.Task.RepositoryID,
		RunID:        wr.Run.ID,
		TaskID:       wr.Task.ID,
		AttemptID:    wr.Attempt.ID,
		StageID:      stage.ID,
		StageType:    contract.StageTypeReview,
		Adapter:      adapterName,
		Input:        input,
	}
	return e.dispatchWithReportRepair(ctx, wr, stage, disp, reportRepairOptions{
		AdapterName:     adapterName,
		StageType:       contract.StageTypeReview,
		EmitLifecycle:   true,
		LifecycleData:   data,
		PreparedSummary: "review " + role + " invocation prepared",
		StartedSummary:  "review " + role + " started",
		Validator:       validator,
	})
}

func reviewBaseInput(wr store.WorkflowRun, stage workflow.StageTemplate, lastReport, lastValidationReport report.Report, snapshot workerSnapshot, snapshotErr error) map[string]any {
	profile := settingString(stage.Settings, "profile")
	intensity := settingString(stage.Settings, "intensity")
	instructions := settingString(stage.Settings, "instructions")
	input := map[string]any{
		"idea":                    wr.Run.Idea,
		"review_profile":          profile,
		"review_intensity":        intensity,
		"review_instructions":     instructions,
		"review_target":           stage.Target,
		"critic_count":            1,
		"arbiter_count":           1,
		"arbitration_internal":    true,
		"reviewer_config":         contract.ReviewerConfig{Profile: profile, Intensity: intensity, Instructions: instructions},
		"contract_markdown":       taskContractMarkdown(wr),
		"implementation_diff_ref": snapshot.DiffArtifactID,
	}
	if lastReport.StageID != "" {
		input["last_stage_report"] = reportInput(lastReport)
	}
	if lastValidationReport.StageID != "" {
		input["validation_report"] = reportInput(lastValidationReport)
	}
	if snapshot.DiffArtifactID != "" {
		input["implementation_snapshot"] = map[string]any{
			"worktree_path":    snapshot.WorktreePath,
			"base_sha":         snapshot.BaseSHA,
			"base_tree_sha":    snapshot.BaseTreeSHA,
			"worker_tree_sha":  snapshot.WorkerTreeSHA,
			"diff_artifact_id": snapshot.DiffArtifactID,
			"snapshot_error":   "",
		}
	}
	if snapshotErr != nil {
		input["implementation_snapshot_error"] = snapshotErr.Error()
	}
	return input
}

func normalizeIntermediateReviewReport(wr store.WorkflowRun, stage store.Stage, rep report.Report) (report.Report, error) {
	rep = stampReport(wr, stage, contract.StageTypeReview, rep)
	if rep.Payload == nil {
		rep.Payload = map[string]any{}
	}
	rep.Payload["review_role"] = contract.ReviewRoleCritic
	if err := rep.Validate(); err != nil {
		return invalidAdapterReport(wr, stage, stage.Adapter, "review critic returned invalid report", rep, err), err
	}
	if rep.Status == report.StatusApproved || rep.Status == report.StatusChangesRequested {
		err := fmt.Errorf("critic status %q is not allowed", rep.Status)
		return invalidAdapterReport(wr, stage, stage.Adapter, "review critic returned arbiter-only status", rep, err), err
	}
	return rep, nil
}

func normalizeArbiterReviewReport(wr store.WorkflowRun, stage store.Stage, template workflow.Template, templateStage workflow.StageTemplate, arbiterRep, criticRep report.Report, criticArtifactID string) (report.Report, error) {
	rep := stampReport(wr, stage, contract.StageTypeReview, arbiterRep)
	if rep.Payload == nil {
		rep.Payload = map[string]any{}
	}
	rep.Payload["review_role"] = contract.ReviewRoleArbiter
	rep.Payload["critic_count"] = 1
	rep.Payload["arbiter_count"] = 1
	rep.Payload["arbitration_internal"] = true
	rep.Payload["review_profile"] = settingString(templateStage.Settings, "profile")
	rep.Payload["review_intensity"] = settingString(templateStage.Settings, "intensity")
	rep.Payload["review_instructions"] = settingString(templateStage.Settings, "instructions")
	rep.Payload["critic_report_artifact_id"] = criticArtifactID
	rep.Payload["max_fix_loops"] = maxFixLoops(template, templateStage)
	if _, ok := rep.Payload["raw_findings"]; !ok {
		rep.Payload["raw_findings"] = reviewRawFindings(criticRep.Payload)
	}
	if _, ok := rep.Payload["arbitration_decisions"]; !ok {
		rep.Payload["arbitration_decisions"] = []any{}
	}
	if err := validateArbitrationDecisions(rep.Payload); err != nil {
		return invalidAdapterReport(wr, stage, stage.Adapter, "review arbiter returned invalid arbitration decisions", rep, err), err
	}
	rep.Payload["accepted_findings"] = acceptedArbitrationDecisions(rep.Payload)
	if !containsString(rep.EvidenceRefs, criticArtifactID) {
		rep.EvidenceRefs = append(rep.EvidenceRefs, criticArtifactID)
	}
	if rep.Status == report.StatusFailed || rep.Status == report.StatusInvalid || rep.Status == report.StatusNeedsInput {
		if err := rep.Validate(); err != nil {
			return invalidAdapterReport(wr, stage, stage.Adapter, "review arbiter returned invalid report", rep, err), err
		}
		return rep, nil
	}
	if rep.Status != report.StatusCompleted && rep.Status != report.StatusApproved && rep.Status != report.StatusChangesRequested {
		err := fmt.Errorf("review arbiter status %q cannot be verdict-routed", rep.Status)
		return invalidAdapterReport(wr, stage, stage.Adapter, "review arbiter returned invalid status", rep, err), err
	}
	if rep.Verdict == nil {
		err := fmt.Errorf("review verdict is required")
		return invalidAdapterReport(wr, stage, stage.Adapter, "review arbiter omitted verdict", rep, err), err
	}
	switch *rep.Verdict {
	case report.ReviewVerdictPass:
		rep.Status = report.StatusCompleted
	case report.ReviewVerdictChangesRequested:
		if len(acceptedArbitrationDecisions(rep.Payload)) == 0 {
			err := fmt.Errorf("changes_requested requires at least one accepted arbitration decision")
			return invalidAdapterReport(wr, stage, stage.Adapter, "review arbiter requested changes without accepted findings", rep, err), err
		}
		rep.Status = report.StatusChangesRequested
	case report.ReviewVerdictBlocked, report.ReviewVerdictEscalate:
		rep.Status = report.StatusNeedsInput
	default:
		err := fmt.Errorf("invalid review verdict %q", *rep.Verdict)
		return invalidAdapterReport(wr, stage, stage.Adapter, "review arbiter returned invalid verdict", rep, err), err
	}
	if err := rep.Validate(); err != nil {
		return invalidAdapterReport(wr, stage, stage.Adapter, "review arbiter returned invalid report", rep, err), err
	}
	return rep, nil
}

func exhaustedReviewReport(rep report.Report, maxLoops int) report.Report {
	if rep.Payload == nil {
		rep.Payload = map[string]any{}
	}
	rep.Payload["arbiter_verdict"] = verdictString(rep.Verdict)
	rep.Payload["fix_loop_exhausted"] = true
	rep.Payload["max_fix_loops"] = maxLoops
	blocked := report.ReviewVerdictBlocked
	rep.Verdict = &blocked
	rep.Status = report.StatusNeedsInput
	rep.Summary = fmt.Sprintf("review fix loop exhausted after %d fix loop attempt(s): %s", maxLoops, rep.Summary)
	return rep
}

func stampReport(wr store.WorkflowRun, stage store.Stage, stageType string, rep report.Report) report.Report {
	if rep.SchemaVersion == 0 {
		rep.SchemaVersion = report.SchemaVersion
	}
	if rep.RunID == "" {
		rep.RunID = wr.Run.ID
	}
	if rep.TaskID == "" {
		rep.TaskID = wr.Task.ID
	}
	if rep.AttemptID == "" {
		rep.AttemptID = wr.Attempt.ID
	}
	if rep.StageID == "" {
		rep.StageID = stage.ID
	}
	if rep.StageType == "" {
		rep.StageType = stageType
	}
	if rep.Actor.Kind == "" {
		rep.Actor = report.Actor{Kind: report.ActorKindAgent, ID: stage.Adapter}
	}
	if rep.Payload == nil {
		rep.Payload = map[string]any{}
	}
	return rep
}

func isFixLoopTransition(runtime runtimeWorkflow, stage workflow.StageTemplate, rep report.Report, nextID string) bool {
	next, ok := runtime.ByID[nextID]
	if !ok || next.Template.Type != workflow.StageTypeImplementation {
		return false
	}
	switch stage.Type {
	case workflow.StageTypeReview:
		return rep.Status == report.StatusChangesRequested
	case workflow.StageTypeValidation:
		return rep.Status == report.StatusFailed
	default:
		return false
	}
}

func (e *Engine) fixLoopExhausted(ctx context.Context, wr store.WorkflowRun, template workflow.Template, stage workflow.StageTemplate) (bool, error) {
	count, err := e.store.CountAttemptsForRun(ctx, wr.Run.ID)
	if err != nil {
		return false, err
	}
	used := count - 1
	if used < 0 {
		used = 0
	}
	return used >= maxFixLoops(template, stage), nil
}

func (e *Engine) startFixLoopAttempt(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, triggerStage runtimeStage, trigger report.Report, nextID string) (store.WorkflowRun, runtimeWorkflow, error) {
	if err := e.store.UpdateAttemptStatus(ctx, wr.Attempt.ID, trigger.Status); err != nil {
		return store.WorkflowRun{}, runtimeWorkflow{}, err
	}
	attempt, _, err := e.store.CreateAttemptForRun(ctx, wr.Run.ID, runtime.Template)
	if err != nil {
		return store.WorkflowRun{}, runtimeWorkflow{}, err
	}
	newWR, err := e.store.GetWorkflowRun(ctx, wr.Run.ID)
	if err != nil {
		return store.WorkflowRun{}, runtimeWorkflow{}, err
	}
	newWR, err = e.configureRuntimeStageAdapters(ctx, newWR, runtime.Template)
	if err != nil {
		return store.WorkflowRun{}, runtimeWorkflow{}, err
	}
	newRuntime, err := e.loadRuntimeWorkflow(ctx, newWR)
	if err != nil {
		return store.WorkflowRun{}, runtimeWorkflow{}, err
	}
	if _, ok := newRuntime.ByID[nextID]; !ok {
		return store.WorkflowRun{}, runtimeWorkflow{}, fmt.Errorf("fix loop target stage %q not found in new attempt", nextID)
	}
	_, err = e.emit(ctx, runEvent(newWR, "fix_loop.attempt_started", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "fix loop attempt started", map[string]any{
		"attempt_id":             attempt.ID,
		"trigger_stage_id":       trigger.StageID,
		"trigger_workflow_stage": triggerStage.Template.ID,
		"trigger_stage_type":     trigger.StageType,
		"trigger_status":         trigger.Status,
		"trigger_verdict":        verdictString(trigger.Verdict),
		"target_workflow_stage":  nextID,
		"max_fix_loops":          maxFixLoops(runtime.Template, triggerStage.Template),
	}))
	if err != nil {
		return store.WorkflowRun{}, runtimeWorkflow{}, err
	}
	return newWR, newRuntime, nil
}

func withFixLoopExhaustion(rep report.Report, maxLoops int) report.Report {
	if rep.Payload == nil {
		rep.Payload = map[string]any{}
	}
	rep.Payload["fix_loop_exhausted"] = true
	rep.Payload["max_fix_loops"] = maxLoops
	rep.Summary = fmt.Sprintf("fix loop exhausted after %d fix loop attempt(s): %s", maxLoops, rep.Summary)
	return rep
}

func maxFixLoops(template workflow.Template, stage workflow.StageTemplate) int {
	if value, ok := intSetting(stage.Settings, "max_fix_loops"); ok {
		return maxInt(0, value)
	}
	if value, ok := intSetting(template.Settings, "max_fix_loops"); ok {
		return maxInt(0, value)
	}
	return defaultMaxFixLoops
}

func intSetting(settings map[string]any, key string) (int, bool) {
	if settings == nil {
		return 0, false
	}
	value, ok := settings[key]
	if !ok || value == nil {
		return 0, false
	}
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		return parsed, err == nil
	default:
		parsed, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
		return parsed, err == nil
	}
}

func cloneInput(input map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range input {
		out[key] = value
	}
	return out
}

func reportInput(rep report.Report) map[string]any {
	out := map[string]any{
		"stage_id":      rep.StageID,
		"stage_type":    rep.StageType,
		"status":        rep.Status,
		"verdict":       verdictString(rep.Verdict),
		"summary":       rep.Summary,
		"evidence_refs": append([]string{}, rep.EvidenceRefs...),
		"payload":       rep.Payload,
		"errors":        append([]string{}, rep.Errors...),
	}
	return out
}

func verdictString(verdict *report.Verdict) string {
	if verdict == nil {
		return ""
	}
	return string(*verdict)
}

func reviewRawFindings(payload map[string]any) []any {
	return anySlice(payloadValue(payload, "raw_findings"))
}

func acceptedFindings(payload map[string]any) []any {
	if explicit := anySlice(payloadValue(payload, "accepted_findings")); explicit != nil {
		return explicit
	}
	return acceptedArbitrationDecisions(payload)
}

func acceptedArbitrationDecisions(payload map[string]any) []any {
	var accepted []any
	for _, decision := range anySlice(payloadValue(payload, "arbitration_decisions")) {
		m, ok := decision.(map[string]any)
		if !ok {
			continue
		}
		classification := firstMapString(m, "classification", "decision", "status")
		if classification == report.ReviewFindingAccepted {
			accepted = append(accepted, decision)
		}
	}
	if accepted == nil {
		return []any{}
	}
	return accepted
}

func validateArbitrationDecisions(payload map[string]any) error {
	for i, decision := range anySlice(payloadValue(payload, "arbitration_decisions")) {
		m, ok := decision.(map[string]any)
		if !ok {
			return fmt.Errorf("arbitration_decisions[%d] must be an object", i)
		}
		classification := firstMapString(m, "classification", "decision", "status")
		switch classification {
		case report.ReviewFindingAccepted, report.ReviewFindingRejected, report.ReviewFindingDeferred, report.ReviewFindingEscalated:
			continue
		default:
			return fmt.Errorf("arbitration_decisions[%d] classification %q is invalid", i, classification)
		}
	}
	return nil
}

func payloadValue(payload map[string]any, key string) any {
	if payload == nil {
		return nil
	}
	return payload[key]
}

func anySlice(value any) []any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []any:
		return typed
	case []map[string]any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	default:
		return nil
	}
}

func firstMapString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := m[key]; ok && value != nil {
			return strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
		}
	}
	return ""
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
