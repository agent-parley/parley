package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/protocol"
	"github.com/agent-parley/parley/internal/shared/report"
)

const defaultReportRepairAttempts = 1

type reportRepairValidator func(report.Report) (report.Report, error)

type reportRepairOptions struct {
	AdapterName     string
	StageType       string
	EmitLifecycle   bool
	LifecycleData   map[string]any
	PreparedSummary string
	StartedSummary  string
	Validator       reportRepairValidator
}

func (e *Engine) dispatchWithReportRepair(ctx context.Context, wr store.WorkflowRun, stage store.Stage, disp contract.Dispatch, opts reportRepairOptions) (report.Report, error) {
	if opts.AdapterName == "" {
		opts.AdapterName = disp.Adapter
	}
	if opts.StageType == "" {
		opts.StageType = disp.StageType
	}
	if opts.PreparedSummary == "" {
		opts.PreparedSummary = "adapter invocation prepared"
	}
	if opts.StartedSummary == "" {
		opts.StartedSummary = "adapter started"
	}
	if opts.Validator == nil {
		opts.Validator = baseReportValidator(wr, stage, opts.StageType, opts.AdapterName)
	}
	if opts.EmitLifecycle {
		if err := e.emitAdapterLifecycle(ctx, wr, stage, opts.AdapterName, opts.PreparedSummary, opts.StartedSummary, opts.LifecycleData); err != nil {
			return report.Report{}, err
		}
	}
	if e.runner == nil {
		return dispatchFailedReport(wr, stage, opts.AdapterName, fmt.Errorf("runner unavailable")), nil
	}
	rep, err := e.runner.Dispatch(ctx, disp)
	if err != nil {
		if errors.Is(err, protocol.ErrSessionClosed) {
			e.markRunnerDown(wr.Run.ID, "runner_disconnected")
		}
		return dispatchFailedReport(wr, stage, opts.AdapterName, err), nil
	}
	return e.validateOrRepairReport(ctx, wr, stage, disp, rep, opts)
}

func (e *Engine) validateOrRepairReport(ctx context.Context, wr store.WorkflowRun, stage store.Stage, disp contract.Dispatch, candidate report.Report, opts reportRepairOptions) (report.Report, error) {
	maxRepairs := defaultReportRepairAttempts
	repairsUsed := 0
	current := candidate
	for {
		validated, validationErr := opts.Validator(current)
		if validationErr == nil && validated.Status != report.StatusInvalid {
			return validated, nil
		}
		if validationErr == nil {
			validationErr = invalidReportStatusError(validated)
		}
		willRepair := repairsUsed < maxRepairs
		if err := e.emitReportInvalid(ctx, wr, stage, opts.AdapterName, validated, validationErr, repairsUsed, maxRepairs, willRepair); err != nil {
			return report.Report{}, err
		}
		if !willRepair {
			return finalInvalidAdapterReport(wr, stage, opts.AdapterName, validated, validationErr, repairsUsed), nil
		}
		repairsUsed++
		repairDisp := reportRepairDispatch(disp, validated, validationErr, repairsUsed, maxRepairs)
		if opts.EmitLifecycle {
			data := cloneRepairInput(opts.LifecycleData)
			data["report_repair"] = true
			data["report_repair_attempt"] = repairsUsed
			data["report_repair_max_attempts"] = maxRepairs
			if err := e.emitAdapterLifecycle(ctx, wr, stage, opts.AdapterName, "report repair invocation prepared", "report repair started", data); err != nil {
				return report.Report{}, err
			}
		}
		repaired, err := e.runner.Dispatch(ctx, repairDisp)
		if err != nil {
			if errors.Is(err, protocol.ErrSessionClosed) {
				e.markRunnerDown(wr.Run.ID, "runner_disconnected")
			}
			return dispatchFailedReport(wr, stage, opts.AdapterName, err), nil
		}
		current = repaired
	}
}

func (e *Engine) emitAdapterLifecycle(ctx context.Context, wr store.WorkflowRun, stage store.Stage, adapterName, preparedSummary, startedSummary string, data map[string]any) error {
	base := cloneRepairInput(data)
	if adapterName != "" {
		base["adapter"] = adapterName
	}
	if _, err := e.emit(ctx, stageEvent(wr, stage, "adapter.invocation_prepared", event.Actor{Kind: event.ActorKindAdapter, ID: adapterName}, preparedSummary, cloneRepairInput(base))); err != nil {
		return err
	}
	_, err := e.emit(ctx, stageEvent(wr, stage, "adapter.started", event.Actor{Kind: event.ActorKindAdapter, ID: adapterName}, startedSummary, cloneRepairInput(base)))
	return err
}

func baseReportValidator(wr store.WorkflowRun, stage store.Stage, stageType, adapterName string) reportRepairValidator {
	return func(rep report.Report) (report.Report, error) {
		stamped := stampReport(wr, stage, stageType, rep)
		if err := stamped.Validate(); err != nil {
			return invalidAdapterReport(wr, stage, adapterName, "adapter returned invalid report", stamped, err), err
		}
		if !validAdapterStatusForStage(stageType, stamped.Status) {
			err := fmt.Errorf("status %q is invalid for stage_type %q", stamped.Status, stageType)
			return invalidAdapterReport(wr, stage, adapterName, "adapter returned invalid report", stamped, err), err
		}
		return stamped, nil
	}
}

func validAdapterStatusForStage(stageType, status string) bool {
	switch status {
	case report.StatusCompleted, report.StatusFailed, report.StatusNeedsInput, report.StatusInvalid:
		return true
	case report.StatusApproved, report.StatusChangesRequested:
		return stageType == contract.StageTypeReview
	default:
		return false
	}
}

func invalidAdapterReport(wr store.WorkflowRun, stage store.Stage, adapterName, summary string, candidate report.Report, validationErr error) report.Report {
	if adapterName == "" {
		adapterName = stage.Adapter
	}
	if summary == "" {
		summary = "adapter returned invalid report"
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: adapterName},
		Status:        report.StatusInvalid,
		Summary:       summary,
		Payload: map[string]any{
			"adapter":        adapterName,
			"invalid_report": reportForRepairInput(candidate),
		},
		Errors: []string{validationErr.Error()},
	}
}

func finalInvalidAdapterReport(wr store.WorkflowRun, stage store.Stage, adapterName string, candidate report.Report, validationErr error, repairsUsed int) report.Report {
	rep := invalidAdapterReport(wr, stage, adapterName, "adapter returned invalid report after bounded repair", candidate, validationErr)
	rep.Payload["report_repair_attempts"] = repairsUsed
	rep.Payload["report_repair_exhausted"] = true
	return rep
}

func invalidReportStatusError(rep report.Report) error {
	if len(rep.Errors) > 0 {
		return errors.New(strings.Join(rep.Errors, "; "))
	}
	if rep.Summary != "" {
		return errors.New(rep.Summary)
	}
	return errors.New("report status is invalid")
}

func (e *Engine) emitReportInvalid(ctx context.Context, wr store.WorkflowRun, stage store.Stage, adapterName string, invalid report.Report, validationErr error, repairsUsed, maxRepairs int, willRepair bool) error {
	data := map[string]any{
		"adapter":                     adapterName,
		"status":                      report.StatusInvalid,
		"validation_error":            validationErr.Error(),
		"invalid_report_status":       invalid.Status,
		"invalid_report_summary":      invalid.Summary,
		"report_repair_attempts_used": repairsUsed,
		"report_repair_max_attempts":  maxRepairs,
		"will_repair":                 willRepair,
	}
	if invalid.Actor.Kind != "" || invalid.Actor.ID != "" {
		data["invalid_report_actor"] = map[string]any{"kind": invalid.Actor.Kind, "id": invalid.Actor.ID}
	}
	_, err := e.emit(ctx, stageEvent(wr, stage, "report.invalid", event.Actor{Kind: event.ActorKindHarness, ID: "manager"}, "adapter report failed schema validation", data))
	return err
}

func reportRepairDispatch(disp contract.Dispatch, invalid report.Report, validationErr error, repairAttempt, maxRepairs int) contract.Dispatch {
	repair := disp
	repair.Input = cloneRepairInput(disp.Input)
	if original, ok := repair.Input["contract_markdown"]; ok {
		repair.Input["original_contract_markdown"] = original
	}
	repair.Input["report_repair"] = true
	repair.Input["report_repair_attempt"] = repairAttempt
	repair.Input["report_repair_max_attempts"] = maxRepairs
	repair.Input["report_validation_error"] = validationErr.Error()
	repair.Input["invalid_report"] = reportForRepairInput(invalid)
	repair.Input["expected_report_schema"] = expectedReportSchema(disp)
	repair.Input["contract_markdown"] = reportRepairContractMarkdown(disp, invalid, validationErr, repairAttempt, maxRepairs)
	return repair
}

func reportRepairContractMarkdown(disp contract.Dispatch, invalid report.Report, validationErr error, repairAttempt, maxRepairs int) string {
	var b strings.Builder
	b.WriteString("Repair the malformed stage report for this same Parley attempt.\n\n")
	b.WriteString("Do not redo the implementation/review/validation work. Do not modify `/project/repo`. Only replace the final report artifact with a schema-valid report for the already-completed stage work.\n\n")
	fmt.Fprintf(&b, "Repair attempt: %d of %d.\n\n", repairAttempt, maxRepairs)
	b.WriteString("## Harness validation error\n\n")
	b.WriteString(validationErr.Error())
	b.WriteString("\n\n## Invalid candidate report\n\n```json\n")
	b.WriteString(jsonBlock(reportForRepairInput(invalid)))
	b.WriteString("\n```\n\n## Expected report envelope\n\n```json\n")
	b.WriteString(jsonBlock(expectedReportSchema(disp)))
	b.WriteString("\n```\n\n")
	if disp.Adapter == "pi" {
		b.WriteString("The Pi adapter stamps `/project/workspace/report.json` into this envelope. Write the stage-specific `/project/workspace/report.json` subset requested in the Required Report section below; include `payload` and `verdict` when that section requires them.\n")
	} else {
		b.WriteString("Return a report that satisfies the envelope fields, status vocabulary, and any stage-specific verdict/payload requirements.\n")
	}
	return b.String()
}

func expectedReportSchema(disp contract.Dispatch) map[string]any {
	schema := map[string]any{
		"schema_version": report.SchemaVersion,
		"run_id":         disp.RunID,
		"task_id":        disp.TaskID,
		"attempt_id":     disp.AttemptID,
		"stage_id":       disp.StageID,
		"stage_type":     disp.StageType,
		"actor":          map[string]any{"kind": "agent", "id": disp.Adapter},
		"status":         []string{report.StatusCompleted, report.StatusFailed, report.StatusNeedsInput, report.StatusInvalid},
		"summary":        "required non-empty summary",
		"evidence_refs":  []string{},
		"payload":        map[string]any{},
		"errors":         []string{"required when status is failed or invalid"},
	}
	if disp.StageType == contract.StageTypeReview {
		schema["status"] = []string{report.StatusCompleted, report.StatusFailed, report.StatusNeedsInput, report.StatusInvalid}
		schema["verdict"] = []string{string(report.ReviewVerdictPass), string(report.ReviewVerdictChangesRequested), string(report.ReviewVerdictBlocked), string(report.ReviewVerdictEscalate), "omit for critic dispatches"}
		schema["payload"] = map[string]any{
			"raw_findings":              []any{},
			"arbitration_decisions":     []any{},
			"accepted_findings":         []any{},
			"residual_risk":             "required for arbiter dispatches",
			"confidence":                "required for arbiter dispatches",
			"critic_report_artifact_id": "required for arbiter dispatches after normalization",
		}
	}
	if disp.Input != nil && disp.Input["input_mode"] == contract.AdapterInputModePlanning {
		if fmt.Sprint(disp.Input["refinement_level"]) == contract.RefinementLevelDeep && fmt.Sprint(disp.Input["force_final_plan"]) != "true" {
			schema["status"] = []string{report.StatusCompleted, report.StatusFailed, report.StatusNeedsInput, report.StatusInvalid}
			schema["payload"] = map[string]any{
				"task_plan_markdown": "required on completed: Markdown task plan containing # Task Plan, the plan-boundary sentence, ## Assumptions, and ## Open Questions",
				"questions":          "required on needs_input: non-empty array of clarifying questions for the human operator",
			}
		} else {
			schema["status"] = []string{report.StatusCompleted, report.StatusFailed, report.StatusInvalid}
			schema["payload"] = map[string]any{
				"task_plan_markdown": "required Markdown task plan containing # Task Plan, the plan-boundary sentence, ## Assumptions, and ## Open Questions",
			}
		}
	}
	return schema
}

func reportForRepairInput(rep report.Report) map[string]any {
	out := map[string]any{
		"schema_version": rep.SchemaVersion,
		"run_id":         rep.RunID,
		"task_id":        rep.TaskID,
		"attempt_id":     rep.AttemptID,
		"stage_id":       rep.StageID,
		"stage_type":     rep.StageType,
		"actor":          map[string]any{"kind": rep.Actor.Kind, "id": rep.Actor.ID},
		"status":         rep.Status,
		"summary":        rep.Summary,
		"evidence_refs":  append([]string{}, rep.EvidenceRefs...),
		"payload":        rep.Payload,
		"errors":         append([]string{}, rep.Errors...),
	}
	if rep.Verdict != nil {
		out["verdict"] = string(*rep.Verdict)
	}
	return out
}

func cloneRepairInput(input map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range input {
		out[key] = value
	}
	return out
}

func jsonBlock(value any) string {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "null"
	}
	return string(content)
}
