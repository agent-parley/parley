package orchestrator

import (
	"fmt"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

func validationReport(wr store.WorkflowRun, prior report.Report) report.Report {
	status := report.StatusCompleted
	summary := "validation gate completed"
	errors := []string{}
	payload := map[string]any{
		"checked_stage_id": prior.StageID,
		"checked_status":   prior.Status,
	}
	if prior.Status != report.StatusCompleted {
		status = report.StatusInvalid
		summary = "validation gate rejected non-completed implementation report"
		errors = append(errors, fmt.Sprintf("implementation status was %s", prior.Status))
	}
	if prior.Payload == nil {
		status = report.StatusInvalid
		summary = "validation gate rejected empty implementation payload"
		errors = append(errors, "implementation payload was empty")
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       wr.ValidationStage.ID,
		StageType:     contract.StageTypeValidation,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "validation_gate"},
		Status:        status,
		Summary:       summary,
		EvidenceRefs:  []string{},
		Payload:       payload,
		Errors:        errors,
	}
}
