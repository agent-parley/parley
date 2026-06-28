package orchestrator

import (
	"fmt"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

func withValidationFailureOutput(stageType string, payload map[string]any, check, summary string, err error) map[string]any {
	if stageType != contract.StageTypeValidation {
		return payload
	}
	if payload == nil {
		payload = map[string]any{}
	}
	if _, ok := payload[report.ValidationOutputPayloadKey]; ok {
		return payload
	}
	message := summary
	if err != nil {
		message = err.Error()
	}
	if message == "" {
		message = "validation stage failed"
	}
	if check == "" {
		check = "validation stage"
	}
	payload[report.ValidationOutputPayloadKey] = report.ValidationOutput{
		Result: report.ValidationResultFailed,
		ChecksRun: []report.ValidationCheck{{
			Name:    check,
			Status:  report.ValidationCheckFailed,
			Summary: summary,
		}},
		Outputs:             []report.ValidationOutputRef{},
		Failures:            []report.ValidationFailure{{Check: check, Message: message, Severity: "error"}},
		Skipped:             []report.ValidationSkippedCheck{},
		EnvNotes:            []string{},
		Confidence:          report.ValidationConfidenceLow,
		SuggestedNextAction: fmt.Sprintf("inspect %s before trusting validation evidence", check),
	}
	return payload
}
