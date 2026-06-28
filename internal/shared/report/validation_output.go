package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const ValidationOutputPayloadKey = "validation_output"

type ValidationResult string

const (
	ValidationResultPassed       ValidationResult = "passed"
	ValidationResultFailed       ValidationResult = "failed"
	ValidationResultInconclusive ValidationResult = "inconclusive"
)

type ValidationCheckStatus string

const (
	ValidationCheckPassed       ValidationCheckStatus = "passed"
	ValidationCheckFailed       ValidationCheckStatus = "failed"
	ValidationCheckSkipped      ValidationCheckStatus = "skipped"
	ValidationCheckInconclusive ValidationCheckStatus = "inconclusive"
)

type ValidationConfidence string

const (
	ValidationConfidenceHigh    ValidationConfidence = "high"
	ValidationConfidenceMedium  ValidationConfidence = "medium"
	ValidationConfidenceLow     ValidationConfidence = "low"
	ValidationConfidenceUnknown ValidationConfidence = "unknown"
)

// ValidationOutput is the typed evidence payload required for validation-stage
// reports. Reports carry it at Payload[ValidationOutputPayloadKey].
type ValidationOutput struct {
	Result              ValidationResult         `json:"result"`
	ChecksRun           []ValidationCheck        `json:"checks_run"`
	Outputs             []ValidationOutputRef    `json:"outputs"`
	Failures            []ValidationFailure      `json:"failures"`
	Skipped             []ValidationSkippedCheck `json:"skipped"`
	EnvNotes            []string                 `json:"env_notes"`
	Confidence          ValidationConfidence     `json:"confidence"`
	SuggestedNextAction string                   `json:"suggested_next_action"`
}

type ValidationCheck struct {
	Name       string                `json:"name"`
	Status     ValidationCheckStatus `json:"status,omitempty"`
	Command    string                `json:"command,omitempty"`
	Summary    string                `json:"summary,omitempty"`
	OutputRefs []string              `json:"output_refs,omitempty"`
}

type ValidationOutputRef struct {
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Kind      string `json:"kind,omitempty"`
	MediaType string `json:"media_type,omitempty"`
	URI       string `json:"uri,omitempty"`
	Summary   string `json:"summary,omitempty"`
}

type ValidationFailure struct {
	Check      string   `json:"check,omitempty"`
	Message    string   `json:"message"`
	Severity   string   `json:"severity,omitempty"`
	OutputRefs []string `json:"output_refs,omitempty"`
}

type ValidationSkippedCheck struct {
	Check  string `json:"check"`
	Reason string `json:"reason"`
}

func (o ValidationOutput) Validate() error {
	var errs []error
	if !validValidationResult(o.Result) {
		errs = append(errs, fmt.Errorf("result must be one of %q, %q, or %q", ValidationResultPassed, ValidationResultFailed, ValidationResultInconclusive))
	}
	if len(o.ChecksRun) == 0 {
		errs = append(errs, errors.New("checks_run must contain at least one check"))
	}
	for i, check := range o.ChecksRun {
		if strings.TrimSpace(check.Name) == "" {
			errs = append(errs, fmt.Errorf("checks_run[%d].name is required", i))
		}
		if check.Status != "" && !validValidationCheckStatus(check.Status) {
			errs = append(errs, fmt.Errorf("checks_run[%d].status %q is invalid", i, check.Status))
		}
	}
	for i, output := range o.Outputs {
		if strings.TrimSpace(output.ID) == "" && strings.TrimSpace(output.URI) == "" && strings.TrimSpace(output.Name) == "" {
			errs = append(errs, fmt.Errorf("outputs[%d] must identify an artifact, URI, or name", i))
		}
	}
	if o.Result == ValidationResultFailed && len(o.Failures) == 0 {
		errs = append(errs, errors.New("failures must contain at least one failure when result=failed"))
	}
	for i, failure := range o.Failures {
		if strings.TrimSpace(failure.Message) == "" {
			errs = append(errs, fmt.Errorf("failures[%d].message is required", i))
		}
	}
	for i, skipped := range o.Skipped {
		if strings.TrimSpace(skipped.Check) == "" {
			errs = append(errs, fmt.Errorf("skipped[%d].check is required", i))
		}
		if strings.TrimSpace(skipped.Reason) == "" {
			errs = append(errs, fmt.Errorf("skipped[%d].reason is required", i))
		}
	}
	if !validValidationConfidence(o.Confidence) {
		errs = append(errs, fmt.Errorf("confidence must be one of %q, %q, %q, or %q", ValidationConfidenceHigh, ValidationConfidenceMedium, ValidationConfidenceLow, ValidationConfidenceUnknown))
	}
	if strings.TrimSpace(o.SuggestedNextAction) == "" {
		errs = append(errs, errors.New("suggested_next_action is required"))
	}
	return errors.Join(errs...)
}

func ValidationOutputFromPayload(payload map[string]any) (ValidationOutput, error) {
	if payload == nil {
		return ValidationOutput{}, fmt.Errorf("%s is required", ValidationOutputPayloadKey)
	}
	raw, ok := payload[ValidationOutputPayloadKey]
	if !ok || raw == nil {
		return ValidationOutput{}, fmt.Errorf("%s is required", ValidationOutputPayloadKey)
	}
	var out ValidationOutput
	switch typed := raw.(type) {
	case ValidationOutput:
		out = typed
	case *ValidationOutput:
		if typed == nil {
			return ValidationOutput{}, fmt.Errorf("%s is required", ValidationOutputPayloadKey)
		}
		out = *typed
	default:
		content, err := json.Marshal(raw)
		if err != nil {
			return ValidationOutput{}, fmt.Errorf("marshal %s: %w", ValidationOutputPayloadKey, err)
		}
		if err := json.Unmarshal(content, &out); err != nil {
			return ValidationOutput{}, fmt.Errorf("parse %s: %w", ValidationOutputPayloadKey, err)
		}
	}
	if err := out.Validate(); err != nil {
		return ValidationOutput{}, fmt.Errorf("%s is invalid: %w", ValidationOutputPayloadKey, err)
	}
	return out, nil
}

func validValidationResult(result ValidationResult) bool {
	switch result {
	case ValidationResultPassed, ValidationResultFailed, ValidationResultInconclusive:
		return true
	default:
		return false
	}
}

func validValidationCheckStatus(status ValidationCheckStatus) bool {
	switch status {
	case ValidationCheckPassed, ValidationCheckFailed, ValidationCheckSkipped, ValidationCheckInconclusive:
		return true
	default:
		return false
	}
}

func validValidationConfidence(confidence ValidationConfidence) bool {
	switch confidence {
	case ValidationConfidenceHigh, ValidationConfidenceMedium, ValidationConfidenceLow, ValidationConfidenceUnknown:
		return true
	default:
		return false
	}
}
