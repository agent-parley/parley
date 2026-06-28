package report

import "testing"

func TestValidationOutputValidate(t *testing.T) {
	valid := validValidationOutput()
	valid.Result = ValidationResultFailed
	valid.ChecksRun[0].Status = ValidationCheckFailed
	valid.Failures = []ValidationFailure{{Check: "go test ./...", Message: "tests failed"}}
	valid.Outputs = []ValidationOutputRef{{URI: "file:///tmp/test.log"}}
	valid.Skipped = []ValidationSkippedCheck{{Check: "race test", Reason: "not requested"}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid failed output rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*ValidationOutput)
	}{
		{name: "invalid result", mutate: func(o *ValidationOutput) { o.Result = ValidationResult("blocked") }},
		{name: "missing checks", mutate: func(o *ValidationOutput) { o.ChecksRun = nil }},
		{name: "empty check name", mutate: func(o *ValidationOutput) { o.ChecksRun[0].Name = " " }},
		{name: "invalid check status", mutate: func(o *ValidationOutput) { o.ChecksRun[0].Status = ValidationCheckStatus("flaky") }},
		{name: "unidentified output", mutate: func(o *ValidationOutput) { o.Outputs = []ValidationOutputRef{{Summary: "missing ref"}} }},
		{name: "failed without failures", mutate: func(o *ValidationOutput) { o.Result = ValidationResultFailed }},
		{name: "empty failure message", mutate: func(o *ValidationOutput) {
			o.Result = ValidationResultFailed
			o.Failures = []ValidationFailure{{Message: " "}}
		}},
		{name: "empty skipped check", mutate: func(o *ValidationOutput) { o.Skipped = []ValidationSkippedCheck{{Check: " ", Reason: "not needed"}} }},
		{name: "empty skipped reason", mutate: func(o *ValidationOutput) { o.Skipped = []ValidationSkippedCheck{{Check: "race test", Reason: " "}} }},
		{name: "invalid confidence", mutate: func(o *ValidationOutput) { o.Confidence = ValidationConfidence("certain") }},
		{name: "missing suggested next action", mutate: func(o *ValidationOutput) { o.SuggestedNextAction = " " }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := validValidationOutput()
			tt.mutate(&out)
			if err := out.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestValidationOutputFromPayload(t *testing.T) {
	valid := validValidationOutput()
	mapPayload := map[string]any{ValidationOutputPayloadKey: map[string]any{
		"result":                "passed",
		"checks_run":            []any{map[string]any{"name": "go test ./...", "status": "passed"}},
		"outputs":               []any{map[string]any{"id": "artifact_diff"}},
		"failures":              []any{},
		"skipped":               []any{},
		"env_notes":             []any{"network=none"},
		"confidence":            "high",
		"suggested_next_action": "continue",
	}}
	if out, err := ValidationOutputFromPayload(mapPayload); err != nil || out.Result != ValidationResultPassed {
		t.Fatalf("map payload parse = %+v, %v", out, err)
	}
	if out, err := ValidationOutputFromPayload(map[string]any{ValidationOutputPayloadKey: &valid}); err != nil || out.Result != ValidationResultPassed {
		t.Fatalf("pointer payload parse = %+v, %v", out, err)
	}

	tests := []struct {
		name    string
		payload map[string]any
	}{
		{name: "nil payload", payload: nil},
		{name: "missing key", payload: map[string]any{}},
		{name: "nil raw", payload: map[string]any{ValidationOutputPayloadKey: nil}},
		{name: "nil pointer", payload: map[string]any{ValidationOutputPayloadKey: (*ValidationOutput)(nil)}},
		{name: "marshal failure", payload: map[string]any{ValidationOutputPayloadKey: map[string]any{"bad": func() {}}}},
		{name: "parse failure", payload: map[string]any{ValidationOutputPayloadKey: "not an object"}},
		{name: "schema failure", payload: map[string]any{ValidationOutputPayloadKey: map[string]any{"result": "failed"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ValidationOutputFromPayload(tt.payload); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
