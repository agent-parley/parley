package report

import (
	"testing"

	"github.com/agent-parley/parley/internal/shared/contract"
)

func TestReportValidate(t *testing.T) {
	base := Report{
		SchemaVersion: SchemaVersion,
		RunID:         "run_1",
		TaskID:        "task_1",
		AttemptID:     "attempt_1",
		StageID:       "stage_1",
		StageType:     contract.StageTypeImplementation,
		Actor:         Actor{Kind: ActorKindAgent, ID: "noop"},
		Status:        StatusCompleted,
		Summary:       "done",
		Payload:       map[string]any{},
		Errors:        []string{},
	}
	tests := []struct {
		name    string
		mutate  func(*Report)
		wantErr bool
	}{
		{name: "valid completed"},
		{name: "valid changes requested", mutate: func(r *Report) { r.Status = StatusChangesRequested }},
		{name: "invalid status rejected", mutate: func(r *Report) { r.Status = "surprised" }, wantErr: true},
		{name: "invalid stage type rejected", mutate: func(r *Report) { r.StageType = "deploy" }, wantErr: true},
		{name: "invalid actor rejected", mutate: func(r *Report) { r.Actor.Kind = "robot" }, wantErr: true},
		{name: "failed requires errors", mutate: func(r *Report) { r.Status = StatusFailed }, wantErr: true},
		{name: "invalid requires errors", mutate: func(r *Report) { r.Status = StatusInvalid }, wantErr: true},
		{name: "invalid with errors accepted", mutate: func(r *Report) { r.Status = StatusInvalid; r.Errors = []string{"bad"} }},
		{name: "valid review verdict", mutate: func(r *Report) {
			r.StageType = contract.StageTypeReview
			verdict := ReviewVerdictChangesRequested
			r.Verdict = &verdict
		}},
		{name: "invalid review verdict rejected", mutate: func(r *Report) {
			r.StageType = contract.StageTypeReview
			verdict := Verdict("request_fix")
			r.Verdict = &verdict
		}, wantErr: true},
		{name: "non-review verdict rejected", mutate: func(r *Report) { verdict := ReviewVerdictPass; r.Verdict = &verdict }, wantErr: true},
		{name: "valid validation output", mutate: func(r *Report) {
			r.StageType = contract.StageTypeValidation
			r.Payload = map[string]any{ValidationOutputPayloadKey: validValidationOutput()}
		}},
		{name: "validation output required for validation stages", mutate: func(r *Report) {
			r.StageType = contract.StageTypeValidation
		}, wantErr: true},
		{name: "malformed validation output rejected", mutate: func(r *Report) {
			r.StageType = contract.StageTypeValidation
			r.Payload = map[string]any{ValidationOutputPayloadKey: map[string]any{"result": "failed", "checks_run": []any{map[string]any{"name": "go test", "status": "failed"}}, "confidence": "high", "suggested_next_action": "fix tests"}}
		}, wantErr: true},
		{name: "failed validation output accepted", mutate: func(r *Report) {
			r.StageType = contract.StageTypeValidation
			r.Status = StatusFailed
			r.Payload = map[string]any{ValidationOutputPayloadKey: failedValidationOutput()}
			r.Errors = []string{"tests failed"}
		}},
		{name: "needs input validation output accepted", mutate: func(r *Report) {
			r.StageType = contract.StageTypeValidation
			r.Status = StatusNeedsInput
			out := validValidationOutput()
			out.Result = ValidationResultInconclusive
			r.Payload = map[string]any{ValidationOutputPayloadKey: out}
		}},
		{name: "invalid validation output accepted when result failed", mutate: func(r *Report) {
			r.StageType = contract.StageTypeValidation
			r.Status = StatusInvalid
			r.Payload = map[string]any{ValidationOutputPayloadKey: failedValidationOutput()}
			r.Errors = []string{"bad report"}
		}},
		{name: "completed validation rejects failed result", mutate: func(r *Report) {
			r.StageType = contract.StageTypeValidation
			r.Payload = map[string]any{ValidationOutputPayloadKey: failedValidationOutput()}
		}, wantErr: true},
		{name: "failed validation rejects inconclusive result", mutate: func(r *Report) {
			r.StageType = contract.StageTypeValidation
			r.Status = StatusFailed
			out := validValidationOutput()
			out.Result = ValidationResultInconclusive
			r.Payload = map[string]any{ValidationOutputPayloadKey: out}
			r.Errors = []string{"inconclusive"}
		}, wantErr: true},
		{name: "needs input validation rejects failed result", mutate: func(r *Report) {
			r.StageType = contract.StageTypeValidation
			r.Status = StatusNeedsInput
			r.Payload = map[string]any{ValidationOutputPayloadKey: failedValidationOutput()}
		}, wantErr: true},
		{name: "invalid validation rejects passed result", mutate: func(r *Report) {
			r.StageType = contract.StageTypeValidation
			r.Status = StatusInvalid
			r.Payload = map[string]any{ValidationOutputPayloadKey: validValidationOutput()}
			r.Errors = []string{"bad report"}
		}, wantErr: true},
		{name: "valid memory update output", mutate: func(r *Report) {
			r.StageType = contract.StageTypeMemoryUpdate
			r.Payload = map[string]any{MemoryUpdateOutputPayloadKey: validMemoryUpdateOutput()}
		}},
		{name: "memory update output required for completed memory stages", mutate: func(r *Report) {
			r.StageType = contract.StageTypeMemoryUpdate
		}, wantErr: true},
		{name: "malformed memory update output rejected", mutate: func(r *Report) {
			r.StageType = contract.StageTypeMemoryUpdate
			r.Payload = map[string]any{MemoryUpdateOutputPayloadKey: map[string]any{"applied": []any{}}}
		}, wantErr: true},
		{name: "failed memory update may omit output", mutate: func(r *Report) {
			r.StageType = contract.StageTypeMemoryUpdate
			r.Status = StatusFailed
			r.Errors = []string{"curator failed"}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep := base
			if tt.mutate != nil {
				tt.mutate(&rep)
			}
			err := rep.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestMemoryUpdateOutputFromPayload(t *testing.T) {
	out := validMemoryUpdateOutput()
	parsed, err := MemoryUpdateOutputFromPayload(map[string]any{MemoryUpdateOutputPayloadKey: out})
	if err != nil {
		t.Fatalf("MemoryUpdateOutputFromPayload() error = %v", err)
	}
	if len(parsed.Applied) != 1 || len(parsed.Merged) != 1 || parsed.Applied[0].EntryID == "" {
		t.Fatalf("parsed output = %#v", parsed)
	}
	if _, err := MemoryUpdateOutputFromPayload(map[string]any{MemoryUpdateOutputPayloadKey: map[string]any{"applied": []any{}}}); err == nil {
		t.Fatalf("expected malformed memory_update_output to fail validation")
	}
}

func TestMemoryUpdateOutputFromPayloadNormalizesMemoryChangeSourceRefs(t *testing.T) {
	out := validMemoryUpdateOutput()
	out.MemoryChanges[0].SourceArtifactRefs = nil
	out.MemoryChanges[0].Freshness.SourceArtifactRefs = []string{"artifact_report", "artifact_report", " "}
	parsed, err := MemoryUpdateOutputFromPayload(map[string]any{MemoryUpdateOutputPayloadKey: out})
	if err != nil {
		t.Fatalf("MemoryUpdateOutputFromPayload() error = %v", err)
	}
	if got := parsed.MemoryChanges[0].SourceArtifactRefs; len(got) != 1 || got[0] != "artifact_report" {
		t.Fatalf("memory change source refs = %#v, want normalized artifact_report", got)
	}
}

func validMemoryUpdateOutput() MemoryUpdateOutput {
	return MemoryUpdateOutput{
		InboxSummary: MemoryInboxSummary{LearningOpportunities: 2, CandidatesGenerated: 2, CandidatesCurated: 2, SourceArtifactRefs: []string{"artifact_report"}},
		Applied: []MemoryCandidateDecision{{
			CandidateID:        "candidate-001",
			CandidateIDs:       []string{"candidate-001"},
			State:              MemoryCandidateApplied,
			Kind:               "lesson",
			Title:              "Reusable lesson",
			Body:               "Persist a durable lesson only after curation.",
			Rationale:          "useful and source-linked",
			EntryID:            "memory_1",
			SourceArtifactRefs: []string{"artifact_report"},
			Freshness:          MemoryFreshness{SourceRunID: "run_1", SourceTaskID: "task_1", SourceArtifactRefs: []string{"artifact_report"}, UpdatedAt: "2026-06-28T00:00:00Z"},
		}},
		Rejected: []MemoryCandidateDecision{},
		Edited:   []MemoryCandidateDecision{},
		Merged: []MemoryCandidateDecision{{
			CandidateIDs:       []string{"candidate-002", "candidate-003"},
			State:              MemoryCandidateMerged,
			Kind:               "gotcha",
			Title:              "Merged gotcha",
			Body:               "Merge overlapping candidates into one memory entry.",
			Rationale:          "overlapping candidates",
			EntryID:            "memory_2",
			SourceArtifactRefs: []string{"artifact_report"},
			Freshness:          MemoryFreshness{SourceRunID: "run_1", SourceTaskID: "task_1", SourceArtifactRefs: []string{"artifact_report"}, UpdatedAt: "2026-06-28T00:00:00Z"},
		}},
		Deferred: []MemoryCandidateDecision{},
		MemoryChanges: []MemoryChange{{
			Action:             MemoryChangeApplied,
			EntryID:            "memory_1",
			CandidateIDs:       []string{"candidate-001"},
			Kind:               "lesson",
			Title:              "Reusable lesson",
			SourceArtifactRefs: []string{"artifact_report"},
			Freshness:          MemoryFreshness{SourceRunID: "run_1", SourceTaskID: "task_1", SourceArtifactRefs: []string{"artifact_report"}, UpdatedAt: "2026-06-28T00:00:00Z"},
		}},
		ActorAuthority:    MemoryActorAuthority{Kind: ActorKindAgent, ID: "pi", Authority: "agent curator approved automatically"},
		SafetyNotes:       []string{},
		StopReportSummary: "project memory update completed",
	}
}

func validValidationOutput() ValidationOutput {
	return ValidationOutput{
		Result: ValidationResultPassed,
		ChecksRun: []ValidationCheck{{
			Name:    "go test ./...",
			Status:  ValidationCheckPassed,
			Command: "go test ./...",
		}},
		Outputs:             []ValidationOutputRef{{ID: "artifact_diff", Name: "diff.patch", Kind: "diff_patch"}},
		Failures:            []ValidationFailure{},
		Skipped:             []ValidationSkippedCheck{},
		EnvNotes:            []string{"network=none"},
		Confidence:          ValidationConfidenceHigh,
		SuggestedNextAction: "continue",
	}
}

func failedValidationOutput() ValidationOutput {
	return ValidationOutput{
		Result: ValidationResultFailed,
		ChecksRun: []ValidationCheck{{
			Name:    "go test ./...",
			Status:  ValidationCheckFailed,
			Command: "go test ./...",
		}},
		Outputs:             []ValidationOutputRef{{ID: "artifact_log", Name: "test.log", Kind: "log"}},
		Failures:            []ValidationFailure{{Check: "go test ./...", Message: "tests failed"}},
		Skipped:             []ValidationSkippedCheck{},
		EnvNotes:            []string{"network=none"},
		Confidence:          ValidationConfidenceMedium,
		SuggestedNextAction: "fix tests",
	}
}
