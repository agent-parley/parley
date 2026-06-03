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
		{name: "invalid status rejected", mutate: func(r *Report) { r.Status = "surprised" }, wantErr: true},
		{name: "failed requires errors", mutate: func(r *Report) { r.Status = StatusFailed }, wantErr: true},
		{name: "invalid requires errors", mutate: func(r *Report) { r.Status = StatusInvalid }, wantErr: true},
		{name: "invalid with errors accepted", mutate: func(r *Report) { r.Status = StatusInvalid; r.Errors = []string{"bad"} }},
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
