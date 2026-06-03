package orchestrator

import (
	"testing"

	"github.com/agent-parley/parley/internal/shared/report"
)

func TestCompletionEventType(t *testing.T) {
	cases := []struct {
		name   string
		actor  string
		status string
		want   string
	}{
		{"agent completed", report.ActorKindAgent, report.StatusCompleted, "adapter.completed"},
		{"agent failed", report.ActorKindAgent, report.StatusFailed, "adapter.failed"},
		{"agent invalid", report.ActorKindAgent, report.StatusInvalid, "adapter.failed"},
		{"harness completed", report.ActorKindHarness, report.StatusCompleted, "harness.completed"},
		{"harness failed", report.ActorKindHarness, report.StatusFailed, "harness.failed"},
		{"harness invalid", report.ActorKindHarness, report.StatusInvalid, "harness.failed"},
		{"human completed", report.ActorKindHuman, report.StatusCompleted, "task.completed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := report.Report{Actor: report.Actor{Kind: tc.actor, ID: "x"}, Status: tc.status}
			if got := completionEventType(rep); got != tc.want {
				t.Fatalf("completionEventType(%s,%s) = %q, want %q", tc.actor, tc.status, got, tc.want)
			}
		})
	}
}
