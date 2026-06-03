package orchestrator

import (
	"testing"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestGraphRoutesStatuses(t *testing.T) {
	g := NewGraph()
	tests := []struct {
		stage  string
		status string
		want   string
	}{
		{contract.StageTypeIdeaIntake, report.StatusCompleted, NodeImplementation},
		{contract.StageTypeIdeaIntake, report.StatusFailed, NodeStopReport},
		{contract.StageTypeIdeaIntake, report.StatusInvalid, NodeStopReport},
		{contract.StageTypeIdeaIntake, report.StatusNeedsInput, NodeStopReport},
		{contract.StageTypeImplementation, report.StatusCompleted, NodeValidation},
		{contract.StageTypeImplementation, report.StatusFailed, NodeStopReport},
		{contract.StageTypeImplementation, report.StatusInvalid, NodeStopReport},
		{contract.StageTypeImplementation, report.StatusNeedsInput, NodeStopReport},
		{contract.StageTypeValidation, report.StatusCompleted, NodeCommit},
		{contract.StageTypeValidation, report.StatusFailed, NodeStopReport},
		{contract.StageTypeValidation, report.StatusInvalid, NodeStopReport},
		{contract.StageTypeValidation, report.StatusNeedsInput, NodeStopReport},
		{contract.StageTypeCommit, report.StatusCompleted, NodePRReady},
		{contract.StageTypeCommit, report.StatusFailed, NodeStopReport},
		{contract.StageTypeCommit, report.StatusInvalid, NodeStopReport},
		{contract.StageTypeCommit, report.StatusNeedsInput, NodeStopReport},
		{contract.StageTypePRReady, report.StatusCompleted, NodeDone},
		{contract.StageTypePRReady, report.StatusFailed, NodeStopReport},
		{contract.StageTypePRReady, report.StatusInvalid, NodeStopReport},
		{contract.StageTypePRReady, report.StatusNeedsInput, NodeStopReport},
	}
	for _, tt := range tests {
		t.Run(tt.stage+"."+tt.status, func(t *testing.T) {
			got, err := g.Next(tt.stage, tt.status)
			if err != nil {
				t.Fatalf("Next error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}
