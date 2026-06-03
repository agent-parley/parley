package orchestrator

import (
	"fmt"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

const (
	NodeImplementation = contract.StageTypeImplementation
	NodeValidation     = contract.StageTypeValidation
	NodeDone           = "done"
	NodeStopReport     = "stop_report"
)

type Graph struct {
	edges map[string]string
}

func NewGraph() Graph {
	g := Graph{edges: map[string]string{}}
	g.add(contract.StageTypeImplementation, report.StatusCompleted, NodeValidation)
	g.add(contract.StageTypeImplementation, report.StatusFailed, NodeStopReport)
	g.add(contract.StageTypeImplementation, report.StatusInvalid, NodeStopReport)
	g.add(contract.StageTypeImplementation, report.StatusNeedsInput, NodeStopReport)
	g.add(contract.StageTypeValidation, report.StatusCompleted, NodeDone)
	g.add(contract.StageTypeValidation, report.StatusFailed, NodeStopReport)
	g.add(contract.StageTypeValidation, report.StatusInvalid, NodeStopReport)
	g.add(contract.StageTypeValidation, report.StatusNeedsInput, NodeStopReport)
	return g
}

func (g Graph) Next(stageType, status string) (string, error) {
	next, ok := g.edges[key(stageType, status)]
	if !ok {
		return "", fmt.Errorf("no workflow edge for %s.%s", stageType, status)
	}
	return next, nil
}

func (g Graph) add(stageType, status, next string) { g.edges[key(stageType, status)] = next }

func key(stageType, status string) string { return stageType + "." + status }
