package orchestrator

import (
	"fmt"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

const (
	NodeIdeaIntake     = contract.StageTypeIdeaIntake
	NodeImplementation = contract.StageTypeImplementation
	NodeValidation     = contract.StageTypeValidation
	NodeCommit         = contract.StageTypeCommit
	NodePRReady        = contract.StageTypePRReady
	NodeDone           = "done"
	NodeStopReport     = "stop_report"
)

type Graph struct {
	edges map[string]string
}

func NewGraph() Graph {
	g := Graph{edges: map[string]string{}}
	g.addStage(contract.StageTypeIdeaIntake, NodeImplementation)
	g.addStage(contract.StageTypeImplementation, NodeValidation)
	g.addStage(contract.StageTypeValidation, NodeCommit)
	g.addStage(contract.StageTypeCommit, NodePRReady)
	g.addStage(contract.StageTypePRReady, NodeDone)
	return g
}

func (g Graph) Next(stageType, status string) (string, error) {
	next, ok := g.edges[key(stageType, status)]
	if !ok {
		return "", fmt.Errorf("no workflow edge for %s.%s", stageType, status)
	}
	return next, nil
}

func (g Graph) Edges() map[string]string {
	out := make(map[string]string, len(g.edges))
	for k, v := range g.edges {
		out[k] = v
	}
	return out
}

func (g Graph) addStage(stageType, completedNext string) {
	g.add(stageType, report.StatusCompleted, completedNext)
	g.add(stageType, report.StatusFailed, NodeStopReport)
	g.add(stageType, report.StatusInvalid, NodeStopReport)
	g.add(stageType, report.StatusNeedsInput, NodeStopReport)
}

func (g Graph) add(stageType, status, next string) { g.edges[key(stageType, status)] = next }

func key(stageType, status string) string { return stageType + "." + status }
