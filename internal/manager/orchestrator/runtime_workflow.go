package orchestrator

import (
	"fmt"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
)

type runtimeWorkflow struct {
	Template workflow.Template
	Stages   []runtimeStage
	ByID     map[string]runtimeStage
	Graph    runtimeGraph
}

type runtimeStage struct {
	Template workflow.StageTemplate
	Stage    store.Stage
}

type runtimeGraph struct {
	start  string
	stopID string
	edges  map[string]string
}

func newRuntimeWorkflow(template workflow.Template, stages []store.Stage) (runtimeWorkflow, error) {
	template = workflow.NormalizeTemplate(template)
	if err := workflow.ValidateTemplate(template); err != nil {
		return runtimeWorkflow{}, err
	}
	graph, err := newRuntimeGraph(template)
	if err != nil {
		return runtimeWorkflow{}, err
	}
	byWorkflowID := map[string]store.Stage{}
	unusedByType := map[string][]store.Stage{}
	for _, stage := range stages {
		if stage.WorkflowStageID != "" {
			byWorkflowID[stage.WorkflowStageID] = stage
		} else {
			unusedByType[stage.StageType] = append(unusedByType[stage.StageType], stage)
		}
	}
	runtimeStages := make([]runtimeStage, 0, len(template.Stages))
	byID := map[string]runtimeStage{}
	for _, templateStage := range template.Stages {
		stage, ok := byWorkflowID[templateStage.ID]
		if !ok {
			candidates := unusedByType[templateStage.Type]
			if len(candidates) > 0 {
				stage = candidates[0]
				stage.WorkflowStageID = templateStage.ID
				unusedByType[templateStage.Type] = candidates[1:]
				ok = true
			}
		}
		if !ok {
			return runtimeWorkflow{}, fmt.Errorf("workflow stage %q (%s) has no persisted runtime stage", templateStage.ID, templateStage.Type)
		}
		runtimeStage := runtimeStage{Template: templateStage, Stage: stage}
		runtimeStages = append(runtimeStages, runtimeStage)
		byID[templateStage.ID] = runtimeStage
	}
	return runtimeWorkflow{Template: template, Stages: runtimeStages, ByID: byID, Graph: graph}, nil
}

func newRuntimeGraph(template workflow.Template) (runtimeGraph, error) {
	if len(template.Stages) == 0 {
		return runtimeGraph{}, fmt.Errorf("workflow template has no stages")
	}
	stageIDs := map[string]bool{}
	startID := ""
	stopID := ""
	for _, stage := range template.Stages {
		stageIDs[stage.ID] = true
		if stage.Type == workflow.StageTypeIdeaRefinement && startID == "" {
			startID = stage.ID
		}
		if stage.Type == workflow.StageTypeStopReport && stopID == "" {
			stopID = stage.ID
		}
	}
	edges := map[string]string{}
	for _, edge := range template.Edges {
		if !stageIDs[edge.From] || !stageIDs[edge.To] {
			return runtimeGraph{}, fmt.Errorf("workflow edge %q -> %q references an unknown stage", edge.From, edge.To)
		}
		key := runtimeEdgeKey(edge.From, edge.On)
		if _, exists := edges[key]; exists {
			return runtimeGraph{}, fmt.Errorf("duplicate workflow edge for %s on %s", edge.From, edge.On)
		}
		edges[key] = edge.To
	}
	return runtimeGraph{start: startID, stopID: stopID, edges: edges}, nil
}

func (g runtimeGraph) Start() string { return g.start }

func (g runtimeGraph) StopID() string { return g.stopID }

func (g runtimeGraph) Next(workflowStageID, status string) (string, bool) {
	if next, ok := g.edges[runtimeEdgeKey(workflowStageID, status)]; ok {
		return next, true
	}
	if status == workflow.OnApproved {
		next, ok := g.edges[runtimeEdgeKey(workflowStageID, workflow.OnCompleted)]
		return next, ok
	}
	return "", false
}

func (g runtimeGraph) Edges() map[string]string {
	out := make(map[string]string, len(g.edges))
	for key, value := range g.edges {
		out[key] = value
	}
	return out
}

func runtimeEdgeKey(workflowStageID, status string) string {
	return workflowStageID + "." + status
}
