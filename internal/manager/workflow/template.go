package workflow

import (
	"errors"
	"fmt"
	"strings"
)

const (
	SchemaVersion = 1

	BalancedPRDeliveryID   = "balanced_pr_delivery"
	AutonomousPRDeliveryID = "autonomous_pr_delivery"
	CarefulReviewID        = "careful_review_delivery"
	DirectCommitID         = "direct_commit_delivery"

	DefaultTemplateID = BalancedPRDeliveryID
)

const (
	StageTypeIdeaRefinement = "idea_refinement"
	StageTypeReview         = "review"
	StageTypeImplementation = "implementation"
	StageTypeValidation     = "validation"
	StageTypeCommit         = "commit"
	StageTypePRCreation     = "pr_creation"
	StageTypeMemoryUpdate   = "memory_update"
	StageTypeStopReport     = "stop_report"
)

const (
	ActorHarness = "harness"
	ActorAgent   = "agent"
	ActorHuman   = "human"
)

const (
	TargetPlan        = "plan"
	TargetCodeChanges = "code_changes"
)

const (
	OnCompleted        = "completed"
	OnApproved         = "approved"
	OnChangesRequested = "changes_requested"
)

type Template struct {
	SchemaVersion int             `json:"schema_version"`
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Description   string          `json:"description"`
	Predefined    bool            `json:"predefined"`
	Recommended   bool            `json:"recommended"`
	Editable      bool            `json:"editable"`
	Stages        []StageTemplate `json:"stages"`
	Edges         []Edge          `json:"edges"`
	Settings      map[string]any  `json:"settings,omitempty"`
}

type StageTemplate struct {
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Label    string         `json:"label"`
	Actor    string         `json:"actor"`
	Target   string         `json:"target,omitempty"`
	Settings map[string]any `json:"settings,omitempty"`
}

type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
	On   string `json:"on"`
}

func PredefinedTemplates() []Template {
	return []Template{
		balancedPRDelivery(),
		autonomousPRDelivery(),
		carefulReviewDelivery(),
		directCommitDelivery(),
	}
}

func DefaultTemplate() Template { return balancedPRDelivery() }

func NormalizeTemplate(template Template) Template {
	template.ID = strings.TrimSpace(template.ID)
	template.Name = strings.TrimSpace(template.Name)
	template.Description = strings.TrimSpace(template.Description)
	if template.SchemaVersion == 0 {
		template.SchemaVersion = SchemaVersion
	}
	for i := range template.Stages {
		stage := &template.Stages[i]
		stage.ID = strings.TrimSpace(stage.ID)
		stage.Type = strings.TrimSpace(stage.Type)
		stage.Label = strings.TrimSpace(stage.Label)
		stage.Actor = strings.TrimSpace(stage.Actor)
		stage.Target = strings.TrimSpace(stage.Target)
	}
	for i := range template.Edges {
		edge := &template.Edges[i]
		edge.From = strings.TrimSpace(edge.From)
		edge.To = strings.TrimSpace(edge.To)
		edge.On = strings.TrimSpace(edge.On)
	}
	return template
}

func ValidateTemplate(template Template) error {
	template = NormalizeTemplate(template)
	var errs []error
	if template.SchemaVersion != SchemaVersion {
		errs = append(errs, fmt.Errorf("schema_version must be %d", SchemaVersion))
	}
	if template.ID == "" {
		errs = append(errs, errors.New("id is required"))
	}
	if template.Name == "" {
		errs = append(errs, errors.New("name is required"))
	}
	if len(template.Stages) == 0 {
		errs = append(errs, errors.New("at least one stage is required"))
	}
	stageIDs := map[string]bool{}
	for _, stage := range template.Stages {
		if stage.ID == "" {
			errs = append(errs, errors.New("stage id is required"))
			continue
		}
		if stageIDs[stage.ID] {
			errs = append(errs, fmt.Errorf("stage id %q is duplicated", stage.ID))
		}
		stageIDs[stage.ID] = true
		if !validStageType(stage.Type) {
			errs = append(errs, fmt.Errorf("stage %q type %q is invalid", stage.ID, stage.Type))
		}
		if !validActor(stage.Actor) {
			errs = append(errs, fmt.Errorf("stage %q actor %q is invalid", stage.ID, stage.Actor))
		}
	}
	for _, edge := range template.Edges {
		if edge.From == "" || edge.To == "" || edge.On == "" {
			errs = append(errs, errors.New("edge from, to, and on are required"))
			continue
		}
		if !stageIDs[edge.From] {
			errs = append(errs, fmt.Errorf("edge from %q does not reference a stage", edge.From))
		}
		if !stageIDs[edge.To] {
			errs = append(errs, fmt.Errorf("edge to %q does not reference a stage", edge.To))
		}
	}
	return errors.Join(errs...)
}

func validStageType(stageType string) bool {
	switch stageType {
	case StageTypeIdeaRefinement,
		StageTypeReview,
		StageTypeImplementation,
		StageTypeValidation,
		StageTypeCommit,
		StageTypePRCreation,
		StageTypeMemoryUpdate,
		StageTypeStopReport:
		return true
	default:
		return false
	}
}

func validActor(actor string) bool {
	switch actor {
	case ActorHarness, ActorAgent, ActorHuman:
		return true
	default:
		return false
	}
}

func balancedPRDelivery() Template {
	stages := []StageTemplate{
		stage("idea_refinement", StageTypeIdeaRefinement, "Idea refinement", ActorHarness, ""),
		stage("plan_review_human", StageTypeReview, "Plan review", ActorHuman, TargetPlan),
		stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""),
		stage("validation", StageTypeValidation, "Validation", ActorHarness, ""),
		stage("change_review_agent", StageTypeReview, "Code review", ActorAgent, TargetCodeChanges),
		stage("commit_feature_branch", StageTypeCommit, "Commit to feature branch", ActorHarness, ""),
		stage("pr_creation", StageTypePRCreation, "PR creation", ActorHarness, ""),
		stage("stop_report", StageTypeStopReport, "Stop/report", ActorHarness, ""),
	}
	return predefined(BalancedPRDeliveryID, "Balanced PR Delivery", "Recommended branch-and-PR workflow with human plan review and agent code review.", true, stages, linearEdges(stages), map[string]any{
		"branch_policy": "feature_branch",
		"pr_behavior":   "create_pr",
		"merge_policy":  "human_stop",
	})
}

func autonomousPRDelivery() Template {
	stages := []StageTemplate{
		stage("idea_refinement", StageTypeIdeaRefinement, "Idea refinement", ActorHarness, ""),
		stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""),
		stage("validation", StageTypeValidation, "Validation", ActorHarness, ""),
		stage("change_review_agent", StageTypeReview, "Code review", ActorAgent, TargetCodeChanges),
		stage("commit_feature_branch", StageTypeCommit, "Commit to feature branch", ActorHarness, ""),
		stage("pr_creation", StageTypePRCreation, "PR creation", ActorHarness, ""),
		stage("memory_update", StageTypeMemoryUpdate, "Memory update", ActorAgent, ""),
		stage("stop_report", StageTypeStopReport, "Stop/report", ActorHarness, ""),
	}
	edges := linearEdges(stages)
	edges = append(edges, Edge{From: "change_review_agent", To: "implementation", On: OnChangesRequested})
	return predefined(AutonomousPRDeliveryID, "Autonomous PR Delivery", "Unattended PR workflow with agent review, fix loop, and memory update.", false, stages, edges, map[string]any{
		"branch_policy": "feature_branch",
		"pr_behavior":   "create_pr",
		"fix_loop":      true,
		"memory_update": true,
	})
}

func carefulReviewDelivery() Template {
	stages := []StageTemplate{
		stage("idea_refinement", StageTypeIdeaRefinement, "Idea refinement", ActorHarness, ""),
		stage("plan_review_human", StageTypeReview, "Plan review", ActorHuman, TargetPlan),
		stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""),
		stage("validation", StageTypeValidation, "Validation", ActorHarness, ""),
		stage("change_review_agent", StageTypeReview, "Agent code review", ActorAgent, TargetCodeChanges),
		stage("change_review_human", StageTypeReview, "Human code review", ActorHuman, TargetCodeChanges),
		stage("commit_feature_branch", StageTypeCommit, "Commit", ActorHarness, ""),
		stage("pr_creation", StageTypePRCreation, "PR creation", ActorHarness, ""),
		stage("stop_report", StageTypeStopReport, "Stop/report", ActorHarness, ""),
	}
	return predefined(CarefulReviewID, "Careful Review Delivery", "Branch-and-PR workflow with human review before implementation and before PR handoff.", false, stages, linearEdges(stages), map[string]any{
		"branch_policy": "feature_branch",
		"pr_behavior":   "create_pr",
		"review_depth":  "careful",
	})
}

func directCommitDelivery() Template {
	stages := []StageTemplate{
		stage("idea_refinement", StageTypeIdeaRefinement, "Idea refinement", ActorHarness, ""),
		stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""),
		stage("validation", StageTypeValidation, "Validation", ActorHarness, ""),
		stage("change_review_agent", StageTypeReview, "Code review", ActorAgent, TargetCodeChanges),
		stage("commit_target_branch", StageTypeCommit, "Commit to target branch", ActorHarness, ""),
		stage("memory_update", StageTypeMemoryUpdate, "Memory update", ActorAgent, ""),
		stage("stop_report", StageTypeStopReport, "Stop/report", ActorHarness, ""),
	}
	return predefined(DirectCommitID, "Direct Commit Delivery", "Advanced opt-in workflow that commits to a target branch instead of creating a PR.", false, stages, linearEdges(stages), map[string]any{
		"advanced":      true,
		"branch_policy": "target_branch",
		"pr_behavior":   "none",
		"memory_update": true,
	})
}

func predefined(id, name, description string, recommended bool, stages []StageTemplate, edges []Edge, settings map[string]any) Template {
	return Template{
		SchemaVersion: SchemaVersion,
		ID:            id,
		Name:          name,
		Description:   description,
		Predefined:    true,
		Recommended:   recommended,
		Editable:      false,
		Stages:        stages,
		Edges:         edges,
		Settings:      settings,
	}
}

func stage(id, stageType, label, actor, target string) StageTemplate {
	return StageTemplate{ID: id, Type: stageType, Label: label, Actor: actor, Target: target}
}

func linearEdges(stages []StageTemplate) []Edge {
	var edges []Edge
	for i := 0; i < len(stages)-1; i++ {
		edges = append(edges, Edge{From: stages[i].ID, To: stages[i+1].ID, On: OnCompleted})
	}
	return edges
}
