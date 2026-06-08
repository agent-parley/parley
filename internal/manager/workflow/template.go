package workflow

import (
	"errors"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/shared/contract"
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
	OnFailed           = "failed"
	OnNeedsInput       = "needs_input"
	OnInvalid          = "invalid"
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
		if stage.Type == StageTypeReview {
			stage.Settings = normalizeReviewSettings(stage.Settings)
		}
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
		if stage.Type == StageTypeReview {
			if err := validateReviewStageSettings(stage); err != nil {
				errs = append(errs, err)
			}
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
		if !validEdgeOn(edge.On) {
			errs = append(errs, fmt.Errorf("edge %q -> %q on %q is invalid", edge.From, edge.To, edge.On))
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

func validEdgeOn(on string) bool {
	switch on {
	case OnCompleted, OnApproved, OnChangesRequested, OnFailed, OnNeedsInput, OnInvalid:
		return true
	default:
		return false
	}
}

func normalizeReviewSettings(settings map[string]any) map[string]any {
	if settings == nil {
		settings = map[string]any{}
	}
	profile := settingString(settings, "profile")
	intensity := settingString(settings, "intensity")
	config := contract.NormalizeReviewerConfig(contract.ReviewerConfig{Profile: profile, Intensity: intensity, Instructions: settingString(settings, "instructions")})
	out := map[string]any{}
	for key, value := range settings {
		out[key] = value
	}
	out["profile"] = config.Profile
	out["intensity"] = config.Intensity
	if config.Instructions != "" {
		out["instructions"] = config.Instructions
	} else {
		delete(out, "instructions")
	}
	delete(out, "critic_count")
	delete(out, "arbiter_count")
	delete(out, "reviewer_count")
	delete(out, "panel")
	delete(out, "arbitration")
	return out
}

func validateReviewStageSettings(stage StageTemplate) error {
	config := contract.ReviewerConfig{
		Profile:      settingString(stage.Settings, "profile"),
		Intensity:    settingString(stage.Settings, "intensity"),
		Instructions: settingString(stage.Settings, "instructions"),
	}
	if err := contract.ValidateReviewerConfig(config); err != nil {
		return fmt.Errorf("stage %q review settings are invalid: %w", stage.ID, err)
	}
	if settingString(stage.Settings, "profile") == "custom" {
		return fmt.Errorf("stage %q review profile %q is not supported in v1", stage.ID, "custom")
	}
	return nil
}

func settingString(settings map[string]any, key string) string {
	if settings == nil {
		return ""
	}
	value, ok := settings[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func balancedPRDelivery() Template {
	stages := []StageTemplate{
		stage("idea_refinement", StageTypeIdeaRefinement, "Idea refinement", ActorHarness, ""),
		reviewStage("plan_review_human", "Plan review", ActorHuman, TargetPlan),
		stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""),
		stage("validation", StageTypeValidation, "Validation", ActorHarness, ""),
		reviewStage("change_review_agent", "Code review", ActorAgent, TargetCodeChanges),
		stage("commit_feature_branch", StageTypeCommit, "Commit to feature branch", ActorHarness, ""),
		stage("pr_creation", StageTypePRCreation, "PR creation", ActorHarness, ""),
		stage("stop_report", StageTypeStopReport, "Stop/report", ActorHarness, ""),
	}
	settings := map[string]any{
		"branch_policy": "feature_branch",
		"pr_behavior":   "create_pr",
		"merge_policy":  "human_stop",
		"fix_loop":      true,
		"max_fix_loops": 3,
	}
	return predefined(BalancedPRDeliveryID, "Balanced PR Delivery", "Recommended branch-and-PR workflow with human plan review and agent code review.", true, stages, fixLoopEdges(stages, defaultEdges(stages)), settings)
}

func autonomousPRDelivery() Template {
	stages := []StageTemplate{
		stage("idea_refinement", StageTypeIdeaRefinement, "Idea refinement", ActorHarness, ""),
		stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""),
		stage("validation", StageTypeValidation, "Validation", ActorHarness, ""),
		reviewStage("change_review_agent", "Code review", ActorAgent, TargetCodeChanges),
		stage("commit_feature_branch", StageTypeCommit, "Commit to feature branch", ActorHarness, ""),
		stage("pr_creation", StageTypePRCreation, "PR creation", ActorHarness, ""),
		stage("memory_update", StageTypeMemoryUpdate, "Memory update", ActorAgent, ""),
		stage("stop_report", StageTypeStopReport, "Stop/report", ActorHarness, ""),
	}
	edges := fixLoopEdges(stages, defaultEdges(stages))
	return predefined(AutonomousPRDeliveryID, "Autonomous PR Delivery", "Unattended PR workflow with agent review, fix loop, and memory update.", false, stages, edges, map[string]any{
		"branch_policy": "feature_branch",
		"pr_behavior":   "create_pr",
		"fix_loop":      true,
		"max_fix_loops": 3,
		"memory_update": true,
	})
}

func carefulReviewDelivery() Template {
	stages := []StageTemplate{
		stage("idea_refinement", StageTypeIdeaRefinement, "Idea refinement", ActorHarness, ""),
		reviewStage("plan_review_human", "Plan review", ActorHuman, TargetPlan),
		stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""),
		stage("validation", StageTypeValidation, "Validation", ActorHarness, ""),
		reviewStage("change_review_agent", "Agent code review", ActorAgent, TargetCodeChanges),
		reviewStage("change_review_human", "Human code review", ActorHuman, TargetCodeChanges),
		stage("commit_feature_branch", StageTypeCommit, "Commit", ActorHarness, ""),
		stage("pr_creation", StageTypePRCreation, "PR creation", ActorHarness, ""),
		stage("stop_report", StageTypeStopReport, "Stop/report", ActorHarness, ""),
	}
	return predefined(CarefulReviewID, "Careful Review Delivery", "Branch-and-PR workflow with human review before implementation and before PR handoff.", false, stages, fixLoopEdges(stages, defaultEdges(stages)), map[string]any{
		"branch_policy": "feature_branch",
		"pr_behavior":   "create_pr",
		"review_depth":  "careful",
		"fix_loop":      true,
		"max_fix_loops": 3,
	})
}

func directCommitDelivery() Template {
	stages := []StageTemplate{
		stage("idea_refinement", StageTypeIdeaRefinement, "Idea refinement", ActorHarness, ""),
		stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""),
		stage("validation", StageTypeValidation, "Validation", ActorHarness, ""),
		reviewStage("change_review_agent", "Code review", ActorAgent, TargetCodeChanges),
		stage("commit_target_branch", StageTypeCommit, "Commit to target branch", ActorHarness, ""),
		stage("memory_update", StageTypeMemoryUpdate, "Memory update", ActorAgent, ""),
		stage("stop_report", StageTypeStopReport, "Stop/report", ActorHarness, ""),
	}
	return predefined(DirectCommitID, "Direct Commit Delivery", "Advanced opt-in workflow that commits to a target branch instead of creating a PR.", false, stages, fixLoopEdges(stages, defaultEdges(stages)), map[string]any{
		"advanced":      true,
		"branch_policy": "target_branch",
		"pr_behavior":   "none",
		"fix_loop":      true,
		"max_fix_loops": 3,
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

func reviewStage(id, label, actor, target string) StageTemplate {
	stage := stage(id, StageTypeReview, label, actor, target)
	stage.Settings = map[string]any{
		"profile":   contract.ReviewProfileGeneralist,
		"intensity": contract.ReviewIntensityNormal,
	}
	return stage
}

func fixLoopEdges(stages []StageTemplate, edges []Edge) []Edge {
	implementationID := ""
	for _, stage := range stages {
		if stage.Type == StageTypeImplementation {
			implementationID = stage.ID
			break
		}
	}
	if implementationID == "" {
		return edges
	}
	for _, stage := range stages {
		switch {
		case stage.Type == StageTypeValidation:
			edges = replaceEdge(edges, stage.ID, OnFailed, implementationID)
		case stage.Type == StageTypeReview && stage.Target == TargetCodeChanges:
			edges = replaceEdge(edges, stage.ID, OnChangesRequested, implementationID)
		}
	}
	return edges
}

func replaceEdge(edges []Edge, from, on, to string) []Edge {
	out := make([]Edge, 0, len(edges)+1)
	replaced := false
	for _, edge := range edges {
		if edge.From == from && edge.On == on {
			if !replaced {
				out = append(out, Edge{From: from, To: to, On: on})
				replaced = true
			}
			continue
		}
		out = append(out, edge)
	}
	if !replaced {
		out = append(out, Edge{From: from, To: to, On: on})
	}
	return out
}

func defaultEdges(stages []StageTemplate) []Edge {
	var edges []Edge
	stopID := ""
	for _, stage := range stages {
		if stage.Type == StageTypeStopReport {
			stopID = stage.ID
			break
		}
	}
	for i := 0; i < len(stages)-1; i++ {
		edges = append(edges,
			Edge{From: stages[i].ID, To: stages[i+1].ID, On: OnCompleted},
			Edge{From: stages[i].ID, To: stages[i+1].ID, On: OnApproved},
		)
		if stopID != "" && stages[i].ID != stopID {
			edges = append(edges,
				Edge{From: stages[i].ID, To: stopID, On: OnFailed},
				Edge{From: stages[i].ID, To: stopID, On: OnInvalid},
				Edge{From: stages[i].ID, To: stopID, On: OnNeedsInput},
			)
		}
	}
	return edges
}
