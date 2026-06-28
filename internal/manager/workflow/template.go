package workflow

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
	"github.com/agent-parley/parley/internal/shared/contract"
)

const (
	SchemaVersion = 1

	BalancedPRDeliveryID   = "balanced_pr_delivery"
	AutonomousPRDeliveryID = "autonomous_pr_delivery"
	CarefulReviewID        = "careful_review_delivery"
	DirectCommitID         = "direct_commit_delivery"
	QuickFixDeliveryID     = "quick_fix_delivery"

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
	TargetPlan               = contract.ReviewTargetPlan
	TargetCodeChanges        = contract.ReviewTargetCodeChanges
	TargetValidationEvidence = contract.ReviewTargetValidationEvidence
	TargetDeliveryResult     = contract.ReviewTargetDeliveryResult
)

const (
	OnCompleted        = "completed"
	OnApproved         = "approved"
	OnChangesRequested = "changes_requested"
	OnFailed           = "failed"
	OnNeedsInput       = "needs_input"
	OnBlocked          = "blocked"
	OnEscalate         = "escalate"
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
	ID              string         `json:"id"`
	Type            string         `json:"type"`
	Label           string         `json:"label"`
	Actor           string         `json:"actor"`
	Target          string         `json:"target,omitempty"`
	ProfileID       string         `json:"profile_id,omitempty"`
	Instructions    string         `json:"instructions,omitempty"`
	Required        *bool          `json:"required,omitempty"`
	ContextSettings map[string]any `json:"context_settings,omitempty"`
	Timeout         string         `json:"timeout,omitempty"`
	MaxAttempts     int            `json:"max_attempts,omitempty"`
	Settings        map[string]any `json:"settings,omitempty"`
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
		quickFixDelivery(),
	}
}

func DefaultTemplate() Template { return balancedPRDelivery() }

func MeetsHumanGateFloor(template Template) bool {
	template = NormalizeTemplate(template)
	for _, stage := range template.Stages {
		if stage.Type == StageTypeReview && stage.Actor == ActorHuman {
			return true
		}
	}
	if settingString(template.Settings, "pr_behavior") != "create_pr" {
		return false
	}
	return mergePolicyRequiresHumanGate(settingString(template.Settings, "merge_policy"))
}

func mergePolicyRequiresHumanGate(policy string) bool {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case "human", "human_stop", "human_merge", "manual", "manual_stop", "manual_merge", "no_auto_merge", "none", "disabled":
		return true
	default:
		return false
	}
}

func NormalizeTemplate(template Template) Template {
	return NormalizeTemplateWithRegistry(template, agentregistry.Defaults())
}

func NormalizeTemplateWithRegistry(template Template, registry agentregistry.Registry) Template {
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
		stage.ProfileID = strings.ToLower(strings.TrimSpace(stage.ProfileID))
		stage.Instructions = strings.TrimSpace(stage.Instructions)
		stage.Timeout = strings.TrimSpace(stage.Timeout)
		if stage.Required == nil {
			stage.Required = boolPtr(true)
		}
		if stage.MaxAttempts == 0 {
			stage.MaxAttempts = 1
		}
		stage.ContextSettings = copyNonEmptyMap(stage.ContextSettings)
		if legacyInstructions := settingString(stage.Settings, "instructions"); legacyInstructions != "" && stage.Instructions == "" {
			stage.Instructions = legacyInstructions
		}
		if stage.Type == StageTypeReview {
			stage.Target = contract.NormalizeReviewTarget(stage.Target)
			stage.Settings = normalizeReviewSettings(stage.Settings, stage.Instructions)
		} else {
			stage.Settings = copyNonEmptyMap(stage.Settings)
			delete(stage.Settings, "instructions")
			if len(stage.Settings) == 0 {
				stage.Settings = nil
			}
		}
		if stage.Actor == ActorAgent && stage.ProfileID == "" {
			if profileID, ok := agentregistry.DefaultProfileIDForStageType(registry, stage.Type); ok {
				stage.ProfileID = profileID
			}
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

func DeriveTemplateEdges(template Template) []Edge {
	template = NormalizeTemplate(template)
	edges := defaultEdges(template.Stages)
	if settingBool(template.Settings, "fix_loop") {
		edges = fixLoopEdges(template.Stages, edges)
	}
	return reviewEscalationEdges(template.Stages, edges)
}

func ValidateTemplate(template Template) error {
	return ValidateTemplateWithRegistry(template, agentregistry.Defaults())
}

func ValidateTemplateWithRegistry(template Template, registry agentregistry.Registry) error {
	template = NormalizeTemplateWithRegistry(template, registry)
	var errs []error
	if err := agentregistry.Validate(registry); err != nil {
		errs = append(errs, fmt.Errorf("agent registry is invalid: %w", err))
	}
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
	stageIDOrder := make([]string, 0, len(template.Stages))
	var startIDs, endIDs []string
	for i, stage := range template.Stages {
		if stage.ID == "" {
			errs = append(errs, errors.New("stage id is required"))
			continue
		}
		if stageIDs[stage.ID] {
			errs = append(errs, fmt.Errorf("stage id %q is duplicated", stage.ID))
		}
		stageIDs[stage.ID] = true
		stageIDOrder = append(stageIDOrder, stage.ID)
		if stage.Type == StageTypeIdeaRefinement {
			startIDs = append(startIDs, stage.ID)
		}
		if stage.Type == StageTypeStopReport {
			endIDs = append(endIDs, stage.ID)
		}
		if !validStageType(stage.Type) {
			errs = append(errs, fmt.Errorf("stage %q type %q is invalid", stage.ID, stage.Type))
		}
		if !validActor(stage.Actor) {
			errs = append(errs, fmt.Errorf("stage %q actor %q is invalid", stage.ID, stage.Actor))
		}
		if stage.Actor == ActorAgent {
			if stage.ProfileID == "" {
				errs = append(errs, fmt.Errorf("stage %q agent profile_id is required", stage.ID))
			} else if _, ok := agentregistry.ProfileByID(registry, stage.ProfileID); !ok {
				errs = append(errs, fmt.Errorf("stage %q profile_id %q does not resolve to an agent profile", stage.ID, stage.ProfileID))
			}
		} else if stage.ProfileID != "" {
			errs = append(errs, fmt.Errorf("stage %q profile_id is only valid for agent stages", stage.ID))
		}
		if stage.MaxAttempts < 1 {
			errs = append(errs, fmt.Errorf("stage %q max_attempts must be at least 1", stage.ID))
		}
		if stage.Timeout != "" {
			duration, err := time.ParseDuration(stage.Timeout)
			if err != nil {
				errs = append(errs, fmt.Errorf("stage %q timeout must be a Go duration such as 30m or 1h: %w", stage.ID, err))
			} else if duration <= 0 {
				errs = append(errs, fmt.Errorf("stage %q timeout must be greater than zero", stage.ID))
			}
		}
		if stage.Type == StageTypeReview {
			if err := validateReviewStageSettings(stage); err != nil {
				errs = append(errs, err)
			}
			if err := validateReviewTargetPlacement(stage, i, template.Stages); err != nil {
				errs = append(errs, err)
			}
		}
	}
	inbound := map[string][]string{}
	outbound := map[string][]string{}
	forward := map[string][]string{}
	reverse := map[string][]string{}
	edgeKeys := map[string]bool{}
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
		if stageIDs[edge.From] && stageIDs[edge.To] && validEdgeOn(edge.On) {
			key := edge.From + "\x00" + edge.On
			if edgeKeys[key] {
				errs = append(errs, fmt.Errorf("duplicate workflow edge from %q on %q", edge.From, edge.On))
			}
			edgeKeys[key] = true
			outbound[edge.From] = append(outbound[edge.From], edge.To)
			inbound[edge.To] = append(inbound[edge.To], edge.From)
			forward[edge.From] = append(forward[edge.From], edge.To)
			reverse[edge.To] = append(reverse[edge.To], edge.From)
		}
	}
	if len(startIDs) != 1 {
		errs = append(errs, fmt.Errorf("exactly one %s start stage is required; found %d", StageTypeIdeaRefinement, len(startIDs)))
	} else if len(inbound[startIDs[0]]) != 0 {
		errs = append(errs, fmt.Errorf("%s start stage %q must have no inbound edges", StageTypeIdeaRefinement, startIDs[0]))
	}
	if len(endIDs) != 1 {
		errs = append(errs, fmt.Errorf("exactly one %s end stage is required; found %d", StageTypeStopReport, len(endIDs)))
	} else if len(outbound[endIDs[0]]) != 0 {
		errs = append(errs, fmt.Errorf("%s end stage %q must have no outbound edges", StageTypeStopReport, endIDs[0]))
	}
	if len(startIDs) == 1 {
		reachable := reachableStageIDs(startIDs[0], forward)
		for _, stageID := range stageIDOrder {
			if !reachable[stageID] {
				errs = append(errs, fmt.Errorf("stage %q is not reachable from %s start stage %q", stageID, StageTypeIdeaRefinement, startIDs[0]))
			}
		}
	}
	if len(endIDs) == 1 {
		canReachEnd := reachableStageIDs(endIDs[0], reverse)
		for _, stageID := range stageIDOrder {
			if !canReachEnd[stageID] {
				errs = append(errs, fmt.Errorf("%s end stage %q is not reachable from stage %q", StageTypeStopReport, endIDs[0], stageID))
			}
		}
	}
	return errors.Join(errs...)
}

func reachableStageIDs(start string, graph map[string][]string) map[string]bool {
	seen := map[string]bool{start: true}
	stack := []string{start}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, next := range graph[current] {
			if seen[next] {
				continue
			}
			seen[next] = true
			stack = append(stack, next)
		}
	}
	return seen
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
	case OnCompleted, OnApproved, OnChangesRequested, OnFailed, OnNeedsInput, OnBlocked, OnEscalate, OnInvalid:
		return true
	default:
		return false
	}
}

func normalizeReviewSettings(settings map[string]any, instructions string) map[string]any {
	if settings == nil {
		settings = map[string]any{}
	}
	profile := settingString(settings, "profile")
	intensity := settingString(settings, "intensity")
	if instructions == "" {
		instructions = settingString(settings, "instructions")
	}
	config := contract.NormalizeReviewerConfig(contract.ReviewerConfig{Profile: profile, Intensity: intensity, Instructions: instructions})
	out := map[string]any{}
	for key, value := range settings {
		out[key] = value
	}
	out["profile"] = config.Profile
	out["intensity"] = config.Intensity
	delete(out, "instructions")
	delete(out, "critic_count")
	delete(out, "arbiter_count")
	delete(out, "reviewer_count")
	delete(out, "panel")
	delete(out, "arbitration")
	return out
}

func validateReviewStageSettings(stage StageTemplate) error {
	if !contract.ValidReviewTarget(stage.Target) {
		return fmt.Errorf("stage %q review target must be one of %q, %q, %q, or %q", stage.ID, TargetPlan, TargetCodeChanges, TargetValidationEvidence, TargetDeliveryResult)
	}
	config := contract.ReviewerConfig{
		Profile:      settingString(stage.Settings, "profile"),
		Intensity:    settingString(stage.Settings, "intensity"),
		Instructions: stage.Instructions,
	}
	if err := contract.ValidateReviewerConfig(config); err != nil {
		return fmt.Errorf("stage %q review settings are invalid: %w", stage.ID, err)
	}
	if settingString(stage.Settings, "profile") == "custom" {
		return fmt.Errorf("stage %q review profile %q is not supported in v1", stage.ID, "custom")
	}
	return nil
}

func validateReviewTargetPlacement(stage StageTemplate, index int, stages []StageTemplate) error {
	switch stage.Target {
	case TargetValidationEvidence:
		if !hasPriorStageType(stages, index, StageTypeValidation) {
			return fmt.Errorf("stage %q review target %q requires a prior %s stage", stage.ID, stage.Target, StageTypeValidation)
		}
	case TargetDeliveryResult:
		if !hasPriorStageType(stages, index, StageTypeCommit, StageTypePRCreation) {
			return fmt.Errorf("stage %q review target %q requires a prior %s or %s stage", stage.ID, stage.Target, StageTypeCommit, StageTypePRCreation)
		}
	}
	return nil
}

func hasPriorStageType(stages []StageTemplate, index int, stageTypes ...string) bool {
	allowed := map[string]bool{}
	for _, stageType := range stageTypes {
		allowed[stageType] = true
	}
	for i := 0; i < index && i < len(stages); i++ {
		if allowed[stages[i].Type] {
			return true
		}
	}
	return false
}

func StageRequired(stage StageTemplate) bool {
	return stage.Required == nil || *stage.Required
}

func boolPtr(value bool) *bool { return &value }

func copyNonEmptyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
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

func settingBool(settings map[string]any, key string) bool {
	if settings == nil {
		return false
	}
	value, ok := settings[key]
	if !ok || value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes", "on", "enabled":
			return true
		default:
			return false
		}
	default:
		return strings.EqualFold(strings.TrimSpace(fmt.Sprint(value)), "true")
	}
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
	return predefined(BalancedPRDeliveryID, "Balanced PR Delivery", "Recommended branch-and-PR workflow with human plan review and agent code review.", true, stages, settings)
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
	return predefined(AutonomousPRDeliveryID, "Autonomous PR Delivery", "Unattended PR workflow with agent review, fix loop, and memory update.", false, stages, map[string]any{
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
	return predefined(CarefulReviewID, "Careful Review Delivery", "Branch-and-PR workflow with human review before implementation and before PR handoff.", false, stages, map[string]any{
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
	return predefined(DirectCommitID, "Direct Commit Delivery", "Advanced opt-in workflow that commits to a target branch instead of creating a PR.", false, stages, map[string]any{
		"advanced":      true,
		"branch_policy": "target_branch",
		"pr_behavior":   "none",
		"fix_loop":      true,
		"max_fix_loops": 3,
		"memory_update": true,
	})
}

func quickFixDelivery() Template {
	stages := []StageTemplate{
		stage("idea_refinement", StageTypeIdeaRefinement, "Idea refinement", ActorHarness, ""),
		stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""),
		stage("commit_feature_branch", StageTypeCommit, "Commit to feature branch", ActorHarness, ""),
		stage("pr_creation", StageTypePRCreation, "PR creation", ActorHarness, ""),
		stage("stop_report", StageTypeStopReport, "Stop/report", ActorHarness, ""),
	}
	return predefined(QuickFixDeliveryID, "Quick Fix Delivery", "Slim branch-and-PR workflow for trivial fixes with PR merge as the human gate.", false, stages, map[string]any{
		"branch_policy": "feature_branch",
		"pr_behavior":   "create_pr",
		"merge_policy":  "human_stop",
		"fix_loop":      false,
	})
}

func predefined(id, name, description string, recommended bool, stages []StageTemplate, settings map[string]any) Template {
	template := Template{
		SchemaVersion: SchemaVersion,
		ID:            id,
		Name:          name,
		Description:   description,
		Predefined:    true,
		Recommended:   recommended,
		Editable:      false,
		Stages:        stages,
		Settings:      settings,
	}
	template.Edges = DeriveTemplateEdges(template)
	return NormalizeTemplate(template)
}

func stage(id, stageType, label, actor, target string) StageTemplate {
	stage := StageTemplate{ID: id, Type: stageType, Label: label, Actor: actor, Target: target, Required: boolPtr(true), MaxAttempts: 1}
	if actor == ActorAgent {
		if profileID, ok := agentregistry.DefaultProfileIDForStageType(agentregistry.Defaults(), stageType); ok {
			stage.ProfileID = profileID
		}
	}
	return stage
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
		case stage.Type == StageTypeReview:
			edges = replaceEdge(edges, stage.ID, OnChangesRequested, implementationID)
		}
	}
	return edges
}

func reviewEscalationEdges(stages []StageTemplate, edges []Edge) []Edge {
	stopID := ""
	for _, stage := range stages {
		if stage.Type == StageTypeStopReport {
			stopID = stage.ID
			break
		}
	}
	for i, stage := range stages {
		if stage.Type != StageTypeReview || stage.Actor != ActorAgent {
			continue
		}
		targetID := ""
		for j := i + 1; j < len(stages); j++ {
			candidate := stages[j]
			if candidate.Type == StageTypeReview && candidate.Actor == ActorHuman && candidate.Target == stage.Target {
				targetID = candidate.ID
				break
			}
		}
		if targetID == "" {
			targetID = stopID
		}
		if targetID == "" {
			continue
		}
		edges = replaceEdge(edges, stage.ID, OnBlocked, targetID)
		edges = replaceEdge(edges, stage.ID, OnEscalate, targetID)
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
			if stages[i].Type == StageTypeReview {
				edges = append(edges,
					Edge{From: stages[i].ID, To: stopID, On: OnBlocked},
					Edge{From: stages[i].ID, To: stopID, On: OnEscalate},
				)
			}
		}
	}
	return edges
}
