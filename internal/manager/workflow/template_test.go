package workflow

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
	"github.com/agent-parley/parley/internal/shared/contract"
)

func TestPredefinedTemplatesValidateAndExposeFiveDeliveryPatterns(t *testing.T) {
	templates := PredefinedTemplates()
	if len(templates) != 5 {
		t.Fatalf("predefined template count = %d, want 5", len(templates))
	}
	seen := map[string]Template{}
	var recommended int
	for _, template := range templates {
		if err := ValidateTemplate(template); err != nil {
			t.Fatalf("template %s is invalid: %v", template.ID, err)
		}
		if !template.Predefined {
			t.Fatalf("template %s is not marked predefined", template.ID)
		}
		if template.Editable {
			t.Fatalf("predefined template %s should be copied before editing", template.ID)
		}
		if template.Recommended {
			recommended++
		}
		seen[template.ID] = template
	}
	if recommended != 1 || !seen[BalancedPRDeliveryID].Recommended {
		t.Fatalf("recommended default mismatch: recommended=%d balanced=%+v", recommended, seen[BalancedPRDeliveryID])
	}
	for _, id := range []string{BalancedPRDeliveryID, AutonomousPRDeliveryID, CarefulReviewID, DirectCommitID, QuickFixDeliveryID} {
		if _, ok := seen[id]; !ok {
			t.Fatalf("missing predefined template %s", id)
		}
	}
	if got := seen[DirectCommitID].Settings["pr_behavior"]; got != "none" {
		t.Fatalf("direct commit pr_behavior = %v, want none", got)
	}
	for _, template := range templates {
		for _, edge := range template.Edges {
			if edge.On == OnApproved && edge.To == "" {
				t.Fatalf("template %s has empty approved edge: %+v", template.ID, edge)
			}
		}
		for _, stage := range template.Stages {
			if stage.Actor == ActorAgent && stage.ProfileID == "" {
				t.Fatalf("agent stage %s/%s has no profile_id", template.ID, stage.ID)
			}
			if stage.Type != StageTypeReview {
				continue
			}
			if stage.Settings["profile"] != contract.ReviewProfileGeneralist || stage.Settings["intensity"] != contract.ReviewIntensityNormal {
				t.Fatalf("review defaults for %s/%s = %#v", template.ID, stage.ID, stage.Settings)
			}
		}
	}
}

func TestPredefinedTemplatesHumanGateFloorClassification(t *testing.T) {
	want := map[string]bool{
		BalancedPRDeliveryID:   true,
		CarefulReviewID:        true,
		QuickFixDeliveryID:     true,
		DirectCommitID:         false,
		AutonomousPRDeliveryID: false,
	}
	seen := map[string]bool{}
	for _, template := range PredefinedTemplates() {
		seen[template.ID] = true
		if got := MeetsHumanGateFloor(template); got != want[template.ID] {
			t.Fatalf("MeetsHumanGateFloor(%s) = %v, want %v", template.ID, got, want[template.ID])
		}
	}
	for id := range want {
		if !seen[id] {
			t.Fatalf("predefined template %s was not classified", id)
		}
	}
}

func TestQuickFixDeliveryIsSlimPRGatedAndOptIn(t *testing.T) {
	template := quickFixDelivery()
	if err := ValidateTemplate(template); err != nil {
		t.Fatalf("quick fix template is invalid: %v", err)
	}
	wantStageOrder := []string{"idea_refinement", "implementation", "commit_feature_branch", "pr_creation", "stop_report"}
	if len(template.Stages) != len(wantStageOrder) {
		t.Fatalf("quick fix stage count = %d, want %d", len(template.Stages), len(wantStageOrder))
	}
	seen := map[string]StageTemplate{}
	for i, stage := range template.Stages {
		if stage.ID != wantStageOrder[i] {
			t.Fatalf("quick fix stage[%d] = %s, want %s", i, stage.ID, wantStageOrder[i])
		}
		seen[stage.ID] = stage
	}
	start, ok := seen["idea_refinement"]
	if !ok {
		t.Fatal("quick fix missing mandatory idea_refinement start stage")
	}
	if start.Type != StageTypeIdeaRefinement || start.Actor != ActorHarness {
		t.Fatalf("idea_refinement = %+v, want harness idea refinement", start)
	}
	end, ok := seen["stop_report"]
	if !ok {
		t.Fatal("quick fix missing mandatory stop_report end stage")
	}
	if end.Type != StageTypeStopReport || end.Actor != ActorHarness {
		t.Fatalf("stop_report = %+v, want harness stop report", end)
	}
	for _, absent := range []string{"plan_review_human", "validation", "change_review_agent"} {
		if _, ok := seen[absent]; ok {
			t.Fatalf("quick fix unexpectedly includes %s", absent)
		}
	}
	for _, stage := range template.Stages {
		if stage.Type == StageTypeValidation || stage.Type == StageTypeReview {
			t.Fatalf("quick fix unexpectedly includes gated/checking stage: %+v", stage)
		}
	}
	if got := template.Settings["branch_policy"]; got != "feature_branch" {
		t.Fatalf("quick fix branch_policy = %v, want feature_branch", got)
	}
	if got := template.Settings["pr_behavior"]; got != "create_pr" {
		t.Fatalf("quick fix pr_behavior = %v, want create_pr", got)
	}
	balancedNoAutoMerge := balancedPRDelivery().Settings["merge_policy"]
	if got := template.Settings["merge_policy"]; got != balancedNoAutoMerge {
		t.Fatalf("quick fix merge_policy = %v, want no-auto-merge value %v", got, balancedNoAutoMerge)
	}
	if got := template.Settings["merge_policy"]; got == "auto" || got == "auto_merge" {
		t.Fatalf("quick fix merge_policy = %v, want no auto-merge", got)
	}
	if got := template.Settings["fix_loop"]; got != false {
		t.Fatalf("quick fix fix_loop = %v, want false", got)
	}
	seenPredefined := map[string]Template{}
	for _, candidate := range PredefinedTemplates() {
		seenPredefined[candidate.ID] = candidate
	}
	registered, ok := seenPredefined[QuickFixDeliveryID]
	if !ok {
		t.Fatalf("quick fix template %q not registered", QuickFixDeliveryID)
	}
	if registered.Recommended {
		t.Fatal("quick fix should be opt-in, not recommended")
	}
	if DefaultTemplateID != BalancedPRDeliveryID || DefaultTemplate().ID != BalancedPRDeliveryID {
		t.Fatalf("default template changed: const=%s template=%s", DefaultTemplateID, DefaultTemplate().ID)
	}
}

func TestReviewTargetValidationAllowsV1Targets(t *testing.T) {
	cases := []struct {
		target string
		middle []StageTemplate
	}{
		{target: TargetPlan, middle: []StageTemplate{reviewStage("review_plan", "Review", ActorAgent, TargetPlan)}},
		{target: TargetCodeChanges, middle: []StageTemplate{reviewStage("review_code_changes", "Review", ActorAgent, TargetCodeChanges)}},
		{target: TargetValidationEvidence, middle: []StageTemplate{stage("validation", StageTypeValidation, "Validation", ActorHarness, ""), reviewStage("review_validation_evidence", "Review", ActorAgent, TargetValidationEvidence)}},
		{target: TargetDeliveryResult, middle: []StageTemplate{stage("commit", StageTypeCommit, "Commit", ActorHarness, ""), reviewStage("review_delivery_result", "Review", ActorAgent, TargetDeliveryResult)}},
	}
	for _, tc := range cases {
		template := validTemplateForTest("t_"+tc.target, tc.middle...)
		if err := ValidateTemplate(template); err != nil {
			t.Fatalf("ValidateTemplate(%s) error = %v", tc.target, err)
		}
	}

	bad := reviewStage("bad_target", "Bad target", ActorAgent, "final_review")
	assertValidateTemplateError(t, validTemplateForTest("bad_target", bad), "review target must be one of")
}

func TestReviewTargetValidationRejectsTargetsBeforeEvidenceProducer(t *testing.T) {
	validationBeforeEvidence := validTemplateForTest("validation_before_evidence", reviewStage("review_validation", "Review", ActorAgent, TargetValidationEvidence), stage("validation", StageTypeValidation, "Validation", ActorHarness, ""))
	assertValidateTemplateError(t, validationBeforeEvidence, "requires a prior validation")

	deliveryBeforeResult := validTemplateForTest("delivery_before_result", stage("validation", StageTypeValidation, "Validation", ActorHarness, ""), reviewStage("review_delivery", "Review", ActorAgent, TargetDeliveryResult), stage("commit", StageTypeCommit, "Commit", ActorHarness, ""))
	assertValidateTemplateError(t, deliveryBeforeResult, "requires a prior commit or pr_creation")
}

func TestReviewProfileDefaultsAndValidation(t *testing.T) {
	cases := map[string]string{
		contract.ReviewProfileGeneralist:  contract.ReviewIntensityNormal,
		contract.ReviewProfileSecurity:    contract.ReviewIntensityStrict,
		contract.ReviewProfileTests:       contract.ReviewIntensityNormal,
		contract.ReviewProfileAdversarial: contract.ReviewIntensityAdversarial,
	}
	for profile, intensity := range cases {
		stage := reviewStage("review_"+profile, "Review", ActorAgent, TargetCodeChanges)
		stage.Settings["profile"] = profile
		delete(stage.Settings, "intensity")
		template := validTemplateForTest("t_"+profile, stage)
		normalized := NormalizeTemplate(template)
		if got := normalized.Stages[1].Settings["intensity"]; got != intensity {
			t.Fatalf("default intensity for %s = %v, want %s", profile, got, intensity)
		}
		if err := ValidateTemplate(normalized); err != nil {
			t.Fatalf("ValidateTemplate(%s) error = %v", profile, err)
		}
	}
	bad := reviewStage("bad", "Bad", ActorAgent, TargetCodeChanges)
	bad.Settings["profile"] = "custom"
	if err := ValidateTemplate(validTemplateForTest("bad", bad)); err == nil {
		t.Fatal("ValidateTemplate accepted custom review profile")
	}
}

func TestStageTemplateCommonFieldDefaultingAndValidation(t *testing.T) {
	template := validTemplateForTest("defaults", StageTemplate{ID: "implementation", Type: StageTypeImplementation, Label: "Implementation", Actor: ActorAgent})
	normalized := NormalizeTemplate(template)
	impl := normalized.Stages[1]
	if impl.ProfileID != agentregistry.ProfilePiHeadlessWorker {
		t.Fatalf("implementation profile_id = %q, want %q", impl.ProfileID, agentregistry.ProfilePiHeadlessWorker)
	}
	if !StageRequired(impl) {
		t.Fatalf("implementation required = false, want default true")
	}
	if impl.MaxAttempts != 1 {
		t.Fatalf("implementation max_attempts = %d, want 1", impl.MaxAttempts)
	}

	badProfile := normalized
	badProfile.Stages[1].ProfileID = "missing_profile"
	assertValidateTemplateError(t, badProfile, "does not resolve to an agent profile")

	harnessProfile := normalized
	harnessProfile.Stages[1].Actor = ActorHarness
	harnessProfile.Stages[1].ProfileID = agentregistry.ProfilePiHeadlessWorker
	assertValidateTemplateError(t, harnessProfile, "profile_id is only valid for agent stages")

	badAttempts := normalized
	badAttempts.Stages[1].MaxAttempts = -1
	assertValidateTemplateError(t, badAttempts, "max_attempts must be at least 1")

	badTimeout := normalized
	badTimeout.Stages[1].Timeout = "soon"
	assertValidateTemplateError(t, badTimeout, "timeout must be a Go duration")

	for _, timeout := range []string{"0s", "-30m"} {
		badTimeout := normalized
		badTimeout.Stages[1].Timeout = timeout
		assertValidateTemplateError(t, badTimeout, "timeout must be greater than zero")
	}
}

func TestStageTemplateCommonFieldsRoundTrip(t *testing.T) {
	required := false
	template := validTemplateForTest("round_trip", StageTemplate{
		ID:              "implementation",
		Type:            StageTypeImplementation,
		Label:           "Implementation",
		Actor:           ActorAgent,
		ProfileID:       agentregistry.ProfilePiInteractivePlanner,
		Instructions:    "Prefer focused changes.",
		Required:        &required,
		ContextSettings: map[string]any{"sources": []any{"project_memory"}},
		Timeout:         "45m",
		MaxAttempts:     2,
	})
	content, err := json.Marshal(NormalizeTemplate(template))
	if err != nil {
		t.Fatalf("marshal template: %v", err)
	}
	var decoded Template
	if err := json.Unmarshal(content, &decoded); err != nil {
		t.Fatalf("unmarshal template: %v", err)
	}
	decoded = NormalizeTemplate(decoded)
	if err := ValidateTemplate(decoded); err != nil {
		t.Fatalf("ValidateTemplate(round trip) error = %v", err)
	}
	impl := decoded.Stages[1]
	if impl.ProfileID != agentregistry.ProfilePiInteractivePlanner || impl.Instructions != "Prefer focused changes." || StageRequired(impl) || impl.Timeout != "45m" || impl.MaxAttempts != 2 {
		t.Fatalf("round-tripped implementation = %+v", impl)
	}
	if got := impl.ContextSettings["sources"]; got == nil {
		t.Fatalf("round-tripped context_settings missing sources: %+v", impl.ContextSettings)
	}
}

func TestValidateTemplateStructuralInvariant(t *testing.T) {
	t.Run("rejects duplicate start", func(t *testing.T) {
		template := validTemplateForTest("duplicate_start", stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""))
		template.Stages = append([]StageTemplate{stage("second_start", StageTypeIdeaRefinement, "Second start", ActorHarness, "")}, template.Stages...)
		template.Edges = DeriveTemplateEdges(template)
		assertValidateTemplateError(t, template, "exactly one idea_refinement")
	})
	t.Run("rejects inbound edge to start", func(t *testing.T) {
		template := validTemplateForTest("inbound_start", stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""))
		template.Edges = append(template.Edges, Edge{From: "implementation", To: "idea_refinement", On: OnBlocked})
		assertValidateTemplateError(t, template, "must have no inbound edges")
	})
	t.Run("rejects duplicate end", func(t *testing.T) {
		template := validTemplateForTest("duplicate_end", stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""))
		template.Stages = append(template.Stages, stage("second_stop", StageTypeStopReport, "Second stop", ActorHarness, ""))
		template.Edges = DeriveTemplateEdges(template)
		assertValidateTemplateError(t, template, "exactly one stop_report")
	})
	t.Run("rejects orphan", func(t *testing.T) {
		template := validTemplateForTest("orphan", stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""))
		template.Stages = append(template.Stages[:len(template.Stages)-1], stage("orphan_review", StageTypeReview, "Orphan review", ActorAgent, TargetCodeChanges), template.Stages[len(template.Stages)-1])
		assertValidateTemplateError(t, template, "not reachable from idea_refinement")
	})
	t.Run("rejects dead end", func(t *testing.T) {
		template := validTemplateForTest("dead_end", stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""))
		template.Edges = []Edge{{From: "idea_refinement", To: "implementation", On: OnCompleted}}
		assertValidateTemplateError(t, template, "stop_report end stage")
	})
	t.Run("allows fix loop cycle", func(t *testing.T) {
		template := validTemplateForTest("fix_loop", stage("implementation", StageTypeImplementation, "Implementation", ActorAgent, ""), stage("validation", StageTypeValidation, "Validation", ActorHarness, ""))
		template.Settings["fix_loop"] = true
		template.Edges = DeriveTemplateEdges(template)
		if err := ValidateTemplate(template); err != nil {
			t.Fatalf("ValidateTemplate rejected valid fix-loop cycle: %v", err)
		}
	})
}

func TestValidateTemplateRejectsUnstructuredGraph(t *testing.T) {
	template := Template{
		SchemaVersion: SchemaVersion,
		ID:            "bad",
		Name:          "Bad",
		Editable:      true,
		Stages: []StageTemplate{
			{ID: "start", Type: StageTypeIdeaRefinement, Actor: ActorHarness, Label: "Start"},
		},
		Edges: []Edge{{From: "start", To: "missing", On: OnCompleted}},
	}
	if err := ValidateTemplate(template); err == nil {
		t.Fatal("ValidateTemplate() succeeded, want error")
	}
}

func validTemplateForTest(id string, middle ...StageTemplate) Template {
	stages := []StageTemplate{stage("idea_refinement", StageTypeIdeaRefinement, "Idea refinement", ActorHarness, "")}
	stages = append(stages, middle...)
	stages = append(stages, stage("stop_report", StageTypeStopReport, "Stop/report", ActorHarness, ""))
	template := Template{SchemaVersion: SchemaVersion, ID: id, Name: "Test template", Editable: true, Stages: stages, Settings: map[string]any{"fix_loop": false}}
	template.Edges = DeriveTemplateEdges(template)
	return template
}

func assertValidateTemplateError(t *testing.T, template Template, want string) {
	t.Helper()
	err := ValidateTemplate(template)
	if err == nil {
		t.Fatalf("ValidateTemplate() succeeded, want error containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("ValidateTemplate() error = %v, want substring %q", err, want)
	}
}
