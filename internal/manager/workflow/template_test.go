package workflow

import (
	"testing"

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
			if stage.Type != StageTypeReview {
				continue
			}
			if stage.Settings["profile"] != contract.ReviewProfileGeneralist || stage.Settings["intensity"] != contract.ReviewIntensityNormal {
				t.Fatalf("review defaults for %s/%s = %#v", template.ID, stage.ID, stage.Settings)
			}
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
		template := Template{SchemaVersion: SchemaVersion, ID: "t_" + profile, Name: "T", Editable: true, Stages: []StageTemplate{stage}, Edges: []Edge{}}
		normalized := NormalizeTemplate(template)
		if got := normalized.Stages[0].Settings["intensity"]; got != intensity {
			t.Fatalf("default intensity for %s = %v, want %s", profile, got, intensity)
		}
		if err := ValidateTemplate(normalized); err != nil {
			t.Fatalf("ValidateTemplate(%s) error = %v", profile, err)
		}
	}
	bad := reviewStage("bad", "Bad", ActorAgent, TargetCodeChanges)
	bad.Settings["profile"] = "custom"
	if err := ValidateTemplate(Template{SchemaVersion: SchemaVersion, ID: "bad", Name: "Bad", Editable: true, Stages: []StageTemplate{bad}}); err == nil {
		t.Fatal("ValidateTemplate accepted custom review profile")
	}
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
