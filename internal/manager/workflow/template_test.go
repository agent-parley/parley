package workflow

import (
	"testing"

	"github.com/agent-parley/parley/internal/shared/contract"
)

func TestPredefinedTemplatesValidateAndExposeFourDeliveryPatterns(t *testing.T) {
	templates := PredefinedTemplates()
	if len(templates) != 4 {
		t.Fatalf("predefined template count = %d, want 4", len(templates))
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
	for _, id := range []string{BalancedPRDeliveryID, AutonomousPRDeliveryID, CarefulReviewID, DirectCommitID} {
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
