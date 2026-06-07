package workflow

import "testing"

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
