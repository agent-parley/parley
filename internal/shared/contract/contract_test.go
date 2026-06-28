package contract

import (
	"strings"
	"testing"
)

func TestNormalizeRefinementLevelCoercesLegacyValueToStandard(t *testing.T) {
	cases := map[string]string{
		"":         RefinementLevelStandard,
		"standard": RefinementLevelStandard,
		"direct":   RefinementLevelDirect,
		"deep":     RefinementLevelStandard,
		" DEEP ":   RefinementLevelStandard,
	}
	for input, want := range cases {
		if got := NormalizeRefinementLevel(input); got != want {
			t.Fatalf("NormalizeRefinementLevel(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestValidateRefinementLevelAllowsOnlyDirectAndStandardVocabulary(t *testing.T) {
	if err := ValidateRefinementLevel("deep"); err != nil {
		t.Fatalf("legacy deep should coerce to standard: %v", err)
	}
	err := ValidateRefinementLevel("max")
	if err == nil {
		t.Fatal("invalid refinement level accepted")
	}
	if strings.Contains(err.Error(), "deep") || !strings.Contains(err.Error(), RefinementLevelDirect) || !strings.Contains(err.Error(), RefinementLevelStandard) {
		t.Fatalf("validation error = %q, want direct/standard only", err.Error())
	}
}

func TestReviewTargetOptionsAreClosedV1Set(t *testing.T) {
	options := ReviewTargetOptions()
	want := map[string]string{
		ReviewTargetPlan:               "Plan",
		ReviewTargetCodeChanges:        "Code changes",
		ReviewTargetValidationEvidence: "Validation evidence",
		ReviewTargetDeliveryResult:     "Delivery result",
	}
	if len(options) != len(want) {
		t.Fatalf("target count = %d, want %d", len(options), len(want))
	}
	for _, option := range options {
		if want[option.ID] != option.Label {
			t.Fatalf("target option = %+v", option)
		}
		if !ValidReviewTarget(option.ID) {
			t.Fatalf("target %q is not valid", option.ID)
		}
		if got := ReviewTargetLabel(option.ID); got != option.Label {
			t.Fatalf("ReviewTargetLabel(%q) = %q, want %q", option.ID, got, option.Label)
		}
		delete(want, option.ID)
	}
	if len(want) != 0 {
		t.Fatalf("missing target options: %#v", want)
	}
	if ValidReviewTarget("final_review") {
		t.Fatal("unknown review target accepted")
	}
}

func TestReviewProfileDefaultsAreClosedV1Set(t *testing.T) {
	profiles := ReviewProfileDefaults()
	if len(profiles) != 4 {
		t.Fatalf("profile count = %d, want 4", len(profiles))
	}
	want := map[string]string{
		ReviewProfileGeneralist:  ReviewIntensityNormal,
		ReviewProfileSecurity:    ReviewIntensityStrict,
		ReviewProfileTests:       ReviewIntensityNormal,
		ReviewProfileAdversarial: ReviewIntensityAdversarial,
	}
	for _, profile := range profiles {
		if want[profile.Profile] != profile.Intensity {
			t.Fatalf("profile default = %+v", profile)
		}
		delete(want, profile.Profile)
	}
	if len(want) != 0 {
		t.Fatalf("missing profile defaults: %#v", want)
	}
	if err := ValidateReviewerConfig(ReviewerConfig{Profile: "custom"}); err == nil {
		t.Fatal("custom review profile accepted")
	}
}
