package contract

import "testing"

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
