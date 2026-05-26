package profiles_test

import (
	"reflect"
	"testing"

	"github.com/agent-parley/parley/internal/profiles"
)

func TestWorkerDefaultIDsExcludeReviewer(t *testing.T) {
	got := profiles.WorkerDefaultIDs()
	want := []string{"pi-standard", "pi-high-context", "pi-readonly-scout"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("worker defaults = %#v, want %#v", got, want)
	}
	if profiles.IsWorkerDefault("pi-reviewer") {
		t.Fatalf("reviewer profile must not be a worker default")
	}
}

func TestReviewerIDsContainOnlyReviewerProfiles(t *testing.T) {
	got := profiles.ReviewerIDs()
	want := []string{"pi-reviewer"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reviewer IDs = %#v, want %#v", got, want)
	}
	for _, id := range profiles.WorkerDefaultIDs() {
		if profiles.IsReviewer(id) {
			t.Fatalf("worker default %q classified as reviewer", id)
		}
	}
}

func TestLookupTrimsAndLabelsKnownProfiles(t *testing.T) {
	profile, ok := profiles.Lookup(" pi-high-context ")
	if !ok {
		t.Fatalf("known profile not found")
	}
	if profile.ID != "pi-high-context" || profile.Label == "" || profile.Role != profiles.RoleWorker || profile.Image == "" {
		t.Fatalf("unexpected profile: %+v", profile)
	}
}

func TestCommandForInputSubstitutesInputPlaceholderWithoutMutatingContract(t *testing.T) {
	profile, _ := profiles.Lookup("pi-standard")
	first := profiles.CommandForInput(profile, "/tmp/input-one.md")
	second := profiles.CommandForInput(profile, "/tmp/input-two.md")
	if first[len(first)-1] != "/tmp/input-one.md" || second[len(second)-1] != "/tmp/input-two.md" {
		t.Fatalf("input placeholder not substituted first=%#v second=%#v", first, second)
	}
	if profile.Command[len(profile.Command)-1] != "{input}" {
		t.Fatalf("profile command mutated: %#v", profile.Command)
	}
}
