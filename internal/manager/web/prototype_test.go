package web

import (
	"strings"
	"testing"
)

func TestPrototypeDataCoversSeedRequirements(t *testing.T) {
	data := NewPrototypeDataWithOptions(PrototypeOptions{})
	if data.Tab != "story" || data.View != "runs" {
		t.Fatalf("default view/tab = %q/%q, want runs/story", data.View, data.Tab)
	}
	if len(data.ActiveRuns) != 1 || data.ActiveRuns[0].View.Run.Status != "running" {
		t.Fatalf("active runs = %#v, want the running seed only", data.ActiveRuns)
	}
	if len(data.RecentRuns) != 4 {
		t.Fatalf("recent runs = %d, want 4 terminal seeds", len(data.RecentRuns))
	}

	wantOutcomes := map[string]bool{"completed": false, "failed": false, "cancelled": false, "abandoned": false, "running": false}
	for _, run := range data.Runs {
		if _, ok := wantOutcomes[run.View.Run.Status]; ok {
			wantOutcomes[run.View.Run.Status] = true
		}
	}
	for outcome, seen := range wantOutcomes {
		if !seen {
			t.Fatalf("missing run outcome %q", outcome)
		}
	}

	wantRunnerHealth := map[string]bool{"connected": false, "suspect": false, "down": false}
	for _, runner := range data.Runners {
		if _, ok := wantRunnerHealth[runner.Runner.Status]; ok {
			wantRunnerHealth[runner.Runner.Status] = true
		}
	}
	for status, seen := range wantRunnerHealth {
		if !seen {
			t.Fatalf("missing runner health %q", status)
		}
	}
	if data.RunnerSummary.Label != "3 runners · 1 down" {
		t.Fatalf("runner summary = %q", data.RunnerSummary.Label)
	}

	families := map[string]bool{}
	for _, ev := range data.Selected.EventViews {
		families[ev.Family] = true
	}
	for _, ev := range data.RunnerEvents {
		families[ev.Family] = true
	}
	for _, family := range []string{"run", "stage", "adapter", "harness", "runner", "artifact", "diff", "security"} {
		if !families[family] {
			t.Fatalf("missing event family %q", family)
		}
	}

	var runningStageOpen bool
	for _, stage := range data.Selected.StageGroups {
		if stage.Stage.Status == "running" && stage.Expanded {
			runningStageOpen = true
		}
		if stage.Stage.Status == "completed" && stage.Expanded {
			t.Fatalf("finished stage %q expanded by default", stage.Label)
		}
	}
	if !runningStageOpen {
		t.Fatal("active running stage was not expanded")
	}

	if data.Selected.View.DiffPatch.ID == "" || !strings.Contains(data.Selected.View.DiffPatch.Preview, "diff --git") {
		t.Fatalf("seed diff.patch missing or trivial: %#v", data.Selected.View.DiffPatch)
	}
	if len(data.Selected.DiffLines) == 0 || !data.Selected.DiffIsLong {
		t.Fatal("long diff.patch was not prepared as collapsible diff lines")
	}
	var sawHTMLDownloadOnly bool
	for _, artifact := range data.Selected.View.ArtifactViews {
		if artifact.MediaType == "text/html" && artifact.DownloadOnly && artifact.Preview == "" {
			sawHTMLDownloadOnly = true
		}
	}
	if !sawHTMLDownloadOnly {
		t.Fatal("raw HTML output was not marked download-only without preview")
	}
}

func TestPrototypeTemplateEscapesDiffAndSuppressesRawHTMLPreview(t *testing.T) {
	renderer, err := NewRenderer()
	if err != nil {
		t.Fatalf("new renderer: %v", err)
	}
	html, err := renderer.ExecutePage("prototype.html", NewPrototypeDataWithOptions(PrototypeOptions{Tab: "review"}))
	if err != nil {
		t.Fatalf("render prototype: %v", err)
	}
	if strings.Contains(html, `<section class="malicious">`) || strings.Contains(html, "<script>alert('x')</script>") {
		t.Fatalf("raw diff HTML/script was rendered: %s", html)
	}
	if !strings.Contains(html, "&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;") {
		t.Fatalf("escaped script from diff not visible in preformatted viewer")
	}
	if !strings.Contains(html, "artifact_html_running") || !strings.Contains(html, "raw HTML treated as download") {
		t.Fatalf("download-only raw HTML artifact marker missing")
	}
}

func TestPrototypeTemplateIsSingleLinearDirection(t *testing.T) {
	renderer, err := NewRenderer()
	if err != nil {
		t.Fatalf("new renderer: %v", err)
	}
	html, err := renderer.ExecutePage("prototype.html", NewPrototypeDataWithOptions(PrototypeOptions{}))
	if err != nil {
		t.Fatalf("render prototype: %v", err)
	}
	for _, forbidden := range []string{"?v=", "Command center", "Stage/event map", "Safety review"} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("single-direction prototype rendered forbidden variation marker %q", forbidden)
		}
	}
	// End-user UI: raw event taxonomy and actor internals must not surface.
	for _, internal := range []string{"Raw type", "stage_type", "workflow engine/manager", "stage.started"} {
		if strings.Contains(html, internal) {
			t.Fatalf("operator UI leaked implementation detail %q", internal)
		}
	}
	if !strings.Contains(html, "Stage timeline") || !strings.Contains(html, "Review") || !strings.Contains(html, "3 runners · 1 down") {
		t.Fatalf("linear shell missing expected run tabs, timeline, or runner summary")
	}
}

func TestPrototypeRunnerViewPreservesSelectedRun(t *testing.T) {
	renderer, err := NewRenderer()
	if err != nil {
		t.Fatalf("new renderer: %v", err)
	}
	html, err := renderer.ExecutePage("prototype.html", NewPrototypeDataWithOptions(PrototypeOptions{View: "runners", RunID: "run_proto_failed", Tab: "review"}))
	if err != nil {
		t.Fatalf("render prototype: %v", err)
	}
	if !strings.Contains(html, `/prototype?run=run_proto_failed&amp;tab=review`) {
		t.Fatalf("runner view did not preserve selected run/tab in back navigation")
	}
}
