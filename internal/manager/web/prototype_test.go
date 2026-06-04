package web

import (
	"strings"
	"testing"
)

func TestPrototypeDataCoversSeedRequirements(t *testing.T) {
	data := NewPrototypeData("swimlanes", "", "")
	if data.Variation != "2" {
		t.Fatalf("Variation = %q, want 2", data.Variation)
	}
	if len(data.Variations) != 3 {
		t.Fatalf("variations = %d, want 3", len(data.Variations))
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

	if data.Selected.View.DiffPatch.ID == "" || !strings.Contains(data.Selected.View.DiffPatch.Preview, "diff --git") {
		t.Fatalf("seed diff.patch missing or trivial: %#v", data.Selected.View.DiffPatch)
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
	html, err := renderer.ExecutePage("prototype.html", NewPrototypeData("3", "", ""))
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
		t.Fatalf("download-only raw HTML output marker missing")
	}
}
