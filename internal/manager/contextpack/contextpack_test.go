package contextpack

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/contract"
)

func TestAssemblerBuildsSourceLabeledStageFilteredBrief(t *testing.T) {
	ctx := context.Background()
	repo := initBriefRepo(t, ctx, map[string]string{
		"README.md":              "# Example\n",
		"go.mod":                 "module example.test/parley\n",
		".parley/rules.md":       "Never bypass approval gates.\n",
		".parley/preferences.md": "Prefer short reports.\n",
	})
	req := briefRequest(repo, contract.StageTypeImplementation)
	assembler := NewAssembler(Options{Now: fixedBriefNow})
	brief, err := assembler.Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	if brief.Name != "Stage brief" || brief.SchemaVersion != SchemaVersion {
		t.Fatalf("brief identity = %+v", brief)
	}
	if got := strings.Join(brief.IndexedSources, ","); got != "workflow_snapshot,repo_evidence,project_rules,project_preferences" {
		t.Fatalf("indexed sources = %s", got)
	}
	for _, label := range []string{SourceWorkflowSnapshot, SourceRepoEvidence, SourceProjectRules, SourceProjectPreferences} {
		if !hasSource(brief, label) {
			t.Fatalf("missing source label %s in %+v", label, brief.Sources)
		}
	}

	rulesSource := sourceByLabel(brief, SourceProjectRules)
	status := itemByLabel(rulesSource, "project_rules_status")
	if status.Authority != SourceItemAuthorityInformational || !strings.Contains(status.Text, "No project rules configured") {
		t.Fatalf("project rules status = %+v", status)
	}
	rulesCandidate := itemByLabel(rulesSource, "candidate_project_rules:.parley/rules.md")
	if rulesCandidate.Authority != SourceItemAuthorityCandidate || !strings.Contains(rulesCandidate.AuthorityNote, "Non-authoritative repo suggestion") {
		t.Fatalf("repo rules candidate = %+v", rulesCandidate)
	}
	for _, item := range rulesSource.Items {
		if item.Authority == SourceItemAuthorityAuthoritative {
			t.Fatalf("empty app-state rules should not yield authoritative project_rules item: %+v", item)
		}
	}

	preferencesSource := sourceByLabel(brief, SourceProjectPreferences)
	preferencesCandidate := itemByLabel(preferencesSource, "candidate_project_preferences:.parley/preferences.md")
	if preferencesCandidate.Authority != SourceItemAuthorityCandidate || !strings.Contains(preferencesCandidate.AuthorityNote, "Non-authoritative repo suggestion") {
		t.Fatalf("repo preferences candidate = %+v", preferencesCandidate)
	}
	if queueDefaults := itemByLabel(preferencesSource, "project_queue_defaults"); queueDefaults.Authority != SourceItemAuthorityAuthoritative {
		t.Fatalf("queue defaults authority = %+v", queueDefaults)
	}
	if !hasDeferredSource(brief, "editable_project_rules_preferences_app_state") {
		t.Fatalf("missing project rules/preferences app-state deferred source: %+v", brief.DeferredSources)
	}

	markdown := Markdown(brief)
	for _, want := range []string{"## Source: workflow_snapshot", "## Source: repo_evidence", "## Source: project_rules", "## Source: project_preferences", "Conflict precedence", "candidate_project_rules:.parley/rules.md", "candidate_project_preferences:.parley/preferences.md", "Authority: `candidate`", "Non-authoritative repo suggestion", "No project rules configured in first-slice project state"} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("markdown missing %q:\n%s", want, markdown)
		}
	}
}

func TestProjectRulesAppStateIsAuthoritativeAndRepoRulesRemainCandidate(t *testing.T) {
	ctx := context.Background()
	repo := initBriefRepo(t, ctx, map[string]string{
		"go.mod":           "module example.test/parley\n",
		".parley/rules.md": "Never bypass approval gates.\n",
	})
	req := briefRequest(repo, contract.StageTypeImplementation)
	req.Project.Description = "Ship only after validation passes."
	brief, err := NewAssembler(Options{Now: fixedBriefNow}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}

	rulesSource := sourceByLabel(brief, SourceProjectRules)
	authoritative := itemByLabel(rulesSource, "project_description")
	if authoritative.Authority != SourceItemAuthorityAuthoritative || authoritative.Text != req.Project.Description {
		t.Fatalf("authoritative rules item = %+v", authoritative)
	}
	if status := itemByLabel(rulesSource, "project_rules_status"); status.Label != "" {
		t.Fatalf("unexpected empty-state status with app-state rules: %+v", status)
	}
	candidate := itemByLabel(rulesSource, "candidate_project_rules:.parley/rules.md")
	if candidate.Authority != SourceItemAuthorityCandidate || candidate.Text != "Never bypass approval gates.\n" {
		t.Fatalf("repo rules candidate = %+v", candidate)
	}
	markdown := Markdown(brief)
	for _, want := range []string{"project_description", "Authority: `authoritative`", "candidate_project_rules:.parley/rules.md", "does not receive project_rules precedence"} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("markdown missing %q:\n%s", want, markdown)
		}
	}
}

func TestAssemblerStageAllowlistOmitsSourcesPerStageType(t *testing.T) {
	ctx := context.Background()
	req := briefRequest("", contract.StageTypeValidation)
	brief, err := NewAssembler(Options{Now: fixedBriefNow}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	if hasSource(brief, SourceProjectPreferences) {
		t.Fatalf("validation stage unexpectedly received preferences: %+v", brief.Sources)
	}
	if !hasSource(brief, SourceWorkflowSnapshot) || !hasSource(brief, SourceRepoEvidence) || !hasSource(brief, SourceProjectRules) {
		t.Fatalf("validation sources = %+v", brief.Sources)
	}
}

func TestAssemblerAppliesPerSourceBounds(t *testing.T) {
	ctx := context.Background()
	repo := initBriefRepo(t, ctx, map[string]string{
		"README.md": strings.Repeat("readme ", 100),
		"go.mod":    "module example.test/parley\n",
	})
	req := briefRequest(repo, contract.StageTypeImplementation)
	brief, err := NewAssembler(Options{Now: fixedBriefNow, Bounds: Bounds{MaxSourceBytes: 120, MaxItemBytes: 60, MaxItemsPerSource: 2, MaxSelectedFiles: 2, MaxGitStatusBytes: 60, MaxGitDiffBytes: 60, MaxGitLogBytes: 60, MaxProjectFileBytes: 60}}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	repoSource := sourceByLabel(brief, SourceRepoEvidence)
	if !repoSource.Truncated {
		t.Fatalf("repo source not marked truncated: %+v", repoSource)
	}
	if len(repoSource.Items) > 2 {
		t.Fatalf("repo source items = %d, want bounded to 2", len(repoSource.Items))
	}
	for _, item := range repoSource.Items {
		if len(item.Text) > 120 {
			t.Fatalf("item %s len=%d exceeds source bound", item.Label, len(item.Text))
		}
	}
}

func TestAssemblerCanRegisterFutureSourceWithoutChangingCore(t *testing.T) {
	ctx := context.Background()
	assembler := NewAssembler(Options{
		Now: fixedBriefNow,
		Allowlist: map[string][]string{
			contract.StageTypeImplementation: {SourceWorkflowSnapshot, "future_memory"},
		},
		Providers: []SourceProvider{futureSourceProvider{}},
	})
	brief, err := assembler.Assemble(ctx, briefRequest("", contract.StageTypeImplementation))
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	if !hasSource(brief, "future_memory") {
		t.Fatalf("future source was not included: %+v", brief.Sources)
	}
}

type futureSourceProvider struct{}

func (futureSourceProvider) Label() string { return "future_memory" }
func (futureSourceProvider) Collect(context.Context, Request, Bounds) (Source, error) {
	return Source{Label: "future_memory", Title: "Future memory", Items: []SourceItem{textItem("note", "registered later")}}, nil
}

func briefRequest(repoPath, stageType string) Request {
	stage := store.Stage{ID: "stage_current", ProjectID: "p1", RunID: "run1", TaskID: "task1", AttemptID: "attempt1", StageType: stageType, Adapter: "noop", Status: store.StageStatusPending}
	stages := []store.Stage{
		{ID: "stage_idea", ProjectID: "p1", RunID: "run1", TaskID: "task1", AttemptID: "attempt1", StageType: contract.StageTypeIdeaIntake, Status: store.StageStatusPending},
		stage,
	}
	return Request{
		Project:        store.Project{ID: "p1", Name: "Project", QueueAutoWhenReady: true, QueueMaxConcurrent: 1, QueueBacklogCap: 100},
		Run:            store.Run{ID: "run1", ProjectID: "p1", TaskID: "task1", Idea: "build", Status: store.RunStatusRunning},
		Task:           store.Task{ID: "task1", ProjectID: "p1", Idea: "build", Status: store.RunStatusRunning},
		Attempt:        store.Attempt{ID: "attempt1", ProjectID: "p1", RunID: "run1", TaskID: "task1", Status: store.RunStatusRunning},
		Stages:         stages,
		CurrentStage:   stage,
		RepositoryPath: repoPath,
	}
}

func hasSource(brief StageBrief, label string) bool {
	return sourceByLabel(brief, label).Label != ""
}

func sourceByLabel(brief StageBrief, label string) Source {
	for _, source := range brief.Sources {
		if source.Label == label {
			return source
		}
	}
	return Source{}
}

func itemByLabel(source Source, label string) SourceItem {
	for _, item := range source.Items {
		if item.Label == label {
			return item
		}
	}
	return SourceItem{}
}

func hasDeferredSource(brief StageBrief, label string) bool {
	for _, source := range brief.DeferredSources {
		if source.Label == label {
			return true
		}
	}
	return false
}

func fixedBriefNow() time.Time { return time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC) }

func initBriefRepo(t *testing.T, ctx context.Context, files map[string]string) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir := t.TempDir()
	runBriefGit(t, ctx, dir, "init")
	runBriefGit(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runBriefGit(t, ctx, dir, "config", "user.name", "Parley Test")
	for rel, content := range files {
		path := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	runBriefGit(t, ctx, dir, "add", ".")
	runBriefGit(t, ctx, dir, "commit", "-m", "initial")
	return dir
}

func runBriefGit(t *testing.T, ctx context.Context, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}
