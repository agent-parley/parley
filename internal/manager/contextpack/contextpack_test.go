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
	if got := strings.Join(brief.IndexedSources, ","); got != "task_plan,repo_evidence,project_rules,workflow_snapshot,planning_artifacts,project_memory,project_preferences" {
		t.Fatalf("indexed sources = %s", got)
	}
	for _, label := range []string{SourceTaskPlan, SourceRepoEvidence, SourceProjectRules, SourceWorkflowSnapshot, SourcePlanningArtifacts, SourceProjectMemory, SourceProjectPreferences} {
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
	if status := itemByLabel(preferencesSource, "project_preferences_status"); status.Authority != SourceItemAuthorityInformational || !strings.Contains(status.Text, "No project preferences configured") {
		t.Fatalf("project preferences status = %+v", status)
	}
	if queueDefaults := itemByLabel(preferencesSource, "project_queue_defaults"); queueDefaults.Authority != SourceItemAuthorityAuthoritative {
		t.Fatalf("queue defaults authority = %+v", queueDefaults)
	}
	if hasDeferredSource(brief, SourceTaskPlan) || hasDeferredSource(brief, SourcePlanningArtifacts) || hasDeferredSource(brief, "private_memory") {
		t.Fatalf("live sources should not be deferred: %+v", brief.DeferredSources)
	}
	if hasDeferredSource(brief, "editable_project_rules_preferences_app_state") {
		t.Fatalf("project rules/preferences app-state is no longer deferred: %+v", brief.DeferredSources)
	}

	markdown := Markdown(brief)
	for _, want := range []string{"## Source: task_plan", "## Source: workflow_snapshot", "## Source: repo_evidence", "## Source: project_rules", "## Source: planning_artifacts", "## Source: project_memory", "## Source: project_preferences", "Conflict precedence", "candidate_project_rules:.parley/rules.md", "candidate_project_preferences:.parley/preferences.md", "Authority: `candidate`", "Non-authoritative repo suggestion", "No project rules configured in Parley app state", "No approved task plan artifact is available", "No supplemental planning artifacts are available", "No curated project memory entries are available"} {
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
	req.Project.ProjectRules = "Ship only after validation passes."
	brief, err := NewAssembler(Options{Now: fixedBriefNow}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}

	rulesSource := sourceByLabel(brief, SourceProjectRules)
	authoritative := itemByLabel(rulesSource, "project_rules")
	if authoritative.Authority != SourceItemAuthorityAuthoritative || authoritative.Text != req.Project.ProjectRules || authoritative.MediaType != "text/markdown" {
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
	for _, want := range []string{"project_rules", "Authority: `authoritative`", "candidate_project_rules:.parley/rules.md", "does not receive project_rules precedence"} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("markdown missing %q:\n%s", want, markdown)
		}
	}
}

func TestProjectPreferencesAppStateIsAuthoritativeAndRepoPreferencesRemainCandidate(t *testing.T) {
	ctx := context.Background()
	repo := initBriefRepo(t, ctx, map[string]string{
		"go.mod":                 "module example.test/parley\n",
		".parley/preferences.md": "Prefer concise final reports.\n",
	})
	req := briefRequest(repo, contract.StageTypeImplementation)
	req.Project.ProjectPreferences = "Prefer detailed validation summaries."
	brief, err := NewAssembler(Options{Now: fixedBriefNow}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}

	preferencesSource := sourceByLabel(brief, SourceProjectPreferences)
	authoritative := itemByLabel(preferencesSource, "project_preferences")
	if authoritative.Authority != SourceItemAuthorityAuthoritative || authoritative.Text != req.Project.ProjectPreferences || authoritative.MediaType != "text/markdown" {
		t.Fatalf("authoritative preferences item = %+v", authoritative)
	}
	if status := itemByLabel(preferencesSource, "project_preferences_status"); status.Label != "" {
		t.Fatalf("unexpected empty-state status with app-state preferences: %+v", status)
	}
	candidate := itemByLabel(preferencesSource, "candidate_project_preferences:.parley/preferences.md")
	if candidate.Authority != SourceItemAuthorityCandidate || candidate.Text != "Prefer concise final reports.\n" {
		t.Fatalf("repo preferences candidate = %+v", candidate)
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
	for _, label := range []string{SourceTaskPlan, SourceRepoEvidence, SourceProjectRules, SourceWorkflowSnapshot, SourcePlanningArtifacts, SourceProjectMemory} {
		if !hasSource(brief, label) {
			t.Fatalf("validation sources missing %s: %+v", label, brief.Sources)
		}
	}
}

func TestAssemblerIncludesApprovedTaskPlanAndPlanningArtifacts(t *testing.T) {
	ctx := context.Background()
	planID := "artifact_plan"
	contractID := "artifact_contract"
	stage := store.Stage{ID: "stage_current", ProjectID: "p1", RunID: "run1", TaskID: "task1", AttemptID: "attempt1", StageType: contract.StageTypeImplementation, Adapter: "noop", Status: store.StageStatusPending}
	req := Request{
		Project: store.Project{ID: "p1", Name: "Project", QueueAutoWhenReady: true, QueueMaxConcurrent: 1, QueueBacklogCap: 100},
		Run:     store.Run{ID: "run1", ProjectID: "p1", TaskID: "task1", Idea: "build", Status: store.RunStatusRunning},
		Task:    store.Task{ID: "task1", ProjectID: "p1", Idea: "build", Status: store.RunStatusRunning},
		Attempt: store.Attempt{ID: "attempt1", ProjectID: "p1", RunID: "run1", TaskID: "task1", Status: store.RunStatusRunning},
		Stages: []store.Stage{
			{ID: "stage_idea", ProjectID: "p1", RunID: "run1", TaskID: "task1", AttemptID: "attempt1", StageType: contract.StageTypeIdeaRefinement, Status: "completed", TaskPlanArtifactID: planID},
			stage,
		},
		Artifacts: []store.Artifact{
			{ID: planID, ProjectID: "p1", RunID: "run1", TaskID: "task1", Kind: SourceTaskPlan, MediaType: "text/markdown", CreatedAt: "2026-06-07T12:00:00Z"},
			{ID: contractID, ProjectID: "p1", RunID: "run1", TaskID: "task1", Kind: "task_contract", MediaType: "text/markdown", CreatedAt: "2026-06-07T12:00:01Z"},
		},
		CurrentStage: stage,
		ReadArtifact: func(_ context.Context, artifactID string) ([]byte, error) {
			switch artifactID {
			case planID:
				return []byte("# Task Plan\n\nImplement the approved plan.\n"), nil
			case contractID:
				return []byte("# Parley Task Contract\n\nOriginal user idea.\n"), nil
			default:
				return nil, os.ErrNotExist
			}
		},
	}
	brief, err := NewAssembler(Options{Now: fixedBriefNow}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	planSource := sourceByLabel(brief, SourceTaskPlan)
	if planSource.PrecedenceRank != 2 {
		t.Fatalf("task_plan precedence = %d, want 2", planSource.PrecedenceRank)
	}
	planItem := itemByLabel(planSource, "approved_task_plan")
	if planItem.Authority != SourceItemAuthorityAuthoritative || !strings.Contains(planItem.Text, "Implement the approved plan") {
		t.Fatalf("approved task plan item = %+v", planItem)
	}
	planningSource := sourceByLabel(brief, SourcePlanningArtifacts)
	if planningSource.PrecedenceRank != 6 {
		t.Fatalf("planning_artifacts precedence = %d, want 6", planningSource.PrecedenceRank)
	}
	contractItem := itemByLabel(planningSource, "planning_artifact:task_contract")
	if contractItem.Authority != SourceItemAuthorityAuthoritative || !strings.Contains(contractItem.Text, "Original user idea") {
		t.Fatalf("planning artifact item = %+v", contractItem)
	}
	if hasDeferredSource(brief, SourceTaskPlan) || hasDeferredSource(brief, SourcePlanningArtifacts) {
		t.Fatalf("task_plan/planning_artifacts should not be deferred once providers are live: %+v", brief.DeferredSources)
	}
	markdown := Markdown(brief)
	for _, want := range []string{"2. `approved_task_plan` (source: `task_plan`)", "6. `planning_artifacts` (source: `planning_artifacts`)", "7. `project_memory` (source: `project_memory`)", "## Source: task_plan", "## Source: planning_artifacts", "## Source: project_memory", "Implement the approved plan.", "Original user idea."} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("markdown missing %q:\n%s", want, markdown)
		}
	}
}

func TestProjectMemorySourceIncludesCuratedEntriesWithinBounds(t *testing.T) {
	ctx := context.Background()
	req := briefRequest("", contract.StageTypeImplementation)
	req.ProjectMemoryEntries = []store.ProjectMemoryEntry{
		{ID: "memory_old", ProjectID: "p1", Kind: store.ProjectMemoryKindLesson, Title: "Old lesson", Body: "validation_mode: broad", SourceRunID: "run0", SourceTaskID: "task0", SourceStageID: "stage0", SourceArtifactID: "artifact0", CuratorStageID: "curator0", SourceSummary: "old source", CreatedAt: "2026-06-07T10:00:00Z", UpdatedAt: "2026-06-07T10:00:00Z"},
		{ID: "memory_new", ProjectID: "p1", Kind: store.ProjectMemoryKindGotcha, Title: "Validation image needs git", Body: "validation_image: git\nInstall git before inspecting worktree snapshots.", SourceRunID: "run1", SourceTaskID: "task1", SourceStageID: "stage1", SourceArtifactID: "artifact1", CuratorStageID: "curator1", SourceSummary: "implementation report", CreatedAt: "2026-06-07T12:00:00Z", UpdatedAt: "2026-06-07T12:00:00Z"},
		{ID: "memory_mid", ProjectID: "p1", Kind: store.ProjectMemoryKindRepoFact, Title: "Middle fact", Body: "default_branch: main", SourceRunID: "run1", SourceTaskID: "task1", SourceStageID: "stage2", SourceArtifactID: "artifact2", CuratorStageID: "curator1", SourceSummary: "validation report", CreatedAt: "2026-06-07T11:00:00Z", UpdatedAt: "2026-06-07T11:00:00Z"},
	}
	brief, err := NewAssembler(Options{Now: fixedBriefNow, Bounds: Bounds{MaxSourceBytes: 4096, MaxItemBytes: 2048, MaxItemsPerSource: 2}}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	memorySource := sourceByLabel(brief, SourceProjectMemory)
	if memorySource.PrecedenceRank != 7 || !memorySource.Truncated || len(memorySource.Items) != 2 {
		t.Fatalf("memory source rank/truncation/items = rank %d truncated %t len %d: %+v", memorySource.PrecedenceRank, memorySource.Truncated, len(memorySource.Items), memorySource)
	}
	newest := itemByLabel(memorySource, "project_memory:gotcha:memory_new")
	if newest.Authority != SourceItemAuthorityInformational || !strings.Contains(newest.AuthorityNote, "precedence rank 7") || !strings.Contains(newest.Text, "Source artifact: `artifact1`") || !strings.Contains(newest.Text, "Install git before inspecting worktree snapshots") {
		t.Fatalf("newest memory item = %+v", newest)
	}
	if itemByLabel(memorySource, "project_memory:lesson:memory_old").Label != "" {
		t.Fatalf("oldest memory entry should be outside per-source bound: %+v", memorySource.Items)
	}
	markdown := Markdown(brief)
	for _, want := range []string{"## Source: project_memory", "- Precedence rank: 7", "Authority: `informational`", "Validation image needs git", "source item count truncated to 2"} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("markdown missing %q:\n%s", want, markdown)
		}
	}
}

func TestProjectMemoryConflictWithHigherPrecedenceSourceRaisesWarning(t *testing.T) {
	ctx := context.Background()
	req := briefRequest("", contract.StageTypeImplementation)
	req.Project.ProjectRules = "delivery_mode: guarded\n"
	req.ProjectMemoryEntries = []store.ProjectMemoryEntry{
		{ID: "memory_conflict", ProjectID: "p1", Kind: store.ProjectMemoryKindDecision, Title: "Old delivery mode", Body: "delivery_mode: fast\nUse the old fast path for local handoffs.", SourceStageID: "stage_old", SourceArtifactID: "artifact_old", CuratorStageID: "curator_old", SourceSummary: "older run", UpdatedAt: "2026-06-07T12:00:00Z"},
	}
	brief, err := NewAssembler(Options{Now: fixedBriefNow}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	memorySource := sourceByLabel(brief, SourceProjectMemory)
	if !hasWarningContaining(memorySource.Warnings, "conflicts with higher-precedence project_rules") || !hasWarningContaining(memorySource.Warnings, "not an override") {
		t.Fatalf("memory conflict warnings = %#v", memorySource.Warnings)
	}
	item := itemByLabel(memorySource, "project_memory:decision:memory_conflict")
	if item.Authority == SourceItemAuthorityAuthoritative {
		t.Fatalf("project memory conflict must not become authoritative: %+v", item)
	}
	markdown := Markdown(brief)
	if !strings.Contains(markdown, "Warning: project_memory entry") || !strings.Contains(markdown, "Prefer project_rules") {
		t.Fatalf("markdown missing conflict warning:\n%s", markdown)
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

func hasWarningContaining(warnings []string, want string) bool {
	for _, warning := range warnings {
		if strings.Contains(warning, want) {
			return true
		}
	}
	return false
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
