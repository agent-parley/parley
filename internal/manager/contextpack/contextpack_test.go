package contextpack

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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
	if got := strings.Join(brief.IndexedSources, ","); got != "task_plan,repo_evidence,project_rules,workflow_snapshot,planning_artifacts,project_memory,project_preferences,external_forge_metadata" {
		t.Fatalf("indexed sources = %s", got)
	}
	for _, label := range []string{SourceTaskPlan, SourceRepoEvidence, SourceProjectRules, SourceWorkflowSnapshot, SourcePlanningArtifacts, SourceProjectMemory, SourceProjectPreferences, SourceExternalForgeMetadata} {
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
	if hasDeferredSource(brief, SourceTaskPlan) || hasDeferredSource(brief, SourcePlanningArtifacts) || hasDeferredSource(brief, "private_memory") || hasDeferredSource(brief, SourceExternalForgeMetadata) {
		t.Fatalf("live sources should not be deferred: %+v", brief.DeferredSources)
	}
	if hasDeferredSource(brief, "editable_project_rules_preferences_app_state") {
		t.Fatalf("project rules/preferences app-state is no longer deferred: %+v", brief.DeferredSources)
	}

	markdown := Markdown(brief)
	for _, want := range []string{"## Source: task_plan", "## Source: workflow_snapshot", "## Source: repo_evidence", "## Source: project_rules", "## Source: planning_artifacts", "## Source: project_memory", "## Source: project_preferences", "## Source: external_forge_metadata", "Conflict precedence", "candidate_project_rules:.parley/rules.md", "candidate_project_preferences:.parley/preferences.md", "Authority: `candidate`", "Non-authoritative repo suggestion", "No project rules configured in Parley app state", "No approved task plan artifact is available", "No supplemental planning artifacts are available", "No curated project memory entries are available", "No linked issue or pull request references were detected"} {
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

func TestAssemblerStageAllowlistFiltersProjectMemoryPerStageType(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		stageType  string
		wantMemory bool
	}{
		{contract.StageTypeIdeaIntake, true},
		{contract.StageTypeIdeaRefinement, true},
		{contract.StageTypeReview, true},
		{contract.StageTypeImplementation, true},
		{contract.StageTypeMemoryUpdate, true},
		{contract.StageTypeValidation, false},
		{contract.StageTypeCommit, false},
		{contract.StageTypePRCreation, false},
		{contract.StageTypePRReady, false},
		{contract.StageTypeStopReport, false},
	} {
		t.Run(tc.stageType, func(t *testing.T) {
			brief, err := NewAssembler(Options{Now: fixedBriefNow}).Assemble(ctx, briefRequest("", tc.stageType))
			if err != nil {
				t.Fatalf("Assemble() error = %v", err)
			}
			if gotMemory := hasSource(brief, SourceProjectMemory); gotMemory != tc.wantMemory {
				t.Fatalf("project_memory presence = %t, want %t: %+v", gotMemory, tc.wantMemory, brief.Sources)
			}
		})
	}

	validationBrief, err := NewAssembler(Options{Now: fixedBriefNow}).Assemble(ctx, briefRequest("", contract.StageTypeValidation))
	if err != nil {
		t.Fatalf("Assemble() validation error = %v", err)
	}
	if hasSource(validationBrief, SourceProjectPreferences) {
		t.Fatalf("validation stage unexpectedly received preferences: %+v", validationBrief.Sources)
	}
	for _, label := range []string{SourceTaskPlan, SourceRepoEvidence, SourceProjectRules, SourceWorkflowSnapshot, SourcePlanningArtifacts} {
		if !hasSource(validationBrief, label) {
			t.Fatalf("validation sources missing %s: %+v", label, validationBrief.Sources)
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
	for _, want := range []string{"2. `approved_task_plan` (source: `task_plan`)", "6. `planning_artifacts` (source: `planning_artifacts`)", "7. `project_memory` (source: `project_memory`)", "9. `external_forge_metadata` (source: `external_forge_metadata`)", "## Source: task_plan", "## Source: planning_artifacts", "## Source: project_memory", "Implement the approved plan.", "Original user idea."} {
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

func TestProjectMemoryConflictReusesCollectedSourceText(t *testing.T) {
	ctx := context.Background()
	planID := "artifact_plan"
	contractID := "artifact_contract"
	stage := store.Stage{ID: "stage_current", ProjectID: "p1", RunID: "run1", TaskID: "task1", AttemptID: "attempt1", StageType: contract.StageTypeImplementation, Adapter: "noop", Status: store.StageStatusPending}
	reads := map[string]int{}
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
		ProjectMemoryEntries: []store.ProjectMemoryEntry{
			{ID: "memory_conflict", ProjectID: "p1", Kind: store.ProjectMemoryKindDecision, Title: "Old modes", Body: "delivery_mode: fast\nhandoff_mode: sync", UpdatedAt: "2026-06-07T12:00:00Z"},
		},
		ReadArtifact: func(_ context.Context, artifactID string) ([]byte, error) {
			reads[artifactID]++
			switch artifactID {
			case planID:
				return []byte("delivery_mode: guarded\n"), nil
			case contractID:
				return []byte("handoff_mode: async\n"), nil
			default:
				return nil, os.ErrNotExist
			}
		},
	}
	brief, err := NewAssembler(Options{Now: fixedBriefNow}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	if reads[planID] != 1 || reads[contractID] != 1 {
		t.Fatalf("artifact reads = %#v, want one read per source provider", reads)
	}
	memorySource := sourceByLabel(brief, SourceProjectMemory)
	if !hasWarningContaining(memorySource.Warnings, "higher-precedence task_plan") || !hasWarningContaining(memorySource.Warnings, "higher-precedence planning_artifacts") {
		t.Fatalf("memory conflict warnings = %#v", memorySource.Warnings)
	}
}

func TestProjectMemoryConflictReusesCollectedRepoEvidence(t *testing.T) {
	ctx := context.Background()
	repo := initBriefRepo(t, ctx, map[string]string{
		"README.md": "repo_mode: guarded\n",
		"go.mod":    "module example.test/parley\n",
	})
	req := briefRequest(repo, contract.StageTypeImplementation)
	req.ProjectMemoryEntries = []store.ProjectMemoryEntry{
		{ID: "memory_conflict", ProjectID: "p1", Kind: store.ProjectMemoryKindDecision, Title: "Old repo mode", Body: "repo_mode: fast", UpdatedAt: "2026-06-07T12:00:00Z"},
	}
	readmePath := filepath.Join(repo, "README.md")
	brief, err := NewAssembler(Options{
		Now: fixedBriefNow,
		Allowlist: map[string][]string{
			contract.StageTypeImplementation: {SourceRepoEvidence, "mutate_repo", SourceProjectMemory},
		},
		Providers: []SourceProvider{mutateRepoProvider{path: readmePath}},
	}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	if _, err := os.Stat(readmePath); !os.IsNotExist(err) {
		t.Fatalf("mutating provider did not remove README.md: %v", err)
	}
	memorySource := sourceByLabel(brief, SourceProjectMemory)
	if !hasWarningContaining(memorySource.Warnings, "higher-precedence repo_evidence") || !hasWarningContaining(memorySource.Warnings, "Prefer repo_evidence") {
		t.Fatalf("memory conflict warnings = %#v", memorySource.Warnings)
	}
}

func TestProjectMemoryConflictScansWorkflowStageSettings(t *testing.T) {
	ctx := context.Background()
	req := briefRequest("", contract.StageTypeImplementation)
	req.WorkflowStageSettings = map[string]any{"review_depth": "strict"}
	req.ProjectMemoryEntries = []store.ProjectMemoryEntry{
		{ID: "memory_conflict", ProjectID: "p1", Kind: store.ProjectMemoryKindDecision, Title: "Old review depth", Body: "review_depth: light", UpdatedAt: "2026-06-07T12:00:00Z"},
	}
	brief, err := NewAssembler(Options{Now: fixedBriefNow}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	workflowSource := sourceByLabel(brief, SourceWorkflowSnapshot)
	settings := itemByLabel(workflowSource, "workflow_stage_settings")
	if settings.Authority != SourceItemAuthorityAuthoritative || !strings.Contains(settings.Text, "review_depth: strict") {
		t.Fatalf("workflow stage settings item = %+v", settings)
	}
	memorySource := sourceByLabel(brief, SourceProjectMemory)
	if !hasWarningContaining(memorySource.Warnings, "higher-precedence workflow_stage_settings") || !hasWarningContaining(memorySource.Warnings, "Prefer workflow_stage_settings") {
		t.Fatalf("memory conflict warnings = %#v", memorySource.Warnings)
	}
}

func TestShortValueTruncatesOnRuneBoundary(t *testing.T) {
	got := shortValue(strings.Repeat("é", 120))
	if !utf8.ValidString(got) {
		t.Fatalf("shortValue produced invalid UTF-8: %q", got)
	}
	if !strings.HasSuffix(got, "…") || len([]rune(got)) != 96 {
		t.Fatalf("shortValue = %q, rune len %d", got, len([]rune(got)))
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

func TestExternalForgeMetadataProviderReadsLinkedIssueAndPR(t *testing.T) {
	ctx := context.Background()
	repo := initBriefRepo(t, ctx, map[string]string{"README.md": "# Example\n"})
	runBriefGit(t, ctx, repo, "remote", "add", "origin", "git@github.com:agent-parley/parley.git")
	req := briefRequest(repo, contract.StageTypeImplementation)
	req.Task.Idea = "Implement issue #157 and PR #42"
	req.Run.Idea = req.Task.Idea
	client := &recordingForgeMetadataClient{}

	brief, err := NewAssembler(Options{Now: fixedBriefNow, Providers: []SourceProvider{ExternalForgeMetadataProvider{Client: client}}}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	if len(client.refs) != 2 {
		t.Fatalf("forge refs = %+v, want issue and PR", client.refs)
	}
	if client.refs[0].Kind != forgeKindIssue || client.refs[0].Number != 157 || client.refs[0].Owner != "agent-parley" || client.refs[0].Repo != "parley" {
		t.Fatalf("issue ref = %+v", client.refs[0])
	}
	if client.refs[1].Kind != forgeKindPullRequest || client.refs[1].Number != 42 {
		t.Fatalf("PR ref = %+v", client.refs[1])
	}

	source := sourceByLabel(brief, SourceExternalForgeMetadata)
	if source.PrecedenceRank != 9 || source.Title != "External forge metadata" {
		t.Fatalf("external forge source = %+v", source)
	}
	issue := itemByLabel(source, "external_forge_metadata:github:issue:agent-parley/parley#157")
	if issue.Authority != SourceItemAuthorityInformational || !strings.Contains(issue.AuthorityNote, "lowest-precedence") || !strings.Contains(issue.Text, "# Issue #157: External metadata") || !strings.Contains(issue.Text, "First issue comment") {
		t.Fatalf("issue metadata item = %+v", issue)
	}
	pr := itemByLabel(source, "external_forge_metadata:github:pull_request:agent-parley/parley#42")
	if pr.Authority != SourceItemAuthorityInformational || !strings.Contains(pr.Text, "# Pull request #42: Linked PR") || !strings.Contains(pr.Text, "PR body") {
		t.Fatalf("PR metadata item = %+v", pr)
	}
	markdown := Markdown(brief)
	for _, want := range []string{"## Source: external_forge_metadata", "- Precedence rank: 9", "External forge metadata", "Pull request #42"} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("markdown missing %q:\n%s", want, markdown)
		}
	}
}

func TestExternalForgeMetadataProviderAppliesBounds(t *testing.T) {
	ctx := context.Background()
	repo := initBriefRepo(t, ctx, map[string]string{"README.md": "# Example\n"})
	runBriefGit(t, ctx, repo, "remote", "add", "origin", "https://github.com/agent-parley/parley.git")
	req := briefRequest(repo, contract.StageTypeImplementation)
	req.Task.Idea = "Implement issue #157"
	req.Run.Idea = req.Task.Idea
	client := &recordingForgeMetadataClient{body: strings.Repeat("metadata body ", 80)}

	brief, err := NewAssembler(Options{Now: fixedBriefNow, Bounds: Bounds{MaxSourceBytes: 180, MaxItemBytes: 120, MaxItemsPerSource: 4}, Providers: []SourceProvider{ExternalForgeMetadataProvider{Client: client}}}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	source := sourceByLabel(brief, SourceExternalForgeMetadata)
	item := itemByLabel(source, "external_forge_metadata:github:issue:agent-parley/parley#157")
	if !source.Truncated || !item.Truncated || len(item.Text) > 120 || !strings.Contains(item.Text, "truncated by Stage brief bounds") {
		t.Fatalf("bounded metadata source=%+v item len=%d item=%+v", source, len(item.Text), item)
	}
}

func TestExternalForgeMetadataPolicyDisabledDegradesGracefully(t *testing.T) {
	ctx := context.Background()
	req := briefRequest("", contract.StageTypeImplementation)
	req.Task.Idea = "See https://github.com/agent-parley/parley/issues/157"
	req.Run.Idea = req.Task.Idea
	client := &recordingForgeMetadataClient{err: ForgeBrokerPolicyError{Message: "forge access disabled by policy"}}

	brief, err := NewAssembler(Options{Now: fixedBriefNow, Providers: []SourceProvider{ExternalForgeMetadataProvider{Client: client}}}).Assemble(ctx, req)
	if err != nil {
		t.Fatalf("Assemble() error = %v", err)
	}
	source := sourceByLabel(brief, SourceExternalForgeMetadata)
	if !hasWarningContaining(source.Warnings, "forge broker policy error") || !hasWarningContaining(source.Warnings, "forge access disabled by policy") {
		t.Fatalf("policy warnings = %#v", source.Warnings)
	}
	status := itemByLabel(source, "external_forge_metadata_unavailable")
	if status.Authority != SourceItemAuthorityInformational || !strings.Contains(status.Text, "no issue or pull request metadata could be read") {
		t.Fatalf("unavailable status item = %+v", status)
	}
	markdown := Markdown(brief)
	if !strings.Contains(markdown, "Warning: forge broker policy error") || !strings.Contains(markdown, "external_forge_metadata_unavailable") {
		t.Fatalf("markdown missing graceful policy warning:\n%s", markdown)
	}
}

func TestForgeMetadataParsersDecodeBrokerJSON(t *testing.T) {
	githubRaw := `{"number":157,"title":"External metadata","state":"OPEN","body":"Issue body","url":"https://github.com/agent-parley/parley/issues/157","author":{"login":"octocat"},"labels":[{"name":"agent:ready"}],"comments":[{"body":"Comment body","createdAt":"2026-06-28T12:00:00Z","author":{"login":"reviewer"}}]}`
	githubMetadata, err := parseGitHubMetadata(githubRaw)
	if err != nil {
		t.Fatalf("parseGitHubMetadata() error = %v", err)
	}
	if githubMetadata.Number != 157 || githubMetadata.Title != "External metadata" || githubMetadata.Author != "octocat" || len(githubMetadata.Labels) != 1 || githubMetadata.Labels[0] != "agent:ready" || len(githubMetadata.Comments) != 1 || githubMetadata.Comments[0].Author != "reviewer" {
		t.Fatalf("github metadata = %+v", githubMetadata)
	}

	giteaRaw := `{"index":42,"subject":"Linked PR","status":"open","description":"PR body","html_url":"https://gitea.example.test/team/repo/pulls/42","poster":{"username":"tea-user"},"labels":[{"name":"ready"},"triaged"]}`
	giteaMetadata, err := parseGenericForgeMetadata(giteaRaw)
	if err != nil {
		t.Fatalf("parseGenericForgeMetadata() error = %v", err)
	}
	if giteaMetadata.Number != 42 || giteaMetadata.Title != "Linked PR" || giteaMetadata.Author != "tea-user" || giteaMetadata.URL == "" || strings.Join(giteaMetadata.Labels, ",") != "ready,triaged" {
		t.Fatalf("gitea metadata = %+v", giteaMetadata)
	}
}

func TestBrokerForgeMetadataClientHelpers(t *testing.T) {
	if _, err := (BrokerForgeMetadataClient{}).Fetch(context.Background(), ForgeMetadataReference{Forge: "unknown"}, 128); err == nil || !strings.Contains(err.Error(), "unsupported forge") {
		t.Fatalf("unsupported forge error = %v", err)
	}
	for _, msg := range []string{"broker rejected by policy", "forge access disabled", "disabled by broker"} {
		if !looksLikeForgePolicyError(msg) {
			t.Fatalf("looksLikeForgePolicyError(%q) = false", msg)
		}
	}
	if looksLikeForgePolicyError("network timeout") {
		t.Fatal("network timeout should not be classified as broker policy")
	}

	slowGH := filepath.Join(t.TempDir(), "gh")
	if err := os.WriteFile(slowGH, []byte("#!/bin/sh\nexec sleep 1\n"), 0o700); err != nil {
		t.Fatalf("write slow gh shim: %v", err)
	}
	started := time.Now()
	_, err := (BrokerForgeMetadataClient{GH: slowGH, Timeout: 20 * time.Millisecond}).Fetch(context.Background(), ForgeMetadataReference{Forge: "github", Owner: "agent-parley", Repo: "parley", Kind: forgeKindIssue, Number: 157}, 128)
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("slow broker error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("broker read timeout took %s", elapsed)
	}
}

type recordingForgeMetadataClient struct {
	refs []ForgeMetadataReference
	body string
	err  error
}

func (c *recordingForgeMetadataClient) Fetch(_ context.Context, ref ForgeMetadataReference, maxBytes int) (ForgeMetadata, error) {
	c.refs = append(c.refs, ref)
	if maxBytes <= 0 {
		return ForgeMetadata{}, os.ErrInvalid
	}
	if c.err != nil {
		return ForgeMetadata{}, c.err
	}
	body := c.body
	if body == "" {
		body = "Brokered metadata body."
	}
	metadata := ForgeMetadata{Number: ref.Number, State: "OPEN", Author: "octocat", URL: forgeReferenceURL(ref), Body: body, Labels: []string{"agent:ready"}}
	if ref.Kind == forgeKindPullRequest {
		metadata.Title = "Linked PR"
		metadata.Body = "PR body"
		return metadata, nil
	}
	metadata.Title = "External metadata"
	metadata.Comments = []ForgeMetadataComment{{Author: "reviewer", CreatedAt: "2026-06-28T12:00:00Z", Body: "First issue comment"}}
	return metadata, nil
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

type mutateRepoProvider struct {
	path string
}

func (mutateRepoProvider) Label() string { return "mutate_repo" }
func (p mutateRepoProvider) Collect(context.Context, Request, Bounds) (Source, error) {
	if err := os.Remove(p.path); err != nil {
		return Source{}, err
	}
	return Source{Label: "mutate_repo", Title: "Mutate repo", Items: []SourceItem{textItem("mutation", "removed selected file")}}, nil
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
