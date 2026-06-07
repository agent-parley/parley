package contextpack

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

const (
	SchemaVersion = 1

	SourceWorkflowSnapshot   = "workflow_snapshot"
	SourceRepoEvidence       = "repo_evidence"
	SourceProjectRules       = "project_rules"
	SourceProjectPreferences = "project_preferences"

	SourceItemAuthorityAuthoritative = "authoritative"
	SourceItemAuthorityCandidate     = "candidate"
	SourceItemAuthorityInformational = "informational"
)

type Bounds struct {
	MaxSourceBytes      int `json:"max_source_bytes"`
	MaxItemBytes        int `json:"max_item_bytes"`
	MaxItemsPerSource   int `json:"max_items_per_source"`
	MaxSelectedFiles    int `json:"max_selected_files"`
	MaxGitStatusBytes   int `json:"max_git_status_bytes"`
	MaxGitDiffBytes     int `json:"max_git_diff_bytes"`
	MaxGitLogBytes      int `json:"max_git_log_bytes"`
	MaxProjectFileBytes int `json:"max_project_file_bytes"`
}

func DefaultBounds() Bounds {
	return Bounds{
		MaxSourceBytes:      12 * 1024,
		MaxItemBytes:        4 * 1024,
		MaxItemsPerSource:   12,
		MaxSelectedFiles:    4,
		MaxGitStatusBytes:   4 * 1024,
		MaxGitDiffBytes:     8 * 1024,
		MaxGitLogBytes:      4 * 1024,
		MaxProjectFileBytes: 4 * 1024,
	}
}

type Request struct {
	Project            store.Project                                 `json:"project"`
	Task               store.Task                                    `json:"task"`
	Run                store.Run                                     `json:"run"`
	Attempt            store.Attempt                                 `json:"attempt"`
	Stages             []store.Stage                                 `json:"stages"`
	Events             []event.Event                                 `json:"events"`
	Artifacts          []store.Artifact                              `json:"artifacts"`
	CurrentStage       store.Stage                                   `json:"current_stage"`
	RepositoryPath     string                                        `json:"repository_path,omitempty"`
	RepositoryWarnings []string                                      `json:"repository_warnings,omitempty"`
	ReadArtifact       func(context.Context, string) ([]byte, error) `json:"-"`
}

type StageBrief struct {
	SchemaVersion      int              `json:"schema_version"`
	Name               string           `json:"name"`
	GeneratedAt        string           `json:"generated_at"`
	ProjectID          string           `json:"project_id"`
	RunID              string           `json:"run_id"`
	TaskID             string           `json:"task_id"`
	AttemptID          string           `json:"attempt_id"`
	StageID            string           `json:"stage_id"`
	StageType          string           `json:"stage_type"`
	Bounds             Bounds           `json:"bounds"`
	IndexedSources     []string         `json:"indexed_sources"`
	AllowedSources     []string         `json:"allowed_sources"`
	Sources            []Source         `json:"sources"`
	DeferredSources    []DeferredSource `json:"deferred_sources"`
	ConflictPrecedence []PrecedenceRule `json:"conflict_precedence"`
	Warnings           []string         `json:"warnings,omitempty"`
}

type Source struct {
	Label          string       `json:"label"`
	Title          string       `json:"title"`
	PrecedenceRank int          `json:"precedence_rank"`
	Included       bool         `json:"included"`
	Summary        string       `json:"summary"`
	Items          []SourceItem `json:"items"`
	Truncated      bool         `json:"truncated"`
	Warnings       []string     `json:"warnings,omitempty"`
}

type SourceItem struct {
	Label         string `json:"label"`
	MediaType     string `json:"media_type"`
	Authority     string `json:"authority,omitempty"`
	AuthorityNote string `json:"authority_note,omitempty"`
	Text          string `json:"text"`
	Bytes         int    `json:"bytes"`
	Truncated     bool   `json:"truncated"`
}

type DeferredSource struct {
	Label  string `json:"label"`
	Reason string `json:"reason"`
}

type PrecedenceRule struct {
	Rank   int    `json:"rank"`
	Label  string `json:"label"`
	Source string `json:"source,omitempty"`
}

type SourceProvider interface {
	Label() string
	Collect(context.Context, Request, Bounds) (Source, error)
}

type Options struct {
	Bounds    Bounds
	Allowlist map[string][]string
	Providers []SourceProvider
	Now       func() time.Time
}

type Assembler struct {
	bounds    Bounds
	allowlist map[string][]string
	providers map[string]SourceProvider
	now       func() time.Time
}

func NewAssembler(opts Options) *Assembler {
	bounds := opts.Bounds
	if bounds == (Bounds{}) {
		bounds = DefaultBounds()
	}
	allowlist := defaultAllowlist()
	if opts.Allowlist != nil {
		allowlist = cloneAllowlist(opts.Allowlist)
	}
	a := &Assembler{
		bounds:    bounds,
		allowlist: allowlist,
		providers: map[string]SourceProvider{},
		now:       opts.Now,
	}
	if a.now == nil {
		a.now = func() time.Time { return time.Now().UTC() }
	}
	for _, provider := range defaultProviders() {
		a.Register(provider)
	}
	for _, provider := range opts.Providers {
		a.Register(provider)
	}
	return a
}

func (a *Assembler) Register(provider SourceProvider) {
	if provider == nil || strings.TrimSpace(provider.Label()) == "" {
		return
	}
	a.providers[provider.Label()] = provider
}

func (a *Assembler) Assemble(ctx context.Context, req Request) (StageBrief, error) {
	allowed := a.allowedSources(req.CurrentStage.StageType)
	brief := StageBrief{
		SchemaVersion:      SchemaVersion,
		Name:               "Stage brief",
		GeneratedAt:        a.now().Format(time.RFC3339),
		ProjectID:          req.Run.ProjectID,
		RunID:              req.Run.ID,
		TaskID:             req.Task.ID,
		AttemptID:          req.Attempt.ID,
		StageID:            req.CurrentStage.ID,
		StageType:          req.CurrentStage.StageType,
		Bounds:             a.bounds,
		IndexedSources:     builtinSourceLabels(),
		AllowedSources:     append([]string(nil), allowed...),
		DeferredSources:    deferredSources(),
		ConflictPrecedence: conflictPrecedence(),
	}
	for _, label := range allowed {
		provider, ok := a.providers[label]
		if !ok {
			brief.Sources = append(brief.Sources, Source{Label: label, Title: label, Included: false, Warnings: []string{"no provider registered for source"}})
			continue
		}
		source, err := provider.Collect(ctx, req, a.bounds)
		if err != nil {
			brief.Sources = append(brief.Sources, Source{Label: label, Title: label, PrecedenceRank: precedenceRank(label), Included: true, Warnings: []string{err.Error()}})
			continue
		}
		if source.Label == "" {
			source.Label = label
		}
		if source.Title == "" {
			source.Title = label
		}
		if source.PrecedenceRank == 0 {
			source.PrecedenceRank = precedenceRank(label)
		}
		source.Included = true
		brief.Sources = append(brief.Sources, applyBounds(source, a.bounds))
	}
	return brief, nil
}

func (a *Assembler) allowedSources(stageType string) []string {
	allowed, ok := a.allowlist[stageType]
	if !ok || len(allowed) == 0 {
		allowed = []string{SourceWorkflowSnapshot, SourceProjectRules}
	}
	return append([]string(nil), allowed...)
}

func defaultProviders() []SourceProvider {
	return []SourceProvider{
		WorkflowSnapshotProvider{},
		RepoEvidenceProvider{},
		ProjectRulesProvider{},
		ProjectPreferencesProvider{},
	}
}

func defaultAllowlist() map[string][]string {
	return map[string][]string{
		contract.StageTypeIdeaIntake:     {SourceWorkflowSnapshot, SourceProjectRules, SourceProjectPreferences},
		contract.StageTypeImplementation: {SourceWorkflowSnapshot, SourceRepoEvidence, SourceProjectRules, SourceProjectPreferences},
		contract.StageTypeValidation:     {SourceWorkflowSnapshot, SourceRepoEvidence, SourceProjectRules},
		contract.StageTypeCommit:         {SourceWorkflowSnapshot, SourceRepoEvidence, SourceProjectRules},
		contract.StageTypePRReady:        {SourceWorkflowSnapshot, SourceRepoEvidence, SourceProjectPreferences},
	}
}

func cloneAllowlist(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for stageType, labels := range in {
		out[stageType] = append([]string(nil), labels...)
	}
	return out
}

func builtinSourceLabels() []string {
	return []string{SourceWorkflowSnapshot, SourceRepoEvidence, SourceProjectRules, SourceProjectPreferences}
}

func deferredSources() []DeferredSource {
	return []DeferredSource{
		{Label: "task_plan", Reason: "deferred until B1 idea refinement produces an approved plan"},
		{Label: "planning_artifacts", Reason: "deferred until B1 planning artifacts exist"},
		{Label: "private_memory", Reason: "deferred until B6 memory-update stage and curated project memory exist"},
		{Label: "editable_project_rules_preferences_app_state", Reason: "deferred until a future slice adds editable Parley app-state project rules/preferences; repo .parley/rules.md and .parley/preferences.md remain non-authoritative candidates until promoted"},
		{Label: "external_forge_metadata", Reason: "deferred until forge issue/PR metadata source is built"},
	}
}

func conflictPrecedence() []PrecedenceRule {
	return []PrecedenceRule{
		{Rank: 1, Label: "current_explicit_user_instruction"},
		{Rank: 2, Label: "approved_task_plan", Source: "task_plan"},
		{Rank: 3, Label: "current_repo_evidence", Source: SourceRepoEvidence},
		{Rank: 4, Label: "project_rules", Source: SourceProjectRules},
		{Rank: 5, Label: "workflow_stage_settings", Source: SourceWorkflowSnapshot},
		{Rank: 6, Label: "planning_artifacts"},
		{Rank: 7, Label: "project_memory"},
		{Rank: 8, Label: "project_preferences", Source: SourceProjectPreferences},
	}
}

func precedenceRank(label string) int {
	switch label {
	case SourceRepoEvidence:
		return 3
	case SourceProjectRules:
		return 4
	case SourceWorkflowSnapshot:
		return 5
	case SourceProjectPreferences:
		return 8
	default:
		return 99
	}
}

func Markdown(brief StageBrief) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Stage brief\n\n")
	fmt.Fprintf(&b, "Schema version: `%d`\n", brief.SchemaVersion)
	fmt.Fprintf(&b, "Project ID: `%s`\n", brief.ProjectID)
	fmt.Fprintf(&b, "Run ID: `%s`\n", brief.RunID)
	fmt.Fprintf(&b, "Task ID: `%s`\n", brief.TaskID)
	fmt.Fprintf(&b, "Attempt ID: `%s`\n", brief.AttemptID)
	fmt.Fprintf(&b, "Stage ID: `%s`\n", brief.StageID)
	fmt.Fprintf(&b, "Stage Type: `%s`\n", brief.StageType)
	fmt.Fprintf(&b, "Generated at: `%s`\n\n", brief.GeneratedAt)
	b.WriteString("## Source policy\n\n")
	b.WriteString("This Stage brief is bounded, source-labeled, and filtered by stage type. If sources conflict, prefer lower precedence rank. Raw transcripts and raw repository dumps are not included.\n\n")
	b.WriteString("### Conflict precedence\n\n")
	for _, rule := range brief.ConflictPrecedence {
		if rule.Source == "" {
			fmt.Fprintf(&b, "%d. `%s`\n", rule.Rank, rule.Label)
			continue
		}
		fmt.Fprintf(&b, "%d. `%s` (source: `%s`)\n", rule.Rank, rule.Label, rule.Source)
	}
	b.WriteString("\n")
	fmt.Fprintf(&b, "Indexed sources: `%s`\n\n", strings.Join(brief.IndexedSources, "`, `"))
	fmt.Fprintf(&b, "Allowed for this stage: `%s`\n\n", strings.Join(brief.AllowedSources, "`, `"))
	if len(brief.DeferredSources) > 0 {
		b.WriteString("### Deferred sources\n\n")
		for _, source := range brief.DeferredSources {
			fmt.Fprintf(&b, "- `%s` — %s\n", source.Label, source.Reason)
		}
		b.WriteString("\n")
	}
	b.WriteString("## Sources\n\n")
	for _, source := range brief.Sources {
		fmt.Fprintf(&b, "## Source: %s\n\n", source.Label)
		fmt.Fprintf(&b, "- Title: %s\n", source.Title)
		fmt.Fprintf(&b, "- Precedence rank: %d\n", source.PrecedenceRank)
		fmt.Fprintf(&b, "- Included: %t\n", source.Included)
		if source.Truncated {
			b.WriteString("- Truncated: true\n")
		}
		if source.Summary != "" {
			fmt.Fprintf(&b, "- Summary: %s\n", source.Summary)
		}
		for _, warning := range source.Warnings {
			fmt.Fprintf(&b, "- Warning: %s\n", warning)
		}
		b.WriteString("\n")
		for _, item := range source.Items {
			fmt.Fprintf(&b, "### %s\n\n", item.Label)
			wroteMetadata := false
			if item.Authority != "" {
				fmt.Fprintf(&b, "- Authority: `%s`\n", item.Authority)
				wroteMetadata = true
			}
			if item.AuthorityNote != "" {
				fmt.Fprintf(&b, "- Authority note: %s\n", item.AuthorityNote)
				wroteMetadata = true
			}
			if wroteMetadata {
				b.WriteString("\n")
			}
			if item.Truncated {
				b.WriteString("> [!warning] Item truncated by Stage brief bounds.\n\n")
			}
			writeCodeBlock(&b, item.MediaType, item.Text)
			b.WriteString("\n")
		}
	}
	return b.String()
}

func writeCodeBlock(b *strings.Builder, mediaType, text string) {
	lang := "text"
	if strings.Contains(mediaType, "json") {
		lang = "json"
	} else if strings.Contains(mediaType, "markdown") {
		lang = "markdown"
	} else if strings.Contains(mediaType, "diff") {
		lang = "diff"
	}
	b.WriteString("````" + lang + "\n")
	b.WriteString(text)
	if !strings.HasSuffix(text, "\n") {
		b.WriteString("\n")
	}
	b.WriteString("````\n")
}

func applyBounds(source Source, bounds Bounds) Source {
	if bounds.MaxItemsPerSource > 0 && len(source.Items) > bounds.MaxItemsPerSource {
		source.Items = source.Items[:bounds.MaxItemsPerSource]
		source.Truncated = true
		source.Warnings = append(source.Warnings, fmt.Sprintf("source item count truncated to %d", bounds.MaxItemsPerSource))
	}
	var used int
	for i := range source.Items {
		item := &source.Items[i]
		text, itemTruncated := truncateText(item.Text, bounds.MaxItemBytes)
		if itemTruncated {
			item.Text = text
			item.Truncated = true
			source.Truncated = true
		}
		if bounds.MaxSourceBytes > 0 {
			remaining := bounds.MaxSourceBytes - used
			if remaining <= 0 {
				item.Text = ""
				item.Truncated = true
				source.Truncated = true
			} else {
				text, sourceTruncated := truncateText(item.Text, remaining)
				if sourceTruncated {
					item.Text = text
					item.Truncated = true
					source.Truncated = true
				}
			}
			used += len(item.Text)
		}
		item.Bytes = len(item.Text)
	}
	return source
}

func truncateText(text string, max int) (string, bool) {
	if max <= 0 || len(text) <= max {
		return text, false
	}
	marker := "\n… truncated by Stage brief bounds …"
	if max <= len(marker) {
		return strings.ToValidUTF8(text[:max], ""), true
	}
	cut := strings.ToValidUTF8(text[:max-len(marker)], "")
	return cut + marker, true
}

type WorkflowSnapshotProvider struct{}

func (WorkflowSnapshotProvider) Label() string { return SourceWorkflowSnapshot }

func (WorkflowSnapshotProvider) Collect(ctx context.Context, req Request, bounds Bounds) (Source, error) {
	source := Source{Label: SourceWorkflowSnapshot, Title: "Workflow snapshot", PrecedenceRank: precedenceRank(SourceWorkflowSnapshot), Summary: "Current stage settings, prior stage outputs, and fix-loop attempt state."}
	stageOrder := stageOrder(req.Stages)
	currentIndex := stageOrder[req.CurrentStage.ID]
	workflow := map[string]any{
		"project_id":     req.Run.ProjectID,
		"run_id":         req.Run.ID,
		"task_id":        req.Task.ID,
		"attempt_id":     req.Attempt.ID,
		"current_stage":  stageSnapshot(req.CurrentStage),
		"graph":          "idea_intake->implementation->validation->commit->pr_ready",
		"stages":         stageSnapshots(req.Stages),
		"attempt_status": req.Attempt.Status,
	}
	source.Items = append(source.Items, jsonItem("workflow_snapshot", workflow))

	priorReports, warnings := collectPriorReports(ctx, req, stageOrder, currentIndex)
	source.Warnings = append(source.Warnings, warnings...)
	source.Items = append(source.Items, jsonItem("prior_stage_outputs", priorReports))

	fixLoops := map[string]any{
		"recorded_attempts": uniqueAttemptIDs(req.Stages),
		"fix_loop_attempts": []string{},
		"note":              "No fix-loop attempt records exist in the first-slice workflow state.",
	}
	source.Items = append(source.Items, jsonItem("fix_loop_attempts", fixLoops))
	return source, nil
}

func stageOrder(stages []store.Stage) map[string]int {
	out := make(map[string]int, len(stages))
	for i, stage := range stages {
		out[stage.ID] = i
	}
	return out
}

func stageSnapshot(stage store.Stage) map[string]any {
	out := map[string]any{
		"id":         stage.ID,
		"type":       stage.StageType,
		"adapter":    stage.Adapter,
		"status":     stage.Status,
		"created_at": stage.CreatedAt,
		"updated_at": stage.UpdatedAt,
	}
	if stage.StageBriefArtifactID != "" {
		out["stage_brief_artifact_id"] = stage.StageBriefArtifactID
	}
	return out
}

func stageSnapshots(stages []store.Stage) []map[string]any {
	out := make([]map[string]any, 0, len(stages))
	for _, stage := range stages {
		out = append(out, stageSnapshot(stage))
	}
	return out
}

type priorStageReport struct {
	StageID          string         `json:"stage_id"`
	StageType        string         `json:"stage_type"`
	Status           string         `json:"status"`
	Summary          string         `json:"summary"`
	EvidenceRefs     []string       `json:"evidence_refs,omitempty"`
	PayloadPreview   map[string]any `json:"payload_preview,omitempty"`
	ReportArtifactID string         `json:"report_artifact_id"`
}

func collectPriorReports(ctx context.Context, req Request, order map[string]int, currentIndex int) ([]priorStageReport, []string) {
	var warnings []string
	if req.ReadArtifact == nil {
		return nil, []string{"artifact reader unavailable; prior stage report contents omitted"}
	}
	var reports []priorStageReport
	for _, artifact := range req.Artifacts {
		if artifact.Kind != "report" {
			continue
		}
		content, err := req.ReadArtifact(ctx, artifact.ID)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("read report artifact %s: %v", artifact.ID, err))
			continue
		}
		var rep report.Report
		if err := json.Unmarshal(content, &rep); err != nil {
			warnings = append(warnings, fmt.Sprintf("decode report artifact %s: %v", artifact.ID, err))
			continue
		}
		idx, ok := order[rep.StageID]
		if !ok || idx >= currentIndex {
			continue
		}
		reports = append(reports, priorStageReport{StageID: rep.StageID, StageType: rep.StageType, Status: rep.Status, Summary: rep.Summary, EvidenceRefs: rep.EvidenceRefs, PayloadPreview: payloadPreview(rep.Payload), ReportArtifactID: artifact.ID})
	}
	sort.Slice(reports, func(i, j int) bool { return order[reports[i].StageID] < order[reports[j].StageID] })
	return reports, warnings
}

func payloadPreview(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	allowed := map[string]bool{
		"branch": true, "commit_sha": true, "diff_artifact_id": true, "task_contract_artifact_id": true,
		"workflow_snapshot_frozen": true, "gate": true, "no_verify": true, "hooks_disabled": true,
	}
	out := map[string]any{}
	for key, value := range payload {
		if !allowed[key] {
			continue
		}
		out[key] = boundedJSONValue(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func boundedJSONValue(value any) any {
	switch typed := value.(type) {
	case string:
		text, _ := truncateText(typed, 512)
		return text
	case bool, float64, int, int64, nil:
		return typed
	case []any:
		limit := len(typed)
		if limit > 6 {
			limit = 6
		}
		out := make([]any, 0, limit)
		for i := 0; i < limit; i++ {
			out = append(out, boundedJSONValue(typed[i]))
		}
		return out
	case map[string]any:
		out := map[string]any{}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for i, key := range keys {
			if i >= 8 {
				break
			}
			out[key] = boundedJSONValue(typed[key])
		}
		return out
	default:
		return fmt.Sprint(value)
	}
}

func uniqueAttemptIDs(stages []store.Stage) []string {
	seen := map[string]bool{}
	var ids []string
	for _, stage := range stages {
		if stage.AttemptID == "" || seen[stage.AttemptID] {
			continue
		}
		seen[stage.AttemptID] = true
		ids = append(ids, stage.AttemptID)
	}
	return ids
}

type RepoEvidenceProvider struct {
	Git string
}

func (p RepoEvidenceProvider) Label() string { return SourceRepoEvidence }

func (p RepoEvidenceProvider) Collect(ctx context.Context, req Request, bounds Bounds) (Source, error) {
	source := Source{Label: SourceRepoEvidence, Title: "Repository evidence", PrecedenceRank: precedenceRank(SourceRepoEvidence), Summary: "Selected files plus bounded git status, diff, and history snippets."}
	source.Warnings = append(source.Warnings, req.RepositoryWarnings...)
	repoPath := strings.TrimSpace(req.RepositoryPath)
	if repoPath == "" {
		source.Items = append(source.Items, textItem("repository_path", "No repository path available for this stage."))
		return source, nil
	}
	source.Items = append(source.Items, textItem("repository_path", repoPath))
	git := p.Git
	if git == "" {
		git = "git"
	}
	for _, cmd := range []struct {
		label string
		limit int
		args  []string
	}{
		{label: "git_status", limit: bounds.MaxGitStatusBytes, args: []string{"status", "--short", "--branch"}},
		{label: "git_diff_stat", limit: bounds.MaxGitStatusBytes, args: []string{"diff", "--stat"}},
		{label: "git_diff", limit: bounds.MaxGitDiffBytes, args: []string{"diff", "--no-ext-diff"}},
		{label: "git_history", limit: bounds.MaxGitLogBytes, args: []string{"log", "--oneline", "-n", "5"}},
	} {
		out, truncated, err := gitOutput(ctx, git, repoPath, cmd.limit, cmd.args...)
		if err != nil {
			source.Warnings = append(source.Warnings, fmt.Sprintf("%s: %v", cmd.label, err))
			continue
		}
		item := textItem(cmd.label, strings.TrimSpace(out))
		item.Truncated = truncated
		source.Items = append(source.Items, item)
	}
	selected := selectedFiles(repoPath, bounds)
	for _, item := range selected {
		source.Items = append(source.Items, item)
	}
	return source, nil
}

func selectedFiles(repoPath string, bounds Bounds) []SourceItem {
	candidates := []string{"README.md", "go.mod", "Makefile", ".parley/config.toml", ".github/workflows/ci.yml"}
	var items []SourceItem
	for _, rel := range candidates {
		if bounds.MaxSelectedFiles > 0 && len(items) >= bounds.MaxSelectedFiles {
			break
		}
		content, truncated, err := readRepoFileBounded(repoPath, rel, bounds.MaxProjectFileBytes)
		if err != nil {
			continue
		}
		item := SourceItem{Label: "selected_file:" + rel, MediaType: mediaTypeForPath(rel), Text: content, Bytes: len(content), Truncated: truncated}
		items = append(items, item)
	}
	if len(items) == 0 {
		return []SourceItem{textItem("selected_files", "No selected repository files found within the first-slice candidates.")}
	}
	return items
}

type ProjectRulesProvider struct{}

func (ProjectRulesProvider) Label() string { return SourceProjectRules }

func (ProjectRulesProvider) Collect(_ context.Context, req Request, bounds Bounds) (Source, error) {
	source := Source{Label: SourceProjectRules, Title: "Project rules", PrecedenceRank: precedenceRank(SourceProjectRules), Summary: "Authoritative project rules from Parley app state only. Candidate repo rules in this source are non-authoritative and have no project_rules precedence until promoted."}
	if strings.TrimSpace(req.Project.Description) != "" {
		source.Items = append(source.Items, withAuthority(textItem("project_description", req.Project.Description), SourceItemAuthorityAuthoritative, "Authoritative Parley app state; store.Project.Description is the first-slice project-rules field."))
	} else {
		source.Items = append(source.Items, withAuthority(textItem("project_rules_status", "No project rules configured in first-slice project state."), SourceItemAuthorityInformational, "Status only; no authoritative project_rules content is configured in Parley app state."))
	}
	if req.RepositoryPath != "" {
		if content, truncated, err := readRepoFileBounded(req.RepositoryPath, ".parley/rules.md", bounds.MaxProjectFileBytes); err == nil {
			item := SourceItem{Label: "candidate_project_rules:.parley/rules.md", MediaType: "text/markdown", Authority: SourceItemAuthorityCandidate, AuthorityNote: "Non-authoritative repo suggestion; not accepted project_rules and does not receive project_rules precedence unless the user promotes it into Parley app state.", Text: content, Bytes: len(content), Truncated: truncated}
			source.Items = append(source.Items, item)
			source.Warnings = append(source.Warnings, ".parley/rules.md is a non-authoritative candidate source, not accepted project_rules until promoted to Parley app state")
		}
	}
	return source, nil
}

type ProjectPreferencesProvider struct{}

func (ProjectPreferencesProvider) Label() string { return SourceProjectPreferences }

func (ProjectPreferencesProvider) Collect(_ context.Context, req Request, bounds Bounds) (Source, error) {
	source := Source{Label: SourceProjectPreferences, Title: "Project preferences", PrecedenceRank: precedenceRank(SourceProjectPreferences), Summary: "Lower-precedence project defaults from Parley app state. Candidate repo preferences in this source are non-authoritative suggestions until promoted."}
	prefs := map[string]any{
		"queue_auto_when_ready": req.Project.QueueAutoWhenReady,
		"queue_max_concurrent":  req.Project.QueueMaxConcurrent,
		"queue_backlog_cap":     req.Project.QueueBacklogCap,
	}
	source.Items = append(source.Items, withAuthority(jsonItem("project_queue_defaults", prefs), SourceItemAuthorityAuthoritative, "Authoritative Parley app state project defaults."))
	if req.RepositoryPath != "" {
		if content, truncated, err := readRepoFileBounded(req.RepositoryPath, ".parley/preferences.md", bounds.MaxProjectFileBytes); err == nil {
			item := SourceItem{Label: "candidate_project_preferences:.parley/preferences.md", MediaType: "text/markdown", Authority: SourceItemAuthorityCandidate, AuthorityNote: "Non-authoritative repo suggestion; not accepted project_preferences and does not receive project_preferences precedence unless the user promotes it into Parley app state.", Text: content, Bytes: len(content), Truncated: truncated}
			source.Items = append(source.Items, item)
			source.Warnings = append(source.Warnings, ".parley/preferences.md is a non-authoritative candidate source, not accepted project_preferences until promoted to Parley app state")
		}
	}
	return source, nil
}

func jsonItem(label string, value any) SourceItem {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		content = []byte(fmt.Sprint(value))
	}
	return SourceItem{Label: label, MediaType: "application/json", Text: string(content), Bytes: len(content)}
}

func textItem(label, text string) SourceItem {
	return SourceItem{Label: label, MediaType: "text/plain", Text: text, Bytes: len(text)}
}

func withAuthority(item SourceItem, authority, note string) SourceItem {
	item.Authority = authority
	item.AuthorityNote = note
	return item
}

func mediaTypeForPath(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".md":
		return "text/markdown"
	case ".toml":
		return "application/toml"
	case ".yml", ".yaml":
		return "application/yaml"
	case ".go":
		return "text/x-go"
	default:
		return "text/plain"
	}
}

func readRepoFileBounded(repoPath, rel string, max int) (string, bool, error) {
	clean := filepath.Clean(rel)
	if clean == "." || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", false, fmt.Errorf("unsafe selected path %s", rel)
	}
	path := filepath.Join(repoPath, clean)
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return "", false, err
	}
	file, err := os.Open(path)
	if err != nil {
		return "", false, err
	}
	defer file.Close()
	limit := int64(max)
	if limit <= 0 {
		limit = 4096
	}
	content, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return "", false, err
	}
	truncated := int64(len(content)) > limit
	if truncated {
		content = content[:limit]
	}
	return strings.ToValidUTF8(string(content), ""), truncated, nil
}

type cappedBuffer struct {
	max       int
	buf       []byte
	truncated bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b.max <= 0 {
		return len(p), nil
	}
	remaining := b.max - len(b.buf)
	if remaining > 0 {
		if remaining > len(p) {
			remaining = len(p)
		}
		b.buf = append(b.buf, p[:remaining]...)
	}
	if remaining < len(p) {
		b.truncated = true
	}
	return len(p), nil
}

func (b *cappedBuffer) String() string { return strings.ToValidUTF8(string(b.buf), "") }

func gitOutput(ctx context.Context, git, dir string, max int, args ...string) (string, bool, error) {
	cmd := exec.CommandContext(ctx, git, args...)
	cmd.Dir = dir
	var stdout, stderr cappedBuffer
	stdout.max = max
	stderr.max = 2048
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return stdout.String(), stdout.truncated, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, msg)
		}
		return stdout.String(), stdout.truncated, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return stdout.String(), stdout.truncated, nil
}
