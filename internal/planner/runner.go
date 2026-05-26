// Package planner generates pre-approval planner and critic drafts for review-gated tasks.
package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/agent-parley/parley/internal/adapters/pi"
	"github.com/agent-parley/parley/internal/containers"
	"github.com/agent-parley/parley/internal/executor"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/pathsafe"
	"github.com/agent-parley/parley/internal/profiles"
	"github.com/agent-parley/parley/internal/worktrees"
)

const (
	ModeDryRun  = "dry-run"
	ModeLocalPi = "local-pi"

	plannerMountPath  = "/parley/planning"
	workspaceMountPath = "/workspace"

	PlannerInputFile = "planner-input.md"
	CriticInputFile  = "critic-input.md"
)

type Runner interface {
	Run(ctx context.Context, input Input) (Result, error)
}

type PreflightRunner interface {
	Preflight(ctx context.Context, input Input) error
}

type Input struct {
	Project  models.Project
	Session  models.PlannerSession
	Messages []models.PlannerMessage
}

type Draft struct {
	Title        string
	Objective    string
	Focus        string
	Boundaries   string
	DoneWhen     string
	Assumptions  []string
	Risks        []string
	GraphPreview []string
}

type Diagnostic struct {
	Name string
	Kind string
	Body string
}

type Result struct {
	Mode           string
	PlannerProfile string
	CriticProfile  string
	Draft          Draft
	PlannerMessage string
	CriticMessage  string
	Summary        string
	Diagnostics    []Diagnostic
}

type DryRunRunner struct{}

func NewDryRunRunner() DryRunRunner { return DryRunRunner{} }

func (DryRunRunner) Run(ctx context.Context, input Input) (Result, error) {
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	default:
	}
	draft := Draft{
		Title:      fallback(input.Session.DraftTitle, summarize(input.Session.Prompt)),
		Objective:  fallback(input.Session.DraftObjective, input.Session.Prompt),
		Focus:      "Planner and critic agent passes ran in dry-run mode. Treat this as the approval-gated task contract; no Pi process, container, worktree, or remote runner was launched.",
		Boundaries: fallback(input.Session.DraftBoundaries, "Keep execution review-gated, respect project settings, and do not touch secrets, generated assets, or explicitly excluded paths."),
		DoneWhen:   fallback(input.Session.DraftDoneWhen, "The plan is approved into a draft task, the task is explicitly queued, and the worker/reviewer loop is accepted."),
		Assumptions: []string{
			"Dry-run planner/critic execution is deterministic and local-only.",
			"Task execution still requires the existing approval gate before any worker attempt can run.",
		},
		Risks: []string{
			"Repo inspection and live Pi planning require explicit experimental local-pi mode.",
			"Review the generated draft before approving it into a task.",
		},
		GraphPreview: []string{"Prompt", "Planner agent", "Critic agent", "Approval", "Worker", "Fresh review", "Final review"},
	}
	if context := strings.TrimSpace(input.Project.AgentContext); context != "" {
		draft.Assumptions = append(draft.Assumptions, "Project context was included in the planning prompt.")
	}
	return Result{
		Mode:           ModeDryRun,
		PlannerProfile: profiles.ProfilePlanner,
		CriticProfile:  profiles.ProfileCritic,
		Draft:          draft,
		PlannerMessage: "Dry-run planner pass produced an approval-gated task draft without launching external processes.",
		CriticMessage:  "Dry-run critic pass found no blocking issue in the draft. Approve only if the scope and boundaries match your intent.",
		Summary:        "Dry-run planner/critic pass completed before task approval.",
		Diagnostics: []Diagnostic{
			{Name: "dry-run-trace.txt", Kind: models.PlannerDiagnosticKindTrace, Body: "Dry-run planner/critic generation completed locally. No Pi process, container, worktree, remote runner, or network was used."},
		},
	}, nil
}

type LocalPiRunner struct {
	worktrees *worktrees.LocalManager
	runtime   containers.Runtime
	adapter   pi.Adapter
}

func NewLocalPiRunner(worktreeManager *worktrees.LocalManager, runtime containers.Runtime, adapter pi.Adapter) *LocalPiRunner {
	return &LocalPiRunner{worktrees: worktreeManager, runtime: runtime, adapter: adapter}
}

func (r *LocalPiRunner) Preflight(ctx context.Context, input Input) error {
	if r == nil || r.runtime == nil || r.worktrees == nil {
		return fmt.Errorf("local pi planner is not configured")
	}
	if strings.TrimSpace(input.Project.RepoPath) == "" {
		return fmt.Errorf("project repo path is required for local-pi planning")
	}
	planner, err := r.adapter.Prepare(ctx, profiles.RolePlanner, profiles.ProfilePlanner, plannerMountPath+"/"+PlannerInputFile)
	if err != nil {
		return err
	}
	critic, err := r.adapter.Prepare(ctx, profiles.RoleCritic, profiles.ProfileCritic, plannerMountPath+"/"+CriticInputFile)
	if err != nil {
		return err
	}
	for _, prepared := range []pi.PreparedInvocation{planner, critic} {
		if strings.TrimSpace(prepared.Image) == "" {
			return fmt.Errorf("Pi profile %q has no image configured", prepared.Profile)
		}
		if len(prepared.Command) == 0 || strings.TrimSpace(prepared.Command[0]) == "" {
			return fmt.Errorf("Pi profile %q has no command configured", prepared.Profile)
		}
	}
	if runtime, ok := r.runtime.(interface{ Preflight(context.Context, string) error }); ok {
		if err := runtime.Preflight(ctx, planner.Image); err != nil {
			return err
		}
		if critic.Image != planner.Image {
			return runtime.Preflight(ctx, critic.Image)
		}
	}
	return nil
}

func (r *LocalPiRunner) Run(ctx context.Context, input Input) (Result, error) {
	if r == nil || r.worktrees == nil || r.runtime == nil {
		return Result{}, fmt.Errorf("local pi planner is not configured")
	}
	if strings.TrimSpace(input.Project.RepoPath) == "" {
		return Result{}, fmt.Errorf("project repo path is required for local-pi planning")
	}
	scratchDir, err := plannerRuntimePath(r.worktrees, input.Project.ID, input.Session.ID)
	if err != nil {
		return Result{}, err
	}
	if err := r.worktrees.PrepareManagedDir(scratchDir); err != nil {
		return Result{}, err
	}
	diagnostics := []Diagnostic{}
	defer func() { _ = r.worktrees.RemoveManagedDir(scratchDir) }()
	worktreeRunID := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	worktreeDir, err := plannerWorktreePath(r.worktrees, input.Project.ID, input.Session.ID, worktreeRunID)
	if err != nil {
		return Result{}, err
	}
	branchName := plannerBranchName(input.Project.ID, input.Session.ID, worktreeRunID)
	if err := r.worktrees.Create(ctx, input.Project.RepoPath, branchName, worktreeDir); err != nil {
		return Result{}, err
	}
	cleaned := false
	cleanup := func() error {
		removeErr := r.worktrees.Remove(context.Background(), worktreeDir)
		branchErr := r.worktrees.RemoveBranch(context.Background(), input.Project.RepoPath, branchName)
		if removeErr != nil {
			return removeErr
		}
		return branchErr
	}
	defer func() {
		if !cleaned {
			_ = cleanup()
		}
	}()

	plannerInput := plannerInputBody(input)
	diagnostics = append(diagnostics, Diagnostic{Name: PlannerInputFile, Kind: models.PlannerDiagnosticKindInput, Body: plannerInput})
	plannerInputPath := filepath.Join(scratchDir, PlannerInputFile)
	if err := pathsafe.WriteFileNoFollow(plannerInputPath, []byte(plannerInput), 0o600); err != nil {
		return Result{}, err
	}
	plannerPrepared, err := r.adapter.Prepare(ctx, profiles.RolePlanner, profiles.ProfilePlanner, plannerMountPath+"/"+PlannerInputFile)
	if err != nil {
		return Result{}, err
	}
	plannerInvocation := r.invocation(plannerPrepared, scratchDir, worktreeDir, filepath.Join(scratchDir, "runtime", "planner"))
	if err := ValidatePlannerInvocation(plannerInvocation, scratchDir, worktreeDir, profiles.PlannerIDs()); err != nil {
		return Result{}, err
	}
	plannerResult, err := r.runtime.Run(ctx, plannerInvocation)
	if err != nil {
		return withDiagnostics(Result{Mode: ModeLocalPi, PlannerProfile: plannerPrepared.Profile, CriticProfile: profiles.ProfileCritic, Summary: "Local Pi planner runtime failed before approval."}, diagnostics), err
	}
	plannerOutput := readFileString(plannerResult.StdoutPath)
	diagnostics = append(diagnostics,
		Diagnostic{Name: "planner-stdout.txt", Kind: models.PlannerDiagnosticKindOutput, Body: plannerOutput},
		Diagnostic{Name: "planner-stderr.txt", Kind: models.PlannerDiagnosticKindRuntimeLog, Body: readFileString(plannerResult.StderrPath)},
	)
	if plannerResult.ExitCode != 0 {
		return withDiagnostics(Result{Mode: ModeLocalPi, PlannerProfile: profiles.ProfilePlanner, CriticProfile: profiles.ProfileCritic, PlannerMessage: fmt.Sprintf("Local Pi planner exited with code %d.", plannerResult.ExitCode), Summary: "Local Pi planner failed before approval."}, diagnostics), fmt.Errorf("planner exited with code %d", plannerResult.ExitCode)
	}
	draft, plannerMessage, parseErr := draftFromPlannerOutput(input.Session, plannerOutput)
	if parseErr != nil {
		return withDiagnostics(Result{Mode: ModeLocalPi, PlannerProfile: profiles.ProfilePlanner, CriticProfile: profiles.ProfileCritic, PlannerMessage: plannerMessage, Summary: "Local Pi planner produced invalid draft JSON before approval."}, diagnostics), fmt.Errorf("invalid planner JSON: %w", parseErr)
	}

	criticInput := criticInputBody(input, draft, plannerOutput)
	diagnostics = append(diagnostics, Diagnostic{Name: CriticInputFile, Kind: models.PlannerDiagnosticKindInput, Body: criticInput})
	criticInputPath := filepath.Join(scratchDir, CriticInputFile)
	if err := pathsafe.WriteFileNoFollow(criticInputPath, []byte(criticInput), 0o600); err != nil {
		return Result{}, err
	}
	criticPrepared, err := r.adapter.Prepare(ctx, profiles.RoleCritic, profiles.ProfileCritic, plannerMountPath+"/"+CriticInputFile)
	if err != nil {
		return Result{}, err
	}
	criticInvocation := r.invocation(criticPrepared, scratchDir, worktreeDir, filepath.Join(scratchDir, "runtime", "critic"))
	if err := ValidatePlannerInvocation(criticInvocation, scratchDir, worktreeDir, profiles.CriticIDs()); err != nil {
		return Result{}, err
	}
	criticResult, err := r.runtime.Run(ctx, criticInvocation)
	if err != nil {
		return withDiagnostics(Result{Mode: ModeLocalPi, PlannerProfile: plannerPrepared.Profile, CriticProfile: criticPrepared.Profile, Draft: draft, PlannerMessage: plannerMessage, Summary: "Local Pi critic runtime failed before approval."}, diagnostics), err
	}
	criticOutput := readFileString(criticResult.StdoutPath)
	diagnostics = append(diagnostics,
		Diagnostic{Name: "critic-stdout.txt", Kind: models.PlannerDiagnosticKindOutput, Body: criticOutput},
		Diagnostic{Name: "critic-stderr.txt", Kind: models.PlannerDiagnosticKindRuntimeLog, Body: readFileString(criticResult.StderrPath)},
	)
	if criticResult.ExitCode != 0 {
		return withDiagnostics(Result{Mode: ModeLocalPi, PlannerProfile: profiles.ProfilePlanner, CriticProfile: profiles.ProfileCritic, Draft: draft, PlannerMessage: plannerMessage, CriticMessage: fmt.Sprintf("Local Pi critic exited with code %d.", criticResult.ExitCode), Summary: "Local Pi critic failed before approval."}, diagnostics), fmt.Errorf("critic exited with code %d", criticResult.ExitCode)
	}
	criticMessage, addedRisks, parseErr := criticFromOutput(criticOutput)
	if parseErr != nil {
		return withDiagnostics(Result{Mode: ModeLocalPi, PlannerProfile: plannerPrepared.Profile, CriticProfile: criticPrepared.Profile, Draft: draft, PlannerMessage: plannerMessage, CriticMessage: criticMessage, Summary: "Local Pi critic produced invalid critique JSON before approval."}, diagnostics), fmt.Errorf("invalid critic JSON: %w", parseErr)
	}
	if len(addedRisks) > 0 {
		draft.Risks = appendUnique(draft.Risks, addedRisks...)
	}
	if err := cleanup(); err != nil {
		return withDiagnostics(Result{Mode: ModeLocalPi, PlannerProfile: plannerPrepared.Profile, CriticProfile: criticPrepared.Profile, Draft: draft, PlannerMessage: plannerMessage, CriticMessage: criticMessage, Summary: "Local Pi planner/critic pass completed, but cleanup failed before approval."}, diagnostics), fmt.Errorf("cleanup planner worktree: %w", err)
	}
	cleaned = true
	return withDiagnostics(Result{Mode: ModeLocalPi, PlannerProfile: plannerPrepared.Profile, CriticProfile: criticPrepared.Profile, Draft: draft, PlannerMessage: plannerMessage, CriticMessage: criticMessage, Summary: "Local Pi planner/critic pass completed before task approval."}, diagnostics), nil
}

func (r *LocalPiRunner) invocation(prepared pi.PreparedInvocation, scratchDir, repoPath, outputDir string) containers.Invocation {
	return containers.Invocation{
		Image:      prepared.Image,
		Command:    prepared.Command,
		Env:        prepared.Env,
		Network:    "none",
		Privileged: false,
		WorkDir:    workspaceMountPath,
		OutputDir:  outputDir,
		Profile:    prepared.Profile,
		Mounts: []containers.Mount{
			{HostPath: scratchDir, ContainerPath: plannerMountPath, Mode: "ro"},
			{HostPath: repoPath, ContainerPath: workspaceMountPath, Mode: "ro"},
		},
	}
}

func ValidatePlannerInvocation(invocation containers.Invocation, scratchDir, repoPath string, allowedProfiles []string) error {
	return executor.ValidateInvocation(invocation, executor.InvocationValidation{
		AllowedExecutables: []string{"pi"},
		AllowedProfiles:    allowedProfiles,
		AllowedHostRoots:   []string{scratchDir, repoPath},
		AllowedEnvPrefixes: profileEnvPrefixes(allowedProfiles),
	})
}

func plannerRuntimePath(manager *worktrees.LocalManager, projectID, sessionID string) (string, error) {
	if manager == nil {
		return "", fmt.Errorf("worktree manager is required")
	}
	projectPart := safePathPart(projectID)
	sessionPart := safePathPart(sessionID)
	if projectPart == "" || sessionPart == "" {
		return "", fmt.Errorf("invalid planner runtime path component")
	}
	return filepath.Join(manager.Root(), "runtime", projectPart, "planner", sessionPart, fmt.Sprintf("%d", time.Now().UTC().UnixNano())), nil
}

func plannerWorktreePath(manager *worktrees.LocalManager, projectID, sessionID, runID string) (string, error) {
	if manager == nil {
		return "", fmt.Errorf("worktree manager is required")
	}
	projectPart := safePathPart(projectID)
	sessionPart := safePathPart(sessionID)
	runPart := safePathPart(runID)
	if projectPart == "" || sessionPart == "" || runPart == "" {
		return "", fmt.Errorf("invalid planner worktree path component")
	}
	return filepath.Join(manager.Root(), "active", projectPart, "planner", sessionPart, runPart), nil
}

func plannerBranchName(projectID, sessionID, runID string) string {
	projectPart := safePathPart(projectID)
	sessionPart := safePathPart(sessionID)
	runPart := safePathPart(runID)
	if projectPart == "" {
		projectPart = "project"
	}
	if sessionPart == "" {
		sessionPart = "session"
	}
	if runPart == "" {
		runPart = fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	}
	return fmt.Sprintf("parley/planner/%s/%s/%s", projectPart, sessionPart, runPart)
}

func plannerInputBody(input Input) string {
	return fmt.Sprintf(`# Planner agent input

You are the planner agent for Parley. Inspect the mounted repository at %[1]s if useful, but do not modify files. Produce one JSON object and no prose outside it.

Required JSON shape:
{
  "title": "short task title",
  "objective": "what should be done",
  "focus": "files, areas, or approach notes",
  "boundaries": "what not to touch and safety limits",
  "done_when": "acceptance criteria",
  "assumptions": ["..."],
  "risks": ["..."],
  "graph_preview": ["Prompt", "Planner agent", "Critic agent", "Approval", "Worker", "Fresh review", "Final review"],
  "summary": "brief human summary"
}

Project: %[2]s
Project context:
%[3]s

User prompt:
%[4]s

Prior planning thread:
%[5]s
`, workspaceMountPath, input.Project.Name, fallback(input.Project.AgentContext, input.Project.Description), input.Session.Prompt, messageThread(input.Messages))
}

func criticInputBody(input Input, draft Draft, plannerOutput string) string {
	encodedDraft, _ := json.MarshalIndent(draft, "", "  ")
	return fmt.Sprintf(`# Critic agent input

You are the critic agent for Parley. Review the planner draft before the user approval gate. Inspect the mounted repository at %[1]s if useful, but do not modify files. Produce one JSON object and no prose outside it.

Required JSON shape:
{
  "verdict": "approve-with-cautions|revise-before-approval",
  "summary": "brief critique summary",
  "risks": ["additional risk or gap"],
  "questions": ["question user should answer before approval"]
}

Project: %[2]s
User prompt:
%[3]s

Planner draft JSON:
~~~json
%[4]s
~~~

Raw planner output:
~~~
%[5]s
~~~
`, workspaceMountPath, input.Project.Name, input.Session.Prompt, string(encodedDraft), plannerOutput)
}

func messageThread(messages []models.PlannerMessage) string {
	if len(messages) == 0 {
		return "(none)"
	}
	var b strings.Builder
	for _, message := range messages {
		body := strings.TrimSpace(message.Body)
		if body == "" {
			continue
		}
		b.WriteString(message.Role)
		b.WriteString(": ")
		b.WriteString(body)
		b.WriteString("\n")
	}
	if b.Len() == 0 {
		return "(none)"
	}
	return b.String()
}

func draftFromPlannerOutput(session models.PlannerSession, output string) (Draft, string, error) {
	var payload struct {
		Title        string   `json:"title"`
		Objective    string   `json:"objective"`
		Focus        string   `json:"focus"`
		Boundaries   string   `json:"boundaries"`
		DoneWhen     string   `json:"done_when"`
		Assumptions  []string `json:"assumptions"`
		Risks        []string `json:"risks"`
		GraphPreview []string `json:"graph_preview"`
		Summary      string   `json:"summary"`
	}
	if err := unmarshalJSONObject(output, &payload); err != nil {
		return Draft{}, "Planner agent returned output, but it was not valid draft JSON.", err
	}
	assumptions, assumptionsErr := normalizeList(payload.Assumptions)
	risks, risksErr := normalizeList(payload.Risks)
	graphPreview, graphErr := normalizeList(payload.GraphPreview)
	if strings.TrimSpace(payload.Title) == "" || strings.TrimSpace(payload.Objective) == "" || strings.TrimSpace(payload.Focus) == "" || strings.TrimSpace(payload.Boundaries) == "" || strings.TrimSpace(payload.DoneWhen) == "" || strings.TrimSpace(payload.Summary) == "" || assumptionsErr != nil || risksErr != nil || graphErr != nil || len(graphPreview) == 0 {
		return Draft{}, "Planner agent returned draft JSON missing required fields.", fmt.Errorf("planner draft JSON missing required fields")
	}
	draft := Draft{
		Title:        strings.TrimSpace(payload.Title),
		Objective:    strings.TrimSpace(payload.Objective),
		Focus:        strings.TrimSpace(payload.Focus),
		Boundaries:   strings.TrimSpace(payload.Boundaries),
		DoneWhen:     strings.TrimSpace(payload.DoneWhen),
		Assumptions:  assumptions,
		Risks:        risks,
		GraphPreview: graphPreview,
	}
	message := strings.TrimSpace(payload.Summary)
	if message == "" {
		message = "Planner agent produced a structured draft for approval."
	}
	return draft, message, nil
}

func criticFromOutput(output string) (string, []string, error) {
	var payload struct {
		Verdict   string   `json:"verdict"`
		Summary   string   `json:"summary"`
		Risks     []string `json:"risks"`
		Questions []string `json:"questions"`
	}
	if err := unmarshalJSONObject(output, &payload); err != nil {
		return "Critic agent returned output, but it was not valid critique JSON.", nil, err
	}
	risks, risksErr := normalizeList(payload.Risks)
	questions, questionsErr := normalizeList(payload.Questions)
	verdict := strings.TrimSpace(payload.Verdict)
	if verdict != "approve-with-cautions" && verdict != "revise-before-approval" || strings.TrimSpace(payload.Summary) == "" || risksErr != nil || questionsErr != nil {
		return "Critic agent returned critique JSON missing required fields.", nil, fmt.Errorf("critic JSON missing required fields")
	}
	var parts []string
	parts = append(parts, "Verdict: "+verdict+".")
	parts = append(parts, strings.TrimSpace(payload.Summary))
	if len(questions) > 0 {
		parts = append(parts, "Questions before approval: "+strings.Join(questions, "; "))
	}
	return strings.Join(parts, " "), risks, nil
}

func unmarshalJSONObject(output string, dst any) error {
	trimmed := strings.TrimSpace(output)
	if trimmed == "" {
		return fmt.Errorf("empty output")
	}
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func fallbackDraft(session models.PlannerSession) Draft {
	return Draft{
		Title:        fallback(session.DraftTitle, summarize(session.Prompt)),
		Objective:    fallback(session.DraftObjective, session.Prompt),
		Focus:        session.DraftFocus,
		Boundaries:   session.DraftBoundaries,
		DoneWhen:     session.DraftDoneWhen,
		Assumptions:  append([]string(nil), session.Assumptions...),
		Risks:        append([]string(nil), session.Risks...),
		GraphPreview: append([]string(nil), session.GraphPreview...),
	}
}

func fallback(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func normalizeList(values []string) ([]string, error) {
	if values == nil {
		return nil, fmt.Errorf("missing list")
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return nil, fmt.Errorf("blank list entry")
		}
		out = append(out, trimmed)
	}
	return out, nil
}

func fallbackList(values, fallback []string) []string {
	if len(values) > 0 {
		return appendUnique(nil, values...)
	}
	return appendUnique(nil, fallback...)
}

func appendUnique(values []string, additions ...string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values)+len(additions))
	for _, value := range append(values, additions...) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func withDiagnostics(result Result, diagnostics []Diagnostic) Result {
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	return result
}

func summarize(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "Planned task"
	}
	if len(value) <= 64 {
		return value
	}
	return strings.TrimSpace(value[:61]) + "..."
}

func safePathPart(value string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(value) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
			continue
		}
		b.WriteRune('-')
	}
	return strings.Trim(b.String(), "-.")
}

func readFileString(path string) string {
	if path == "" {
		return ""
	}
	data, err := pathsafe.ReadFileNoFollow(path)
	if err != nil {
		return ""
	}
	return string(data)
}

func profileEnvPrefixes(profileIDs []string) []string {
	seen := map[string]struct{}{}
	var prefixes []string
	for _, id := range profileIDs {
		profile, ok := profiles.Lookup(id)
		if !ok {
			continue
		}
		for _, prefix := range profile.EnvPrefixes {
			if _, ok := seen[prefix]; ok {
				continue
			}
			seen[prefix] = struct{}{}
			prefixes = append(prefixes, prefix)
		}
	}
	return prefixes
}
