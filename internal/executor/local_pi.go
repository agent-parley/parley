package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/agent-parley/parley/internal/adapters/pi"
	"github.com/agent-parley/parley/internal/containers"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/pathsafe"
	"github.com/agent-parley/parley/internal/profiles"
	"github.com/agent-parley/parley/internal/worktrees"
)

const (
	attemptMountPath = "/parley/attempt"
	worktreeMountPath = "/workspace"
)

type CleanupPolicy struct {
	RetainWorktreeOnFailure bool
}

type LocalPiRunner struct {
	worktrees     *worktrees.LocalManager
	runtime       containers.Runtime
	adapter       pi.Adapter
	cleanupPolicy CleanupPolicy
}

func NewLocalPiRunner(worktreeManager *worktrees.LocalManager, runtime containers.Runtime, adapter pi.Adapter) *LocalPiRunner {
	return &LocalPiRunner{worktrees: worktreeManager, runtime: runtime, adapter: adapter, cleanupPolicy: CleanupPolicy{RetainWorktreeOnFailure: false}}
}

func (r *LocalPiRunner) Preflight(ctx context.Context, input AttemptInput) error {
	if r == nil || r.runtime == nil {
		return fmt.Errorf("local pi runner is not configured")
	}
	workerProfile := profileOrDefault(input.Task.Adapter, "pi-standard")
	if !profiles.IsWorkerDefault(workerProfile) {
		return fmt.Errorf("Pi profile %q is not allowed for worker attempts", workerProfile)
	}
	worker, err := r.adapter.Prepare(ctx, "worker", workerProfile, attemptMountPath+"/worker-input.md")
	if err != nil {
		return err
	}
	reviewer, err := r.adapter.Prepare(ctx, "reviewer", "pi-reviewer", attemptMountPath+"/reviewer-input.md")
	if err != nil {
		return err
	}
	for _, prepared := range []pi.PreparedInvocation{worker, reviewer} {
		if strings.TrimSpace(prepared.Image) == "" {
			return fmt.Errorf("Pi profile %q has no image configured", prepared.Profile)
		}
		if len(prepared.Command) == 0 || strings.TrimSpace(prepared.Command[0]) == "" {
			return fmt.Errorf("Pi profile %q has no command configured", prepared.Profile)
		}
	}
	if runtime, ok := r.runtime.(interface{ Preflight(context.Context, string) error }); ok {
		if err := runtime.Preflight(ctx, worker.Image); err != nil {
			return err
		}
		if reviewer.Image != worker.Image {
			return runtime.Preflight(ctx, reviewer.Image)
		}
	}
	return nil
}

func (r *LocalPiRunner) RunAttempt(ctx context.Context, input AttemptInput) (AttemptResult, error) {
	if r == nil || r.worktrees == nil || r.runtime == nil {
		return AttemptResult{}, fmt.Errorf("local pi runner is not configured")
	}
	artifactDir := input.ArtifactDir
	if strings.TrimSpace(artifactDir) == "" {
		return AttemptResult{}, fmt.Errorf("attempt artifact directory is required")
	}
	scratchDir, err := r.worktrees.RuntimePath(input.Project.ID, input.Run.ID, input.Task.ID, input.Attempt.Number)
	if err != nil {
		return AttemptResult{}, err
	}
	if err := r.worktrees.PrepareManagedDir(scratchDir); err != nil {
		return AttemptResult{}, err
	}
	defer func() { _ = r.worktrees.RemoveManagedDir(scratchDir) }()
	worktreePath, err := r.worktrees.WorktreePath(input.Project.ID, input.Run.ID, input.Task.ID, input.Attempt.Number)
	if err != nil {
		return AttemptResult{}, err
	}
	branchName := input.Task.BranchName
	if strings.TrimSpace(branchName) == "" {
		branchName = worktrees.BranchName(input.Task.ID, input.Attempt.Number)
	}
	if err := r.worktrees.Create(ctx, input.Project.RepoPath, branchName, worktreePath); err != nil {
		return AttemptResult{}, err
	}
	succeeded := false
	defer func() {
		if succeeded || !r.cleanupPolicy.RetainWorktreeOnFailure {
			_ = r.worktrees.Remove(context.Background(), worktreePath)
		}
	}()

	workerInput := workerInputBody(input, worktreePath)
	workerInputPath := scratchFile(scratchDir, "worker-input.md")
	if err := pathsafe.WriteFileNoFollow(workerInputPath, []byte(workerInput), 0o600); err != nil {
		return AttemptResult{}, err
	}
	workerPrepared, err := r.adapter.Prepare(ctx, roleOrDefault(input.Task.Role, "worker"), profileOrDefault(input.Task.Adapter, "pi-standard"), attemptMountPath+"/worker-input.md")
	if err != nil {
		return AttemptResult{}, err
	}
	workerInvocation := r.containerInvocation(workerPrepared, scratchDir, worktreePath, scratchFile(scratchDir, "runtime", "worker"))
	if err := ValidateInvocation(workerInvocation, localPiValidationRules(scratchDir, worktreePath)); err != nil {
		return AttemptResult{}, err
	}
	input.emitProgress(models.EventAttemptWorkerStarted, "Local Pi worker step started.", map[string]any{"mode": "local-pi", "profile": workerPrepared.Profile, "attempt": input.Attempt.Number})
	workerResult, err := r.runtime.Run(ctx, workerInvocation)
	if err != nil {
		input.emitProgress(models.EventAttemptWorkerFinished, "Local Pi worker runtime failed.", map[string]any{"mode": "local-pi", "profile": workerPrepared.Profile, "attempt": input.Attempt.Number, "status": "failed"})
		return AttemptResult{}, err
	}
	if workerResult.ExitCode != 0 {
		input.emitProgress(models.EventAttemptWorkerFinished, "Local Pi worker step failed.", map[string]any{"mode": "local-pi", "profile": workerPrepared.Profile, "attempt": input.Attempt.Number, "status": "failed", "exit_code": workerResult.ExitCode})
		changedFiles, diff := r.captureWorktreeState(ctx, worktreePath)
		summary := fmt.Sprintf("Local Pi worker failed with exit code %d. Worktree was cleaned up after diagnostics were captured.", workerResult.ExitCode)
		return localPiAttemptResult(input, workerInput, workerResult, nil, changedFiles, diff, summary), fmt.Errorf("worker exited with code %d", workerResult.ExitCode)
	}
	input.emitProgress(models.EventAttemptWorkerFinished, "Local Pi worker step completed.", map[string]any{"mode": "local-pi", "profile": workerPrepared.Profile, "attempt": input.Attempt.Number, "status": "completed", "exit_code": workerResult.ExitCode})

	changedFiles, err := r.worktrees.ChangedFiles(ctx, worktreePath)
	if err != nil {
		return AttemptResult{}, err
	}
	diff, err := r.worktrees.DiffIncludingUntracked(ctx, worktreePath)
	if err != nil {
		return AttemptResult{}, err
	}

	reviewerInput := reviewerInputBody(input, changedFiles, string(diff))
	reviewerInputPath := scratchFile(scratchDir, "reviewer-input.md")
	if err := pathsafe.WriteFileNoFollow(reviewerInputPath, []byte(reviewerInput), 0o600); err != nil {
		return AttemptResult{}, err
	}
	reviewerPrepared, err := r.adapter.Prepare(ctx, "reviewer", "pi-reviewer", attemptMountPath+"/reviewer-input.md")
	if err != nil {
		return AttemptResult{}, err
	}
	reviewerInvocation := r.containerInvocation(reviewerPrepared, scratchDir, worktreePath, scratchFile(scratchDir, "runtime", "reviewer"))
	if err := ValidateInvocation(reviewerInvocation, reviewerValidationRules(scratchDir, worktreePath)); err != nil {
		return AttemptResult{}, err
	}
	input.emitProgress(models.EventAttemptReviewerStarted, "Local Pi reviewer step started.", map[string]any{"mode": "local-pi", "profile": reviewerPrepared.Profile, "attempt": input.Attempt.Number})
	reviewerResult, err := r.runtime.Run(ctx, reviewerInvocation)
	if err != nil {
		input.emitProgress(models.EventAttemptReviewerFinished, "Local Pi reviewer runtime failed.", map[string]any{"mode": "local-pi", "profile": reviewerPrepared.Profile, "attempt": input.Attempt.Number, "status": "failed"})
		return AttemptResult{}, err
	}
	if reviewerResult.ExitCode != 0 {
		input.emitProgress(models.EventAttemptReviewerFinished, "Local Pi reviewer step failed.", map[string]any{"mode": "local-pi", "profile": reviewerPrepared.Profile, "attempt": input.Attempt.Number, "status": "failed", "exit_code": reviewerResult.ExitCode})
		summary := fmt.Sprintf("Local Pi reviewer failed with exit code %d. Worker exit code: %d. Worktree was cleaned up after diagnostics were captured.", reviewerResult.ExitCode, workerResult.ExitCode)
		return localPiAttemptResult(input, workerInput, workerResult, &reviewerResult, changedFiles, diff, summary), fmt.Errorf("reviewer exited with code %d", reviewerResult.ExitCode)
	}
	input.emitProgress(models.EventAttemptReviewerFinished, "Local Pi reviewer step completed.", map[string]any{"mode": "local-pi", "profile": reviewerPrepared.Profile, "attempt": input.Attempt.Number, "status": "completed", "exit_code": reviewerResult.ExitCode})

	succeeded = true
	summary := fmt.Sprintf("Local Pi execution completed in a managed worktree. Worker exit code: %d. Reviewer exit code: %d. Worktree was cleaned up after artifacts were captured.", workerResult.ExitCode, reviewerResult.ExitCode)
	return localPiAttemptResult(input, workerInput, workerResult, &reviewerResult, changedFiles, diff, summary), nil
}

func (r *LocalPiRunner) captureWorktreeState(ctx context.Context, worktreePath string) ([]string, []byte) {
	changedFiles, err := r.worktrees.ChangedFiles(ctx, worktreePath)
	if err != nil {
		changedFiles = []string{"failed to capture changed files: " + err.Error()}
	}
	diff, err := r.worktrees.DiffIncludingUntracked(ctx, worktreePath)
	if err != nil {
		diff = []byte("failed to capture diff: " + err.Error() + "\n")
	}
	return changedFiles, diff
}

func localPiAttemptResult(input AttemptInput, workerInput string, workerResult containers.Result, reviewerResult *containers.Result, changedFiles []string, diff []byte, summary string) AttemptResult {
	changedBody := strings.Join(changedFiles, "\n")
	if changedBody != "" {
		changedBody += "\n"
	}
	workerExitCode := workerResult.ExitCode
	files := []OutputFile{
		{Name: "worker-input.md", Kind: models.ArtifactKindWorkerInput, Sensitivity: models.SensitivityInternal, Body: workerInput},
		{Name: "worker-output.md", Kind: models.ArtifactKindWorkerOutput, Body: capturedOutput("Worker", workerResult)},
		{Name: "summary.md", Kind: models.ArtifactKindSummary, Body: "# Summary\n\n" + summary + "\n"},
		{Name: "changed-files.txt", Kind: models.ArtifactKindChangedFiles, Body: changedBody},
		{Name: "diff.patch", Kind: models.ArtifactKindDiff, Body: string(diff)},
		{Name: "findings.json", Kind: models.ArtifactKindFindings, Body: findingsBody(workerResult.ExitCode, reviewerResult)},
		{Name: "runtime/worker/stdout.txt", Kind: models.ArtifactKindWorkerOutput, Sensitivity: models.SensitivityInternal, Body: readFileString(workerResult.StdoutPath)},
		{Name: "runtime/worker/stderr.txt", Kind: models.ArtifactKindWorkerOutput, Sensitivity: models.SensitivityInternal, Body: readFileString(workerResult.StderrPath)},
		{Name: "checkpoints/worker.json", Kind: models.ArtifactKindCheckpoint, Sensitivity: models.SensitivityInternal, Body: CheckpointBody(input, "worker", roleOrDefault(input.Task.Role, "worker"), profileOrDefault(input.Task.Adapter, "pi-standard"), exitStatus(workerResult.ExitCode), fmt.Sprintf("Local Pi worker step completed with exit code %d.", workerResult.ExitCode), "Run fresh review when worker succeeds; otherwise inspect internal diagnostics and request a fix.", &workerExitCode, []string{"worker-output.md", "changed-files.txt", "diff.patch"})},
	}
	if reviewerResult != nil {
		reviewerExitCode := reviewerResult.ExitCode
		files = append(files,
			OutputFile{Name: "review.md", Kind: models.ArtifactKindReview, Body: capturedOutput("Fresh review", *reviewerResult)},
			OutputFile{Name: "runtime/reviewer/stdout.txt", Kind: models.ArtifactKindReview, Sensitivity: models.SensitivityInternal, Body: readFileString(reviewerResult.StdoutPath)},
			OutputFile{Name: "runtime/reviewer/stderr.txt", Kind: models.ArtifactKindReview, Sensitivity: models.SensitivityInternal, Body: readFileString(reviewerResult.StderrPath)},
			OutputFile{Name: "checkpoints/reviewer.json", Kind: models.ArtifactKindCheckpoint, Sensitivity: models.SensitivityInternal, Body: CheckpointBody(input, "reviewer", "reviewer", "pi-reviewer", exitStatus(reviewerResult.ExitCode), fmt.Sprintf("Local Pi reviewer step completed with exit code %d.", reviewerResult.ExitCode), "Use reviewer findings during final review or next fix attempt.", &reviewerExitCode, []string{"review.md", "findings.json"})},
		)
	} else {
		files = append(files, OutputFile{Name: "review.md", Kind: models.ArtifactKindReview, Body: "# Fresh review\n\nReviewer did not run because the worker step failed.\n"})
	}
	return AttemptResult{Files: files, Summary: summary}
}

func (r *LocalPiRunner) containerInvocation(prepared pi.PreparedInvocation, scratchDir, worktreePath, outputDir string) containers.Invocation {
	return containers.Invocation{
		Image:      prepared.Image,
		Command:    prepared.Command,
		Env:        prepared.Env,
		Network:    "none",
		Privileged: false,
		WorkDir:    worktreeMountPath,
		OutputDir:  outputDir,
		Profile:    prepared.Profile,
		Mounts: []containers.Mount{
			{HostPath: scratchDir, ContainerPath: attemptMountPath, Mode: "ro"},
			{HostPath: worktreePath, ContainerPath: worktreeMountPath, Mode: "rw"},
		},
	}
}

func localPiValidationRules(scratchDir, worktreePath string) InvocationValidation {
	return InvocationValidation{
		AllowedExecutables: []string{"pi"},
		AllowedProfiles:    profiles.WorkerDefaultIDs(),
		AllowedHostRoots:   []string{scratchDir, worktreePath},
		AllowedEnvPrefixes: profileEnvPrefixes(profiles.WorkerDefaultIDs()),
	}
}

func reviewerValidationRules(scratchDir, worktreePath string) InvocationValidation {
	return InvocationValidation{
		AllowedExecutables: []string{"pi"},
		AllowedProfiles:    profiles.ReviewerIDs(),
		AllowedHostRoots:   []string{scratchDir, worktreePath},
		AllowedEnvPrefixes: profileEnvPrefixes(profiles.ReviewerIDs()),
	}
}

func scratchFile(root string, parts ...string) string {
	return filepath.Join(append([]string{root}, parts...)...)
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

func workerInputBody(input AttemptInput, worktreePath string) string {
	return fmt.Sprintf("# Worker input\n\n## Task\n%s\n\n## Focus\n%s\n\n## Acceptance criteria\n%s\n\n## Boundaries\n%s\n\n%sRepository worktree: `%s`\n", input.Task.Objective, input.Task.Focus, input.Task.AcceptanceCriteria, input.Task.ExcludedPaths, ResumeCheckpointSection(input.ResumeCheckpoints), worktreePath)
}

func reviewerInputBody(input AttemptInput, changedFiles []string, diff string) string {
	changed := strings.Join(changedFiles, "\n")
	if changed == "" {
		changed = "(none)"
	}
	if diff == "" {
		diff = "(empty diff)"
	}
	return fmt.Sprintf("# Fresh review input\n\n## Task\n%s\n\n## Acceptance criteria\n%s\n\n%s## Changed files\n%s\n\n## Diff\n```diff\n%s\n```\n", input.Task.Objective, input.Task.AcceptanceCriteria, ResumeCheckpointSection(input.ResumeCheckpoints), changed, diff)
}

func exitStatus(exitCode int) string {
	if exitCode == 0 {
		return "completed"
	}
	return "failed"
}

func capturedOutput(title string, result containers.Result) string {
	var b strings.Builder
	b.WriteString("# ")
	b.WriteString(title)
	b.WriteString("\n\n")
	b.WriteString(fmt.Sprintf("Exit code: %d\n\n", result.ExitCode))
	b.WriteString("Runtime stdout/stderr were captured as internal diagnostics and are excluded from normal artifact preview and handoff. Use the task Diagnostics tab for local-only inspection.\n")
	return b.String()
}

func findingsBody(workerExitCode int, reviewerResult *containers.Result) string {
	verdict := "local-pi-review-complete"
	body := map[string]any{"verdict": verdict, "worker_exit_code": workerExitCode, "findings": []any{}}
	if reviewerResult == nil {
		body["verdict"] = "local-pi-worker-failed"
		body["reviewer_status"] = "not-run"
	} else {
		body["reviewer_exit_code"] = reviewerResult.ExitCode
		if workerExitCode != 0 || reviewerResult.ExitCode != 0 {
			body["verdict"] = "local-pi-review-failed"
		}
	}
	data, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return "{\n  \"verdict\": \"local-pi-review-complete\",\n  \"findings\": []\n}\n"
	}
	return string(data) + "\n"
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

func roleOrDefault(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func profileOrDefault(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
