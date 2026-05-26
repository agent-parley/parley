package executor

import (
	"context"
	"fmt"

	"github.com/agent-parley/parley/internal/models"
)

type DryRunRunner struct{}

func NewDryRunRunner() DryRunRunner { return DryRunRunner{} }

func (DryRunRunner) RunAttempt(ctx context.Context, input AttemptInput) (AttemptResult, error) {
	select {
	case <-ctx.Done():
		return AttemptResult{}, ctx.Err()
	default:
	}
	runnerName := input.Runner.Name
	if runnerName == "" {
		runnerName = input.Runner.ID
	}
	workerInput := fmt.Sprintf("# Worker input\n\n%s\n\n%sThis dry-run prototype is labeled with simulated runner record %q. It does not launch Pi, Git, worktrees, containers, remote processes, or remote runners yet.\n", input.Task.Objective, ResumeCheckpointSection(input.ResumeCheckpoints), runnerName)
	workerCheckpoint := CheckpointBody(input, "worker", roleOrDefault(input.Task.Role, "worker"), profileOrDefault(input.Task.Adapter, "pi-standard"), "completed", "Dry-run worker step completed without changing repository files.", "Review placeholder outputs and decide whether to accept or request a fix.", nil, []string{"worker-output.md", "summary.md"})
	reviewerCheckpoint := CheckpointBody(input, "reviewer", "reviewer", "pi-reviewer", "completed", "Dry-run reviewer step completed with no findings.", "Final review can accept the task or request a fix.", nil, []string{"review.md", "findings.json"})
	files := []OutputFile{
		{Name: "worker-input.md", Kind: models.ArtifactKindWorkerInput, Sensitivity: models.SensitivityInternal, Body: workerInput},
		{Name: "worker-output.md", Kind: models.ArtifactKindWorkerOutput, Body: fmt.Sprintf("# Prototype worker output\n\nDry-run completed locally using simulated runner record %q. Resume checkpoints available: %d. Pi/container/remote runner execution is scaffolded but not enabled in this prototype.\n", runnerName, len(input.ResumeCheckpoints))},
		{Name: "summary.md", Kind: models.ArtifactKindSummary, Body: "# Summary\n\nDry-run completed. No repository files were changed.\n"},
		{Name: "changed-files.txt", Kind: models.ArtifactKindChangedFiles, Body: ""},
		{Name: "diff.patch", Kind: models.ArtifactKindDiff, Body: ""},
		{Name: "review.md", Kind: models.ArtifactKindReview, Body: "# Prototype review\n\nNo diff was produced. Real fresh-reviewer execution is the next implementation step.\n"},
		{Name: "findings.json", Kind: models.ArtifactKindFindings, Body: "{\n  \"verdict\": \"prototype-placeholder\",\n  \"findings\": []\n}\n"},
		{Name: "checkpoints/worker.json", Kind: models.ArtifactKindCheckpoint, Sensitivity: models.SensitivityInternal, Body: workerCheckpoint},
		{Name: "checkpoints/reviewer.json", Kind: models.ArtifactKindCheckpoint, Sensitivity: models.SensitivityInternal, Body: reviewerCheckpoint},
	}
	return AttemptResult{Files: files, Summary: "Prototype worker and fresh review completed with placeholder outputs."}, nil
}
