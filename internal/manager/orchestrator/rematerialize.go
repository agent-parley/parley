package orchestrator

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/runner/worktree"
)

func (e *Engine) rematerializeWorktreeFromSnapshot(ctx context.Context, wr store.WorkflowRun, snapshot workerSnapshot) (workerSnapshot, error) {
	if snapshot.BaseSHA == "" {
		return snapshot, fmt.Errorf("base sha is required")
	}
	if snapshot.DiffArtifactID == "" {
		return snapshot, fmt.Errorf("diff artifact id is required")
	}
	if wr.Task.RepositoryID == "" {
		return snapshot, fmt.Errorf("repository id is required")
	}
	repo, err := e.store.GetRepository(ctx, wr.Task.RepositoryID)
	if err != nil {
		return snapshot, err
	}
	repoPath := strings.TrimSpace(repo.Path)
	if repoPath == "" {
		return snapshot, fmt.Errorf("repository %s path is required", repo.ID)
	}

	git := e.gitExecutable
	if git == "" {
		git = "git"
	}
	if _, err := runGitCommand(ctx, git, repoPath, "worktree", "prune"); err != nil {
		return snapshot, fmt.Errorf("prune stale worktrees: %w", err)
	}
	wt, err := worktree.Create(ctx, worktree.CreateOptions{
		DataRoot:   e.dataRoot,
		ProjectID:  wr.Run.ProjectID,
		RunID:      wr.Run.ID,
		TaskID:     wr.Task.ID,
		AttemptID:  wr.Attempt.ID,
		SourceRepo: repoPath,
		BaseRef:    snapshot.BaseSHA,
		Git:        git,
	})
	if err != nil {
		return snapshot, err
	}

	artifact, patch, err := e.store.GetArtifact(ctx, snapshot.DiffArtifactID)
	if err != nil {
		return snapshot, err
	}
	if artifact.RunID != wr.Run.ID || artifact.TaskID != wr.Task.ID {
		return snapshot, fmt.Errorf("diff artifact %s belongs to run %s task %s", artifact.ID, artifact.RunID, artifact.TaskID)
	}
	if artifact.Kind != "diff_patch" {
		return snapshot, fmt.Errorf("diff artifact %s has kind %q", artifact.ID, artifact.Kind)
	}
	if err := applyDiffPatch(ctx, git, wt.Path, patch); err != nil {
		return snapshot, fmt.Errorf("apply diff artifact %s: %w", artifact.ID, err)
	}

	baseSHA, baseTreeSHA, workerTreeSHA, err := snapshotGitWorktree(ctx, git, wt.Path)
	if err != nil {
		return snapshot, err
	}
	if baseSHA != snapshot.BaseSHA {
		return snapshot, fmt.Errorf("rematerialized base sha %s does not match persisted base sha %s", baseSHA, snapshot.BaseSHA)
	}
	snapshot.WorktreePath = wt.Path
	snapshot.BaseSHA = baseSHA
	snapshot.BaseTreeSHA = baseTreeSHA
	snapshot.WorkerTreeSHA = workerTreeSHA
	return snapshot, nil
}

func applyDiffPatch(ctx context.Context, git, worktreePath string, patch []byte) error {
	if len(bytes.TrimSpace(patch)) == 0 {
		return nil
	}
	tempDir, err := os.MkdirTemp("", "parley-rematerialize-diff-*")
	if err != nil {
		return fmt.Errorf("create diff temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	patchPath := filepath.Join(tempDir, "diff.patch")
	if err := os.WriteFile(patchPath, patch, 0o600); err != nil {
		return fmt.Errorf("write diff temp file: %w", err)
	}
	if _, err := runGitCommand(ctx, git, worktreePath, "apply", "--check", "--binary", patchPath); err != nil {
		return fmt.Errorf("diff artifact is not an applicable binary git patch: %w", err)
	}
	if _, err := runGitCommand(ctx, git, worktreePath, "apply", "--binary", patchPath); err != nil {
		return fmt.Errorf("git apply: %w", err)
	}
	return nil
}
