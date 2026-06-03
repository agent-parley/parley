// Package worktree creates per-task git worktrees and captures harness-owned
// diff.patch artifacts after a worker exits.
package worktree

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// CreateOptions describes one task worktree rooted at
// <data-root>/projects/<project>/worktrees/<run>/<task>/. AttemptID is optional;
// when set, it appends one more path segment so retry attempts do not collide.
type CreateOptions struct {
	DataRoot   string
	ProjectID  string
	RunID      string
	TaskID     string
	AttemptID  string
	SourceRepo string
	BaseRef    string
	Git        string
}

// Worktree is the created git worktree.
type Worktree struct {
	Path       string
	SourceRepo string
	ProjectID  string
	RunID      string
	TaskID     string
}

// Create creates a real git worktree for one task attempt.
func Create(ctx context.Context, opts CreateOptions) (Worktree, error) {
	if opts.DataRoot == "" {
		return Worktree{}, fmt.Errorf("data root is required")
	}
	if opts.ProjectID == "" || opts.RunID == "" || opts.TaskID == "" {
		return Worktree{}, fmt.Errorf("project_id, run_id, and task_id are required")
	}
	if opts.SourceRepo == "" {
		return Worktree{}, fmt.Errorf("source repo is required")
	}
	git := opts.Git
	if git == "" {
		git = "git"
	}
	baseRef := opts.BaseRef
	if baseRef == "" {
		baseRef = "HEAD"
	}

	path := filepath.Join(opts.DataRoot, "projects", opts.ProjectID, "worktrees", opts.RunID, opts.TaskID)
	if opts.AttemptID != "" {
		path = filepath.Join(path, opts.AttemptID)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return Worktree{}, fmt.Errorf("create worktree parent: %w", err)
	}
	if _, err := runGit(ctx, git, opts.SourceRepo, "rev-parse", "--show-toplevel"); err != nil {
		return Worktree{}, fmt.Errorf("verify source repo: %w", err)
	}
	if _, err := runGit(ctx, git, opts.SourceRepo, "worktree", "add", "--detach", path, baseRef); err != nil {
		return Worktree{}, fmt.Errorf("create git worktree: %w", err)
	}
	return Worktree{Path: path, SourceRepo: opts.SourceRepo, ProjectID: opts.ProjectID, RunID: opts.RunID, TaskID: opts.TaskID}, nil
}

// CaptureDiff captures tracked, staged, unstaged, and untracked changes as a
// binary git patch. outputPath is optional; when set, the patch is written there.
func CaptureDiff(ctx context.Context, worktreePath, outputPath string) ([]byte, error) {
	if worktreePath == "" {
		return nil, fmt.Errorf("worktree path is required")
	}
	tempIndex, cleanup, err := tempIndex(ctx, worktreePath)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	env := append(os.Environ(), "GIT_INDEX_FILE="+tempIndex)
	// Intent-to-add makes untracked files appear in `git diff HEAD`. Running it
	// against a temporary index avoids mutating the worker worktree's real index.
	if _, err := runGitEnv(ctx, "git", worktreePath, env, "add", "-N", "."); err != nil {
		return nil, fmt.Errorf("mark untracked files for diff: %w", err)
	}
	diff, err := runGitEnv(ctx, "git", worktreePath, env, "diff", "--binary", "--no-ext-diff", "HEAD")
	if err != nil {
		return nil, fmt.Errorf("capture diff: %w", err)
	}
	if outputPath != "" {
		if err := os.MkdirAll(filepath.Dir(outputPath), 0o700); err != nil {
			return nil, fmt.Errorf("create diff output dir: %w", err)
		}
		if err := os.WriteFile(outputPath, diff, 0o600); err != nil {
			return nil, fmt.Errorf("write diff.patch: %w", err)
		}
	}
	return diff, nil
}

func tempIndex(ctx context.Context, worktreePath string) (string, func(), error) {
	indexPathBytes, err := runGit(ctx, "git", worktreePath, "rev-parse", "--git-path", "index")
	if err != nil {
		return "", func() {}, fmt.Errorf("resolve git index: %w", err)
	}
	indexPath := filepath.Clean(string(bytes.TrimSpace(indexPathBytes)))
	if !filepath.IsAbs(indexPath) {
		indexPath = filepath.Join(worktreePath, indexPath)
	}
	tempDir, err := os.MkdirTemp("", "parley-git-index-*")
	if err != nil {
		return "", func() {}, fmt.Errorf("create temp git index: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(tempDir) }
	tempIndex := filepath.Join(tempDir, "index")
	if err := copyFile(indexPath, tempIndex); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("copy git index: %w", err)
	}
	return tempIndex, cleanup, nil
}

func copyFile(src, dst string) error {
	content, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, content, 0o600)
}

func runGit(ctx context.Context, git, dir string, args ...string) ([]byte, error) {
	return runGitEnv(ctx, git, dir, nil, args...)
}

func runGitEnv(ctx context.Context, git, dir string, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, git, args...)
	cmd.Dir = dir
	if env != nil {
		cmd.Env = env
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("git %v: %w: %s", args, err, stderr.String())
		}
		return nil, fmt.Errorf("git %v: %w", args, err)
	}
	return stdout.Bytes(), nil
}
