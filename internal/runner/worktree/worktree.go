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

// Path returns the canonical harness path for a task worktree.
func Path(opts CreateOptions) string {
	path := filepath.Join(opts.DataRoot, "projects", opts.ProjectID, "worktrees", opts.RunID, opts.TaskID)
	if opts.AttemptID != "" {
		path = filepath.Join(path, opts.AttemptID)
	}
	return path
}

// Locate returns the existing worktree path for a run/task. It prefers the
// attempt-scoped path used by newer adapters, then falls back to the original
// run/task path used by the M2 sample adapter.
func Locate(dataRoot, projectID, runID, taskID, attemptID string) (string, error) {
	if projectID == "" {
		projectID = "default"
	}
	if attemptID != "" {
		attemptPath := Path(CreateOptions{DataRoot: dataRoot, ProjectID: projectID, RunID: runID, TaskID: taskID, AttemptID: attemptID})
		if isGitWorktree(attemptPath) {
			return attemptPath, nil
		}
	}
	legacyPath := Path(CreateOptions{DataRoot: dataRoot, ProjectID: projectID, RunID: runID, TaskID: taskID})
	if isGitWorktree(legacyPath) {
		return legacyPath, nil
	}
	return "", fmt.Errorf("worktree not found for run %s task %s", runID, taskID)
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

	path := Path(opts)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return Worktree{}, fmt.Errorf("create worktree parent: %w", err)
	}
	if _, err := runGit(ctx, git, opts.SourceRepo, "rev-parse", "--show-toplevel"); err != nil {
		return Worktree{}, fmt.Errorf("verify source repo: %w", err)
	}
	if isGitWorktree(path) {
		return Worktree{Path: path, SourceRepo: opts.SourceRepo, ProjectID: opts.ProjectID, RunID: opts.RunID, TaskID: opts.TaskID}, nil
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

// ListUntrackedFiles returns the worktree's untracked, non-ignored files as
// paths relative to the worktree root. It mirrors what CaptureDiff sweeps in
// (`git add -N .` honours .gitignore the same way), so a before/after snapshot
// identifies files a step newly created. NUL-delimited output avoids
// core.quotePath escaping surprises.
func ListUntrackedFiles(ctx context.Context, worktreePath string) ([]string, error) {
	if worktreePath == "" {
		return nil, fmt.Errorf("worktree path is required")
	}
	out, err := runGit(ctx, "git", worktreePath, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, fmt.Errorf("list untracked files: %w", err)
	}
	var files []string
	for _, part := range bytes.Split(out, []byte{0}) {
		if len(part) > 0 {
			files = append(files, string(part))
		}
	}
	return files, nil
}

// RemoveCreatedUntracked deletes untracked files present now but absent from
// the `baseline` snapshot — i.e. files created since the snapshot was taken.
// It is used to strip artifacts a validation command leaves in the worktree
// (e.g. the binary `go build ./...` writes), which would otherwise pollute the
// validation-stage diff artifact. Callers MUST pass a baseline taken from a
// successful ListUntrackedFiles; never call with a baseline from a failed
// snapshot, or worker-created files would be removed. It does not revert
// modifications to already-tracked files. Returns the removed relative paths.
func RemoveCreatedUntracked(ctx context.Context, worktreePath string, baseline []string) ([]string, error) {
	current, err := ListUntrackedFiles(ctx, worktreePath)
	if err != nil {
		return nil, err
	}
	keep := make(map[string]struct{}, len(baseline))
	for _, path := range baseline {
		keep[path] = struct{}{}
	}
	var removed []string
	for _, rel := range current {
		if _, ok := keep[rel]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(worktreePath, rel)); err != nil && !os.IsNotExist(err) {
			return removed, fmt.Errorf("remove created file %s: %w", rel, err)
		}
		removed = append(removed, rel)
	}
	return removed, nil
}

func isGitWorktree(path string) bool {
	if path == "" {
		return false
	}
	if info, err := os.Stat(path); err != nil || !info.IsDir() {
		return false
	}
	if _, err := os.Stat(filepath.Join(path, ".git")); err == nil {
		return true
	}
	out, err := runGit(context.Background(), "git", path, "rev-parse", "--is-inside-work-tree")
	return err == nil && string(bytes.TrimSpace(out)) == "true"
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
