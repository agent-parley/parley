// Package worktrees manages local Git worktrees for guarded execution.
package worktrees

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"github.com/agent-parley/parley/internal/pathsafe"
)

type Manager interface {
	Create(ctx context.Context, repoPath, branchName, destination string) error
	DiffIncludingUntracked(ctx context.Context, worktreePath string) ([]byte, error)
	ChangedFiles(ctx context.Context, worktreePath string) ([]string, error)
	Remove(ctx context.Context, worktreePath string) error
}

type LocalManager struct {
	root string
}

func NewLocalManager(dataRoot string) *LocalManager {
	return &LocalManager{root: filepath.Join(dataRoot, "worktrees")}
}

func (m *LocalManager) Root() string {
	return m.root
}

func (m *LocalManager) WorktreePath(projectID, runID, taskID string, attemptNumber int) (string, error) {
	return m.pathFor("active", projectID, runID, taskID, attemptNumber)
}

func (m *LocalManager) RuntimePath(projectID, runID, taskID string, attemptNumber int) (string, error) {
	return m.pathFor("runtime", projectID, runID, taskID, attemptNumber)
}

func (m *LocalManager) pathFor(prefix, projectID, runID, taskID string, attemptNumber int) (string, error) {
	if attemptNumber <= 0 {
		return "", fmt.Errorf("attempt number must be positive")
	}
	parts := []string{prefix, projectID, runID, taskID, fmt.Sprintf("%d", attemptNumber)}
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		value := sanitizePathPart(part)
		if value == "" {
			return "", fmt.Errorf("invalid worktree path component")
		}
		clean = append(clean, value)
	}
	return filepath.Join(append([]string{m.root}, clean...)...), nil
}

func BranchName(taskID string, attemptNumber int) string {
	base := sanitizeBranchPart(taskID)
	if base == "" {
		base = "task"
	}
	return fmt.Sprintf("parley/%s/attempt-%d", base, attemptNumber)
}

func (m *LocalManager) Create(ctx context.Context, repoPath, branchName, destination string) error {
	if strings.TrimSpace(branchName) == "" {
		return fmt.Errorf("branch name is required")
	}
	if err := m.prepareManagedParent(destination); err != nil {
		return err
	}
	if err := m.ensureManagedPath(destination); err != nil {
		return err
	}
	if info, err := os.Lstat(destination); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("worktree path must not be a symlink")
		}
		if err := verifyExistingWorktree(ctx, repoPath, destination, branchName); err != nil {
			return err
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return runGit(ctx, repoPath, "worktree", "add", "-b", branchName, destination, "HEAD")
}

// DiffIncludingUntracked marks untracked files with intent-to-add before diffing so the patch includes new files.
func (m *LocalManager) DiffIncludingUntracked(ctx context.Context, worktreePath string) ([]byte, error) {
	if err := m.ensureManagedPath(worktreePath); err != nil {
		return nil, err
	}
	if err := runGit(ctx, worktreePath, "add", "-N", "."); err != nil {
		return nil, err
	}
	return gitOutput(ctx, worktreePath, "diff", "--binary")
}

func (m *LocalManager) ChangedFiles(ctx context.Context, worktreePath string) ([]string, error) {
	if err := m.ensureManagedPath(worktreePath); err != nil {
		return nil, err
	}
	out, err := gitOutput(ctx, worktreePath, "status", "--porcelain=v1")
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == "" || len(line) < 4 {
			continue
		}
		path := strings.TrimSpace(line[3:])
		if strings.Contains(path, " -> ") {
			parts := strings.Split(path, " -> ")
			path = parts[len(parts)-1]
		}
		path = strings.Trim(path, "\"")
		if path != "" {
			seen[path] = struct{}{}
		}
	}
	files := make([]string, 0, len(seen))
	for path := range seen {
		files = append(files, path)
	}
	sort.Strings(files)
	return files, nil
}

func (m *LocalManager) Remove(ctx context.Context, worktreePath string) error {
	if err := m.ensureManagedPath(worktreePath); err != nil {
		return err
	}
	if _, err := os.Stat(worktreePath); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if err := runGit(ctx, worktreePath, "worktree", "remove", "--force", worktreePath); err != nil {
		return err
	}
	return os.RemoveAll(worktreePath)
}

func (m *LocalManager) RemoveBranch(ctx context.Context, repoPath, branchName string) error {
	branchName = strings.TrimSpace(branchName)
	if branchName == "" {
		return fmt.Errorf("branch name is required")
	}
	out, err := gitOutput(ctx, repoPath, "branch", "--list", branchName)
	if err != nil {
		return fmt.Errorf("check branch %q: %w", branchName, err)
	}
	if strings.TrimSpace(string(out)) == "" {
		return nil
	}
	return runGit(ctx, repoPath, "branch", "-D", branchName)
}

func (m *LocalManager) RemoveManagedDir(path string) error {
	if err := m.ensureManagedPath(path); err != nil {
		return err
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	return os.RemoveAll(path)
}

func (m *LocalManager) PrepareManagedDir(path string) error {
	if err := m.prepareManagedParent(path); err != nil {
		return err
	}
	if err := m.ensureManagedPath(path); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("managed directory must not be a symlink")
		}
		if !info.IsDir() {
			return fmt.Errorf("managed path is not a directory")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Mkdir(path, 0o700)
}

func (m *LocalManager) prepareManagedParent(path string) error {
	root, err := m.resolvedRoot()
	if err != nil {
		return err
	}
	candidate, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if candidate == root || !pathIsWithin(root, candidate) {
		return fmt.Errorf("managed path is outside root")
	}
	parent := filepath.Dir(candidate)
	if parent == root {
		return nil
	}
	rel, err := filepath.Rel(root, parent)
	if err != nil {
		return err
	}
	current := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid managed path component")
		}
		current = filepath.Join(current, part)
		if info, err := os.Lstat(current); err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("managed parent must not contain symlinks")
			}
			if !info.IsDir() {
				return fmt.Errorf("managed parent contains a non-directory")
			}
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := os.Mkdir(current, 0o700); err != nil && !os.IsExist(err) {
			return err
		}
	}
	return nil
}

func (m *LocalManager) ensureManagedPath(path string) error {
	root, err := m.resolvedRoot()
	if err != nil {
		return err
	}
	candidate, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if candidate == root {
		return fmt.Errorf("worktree path must be below managed root")
	}
	if !pathIsWithin(root, candidate) {
		return fmt.Errorf("worktree path is outside managed root")
	}
	if info, err := os.Lstat(candidate); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("worktree path must not be a symlink")
		}
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			return err
		}
		if !pathIsWithin(root, resolved) {
			return fmt.Errorf("resolved worktree path is outside managed root")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	parent, err := nearestExistingParent(filepath.Dir(candidate))
	if err != nil {
		return err
	}
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return err
	}
	if !pathIsWithin(root, resolvedParent) {
		return fmt.Errorf("resolved worktree parent is outside managed root")
	}
	return nil
}

func (m *LocalManager) resolvedRoot() (string, error) {
	if err := pathsafe.MkdirAllNoSymlink(m.root, 0o700); err != nil {
		return "", err
	}
	root, err := filepath.EvalSymlinks(m.root)
	if err != nil {
		return "", err
	}
	return filepath.Abs(root)
}

func nearestExistingParent(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	for {
		if info, err := os.Lstat(path); err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				return "", fmt.Errorf("worktree parent must not be a symlink")
			}
			if !info.IsDir() {
				return "", fmt.Errorf("worktree parent is not a directory")
			}
			return path, nil
		} else if !os.IsNotExist(err) {
			return "", err
		}
		next := filepath.Dir(path)
		if next == path {
			return "", fmt.Errorf("no existing parent for worktree path")
		}
		path = next
	}
}

func pathIsWithin(root, candidate string) bool {
	return pathsafe.Within(root, candidate)
}

func sanitizePathPart(value string) string {
	value = strings.TrimSpace(value)
	var b strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), ".-")
	if out == "" || out == "." || out == ".." {
		return ""
	}
	return out
}

func sanitizeBranchPart(value string) string {
	value = strings.TrimSpace(value)
	var b strings.Builder
	lastSlash := false
	for _, r := range value {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
			lastSlash = false
		case r == '/':
			if !lastSlash {
				b.WriteRune('/')
				lastSlash = true
			}
		default:
			b.WriteRune('-')
			lastSlash = false
		}
	}
	out := strings.Trim(b.String(), "/.-")
	out = strings.ReplaceAll(out, "..", "-")
	out = strings.TrimSuffix(out, ".lock")
	if out == "" || strings.HasPrefix(out, "-") {
		return ""
	}
	return out
}

func verifyExistingWorktree(ctx context.Context, repoPath, destination, branchName string) error {
	if _, err := os.Stat(filepath.Join(destination, ".git")); err != nil {
		return fmt.Errorf("existing worktree path is not a git worktree")
	}
	topLevel, err := gitOutput(ctx, destination, "rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("verify existing worktree: %w", err)
	}
	resolvedTop, err := filepath.EvalSymlinks(strings.TrimSpace(string(topLevel)))
	if err != nil {
		return err
	}
	resolvedDestination, err := filepath.EvalSymlinks(destination)
	if err != nil {
		return err
	}
	if resolvedTop != resolvedDestination {
		return fmt.Errorf("existing worktree top-level does not match destination")
	}
	commonDir, err := gitOutput(ctx, destination, "rev-parse", "--git-common-dir")
	if err != nil {
		return fmt.Errorf("verify existing worktree origin: %w", err)
	}
	expectedCommonDir, err := gitOutput(ctx, repoPath, "rev-parse", "--git-common-dir")
	if err != nil {
		return fmt.Errorf("verify expected repository origin: %w", err)
	}
	resolvedExpectedCommon, err := resolveGitPath(repoPath, strings.TrimSpace(string(expectedCommonDir)))
	if err != nil {
		return err
	}
	resolvedCommon, err := resolveGitPath(destination, strings.TrimSpace(string(commonDir)))
	if err != nil {
		return err
	}
	if !pathsafe.Within(resolvedExpectedCommon, resolvedCommon) {
		return fmt.Errorf("existing worktree is not attached to the expected repository")
	}
	currentBranch, err := gitOutput(ctx, destination, "symbolic-ref", "--short", "HEAD")
	if err != nil {
		return fmt.Errorf("verify existing worktree branch: %w", err)
	}
	if strings.TrimSpace(string(currentBranch)) != branchName {
		return fmt.Errorf("existing worktree branch does not match requested branch")
	}
	status, err := gitOutput(ctx, destination, "status", "--porcelain=v1")
	if err != nil {
		return fmt.Errorf("verify existing worktree status: %w", err)
	}
	if strings.TrimSpace(string(status)) != "" {
		return fmt.Errorf("existing worktree is not clean")
	}
	return nil
}

func resolveGitPath(baseDir, path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("git path is empty")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(baseDir, path)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(resolved)
}

func runGit(ctx context.Context, dir string, args ...string) error {
	out, err := gitOutput(ctx, dir, args...)
	if err != nil {
		if len(out) > 0 {
			return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
		}
		return fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func gitOutput(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	var combined bytes.Buffer
	cmd.Stdout = &combined
	cmd.Stderr = &combined
	err := cmd.Run()
	return combined.Bytes(), err
}
