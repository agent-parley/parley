package worktrees_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/pathsafe"
	"github.com/agent-parley/parley/internal/worktrees"
)

func TestWorktreeAndRuntimePathsAreSanitizedUnderRoot(t *testing.T) {
	manager := worktrees.NewLocalManager(t.TempDir())
	worktreePath, err := manager.WorktreePath("../project", "run/id", "task\nname", 1)
	if err != nil { t.Fatal(err) }
	runtimePath, err := manager.RuntimePath("../project", "run/id", "task\nname", 1)
	if err != nil { t.Fatal(err) }
	for _, path := range []string{worktreePath, runtimePath} {
		if !pathsafe.Within(manager.Root(), path) {
			t.Fatalf("path escaped root: %s", path)
		}
		if strings.Contains(path, "..") || strings.Contains(path, "\n") {
			t.Fatalf("path was not sanitized: %q", path)
		}
	}
}

func TestPrepareManagedDirRejectsOutsideRoot(t *testing.T) {
	manager := worktrees.NewLocalManager(t.TempDir())
	if err := manager.PrepareManagedDir(filepath.Join(t.TempDir(), "outside")); err == nil {
		t.Fatalf("expected outside managed root rejection")
	}
}

func TestPrepareManagedDirRejectsSymlinkParent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires extra Windows privileges")
	}
	dataRoot := t.TempDir()
	manager := worktrees.NewLocalManager(dataRoot)
	if err := os.MkdirAll(manager.Root(), 0o700); err != nil { t.Fatal(err) }
	outside := t.TempDir()
	link := filepath.Join(manager.Root(), "link")
	if err := os.Symlink(outside, link); err != nil { t.Fatal(err) }
	if err := manager.PrepareManagedDir(filepath.Join(link, "child")); err == nil {
		t.Fatalf("expected symlink parent rejection")
	}
}

func TestBranchNameSanitizesUnsafeTaskID(t *testing.T) {
	branch := worktrees.BranchName("../task lock\nname", 3)
	if !strings.HasPrefix(branch, "parley/") || !strings.HasSuffix(branch, "/attempt-3") {
		t.Fatalf("unexpected branch shape: %q", branch)
	}
	if strings.Contains(branch, "..") || strings.Contains(branch, " ") || strings.Contains(branch, "\n") || strings.Contains(branch, ".lock") {
		t.Fatalf("branch not sanitized: %q", branch)
	}
}
