package pathsafe_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/agent-parley/parley/internal/pathsafe"
)

func TestWithinAllowsRootAndChildrenButRejectsSiblingPrefix(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	child := filepath.Join(root, "child")
	sibling := root + "2"
	if !pathsafe.Within(root, root) || !pathsafe.Within(root, child) {
		t.Fatalf("root and child should be within root")
	}
	if pathsafe.Within(root, sibling) || pathsafe.Within(root, filepath.Dir(root)) {
		t.Fatalf("sibling/parent should not be within root")
	}
}

func TestResolvedExistingPathRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires extra Windows privileges")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("x"), 0o600); err != nil { t.Fatal(err) }
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil { t.Fatal(err) }
	if _, err := pathsafe.ResolvedExistingPath(link); err == nil {
		t.Fatalf("expected symlink rejection")
	}
}

func TestMkdirAllNoSymlinkRejectsSymlinkParent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires extra Windows privileges")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0o700); err != nil { t.Fatal(err) }
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil { t.Fatal(err) }
	if err := pathsafe.MkdirAllNoSymlink(filepath.Join(link, "child"), 0o700); err == nil {
		t.Fatalf("expected symlink parent rejection")
	}
}
