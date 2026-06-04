package worktree

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateAndCaptureDiff(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	ctx := context.Background()
	source := initSourceRepo(t, ctx)
	dataRoot := t.TempDir()

	wt, err := Create(ctx, CreateOptions{
		DataRoot:   dataRoot,
		ProjectID:  "p1",
		RunID:      "run1",
		TaskID:     "task1",
		SourceRepo: source,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	wantPath := filepath.Join(dataRoot, "projects", "p1", "worktrees", "run1", "task1")
	if wt.Path != wantPath {
		t.Fatalf("worktree path = %q, want %q", wt.Path, wantPath)
	}
	if _, err := os.Stat(filepath.Join(wt.Path, ".git")); err != nil {
		t.Fatalf("worktree .git file missing: %v", err)
	}

	if err := os.WriteFile(filepath.Join(wt.Path, "README.md"), []byte("hello\nchanged\n"), 0o600); err != nil {
		t.Fatalf("modify tracked file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wt.Path, "new.txt"), []byte("new file\n"), 0o600); err != nil {
		t.Fatalf("write untracked file: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "diff.patch")
	diff, err := CaptureDiff(ctx, wt.Path, outputPath)
	if err != nil {
		t.Fatalf("CaptureDiff() error = %v", err)
	}
	text := string(diff)
	for _, want := range []string{"diff --git a/README.md b/README.md", "+changed", "diff --git a/new.txt b/new.txt", "+new file"} {
		if !strings.Contains(text, want) {
			t.Fatalf("diff missing %q:\n%s", want, text)
		}
	}
	status := string(runGitOutput(t, ctx, wt.Path, "status", "--porcelain=v1"))
	if !strings.Contains(status, "?? new.txt") {
		t.Fatalf("CaptureDiff mutated the real index; status = %q", status)
	}
	written, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read written diff.patch: %v", err)
	}
	if string(written) != text {
		t.Fatalf("written diff.patch differs from returned diff")
	}
}

func TestCreateCanScopeByAttempt(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	ctx := context.Background()
	source := initSourceRepo(t, ctx)
	dataRoot := t.TempDir()

	wt, err := Create(ctx, CreateOptions{
		DataRoot:   dataRoot,
		ProjectID:  "p1",
		RunID:      "run1",
		TaskID:     "task1",
		AttemptID:  "attempt1",
		SourceRepo: source,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	wantPath := filepath.Join(dataRoot, "projects", "p1", "worktrees", "run1", "task1", "attempt1")
	if wt.Path != wantPath {
		t.Fatalf("worktree path = %q, want %q", wt.Path, wantPath)
	}
}

func TestRemoveCreatedUntrackedStripsBuildOutput(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	ctx := context.Background()
	source := initSourceRepo(t, ctx)
	wt, err := Create(ctx, CreateOptions{DataRoot: t.TempDir(), ProjectID: "p1", RunID: "run1", TaskID: "task1", SourceRepo: source})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Worker output exists before validation runs.
	if err := os.WriteFile(filepath.Join(wt.Path, "m4-live.txt"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write worker file: %v", err)
	}
	baseline, err := ListUntrackedFiles(ctx, wt.Path)
	if err != nil {
		t.Fatalf("ListUntrackedFiles() error = %v", err)
	}

	// Validation build drops a compiled binary into the worktree.
	if err := os.WriteFile(filepath.Join(wt.Path, "m4live"), make([]byte, 40_000), 0o755); err != nil {
		t.Fatalf("write build artifact: %v", err)
	}
	removed, err := RemoveCreatedUntracked(ctx, wt.Path, baseline)
	if err != nil {
		t.Fatalf("RemoveCreatedUntracked() error = %v", err)
	}
	if len(removed) != 1 || removed[0] != "m4live" {
		t.Fatalf("removed = %v, want [m4live]", removed)
	}
	if _, err := os.Stat(filepath.Join(wt.Path, "m4live")); !os.IsNotExist(err) {
		t.Fatalf("build artifact not removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(wt.Path, "m4-live.txt")); err != nil {
		t.Fatalf("worker file wrongly removed: %v", err)
	}

	diff, err := CaptureDiff(ctx, wt.Path, "")
	if err != nil {
		t.Fatalf("CaptureDiff() error = %v", err)
	}
	if text := string(diff); !strings.Contains(text, "m4-live.txt") || strings.Contains(text, "m4live\n") || strings.Contains(text, "+++ b/m4live") {
		t.Fatalf("diff should contain only the worker file:\n%s", text)
	}
}

func initSourceRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	runGitCmd(t, ctx, dir, "init")
	runGitCmd(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runGitCmd(t, ctx, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runGitCmd(t, ctx, dir, "add", "README.md")
	runGitCmd(t, ctx, dir, "commit", "-m", "initial")
	return dir
}

func runGitCmd(t *testing.T, ctx context.Context, dir string, args ...string) {
	t.Helper()
	_ = runGitOutput(t, ctx, dir, args...)
}

func runGitOutput(t *testing.T, ctx context.Context, dir string, args ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
	return out
}
