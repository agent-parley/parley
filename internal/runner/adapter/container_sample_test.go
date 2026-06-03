package adapter

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/runner/provider"
	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
)

func TestContainerSampleTransfersOutputAndDiffArtifacts(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	ctx := context.Background()
	source := initAdapterSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	reference := mkdirAdapterDir(t, t.TempDir(), "reference")
	agentState := mkdirAdapterDir(t, t.TempDir(), "agent-state")
	fake := &fakeSandboxProvider{}
	adapter := NewContainerSample(ContainerSampleOptions{
		Provider:       fake,
		DataRoot:       dataRoot,
		ProjectID:      "p1",
		SourceRepo:     source,
		ReferenceRoot:  reference,
		AgentStateRoot: agentState,
	})
	sink := &recordingSink{}
	disp := contract.Dispatch{RunID: "run1", TaskID: "task1", AttemptID: "attempt1", StageID: "stage1", StageType: contract.StageTypeImplementation, Adapter: adapter.Name(), Input: map[string]any{}}

	rep, err := adapter.Run(ctx, disp, sink)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := rep.Validate(); err != nil {
		t.Fatalf("report invalid: %v", err)
	}
	if len(rep.EvidenceRefs) != 2 {
		t.Fatalf("EvidenceRefs len = %d, want 2", len(rep.EvidenceRefs))
	}
	if fake.inv.ContainerImage == "" || fake.inv.WorkDir != "/project/repo" {
		t.Fatalf("fake provider saw incomplete invocation: %+v", fake.inv)
	}
	wantWorktree := filepath.Join(dataRoot, "projects", "p1", "worktrees", "run1", "task1")
	if got := mountHost(fake.inv, "/project/repo"); got != wantWorktree {
		t.Fatalf("worktree mount host = %q, want %q", got, wantWorktree)
	}

	artifacts := map[string]string{}
	for _, artifact := range sink.artifacts {
		artifacts[artifact.Name] = string(artifact.Content)
	}
	if artifacts["out"] != "hi\n" {
		t.Fatalf("out artifact = %q, want hi", artifacts["out"])
	}
	if !strings.Contains(artifacts["diff.patch"], "diff --git a/container-sample.txt b/container-sample.txt") {
		t.Fatalf("diff artifact missing container-sample change:\n%s", artifacts["diff.patch"])
	}
}

func mountHost(inv provider.PreparedInvocation, containerPath string) string {
	for _, mount := range inv.Mounts {
		if mount.Container == containerPath {
			return mount.Host
		}
	}
	return ""
}

type fakeSandboxProvider struct {
	inv provider.PreparedInvocation
}

func (f *fakeSandboxProvider) Name() string { return "fake" }

func (f *fakeSandboxProvider) Run(ctx context.Context, inv provider.PreparedInvocation, sink runnerio.Sink) (provider.Result, error) {
	f.inv = inv
	var repoDir, workspaceDir string
	for _, mount := range inv.Mounts {
		switch mount.Container {
		case "/project/repo":
			repoDir = mount.Host
		case "/project/workspace":
			workspaceDir = mount.Host
		}
	}
	if repoDir == "" || workspaceDir == "" {
		return provider.Result{}, os.ErrNotExist
	}
	if err := os.WriteFile(filepath.Join(workspaceDir, "out"), []byte("hi\n"), 0o600); err != nil {
		return provider.Result{}, err
	}
	if err := os.WriteFile(filepath.Join(repoDir, "container-sample.txt"), []byte("hi\n"), 0o600); err != nil {
		return provider.Result{}, err
	}
	if err := sink.Emit(ctx, event.Event{SchemaVersion: event.SchemaVersion, Type: "adapter.output", Actor: event.Actor{Kind: event.ActorKindAdapter, ID: inv.Adapter}, Summary: "hi", Data: map[string]any{"line": "hi"}}); err != nil {
		return provider.Result{}, err
	}
	now := time.Now().UTC()
	return provider.Result{ExitCode: 0, StartedAt: now, EndedAt: now}, nil
}

type recordingSink struct {
	events    []event.Event
	artifacts []runnerio.Artifact
}

func (s *recordingSink) Emit(_ context.Context, ev event.Event) error {
	s.events = append(s.events, ev)
	return nil
}

func (s *recordingSink) Artifact(_ context.Context, art runnerio.Artifact) error {
	s.artifacts = append(s.artifacts, art)
	return nil
}

func initAdapterSourceRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	runAdapterGit(t, ctx, dir, "init")
	runAdapterGit(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runAdapterGit(t, ctx, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runAdapterGit(t, ctx, dir, "add", "README.md")
	runAdapterGit(t, ctx, dir, "commit", "-m", "initial")
	return dir
}

func runAdapterGit(t *testing.T, ctx context.Context, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}

func mkdirAdapterDir(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(parts...)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	return path
}
