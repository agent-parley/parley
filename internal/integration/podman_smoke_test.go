//go:build integration || podman

package integration_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/runner/adapter"
	"github.com/agent-parley/parley/internal/runner/provider"
	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
)

func TestPodmanContainerSampleSmoke(t *testing.T) {
	podmanPath, err := exec.LookPath("podman")
	if err != nil {
		t.Skip("podman not available")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	source := initPodmanSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	reference := mkdirPodmanDir(t, t.TempDir(), "reference")
	agentState := mkdirPodmanDir(t, t.TempDir(), "agent-state")
	image := os.Getenv("PARLEY_PODMAN_TEST_IMAGE")
	if image == "" {
		image = "docker.io/library/alpine:3.20"
	}
	containerName := "parley-smoke-" + strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
	defer exec.Command(podmanPath, "rm", "-f", containerName).Run()

	podman := &provider.Podman{
		Executable: podmanPath,
		Policy: provider.PreflightPolicy{
			WorktreeRoots:  []string{dataRoot},
			ArtifactRoots:  []string{dataRoot},
			ReferenceRoots: []string{reference},
			AgentStateRoot: agentState,
		},
	}
	sample := adapter.NewContainerSample(adapter.ContainerSampleOptions{
		Provider:       podman,
		DataRoot:       dataRoot,
		ProjectID:      "p1",
		SourceRepo:     source,
		ReferenceRoot:  reference,
		AgentStateRoot: agentState,
		Image:          image,
		ContainerName:  containerName,
	})
	sink := &podmanRecordingSink{}

	outsideRoot := mkdirPodmanDir(t, t.TempDir(), "outside")
	_, err = podman.Run(ctx, provider.PreparedInvocation{
		Adapter:        sample.Name(),
		ContainerImage: image,
		Mounts:         []provider.Mount{{Host: outsideRoot, Container: "/project/repo", Mode: "rw"}},
		Command:        []string{"sh", "-c", "echo should-not-run"},
		Network:        provider.NetworkNone,
	}, sink)
	if !errors.Is(err, provider.ErrPathOutsideRegisteredRoots) {
		t.Fatalf("unsafe mount error = %v, want ErrPathOutsideRegisteredRoots", err)
	}
	disp := contract.Dispatch{RunID: "run1", TaskID: "task1", AttemptID: "attempt1", StageID: "stage1", StageType: contract.StageTypeImplementation, Adapter: sample.Name(), Input: map[string]any{}}

	rep, err := sample.Run(ctx, disp, sink)
	if err != nil {
		t.Fatalf("container sample run: %v", err)
	}
	if err := rep.Validate(); err != nil {
		t.Fatalf("report invalid: %v", err)
	}

	artifacts := map[string]string{}
	for _, artifact := range sink.artifacts {
		artifacts[artifact.Name] = string(artifact.Content)
	}
	if artifacts["out"] != "hi\n" {
		t.Fatalf("out artifact = %q, want hi", artifacts["out"])
	}
	if !strings.Contains(artifacts["diff.patch"], "container-sample.txt") {
		t.Fatalf("diff.patch missing container sample change:\n%s", artifacts["diff.patch"])
	}

	var sawOutput bool
	for _, ev := range sink.events {
		if ev.Type == "adapter.output" && ev.Summary == "hi" {
			sawOutput = true
		}
	}
	if !sawOutput {
		t.Fatalf("expected adapter.output event for container stdout, got %+v", sink.events)
	}

	if err := exec.CommandContext(ctx, podmanPath, "container", "exists", containerName).Run(); err == nil {
		t.Fatalf("container %s still exists; podman run should use --rm", containerName)
	}
}

type podmanRecordingSink struct {
	mu        sync.Mutex
	events    []event.Event
	artifacts []runnerio.Artifact
}

func (s *podmanRecordingSink) Emit(_ context.Context, ev event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return nil
}

func (s *podmanRecordingSink) Artifact(_ context.Context, art runnerio.Artifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.artifacts = append(s.artifacts, art)
	return nil
}

func initPodmanSourceRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	runPodmanGit(t, ctx, dir, "init")
	runPodmanGit(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runPodmanGit(t, ctx, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runPodmanGit(t, ctx, dir, "add", "README.md")
	runPodmanGit(t, ctx, dir, "commit", "-m", "initial")
	return dir
}

func runPodmanGit(t *testing.T, ctx context.Context, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}

func mkdirPodmanDir(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(parts...)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	return path
}
