//go:build integration

package provider

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/event"
)

func TestM5LivePodmanCancelStopsContainer(t *testing.T) {
	if os.Getenv("PARLEY_M5_LIVE") != "1" {
		t.Skip("set PARLEY_M5_LIVE=1 to run guarded host cancel test")
	}
	podmanPath, err := exec.LookPath("podman")
	if err != nil {
		t.Skip("podman not available")
	}
	image := getenvProviderIntegration("PARLEY_M5_CANCEL_IMAGE", "docker.io/library/alpine:3.20")
	if err := exec.Command(podmanPath, "image", "exists", image).Run(); err != nil {
		t.Skipf("cancel image %q not available", image)
	}
	root := t.TempDir()
	worktree := mkdirProviderDir(t, root, "worktree")
	artifact := mkdirProviderDir(t, root, "artifact")
	containerName := "parley-m5-cancel-" + strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
	defer exec.Command(podmanPath, "rm", "-f", containerName).Run()

	podman := &Podman{Executable: podmanPath, Policy: PreflightPolicy{WorktreeRoots: []string{worktree}, ArtifactRoots: []string{artifact}}}
	inv := PreparedInvocation{
		Adapter:        "container_sample",
		ContainerImage: image,
		Mounts: []Mount{
			{Host: worktree, Container: "/project/repo", Mode: "rw", Relabel: "Z"},
			{Host: artifact, Container: "/project/workspace", Mode: "rw", Relabel: "Z"},
		},
		Command:       []string{"sh", "-c", "echo started; sleep 60"},
		Network:       NetworkNone,
		ContainerName: containerName,
		WorkDir:       "/project/repo",
	}
	ctx, cancel := context.WithCancel(context.Background())
	sink := &liveCancelSink{cancel: cancel}
	_, err = podman.Run(ctx, inv, sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	out, _ := exec.Command(podmanPath, "ps", "--format", "{{.Names}}").Output()
	if strings.Contains(string(out), containerName) {
		t.Fatalf("container %s still running after cancel", containerName)
	}
	if !sink.sawStarted {
		t.Fatal("cancel sink never observed container output")
	}
	if _, err := os.Stat(filepath.Join(worktree, ".")); err != nil {
		t.Fatalf("worktree unavailable after cancel: %v", err)
	}
}

type liveCancelSink struct {
	cancel     context.CancelFunc
	sawStarted bool
}

func (s *liveCancelSink) Emit(_ context.Context, ev event.Event) error {
	if strings.Contains(ev.Summary, "started") {
		s.sawStarted = true
		s.cancel()
	}
	return nil
}

func (s *liveCancelSink) Artifact(context.Context, runnerio.Artifact) error { return nil }

func getenvProviderIntegration(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
