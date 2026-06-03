package provider

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/event"
)

func TestPodmanRunStopsContainerOnContextCancel(t *testing.T) {
	root := t.TempDir()
	worktree := mkdirProviderDir(t, root, "worktree")
	artifact := mkdirProviderDir(t, root, "artifact")
	script := filepath.Join(root, "fake-podman")
	logPath := filepath.Join(root, "calls.log")
	pidPath := filepath.Join(root, "run.pid")
	writeFakePodman(t, script, logPath, pidPath)

	podman := &Podman{
		Executable: script,
		Policy: PreflightPolicy{
			WorktreeRoots: []string{worktree},
			ArtifactRoots: []string{artifact},
		},
	}
	inv := PreparedInvocation{
		Adapter:        "container_sample",
		ContainerImage: "fake-image",
		Mounts: []Mount{
			{Host: worktree, Container: "/project/repo", Mode: "rw"},
			{Host: artifact, Container: "/project/workspace", Mode: "rw"},
		},
		Command:       []string{"sh", "-c", "sleep forever"},
		Network:       NetworkNone,
		ContainerName: "parley-cancel-test",
	}
	ctx, cancel := context.WithCancel(context.Background())
	sink := &cancelSink{cancel: cancel}

	_, err := podman.Run(ctx, inv, sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	calls, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatalf("read fake podman log: %v", readErr)
	}
	if !strings.Contains(string(calls), "run --rm --name parley-cancel-test") {
		t.Fatalf("fake podman log missing run call:\n%s", calls)
	}
	if !strings.Contains(string(calls), "stop --time 5 parley-cancel-test") {
		t.Fatalf("fake podman log missing graceful stop call:\n%s", calls)
	}
}

type cancelSink struct {
	cancel context.CancelFunc
}

func (s *cancelSink) Emit(_ context.Context, _ event.Event) error {
	s.cancel()
	return nil
}

func (s *cancelSink) Artifact(context.Context, runnerio.Artifact) error { return nil }

func writeFakePodman(t *testing.T, path, logPath, pidPath string) {
	t.Helper()
	script := `#!/bin/sh
set -eu
echo "$@" >> "` + logPath + `"
if [ "$1" = "run" ]; then
  echo $$ > "` + pidPath + `"
  echo started
  trap 'exit 0' TERM INT
  while true; do sleep 1 & wait $!; done
fi
if [ "$1" = "stop" ]; then
  if [ -f "` + pidPath + `" ]; then
    kill -TERM "$(cat "` + pidPath + `")" 2>/dev/null || true
  fi
  exit 0
fi
exit 0
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake podman: %v", err)
	}
}

func mkdirProviderDir(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(parts...)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	return path
}
