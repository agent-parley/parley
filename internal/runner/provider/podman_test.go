package provider

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/event"
)

func TestPodmanRunCallsPreflightBeforeExec(t *testing.T) {
	root := t.TempDir()
	worktree := mkdirProviderDir(t, root, "worktree")
	outside := mkdirProviderDir(t, root, "outside")
	podman := &Podman{
		Executable: filepath.Join(root, "missing-podman"),
		Policy:     PreflightPolicy{WorktreeRoots: []string{worktree}},
	}

	_, err := podman.Run(context.Background(), PreparedInvocation{
		Adapter:        "container_sample",
		ContainerImage: "fake-image",
		Mounts:         []Mount{{Host: outside, Container: "/project/repo", Mode: "rw"}},
		Command:        []string{"sh", "-c", "echo should-not-run"},
		Network:        NetworkNone,
	}, &cancelSink{cancel: func() {}})
	if !errors.Is(err, ErrPathOutsideRegisteredRoots) {
		t.Fatalf("Run() error = %v, want ErrPathOutsideRegisteredRoots", err)
	}
}

func TestPodmanRunArgsUseRootlessEphemeralContainer(t *testing.T) {
	args := podmanRunArgs(PreparedInvocation{
		Adapter:        "container_sample",
		ContainerImage: "docker.io/library/alpine:3.20",
		Mounts: []Mount{
			{Host: "/tmp/worktree", Container: "/project/repo", Mode: "rw", Relabel: "Z"},
			{Host: "/tmp/reference", Container: "/project/reference", Mode: "ro"},
		},
		Env:     map[string]string{"HARNESS_TASK_ID": "task1", "HARNESS_RUN_ID": "run1"},
		Command: []string{"sh", "-c", "echo hi"},
		WorkDir: "/project/repo",
		Network: NetworkNone,
		User:    "1000:1000",
		UserNS:  "keep-id",
	}, "parley-test")

	wantPrefix := []string{"run", "--rm", "--name", "parley-test", "--user", "1000:1000", "--userns", "keep-id", "--network", "none", "--workdir", "/project/repo"}
	if !reflect.DeepEqual(args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("args prefix = %#v, want %#v", args[:len(wantPrefix)], wantPrefix)
	}
	wantTail := []string{"--volume", "/tmp/worktree:/project/repo:rw,Z", "--volume", "/tmp/reference:/project/reference:ro", "docker.io/library/alpine:3.20", "sh", "-c", "echo hi"}
	if !reflect.DeepEqual(args[len(args)-len(wantTail):], wantTail) {
		t.Fatalf("args tail = %#v, want %#v", args[len(args)-len(wantTail):], wantTail)
	}
}

func TestPodmanRunStreamsStdoutAndStderr(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "fake-podman")
	writeFakePodmanOutput(t, script)

	podman := &Podman{Executable: script}
	sink := &recordingOutputSink{}
	result, err := podman.Run(context.Background(), PreparedInvocation{
		Adapter:        "container_sample",
		ContainerImage: "fake-image",
		Command:        []string{"sh", "-c", "echo hi"},
		Network:        NetworkNone,
	}, sink)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("Run() exit code = %d, want 0", result.ExitCode)
	}

	assertOutputEvent(t, sink.events, "stdout", "hello")
	assertOutputEvent(t, sink.events, "stdout", "partial-out")
	assertOutputEvent(t, sink.events, "stderr", "warn")
	assertOutputEvent(t, sink.events, "stderr", "partial-err")
}

func writeFakePodmanOutput(t *testing.T, path string) {
	t.Helper()
	script := `#!/bin/sh
set -eu
if [ "$1" = "run" ]; then
  printf 'hello\npartial-out'
  printf 'warn\npartial-err' >&2
  exit 0
fi
exit 0
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake podman: %v", err)
	}
}

func assertOutputEvent(t *testing.T, events []event.Event, stream, line string) {
	t.Helper()
	for _, ev := range events {
		if ev.Type == "adapter.output" && ev.Data["stream"] == stream && ev.Data["line"] == line {
			return
		}
	}
	t.Fatalf("missing %s output %q in %+v", stream, line, events)
}

func TestOutputWriterHandlesLineLongerThanScannerLimit(t *testing.T) {
	longLine := strings.Repeat("x", 70*1024)
	sink := &recordingOutputSink{}
	writer := newOutputWriter(context.Background(), sink, "container_sample", "stdout")

	if _, err := writer.Write([]byte(longLine)); err != nil {
		t.Fatalf("outputWriter.Write() error = %v, want nil", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("outputWriter.Flush() error = %v, want nil", err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("outputWriter emitted %d events, want 1", len(sink.events))
	}
	ev := sink.events[0]
	if ev.Summary != longLine {
		t.Fatalf("event summary length = %d, want %d", len(ev.Summary), len(longLine))
	}
	gotLine, ok := ev.Data["line"].(string)
	if !ok {
		t.Fatalf("event line type = %T, want string", ev.Data["line"])
	}
	if gotLine != longLine {
		t.Fatalf("event line length = %d, want %d", len(gotLine), len(longLine))
	}
	if got := ev.Data["stream"]; got != "stdout" {
		t.Fatalf("event stream = %v, want stdout", got)
	}
}

type recordingOutputSink struct {
	mu     sync.Mutex
	events []event.Event
}

func (s *recordingOutputSink) Emit(_ context.Context, ev event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return nil
}

func (s *recordingOutputSink) Artifact(context.Context, runnerio.Artifact) error { return nil }
