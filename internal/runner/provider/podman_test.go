package provider

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
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

func TestStreamOutputHandlesLineLongerThanScannerLimit(t *testing.T) {
	longLine := strings.Repeat("x", 70*1024)
	sink := &recordingOutputSink{}
	errCh := make(chan error, 1)

	streamOutput(context.Background(), sink, "container_sample", "stdout", strings.NewReader(longLine), errCh)

	if err := <-errCh; err != nil {
		t.Fatalf("streamOutput() error = %v, want nil", err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("streamOutput() emitted %d events, want 1", len(sink.events))
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
	events []event.Event
}

func (s *recordingOutputSink) Emit(_ context.Context, ev event.Event) error {
	s.events = append(s.events, ev)
	return nil
}

func (s *recordingOutputSink) Artifact(context.Context, runnerio.Artifact) error { return nil }
