package provider

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
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
