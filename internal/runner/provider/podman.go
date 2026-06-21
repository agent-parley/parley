package provider

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/event"
)

const defaultPodmanExecutable = "podman"

// Podman runs prepared invocations with rootless podman run. The caller owns
// policy construction; Run always calls Preflight before spawning podman.
type Podman struct {
	Executable string
	Policy     PreflightPolicy
}

func NewPodman(policy PreflightPolicy) *Podman {
	return &Podman{Executable: defaultPodmanExecutable, Policy: policy}
}

func (p *Podman) Name() string { return "podman" }

func (p *Podman) Run(ctx context.Context, inv PreparedInvocation, sink runnerio.Sink) (Result, error) {
	if err := Preflight(inv, p.Policy); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	executable := p.Executable
	if executable == "" {
		executable = defaultPodmanExecutable
	}
	containerName := inv.ContainerName
	if containerName == "" {
		containerName = generatedContainerName()
	}

	args := podmanRunArgs(inv, containerName)
	cmd := exec.Command(executable, args...)
	stdout := newOutputWriter(ctx, sink, inv.Adapter, "stdout")
	stderr := newOutputWriter(ctx, sink, inv.Adapter, "stderr")
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	result := Result{StartedAt: time.Now().UTC()}
	if err := cmd.Start(); err != nil {
		result.EndedAt = time.Now().UTC()
		return result, fmt.Errorf("start podman: %w", err)
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	var waitErr error
	select {
	case waitErr = <-waitCh:
	case <-ctx.Done():
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = exec.CommandContext(stopCtx, executable, "stop", "--time", "5", containerName).Run()
		cancel()
		select {
		case waitErr = <-waitCh:
		case <-time.After(3 * time.Second):
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			waitErr = <-waitCh
		}
	}
	result.EndedAt = time.Now().UTC()
	result.ExitCode = exitCode(waitErr)

	var joined []error
	if err := stdout.Flush(); err != nil {
		joined = append(joined, err)
	}
	if err := stderr.Flush(); err != nil {
		joined = append(joined, err)
	}
	if ctx.Err() != nil {
		joined = append(joined, fmt.Errorf("podman run canceled: %w", ctx.Err()))
	}
	if waitErr != nil {
		joined = append(joined, fmt.Errorf("podman run exited with code %d: %w", result.ExitCode, waitErr))
	}
	return result, errors.Join(joined...)
}

type outputWriter struct {
	ctx       context.Context
	sink      runnerio.Sink
	adapterID string
	stream    string

	mu  sync.Mutex
	buf []byte
	err error
}

func newOutputWriter(ctx context.Context, sink runnerio.Sink, adapterID, stream string) *outputWriter {
	return &outputWriter{ctx: ctx, sink: sink, adapterID: adapterID, stream: stream}
}

func (w *outputWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, b := range p {
		w.buf = append(w.buf, b)
		if b == '\n' {
			w.emitLocked()
		}
	}
	return len(p), nil
}

func (w *outputWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buf) > 0 {
		w.emitLocked()
	}
	return w.err
}

func (w *outputWriter) emitLocked() {
	line := trimScannerLineEnding(string(w.buf))
	w.buf = w.buf[:0]
	if w.err != nil {
		return
	}
	if err := w.sink.Emit(w.ctx, event.Event{
		SchemaVersion: event.SchemaVersion,
		Type:          "adapter.output",
		Actor:         event.Actor{Kind: event.ActorKindAdapter, ID: w.adapterID},
		Summary:       line,
		Data: map[string]any{
			"provider": "podman",
			"stream":   w.stream,
			"line":     line,
		},
	}); err != nil {
		w.err = fmt.Errorf("emit podman %s output: %w", w.stream, err)
	}
}

func podmanRunArgs(inv PreparedInvocation, containerName string) []string {
	args := []string{"run", "--rm", "--name", containerName}
	user := inv.User
	if user == "" {
		user = currentUserSpec()
	}
	args = append(args, "--user", user)

	userNS := inv.UserNS
	if userNS == "" {
		userNS = "keep-id"
	}
	args = append(args, "--userns", userNS)
	args = append(args, "--network", string(normalizeNetwork(inv.Network)))

	if inv.WorkDir != "" {
		args = append(args, "--workdir", inv.WorkDir)
	}

	keys := make([]string, 0, len(inv.Env))
	for key := range inv.Env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "--env", key+"="+inv.Env[key])
	}

	for _, mount := range inv.Mounts {
		volume := mount.Host + ":" + mount.Container + ":" + mount.Mode
		if mount.Relabel != "" {
			volume += "," + mount.Relabel
		}
		args = append(args, "--volume", volume)
	}

	args = append(args, inv.ContainerImage)
	args = append(args, inv.Command...)
	return args
}

func trimScannerLineEnding(line string) string {
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func generatedContainerName() string {
	return "parley-" + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
}
