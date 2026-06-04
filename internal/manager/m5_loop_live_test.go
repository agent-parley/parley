//go:build integration

package manager

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	runnerworktree "github.com/agent-parley/parley/internal/runner/worktree"
	"github.com/agent-parley/parley/internal/shared/contract"
)

func TestM5LivePiRunnerKillInFlight(t *testing.T) {
	if os.Getenv("PARLEY_M5_LOOP_LIVE") != "1" {
		t.Skip("set PARLEY_M5_LOOP_LIVE=1 with podman, Pi auth/image, and PARLEY_RUNNER_BIN to run")
	}
	podmanPath, err := exec.LookPath("podman")
	if err != nil {
		t.Skip("podman not available")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	runnerBin := os.Getenv("PARLEY_RUNNER_BIN")
	if runnerBin == "" {
		t.Skip("PARLEY_RUNNER_BIN not set")
	}
	if _, err := exec.LookPath(runnerBin); err != nil {
		if _, statErr := os.Stat(runnerBin); statErr != nil {
			t.Skipf("runner binary not available: %v", err)
		}
	}
	authPath := os.Getenv("PARLEY_PI_AUTH_JSON")
	if authPath == "" {
		t.Skip("PARLEY_PI_AUTH_JSON not set")
	}
	if _, err := os.Stat(authPath); err != nil {
		t.Skipf("PARLEY_PI_AUTH_JSON not readable: %v", err)
	}
	piImage := getenvM5Live("PARLEY_PI_IMAGE", "localhost/parley-pi-worker:0.78.0")
	if err := exec.Command(podmanPath, "image", "exists", piImage).Run(); err != nil {
		t.Skipf("Pi worker image %q not available", piImage)
	}
	if os.Getenv("PARLEY_PI_NETWORK") == "" && os.Getenv("PARLEY_PI_ALLOW_FULL_EGRESS") == "" {
		t.Setenv("PARLEY_PI_NETWORK", "bridge")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	source := initM5LiveSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	projectID := "m5-live-loop"
	t.Setenv("PARLEY_SOURCE_REPO", source)
	t.Setenv("PARLEY_PROJECT_ID", projectID)
	t.Setenv("PARLEY_AGENT_STATE_ROOT", filepath.Join(dataRoot, "agent-state"))
	t.Setenv("PARLEY_ARTIFACT_DIR", "")

	app, err := New(ctx, Config{Addr: "127.0.0.1:0", DataDir: dataRoot, RunnerBin: runnerBin, Adapter: "pi"})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	defer func() { _ = app.close(context.Background()) }()

	client := app.runner.current()
	if client == nil {
		t.Fatal("runner client unavailable")
	}
	runnerPID, ok := client.ChildPID()
	if !ok {
		t.Fatal("runner child pid unavailable")
	}
	containerPrefix := "parley-" + strconv.Itoa(runnerPID) + "-"
	defer removePodmanContainersWithPrefix(t, podmanPath, containerPrefix)

	runID, err := app.engine.StartRun(ctx, "Create m5-live-runner-kill.txt containing hello from the M5 live runner-kill test.")
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	inFlight := waitForM5LivePiContainer(t, ctx, app.store, runID, dataRoot, projectID, podmanPath, containerPrefix)
	defer removePodmanContainer(t, podmanPath, inFlight.containerName)

	proc, err := os.FindProcess(runnerPID)
	if err != nil {
		t.Fatalf("find runner process: %v", err)
	}
	if err := proc.Kill(); err != nil {
		t.Fatalf("kill runner process: %v", err)
	}

	terminal := waitForM5LiveTerminal(t, ctx, app.store, runID)
	if terminal.Run.Status != store.RunStatusFailed {
		dumpM5LiveBundle(t, terminal)
		t.Fatalf("run status = %s, want %s", terminal.Run.Status, store.RunStatusFailed)
	}
	if !bundleHasEvent(terminal, "run.failed", "reason", "runner_disconnected") {
		dumpM5LiveBundle(t, terminal)
		t.Fatalf("missing run.failed reason=runner_disconnected")
	}
	waitForM5LiveRunnerDownEvent(t, ctx, app.store, currentRunnerID(app))
	requireM5LiveStatePreserved(t, dataRoot, projectID, terminal)
}

type m5LiveInFlight struct {
	containerName string
}

func waitForM5LivePiContainer(t *testing.T, ctx context.Context, st *store.Store, runID, dataRoot, projectID, podmanPath, containerPrefix string) m5LiveInFlight {
	t.Helper()
	var last store.RunBundle
	for {
		bundle, err := st.RunBundle(ctx, runID)
		if err != nil {
			t.Fatalf("bundle: %v", err)
		}
		last = bundle
		if terminalRunStatus(bundle.Run.Status) {
			dumpM5LiveBundle(t, bundle)
			t.Fatalf("run reached terminal status %s before Pi container was observed", bundle.Run.Status)
		}
		if implementationStageRunning(bundle.Stages) {
			worktreePath := runnerworktree.Path(runnerworktree.CreateOptions{DataRoot: dataRoot, ProjectID: projectID, RunID: bundle.Run.ID, TaskID: bundle.Task.ID, AttemptID: bundle.Attempt.ID})
			if _, err := os.Stat(worktreePath); err == nil {
				containerName, err := findPodmanContainerWithPrefix(ctx, podmanPath, containerPrefix)
				if err != nil {
					t.Fatalf("podman ps: %v", err)
				}
				if containerName != "" {
					return m5LiveInFlight{containerName: containerName}
				}
			}
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			dumpM5LiveBundle(t, last)
			t.Fatalf("timed out waiting for in-flight Pi container: %v", ctx.Err())
		}
	}
}

func waitForM5LiveTerminal(t *testing.T, ctx context.Context, st *store.Store, runID string) store.RunBundle {
	t.Helper()
	var last store.RunBundle
	for {
		bundle, err := st.RunBundle(ctx, runID)
		if err != nil {
			t.Fatalf("bundle: %v", err)
		}
		last = bundle
		if terminalRunStatus(bundle.Run.Status) {
			return bundle
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			dumpM5LiveBundle(t, last)
			t.Fatalf("timed out waiting for terminal run: %v", ctx.Err())
		}
	}
}

func waitForM5LiveRunnerDownEvent(t *testing.T, ctx context.Context, st *store.Store, runnerID string) {
	t.Helper()
	for {
		events, err := st.ListRunnerEvents(ctx, runnerID)
		if err != nil {
			t.Fatalf("list runner events: %v", err)
		}
		for _, ev := range events {
			if ev.Type == "runner.down" {
				return
			}
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("timed out waiting for runner.down event: %v", ctx.Err())
		}
	}
}

func requireM5LiveStatePreserved(t *testing.T, dataRoot, projectID string, bundle store.RunBundle) {
	t.Helper()
	worktreePath, err := runnerworktree.Locate(dataRoot, projectID, bundle.Run.ID, bundle.Task.ID, bundle.Attempt.ID)
	if err != nil {
		t.Fatalf("locate preserved worktree: %v", err)
	}
	if info, err := os.Stat(worktreePath); err != nil || !info.IsDir() {
		t.Fatalf("worktree %s not preserved: info=%v err=%v", worktreePath, info, err)
	}
	runStateDir := filepath.Join(dataRoot, "agent-state", "runs", bundle.Run.ID, bundle.Task.ID, bundle.Attempt.ID)
	if info, err := os.Stat(runStateDir); err != nil || !info.IsDir() {
		t.Fatalf("agent state %s not preserved: info=%v err=%v", runStateDir, info, err)
	}
	workerInputPath := filepath.Join(dataRoot, "projects", projectID, "artifacts", bundle.Run.ID, bundle.Task.ID, bundle.Attempt.ID, "worker-input.md")
	if info, err := os.Stat(workerInputPath); err != nil || info.IsDir() {
		t.Fatalf("worker input artifact %s not preserved: info=%v err=%v", workerInputPath, info, err)
	}
	for _, artifact := range bundle.Artifacts {
		if artifact.Kind != "event_log" {
			continue
		}
		info, err := os.Stat(artifact.Path)
		if err != nil {
			t.Fatalf("event log artifact %s unreadable: %v", artifact.Path, err)
		}
		if info.Size() == 0 {
			t.Fatalf("event log artifact %s is empty", artifact.Path)
		}
		return
	}
	t.Fatal("missing event_log artifact")
}

func implementationStageRunning(stages []store.Stage) bool {
	for _, stage := range stages {
		if stage.StageType == contract.StageTypeImplementation && stage.Status == store.StageStatusRunning {
			return true
		}
	}
	return false
}

func terminalRunStatus(status string) bool {
	switch status {
	case store.RunStatusCompleted, store.RunStatusFailed, store.RunStatusInvalid, store.RunStatusNeedsInput, store.RunStatusCancelled:
		return true
	default:
		return false
	}
}

func bundleHasEvent(bundle store.RunBundle, typ, key string, value any) bool {
	for _, ev := range bundle.Events {
		if ev.Type == typ && ev.Data != nil && ev.Data[key] == value {
			return true
		}
	}
	return false
}

func currentRunnerID(app *App) string {
	app.mu.Lock()
	defer app.mu.Unlock()
	return app.runnerID
}

func findPodmanContainerWithPrefix(ctx context.Context, podmanPath, prefix string) (string, error) {
	out, err := exec.CommandContext(ctx, podmanPath, "ps", "--format", "{{.Names}}").Output()
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(out), "\n") {
		name := strings.TrimSpace(line)
		if strings.HasPrefix(name, prefix) {
			return name, nil
		}
	}
	return "", nil
}

func removePodmanContainer(t *testing.T, podmanPath, name string) {
	t.Helper()
	if name == "" {
		return
	}
	_ = exec.Command(podmanPath, "rm", "-f", name).Run()
}

func removePodmanContainersWithPrefix(t *testing.T, podmanPath, prefix string) {
	t.Helper()
	out, err := exec.Command(podmanPath, "ps", "-a", "--format", "{{.Names}}").Output()
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(out), "\n") {
		name := strings.TrimSpace(line)
		if strings.HasPrefix(name, prefix) {
			_ = exec.Command(podmanPath, "rm", "-f", name).Run()
		}
	}
}

func initM5LiveSourceRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	runM5LiveGit(t, ctx, dir, "init")
	runM5LiveGit(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runM5LiveGit(t, ctx, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/m5live\n\ngo 1.22\n"), 0o600); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n\nfunc main() {}\n"), 0o600); err != nil {
		t.Fatalf("write main.go: %v", err)
	}
	runM5LiveGit(t, ctx, dir, "add", ".")
	runM5LiveGit(t, ctx, dir, "commit", "-m", "initial")
	return dir
}

func runM5LiveGit(t *testing.T, ctx context.Context, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func dumpM5LiveBundle(t *testing.T, bundle store.RunBundle) {
	t.Helper()
	t.Logf("RUN status=%s events=%d artifacts=%d", bundle.Run.Status, len(bundle.Events), len(bundle.Artifacts))
	for _, stage := range bundle.Stages {
		t.Logf("STAGE %-16s status=%s adapter=%s", stage.StageType, stage.Status, stage.Adapter)
	}
	for _, ev := range bundle.Events {
		t.Logf("EVENT #%d %s summary=%q data=%v", ev.Sequence, ev.Type, ev.Summary, ev.Data)
	}
}

func getenvM5Live(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
