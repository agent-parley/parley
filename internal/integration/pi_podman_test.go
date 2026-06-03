//go:build integration || pi_podman

package integration_test

import (
	"context"
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
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestPiPodmanWorkerSmoke(t *testing.T) {
	if os.Getenv("PARLEY_PI_LIVE") != "1" {
		t.Skip("set PARLEY_PI_LIVE=1 with podman, a built Pi image, and PARLEY_PI_AUTH_JSON to run")
	}
	podmanPath, err := exec.LookPath("podman")
	if err != nil {
		t.Skip("podman not available")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	authPath := os.Getenv("PARLEY_PI_AUTH_JSON")
	if authPath == "" {
		t.Skip("PARLEY_PI_AUTH_JSON not set")
	}
	if _, err := os.Stat(authPath); err != nil {
		t.Skipf("PARLEY_PI_AUTH_JSON not readable: %v", err)
	}
	image := os.Getenv("PARLEY_PI_IMAGE")
	if image == "" {
		image = "localhost/parley-pi-worker:0.78.0"
	}
	if err := exec.Command(podmanPath, "image", "exists", image).Run(); err != nil {
		t.Skipf("Pi worker image %q not built; build with: podman build -t localhost/parley-pi-worker:0.78.0 build/pi", image)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	source := initPiIntegrationSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	reference := mkdirPiIntegrationDir(t, t.TempDir(), "reference")
	agentState := mkdirPiIntegrationDir(t, t.TempDir(), "agent-state")
	containerName := "parley-pi-smoke-" + strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
	defer exec.Command(podmanPath, "rm", "-f", containerName).Run()

	podman := &provider.Podman{
		Executable: podmanPath,
		Policy: provider.PreflightPolicy{
			RepoRoots:      []string{source},
			WorktreeRoots:  []string{dataRoot},
			ArtifactRoots:  []string{dataRoot},
			ReferenceRoots: []string{reference},
			AgentStateRoot: agentState,
		},
	}
	pi := adapter.NewPi(adapter.PiOptions{
		Provider:           podman,
		CredentialStrategy: adapter.AuthJSONCredentialStrategy{SourcePath: authPath},
		DataRoot:           dataRoot,
		ProjectID:          "p1",
		SourceRepo:         source,
		ReferenceRoot:      reference,
		AgentStateRoot:     agentState,
		Image:              image,
		PiProvider:         getenvPiIntegration("PARLEY_PI_PROVIDER", "openai-codex"),
		Model:              getenvPiIntegration("PARLEY_PI_MODEL", "gpt-5.5"),
		Thinking:           getenvPiIntegration("PARLEY_PI_THINKING", "high"),
		ContainerName:      containerName,
	})
	sink := &piIntegrationSink{}
	disp := contract.Dispatch{
		RunID:     "run1",
		TaskID:    "task1",
		AttemptID: "attempt1",
		StageID:   "stage1",
		StageType: contract.StageTypeImplementation,
		Adapter:   pi.Name(),
		Input: map[string]any{
			"contract_markdown": "Create a file named m3-pi-live.txt in /project/repo containing the line `hello from live pi`. Then write the required report.json.",
		},
	}

	rep, err := pi.Run(ctx, disp, sink)
	if err != nil {
		t.Fatalf("Pi run failed: %v", err)
	}
	if rep.Status != report.StatusCompleted {
		t.Fatalf("report status = %s, errors=%v", rep.Status, rep.Errors)
	}
	artifacts := map[string]string{}
	for _, artifact := range sink.artifacts {
		artifacts[artifact.Name] = string(artifact.Content)
	}
	if !strings.Contains(artifacts["diff.patch"], "m3-pi-live.txt") {
		t.Fatalf("diff.patch missing live file change:\n%s", artifacts["diff.patch"])
	}
}

type piIntegrationSink struct {
	mu        sync.Mutex
	events    []event.Event
	artifacts []runnerio.Artifact
}

func (s *piIntegrationSink) Emit(_ context.Context, ev event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return nil
}

func (s *piIntegrationSink) Artifact(_ context.Context, art runnerio.Artifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.artifacts = append(s.artifacts, art)
	return nil
}

func initPiIntegrationSourceRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	runPiIntegrationGit(t, ctx, dir, "init")
	runPiIntegrationGit(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runPiIntegrationGit(t, ctx, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runPiIntegrationGit(t, ctx, dir, "add", "README.md")
	runPiIntegrationGit(t, ctx, dir, "commit", "-m", "initial")
	return dir
}

func runPiIntegrationGit(t *testing.T, ctx context.Context, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}

func mkdirPiIntegrationDir(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(parts...)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	return path
}

func getenvPiIntegration(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
