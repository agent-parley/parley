//go:build integration || pi_podman

package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	managerhttp "github.com/agent-parley/parley/internal/manager/http"
	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/runnerclient"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/runner/adapter"
	"github.com/agent-parley/parley/internal/runner/provider"
	runnersession "github.com/agent-parley/parley/internal/runner/session"
	"github.com/agent-parley/parley/internal/shared/ids"
)

func TestM4PiFullLoopLive(t *testing.T) {
	if os.Getenv("PARLEY_M4_LIVE") != "1" {
		t.Skip("set PARLEY_M4_LIVE=1 with podman, Pi auth/image, and validation image/cache to run")
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
	piImage := getenvPiIntegration("PARLEY_PI_IMAGE", "localhost/parley-pi-worker:0.78.0")
	if err := exec.Command(podmanPath, "image", "exists", piImage).Run(); err != nil {
		t.Skipf("Pi worker image %q not built", piImage)
	}
	validationImage := os.Getenv("PARLEY_VALIDATION_IMAGE")
	if validationImage == "" {
		t.Skip("PARLEY_VALIDATION_IMAGE must be set for live M4 validation")
	}
	if err := exec.Command(podmanPath, "image", "exists", validationImage).Run(); err != nil {
		t.Skipf("validation image %q not available", validationImage)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	source := initM4LiveSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	projectID := "p1"
	reference := mkdirPiIntegrationDir(t, t.TempDir(), "reference")
	agentState := mkdirPiIntegrationDir(t, t.TempDir(), "agent-state")
	piContainerName := "parley-m4-pi-" + strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
	validationContainerName := "parley-m4-validation-" + strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
	defer exec.Command(podmanPath, "rm", "-f", piContainerName).Run()
	defer exec.Command(podmanPath, "rm", "-f", validationContainerName).Run()

	piPodman := &provider.Podman{Executable: podmanPath, Policy: provider.PreflightPolicy{RepoRoots: []string{source}, WorktreeRoots: []string{dataRoot}, ArtifactRoots: []string{dataRoot}, ReferenceRoots: []string{reference}, AgentStateRoot: agentState}}
	validationPodman := &provider.Podman{Executable: podmanPath, Policy: provider.PreflightPolicy{RepoRoots: []string{source}, WorktreeRoots: []string{dataRoot}, ArtifactRoots: []string{dataRoot}, ReferenceRoots: []string{reference}, AgentStateRoot: agentState, AllowedNetworks: parseM4LiveNetworks(os.Getenv("PARLEY_VALIDATION_ALLOWED_NETWORKS"))}}
	pi := adapter.NewPi(adapter.PiOptions{
		Provider:           piPodman,
		CredentialStrategy: adapter.AuthJSONCredentialStrategy{SourcePath: authPath},
		DataRoot:           dataRoot,
		ProjectID:          projectID,
		SourceRepo:         source,
		ReferenceRoot:      reference,
		AgentStateRoot:     agentState,
		Image:              piImage,
		PiProvider:         getenvPiIntegration("PARLEY_PI_PROVIDER", "openai-codex"),
		Model:              getenvPiIntegration("PARLEY_PI_MODEL", "gpt-5.5"),
		Thinking:           getenvPiIntegration("PARLEY_PI_THINKING", "high"),
		Network:            provider.NetworkBridge,
		AppendSystemExtra:  "For this live M4 verification, create exactly one file m4-live.txt containing `hello from m4 live`.",
		ContainerName:      piContainerName,
	})
	validation := adapter.NewValidation(adapter.ValidationOptions{
		Provider:       validationPodman,
		DataRoot:       dataRoot,
		ProjectID:      projectID,
		SourceRepo:     source,
		ReferenceRoot:  reference,
		AgentStateRoot: agentState,
		Image:          validationImage,
		Command:        getenvPiIntegration("PARLEY_VALIDATION_CMD", "go build ./... && go test ./..."),
		Network:        provider.Network(os.Getenv("PARLEY_VALIDATION_NETWORK")),
		ContainerName:  validationContainerName,
	})

	serverCtx, stop := context.WithCancel(context.Background())
	defer stop()
	srv, url, err := runnersession.Listen(runnersession.WithAdapters(pi, validation))
	if err != nil {
		t.Fatalf("listen runner: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(serverCtx) }()

	client, err := runnerclient.Dial(ctx, url, ids.New("runner"))
	if err != nil {
		t.Fatalf("dial runner: %v", err)
	}
	st, err := store.Open(ctx, filepath.Join(dataRoot, "manager"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	renderer, err := web.NewRenderer()
	if err != nil {
		t.Fatalf("renderer: %v", err)
	}
	hub := managerhttp.NewHub()
	engine := orchestrator.NewEngineWithOptions(st, client, renderer, hub, orchestrator.EngineOptions{ImplementationAdapter: pi.Name(), ValidationAdapter: validation.Name(), DataRoot: dataRoot, ProjectID: projectID})
	client.SetHandlers(engine.HandleRunnerEvent, engine.HandleRunnerArtifact, engine.HandleRunnerReport, engine.HandleRunnerResult, engine.HandleRunnerLog)

	runID, err := engine.StartRun(ctx, "Create m4-live.txt containing hello from m4 live. Keep the Go module buildable.")
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	var bundle store.RunBundle
	for {
		bundle, err = st.RunBundle(ctx, runID)
		if err != nil {
			t.Fatalf("bundle: %v", err)
		}
		if bundle.Run.Status == store.RunStatusCompleted || bundle.Run.Status == store.RunStatusFailed || bundle.Run.Status == store.RunStatusInvalid {
			break
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("live M4 run timed out; last status=%s", bundle.Run.Status)
		}
	}
	if bundle.Run.Status != store.RunStatusCompleted {
		for _, s := range bundle.Stages {
			t.Logf("STAGE %-16s status=%s adapter=%s", s.StageType, s.Status, s.Adapter)
		}
		for _, ev := range bundle.Events {
			sum := ev.Summary
			if len(sum) > 240 {
				sum = sum[:240] + "…"
			}
			t.Logf("EV [%s] %s", ev.Type, sum)
			if id, ok := ev.Data["report_artifact_id"].(string); ok && id != "" {
				if _, content, gerr := st.GetArtifact(ctx, id); gerr == nil {
					t.Logf("  -> report %s: %s", id, string(content))
				}
			}
			if id, ok := ev.Data["diff_artifact_id"].(string); ok && id != "" {
				if _, content, gerr := st.GetArtifact(ctx, id); gerr == nil {
					t.Logf("  -> diff %s bytes=%d", id, len(content))
				}
			}
		}
		t.Fatalf("live M4 run status=%s events=%d artifacts=%d", bundle.Run.Status, len(bundle.Events), len(bundle.Artifacts))
	}
	_ = client.Close(context.Background())
	stop()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func initM4LiveSourceRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	runFullLoopGit(t, ctx, dir, "init")
	runFullLoopGit(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runFullLoopGit(t, ctx, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/m4live\n\ngo 1.22\n"), 0o600); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n\nfunc main() {}\n"), 0o600); err != nil {
		t.Fatalf("write main.go: %v", err)
	}
	runFullLoopGit(t, ctx, dir, "add", ".")
	runFullLoopGit(t, ctx, dir, "commit", "-m", "initial")
	return dir
}

func parseM4LiveNetworks(raw string) []provider.Network {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	networks := make([]provider.Network, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			networks = append(networks, provider.Network(part))
		}
	}
	return networks
}
