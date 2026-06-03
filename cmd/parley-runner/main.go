package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/agent-parley/parley/internal/runner/adapter"
	"github.com/agent-parley/parley/internal/runner/provider"
	runnersession "github.com/agent-parley/parley/internal/runner/session"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	extraAdapters, err := configuredAdapters()
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner config: %v\n", err)
		os.Exit(1)
	}
	srv, url, err := runnersession.Listen(runnersession.WithAdapters(extraAdapters...))
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner listen: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("READY %s\n", url)
	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "runner serve: %v\n", err)
		os.Exit(1)
	}
}

func configuredAdapters() ([]adapter.AgentAdapter, error) {
	switch name := os.Getenv("PARLEY_ADAPTER"); name {
	case "", "noop":
		return nil, nil
	case "container_sample":
		return []adapter.AgentAdapter{containerSampleAdapter()}, nil
	default:
		return nil, fmt.Errorf("unsupported PARLEY_ADAPTER %q", name)
	}
}

func containerSampleAdapter() adapter.AgentAdapter {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}
	dataRoot := cleanPath(getenv("PARLEY_DATA_DIR", ".parley-data"))
	sourceRepo := cleanPath(getenv("PARLEY_SOURCE_REPO", cwd))
	artifactDir := cleanOptionalPath(os.Getenv("PARLEY_ARTIFACT_DIR"))
	referenceRoot := cleanOptionalPath(os.Getenv("PARLEY_REFERENCE_ROOT"))
	if referenceRoot == "" && pathExists("/project/reference") {
		referenceRoot = "/project/reference"
	}
	agentStateRoot := cleanOptionalPath(os.Getenv("PARLEY_AGENT_STATE_ROOT"))

	artifactRoots := []string{dataRoot}
	if artifactDir != "" {
		artifactRoots = append(artifactRoots, artifactDir)
	}
	referenceRoots := []string{}
	if referenceRoot != "" {
		referenceRoots = append(referenceRoots, referenceRoot)
	}

	podman := provider.NewPodman(provider.PreflightPolicy{
		RepoRoots:      []string{sourceRepo},
		WorktreeRoots:  []string{dataRoot},
		ArtifactRoots:  artifactRoots,
		ReferenceRoots: referenceRoots,
		AgentStateRoot: agentStateRoot,
	})
	if executable := getenv("PARLEY_PODMAN_EXECUTABLE", os.Getenv("PARLEY_PODMAN")); executable != "" {
		podman.Executable = executable
	}

	return adapter.NewContainerSample(adapter.ContainerSampleOptions{
		Provider:       podman,
		DataRoot:       dataRoot,
		ProjectID:      getenv("PARLEY_PROJECT_ID", "default"),
		SourceRepo:     sourceRepo,
		ArtifactDir:    artifactDir,
		ReferenceRoot:  referenceRoot,
		AgentStateRoot: agentStateRoot,
		Image:          os.Getenv("PARLEY_CONTAINER_IMAGE"),
	})
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func cleanPath(path string) string {
	if path == "" {
		return path
	}
	if abs, err := filepath.Abs(path); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(path)
}

func cleanOptionalPath(path string) string {
	if path == "" {
		return ""
	}
	return cleanPath(path)
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
