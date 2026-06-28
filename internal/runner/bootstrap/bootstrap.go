package bootstrap

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent-parley/parley/internal/runner/adapter"
	"github.com/agent-parley/parley/internal/runner/provider"
	runnersession "github.com/agent-parley/parley/internal/runner/session"
)

func Run(ctx context.Context) error {
	extraAdapters, err := configuredAdapters()
	if err != nil {
		return fmt.Errorf("runner config: %w", err)
	}
	srv, url, err := runnersession.Listen(runnersession.WithAdapters(extraAdapters...))
	if err != nil {
		return fmt.Errorf("runner listen: %w", err)
	}
	fmt.Printf("READY %s\n", url)
	if err := srv.Serve(ctx); err != nil {
		return fmt.Errorf("runner serve: %w", err)
	}
	return nil
}

func configuredAdapters() ([]adapter.AgentAdapter, error) {
	validation := validationAdapter()
	switch name := os.Getenv("PARLEY_ADAPTER"); name {
	case "", "noop":
		return []adapter.AgentAdapter{validation}, nil
	case "container_sample":
		return []adapter.AgentAdapter{containerSampleAdapter(), validation}, nil
	case "pi":
		return []adapter.AgentAdapter{piAdapter(), validation}, nil
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

func validationAdapter() adapter.AgentAdapter {
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
		RepoRoots:       []string{sourceRepo},
		WorktreeRoots:   []string{dataRoot},
		ArtifactRoots:   artifactRoots,
		ReferenceRoots:  referenceRoots,
		AgentStateRoot:  agentStateRoot,
		AllowedNetworks: parseNetworks(os.Getenv("PARLEY_VALIDATION_ALLOWED_NETWORKS")),
	})
	if executable := getenv("PARLEY_PODMAN_EXECUTABLE", os.Getenv("PARLEY_PODMAN")); executable != "" {
		podman.Executable = executable
	}

	return adapter.NewValidation(adapter.ValidationOptions{
		Provider:       podman,
		DataRoot:       dataRoot,
		ProjectID:      getenv("PARLEY_PROJECT_ID", "default"),
		SourceRepo:     sourceRepo,
		ArtifactDir:    artifactDir,
		ReferenceRoot:  referenceRoot,
		AgentStateRoot: agentStateRoot,
		Image:          getenv("PARLEY_VALIDATION_IMAGE", os.Getenv("PARLEY_CONTAINER_IMAGE")),
		Command:        getenv("PARLEY_VALIDATION_CMD", ""),
		Network:        validationNetworkFromEnv(),
	})
}

func piAdapter() adapter.AgentAdapter {
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
	agentStateRoot := cleanPath(getenv("PARLEY_AGENT_STATE_ROOT", filepath.Join(dataRoot, "agent-state")))
	authPath := cleanPath(getenv("PARLEY_PI_AUTH_JSON", filepath.Join(agentStateRoot, "auth.json")))
	workspaceRoot := cleanOptionalPath(os.Getenv("PARLEY_WORKSPACE_ROOT"))

	artifactRoots := []string{dataRoot}
	if artifactDir != "" {
		artifactRoots = append(artifactRoots, artifactDir)
	}
	if workspaceRoot != "" {
		artifactRoots = append(artifactRoots, workspaceRoot)
	}
	referenceRoots := []string{}
	if referenceRoot != "" {
		referenceRoots = append(referenceRoots, referenceRoot)
	}

	podman := provider.NewPodman(provider.PreflightPolicy{
		RepoRoots:       []string{sourceRepo},
		WorktreeRoots:   []string{dataRoot},
		ArtifactRoots:   artifactRoots,
		ReferenceRoots:  referenceRoots,
		AgentStateRoot:  agentStateRoot,
		AllowedNetworks: parseNetworks(os.Getenv("PARLEY_PI_ALLOWED_NETWORKS")),
	})
	if executable := getenv("PARLEY_PODMAN_EXECUTABLE", os.Getenv("PARLEY_PODMAN")); executable != "" {
		podman.Executable = executable
	}

	image := getenv("PARLEY_PI_IMAGE", os.Getenv("PARLEY_CONTAINER_IMAGE"))
	return adapter.NewPi(adapter.PiOptions{
		Provider:           podman,
		CredentialStrategy: adapter.AuthJSONCredentialStrategy{SourcePath: authPath},
		DataRoot:           dataRoot,
		ProjectID:          getenv("PARLEY_PROJECT_ID", "default"),
		SourceRepo:         sourceRepo,
		ArtifactDir:        artifactDir,
		WorkspaceRoot:      workspaceRoot,
		ReferenceRoot:      referenceRoot,
		AgentStateRoot:     agentStateRoot,
		Image:              image,
		PiProvider:         getenv("PARLEY_PI_PROVIDER", ""),
		Model:              getenv("PARLEY_PI_MODEL", ""),
		Thinking:           getenv("PARLEY_PI_THINKING", ""),
		Network:            piNetworkFromEnv(),
	})
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func validationNetworkFromEnv() provider.Network {
	if network := os.Getenv("PARLEY_VALIDATION_NETWORK"); network != "" {
		return provider.Network(network)
	}
	if os.Getenv("PARLEY_VALIDATION_ALLOW_FULL_EGRESS") == "1" {
		return provider.NetworkBridge
	}
	return provider.NetworkNone
}

func piNetworkFromEnv() provider.Network {
	if network := os.Getenv("PARLEY_PI_NETWORK"); network != "" {
		return provider.Network(network)
	}
	if os.Getenv("PARLEY_PI_ALLOW_FULL_EGRESS") == "1" {
		return provider.NetworkBridge
	}
	return provider.NetworkNone
}

func parseNetworks(raw string) []provider.Network {
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
