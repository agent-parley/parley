// Package provider defines the SandboxProvider seam (0046) and the Podman
// implementation. A provider runs a single prepared, validated invocation in an
// isolated sandbox and streams its output back through the runner session.
package provider

import (
	"context"
	"time"

	"github.com/agent-parley/parley/internal/runner/runnerio"
)

// Network is the container network policy. The skeleton defaults to none; host
// mode is rejected by preflight unless policy explicitly allows it.
type Network string

const (
	NetworkNone   Network = "none"
	NetworkBridge Network = "bridge"
	NetworkHost   Network = "host"
)

// Mount is a single host->container bind mount.
type Mount struct {
	Host       string // host path (validated against registered roots)
	Container  string // path inside the container
	Mode       string // "ro" | "rw"
	Relabel    string // "" | "Z" (private SELinux relabel) | "z" (shared)
	Credential bool   // credential mount: must resolve under the agent-state root
}

// PreparedInvocation mirrors the adapter invocation model in
// architecture/harness-kickstart-core/07-agent-adapters.md. Adapters propose it;
// the harness validates and finalizes it (Preflight) before execution.
type PreparedInvocation struct {
	Adapter        string
	Profile        string
	Role           string
	ContainerImage string
	Mounts         []Mount
	Env            map[string]string
	Command        []string
	WorkDir        string
	Network        Network
	User           string // "uid:gid"; empty lets the provider default to keep-id mapping
	UserNS         string // e.g. "keep-id"
	Privileged     bool
	ContainerName  string
}

// Result is the terminal outcome of a sandbox run.
type Result struct {
	ExitCode  int
	StartedAt time.Time
	EndedAt   time.Time
}

// SandboxProvider runs one prepared invocation in an isolated sandbox.
type SandboxProvider interface {
	Name() string
	Run(ctx context.Context, inv PreparedInvocation, sink runnerio.Sink) (Result, error)
}
