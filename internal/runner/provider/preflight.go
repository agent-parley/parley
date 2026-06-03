package provider

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	ErrHostHomeMount                    = errors.New("host home mount rejected")
	ErrPathOutsideRegisteredRoots       = errors.New("mount path outside registered roots")
	ErrRawSecretEnv                     = errors.New("raw token or secret environment passthrough rejected")
	ErrPrivilegedContainer              = errors.New("privileged container rejected")
	ErrRuntimeSocketMount               = errors.New("container runtime socket mount rejected")
	ErrHostNetwork                      = errors.New("host network rejected")
	ErrInvalidNetworkPolicy             = errors.New("invalid network policy")
	ErrCredentialMountOutsideAgentState = errors.New("credential mount outside agent-state root")
	ErrInvalidMountMode                 = errors.New("invalid mount mode")
	ErrInvalidUserMapping               = errors.New("invalid container user mapping")
	ErrInvalidUserNamespace             = errors.New("invalid container user namespace")
	ErrSymlinkEscape                    = errors.New("mount symlink escapes allowed root")
)

// PreflightPolicy is the control-plane allowlist used to finalize a prepared
// invocation before any container process starts. Non-credential mounts must
// resolve under one of the registered roots. Credential mounts must resolve
// under AgentStateRoot instead.
type PreflightPolicy struct {
	RepoRoots        []string
	WorktreeRoots    []string
	ArtifactRoots    []string
	ReferenceRoots   []string
	AgentStateRoot   string
	AllowHostNetwork bool

	// HomeDir is optional and exists to make host-home rejection testable. When
	// empty, os.UserHomeDir is used.
	HomeDir string
}

// Preflight validates an adapter-prepared invocation against the finalized
// control-plane policy. It reports all detected reject reasons with errors.Join
// so callers and tests can use errors.Is against the exported sentinels.
func Preflight(inv PreparedInvocation, policy PreflightPolicy) error {
	var errs []error

	if inv.Privileged {
		errs = append(errs, fmt.Errorf("%w: adapter requested privileged container", ErrPrivilegedContainer))
	}
	if inv.User != "" && inv.User != currentUserSpec() {
		errs = append(errs, fmt.Errorf("%w: user=%q want %q", ErrInvalidUserMapping, inv.User, currentUserSpec()))
	}
	if inv.UserNS != "" && inv.UserNS != "keep-id" {
		errs = append(errs, fmt.Errorf("%w: userns=%q", ErrInvalidUserNamespace, inv.UserNS))
	}

	switch normalizeNetwork(inv.Network) {
	case NetworkNone, NetworkBridge:
	case NetworkHost:
		if !policy.AllowHostNetwork {
			errs = append(errs, fmt.Errorf("%w: network=%s", ErrHostNetwork, NetworkHost))
		}
	default:
		errs = append(errs, fmt.Errorf("%w: network=%q", ErrInvalidNetworkPolicy, inv.Network))
	}

	for name := range inv.Env {
		if secretEnvName(name) {
			errs = append(errs, fmt.Errorf("%w: env %s", ErrRawSecretEnv, name))
		}
	}

	registeredRoots, rootErrs := canonicalRoots(policy.allRegisteredRoots(), "registered root")
	errs = append(errs, rootErrs...)
	agentRoot, agentRootErr := canonicalRoot(policy.AgentStateRoot, "agent-state root")
	if agentRootErr != nil && hasCredentialMount(inv.Mounts) {
		errs = append(errs, fmt.Errorf("%w: %v", ErrCredentialMountOutsideAgentState, agentRootErr))
	}

	home := resolveHome(policy.HomeDir)

	for i, mount := range inv.Mounts {
		prefix := fmt.Sprintf("mount[%d] %s -> %s", i, mount.Host, mount.Container)
		if mount.Mode != "ro" && mount.Mode != "rw" {
			errs = append(errs, fmt.Errorf("%w: %s mode=%q", ErrInvalidMountMode, prefix, mount.Mode))
		}
		if mount.Relabel != "" && mount.Relabel != "Z" && mount.Relabel != "z" {
			errs = append(errs, fmt.Errorf("%w: %s relabel=%q", ErrInvalidMountMode, prefix, mount.Relabel))
		}
		if mount.Container == "" || !filepath.IsAbs(mount.Container) {
			errs = append(errs, fmt.Errorf("%w: %s container path must be absolute", ErrInvalidMountMode, prefix))
		}

		cleanHost, realHost, resolveErr := resolveMountPath(mount.Host)
		if strings.HasPrefix(strings.TrimSpace(mount.Host), "~") || pathWithin(home.clean, cleanHost) || pathWithin(home.real, realHost) {
			errs = append(errs, fmt.Errorf("%w: %s", ErrHostHomeMount, prefix))
		}
		if runtimeSocketPath(cleanHost) || runtimeSocketPath(realHost) || runtimeSocketPath(mount.Container) {
			errs = append(errs, fmt.Errorf("%w: %s", ErrRuntimeSocketMount, prefix))
		}
		if resolveErr != nil {
			errs = append(errs, fmt.Errorf("%w: %s resolve host path: %v", ErrPathOutsideRegisteredRoots, prefix, resolveErr))
			continue
		}

		if mount.Credential {
			if agentRootErr != nil || agentRoot.real == "" {
				errs = append(errs, fmt.Errorf("%w: %s has no usable agent-state root", ErrCredentialMountOutsideAgentState, prefix))
				continue
			}
			lexical := pathWithin(agentRoot.clean, cleanHost)
			real := pathWithin(agentRoot.real, realHost)
			if lexical && !real {
				errs = append(errs, fmt.Errorf("%w: %s", ErrSymlinkEscape, prefix))
			}
			if !real {
				errs = append(errs, fmt.Errorf("%w: %s", ErrCredentialMountOutsideAgentState, prefix))
			}
			continue
		}

		if len(registeredRoots) == 0 {
			errs = append(errs, fmt.Errorf("%w: %s no registered roots configured", ErrPathOutsideRegisteredRoots, prefix))
			continue
		}
		lexical := false
		real := false
		for _, root := range registeredRoots {
			lexical = lexical || pathWithin(root.clean, cleanHost)
			real = real || pathWithin(root.real, realHost)
		}
		if lexical && !real {
			errs = append(errs, fmt.Errorf("%w: %s", ErrSymlinkEscape, prefix))
		}
		if !lexical && !real {
			errs = append(errs, fmt.Errorf("%w: %s", ErrPathOutsideRegisteredRoots, prefix))
		}
		if lexical && !real {
			continue
		}
		if !real {
			errs = append(errs, fmt.Errorf("%w: %s", ErrPathOutsideRegisteredRoots, prefix))
		}
	}

	return errors.Join(errs...)
}

func (p PreflightPolicy) allRegisteredRoots() []string {
	roots := make([]string, 0, len(p.RepoRoots)+len(p.WorktreeRoots)+len(p.ArtifactRoots)+len(p.ReferenceRoots))
	roots = append(roots, p.RepoRoots...)
	roots = append(roots, p.WorktreeRoots...)
	roots = append(roots, p.ArtifactRoots...)
	roots = append(roots, p.ReferenceRoots...)
	return roots
}

type canonicalPath struct {
	clean string
	real  string
}

func canonicalRoots(paths []string, label string) ([]canonicalPath, []error) {
	roots := make([]canonicalPath, 0, len(paths))
	var errs []error
	for _, p := range paths {
		root, err := canonicalRoot(p, label)
		if err != nil {
			errs = append(errs, fmt.Errorf("%w: %s %q: %v", ErrPathOutsideRegisteredRoots, label, p, err))
			continue
		}
		roots = append(roots, root)
	}
	return roots, errs
}

func canonicalRoot(path, label string) (canonicalPath, error) {
	if strings.TrimSpace(path) == "" {
		return canonicalPath{}, fmt.Errorf("%s path is required", label)
	}
	clean, err := cleanAbs(path)
	if err != nil {
		return canonicalPath{}, err
	}
	real, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return canonicalPath{}, err
	}
	return canonicalPath{clean: filepath.Clean(clean), real: filepath.Clean(real)}, nil
}

func resolveMountPath(path string) (string, string, error) {
	clean, err := cleanAbs(path)
	if err != nil {
		return "", "", err
	}
	real, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return clean, "", err
	}
	return filepath.Clean(clean), filepath.Clean(real), nil
}

func cleanAbs(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("path is required")
	}
	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", err
		}
		path = abs
	}
	return filepath.Clean(path), nil
}

func resolveHome(override string) canonicalPath {
	home := override
	if home == "" {
		if found, err := os.UserHomeDir(); err == nil {
			home = found
		}
	}
	if home == "" {
		return canonicalPath{}
	}
	clean, err := cleanAbs(home)
	if err != nil {
		return canonicalPath{}
	}
	real, err := filepath.EvalSymlinks(clean)
	if err != nil {
		real = clean
	}
	return canonicalPath{clean: filepath.Clean(clean), real: filepath.Clean(real)}
}

func pathWithin(root, path string) bool {
	if root == "" || path == "" {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func currentUserSpec() string {
	return strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid())
}

func normalizeNetwork(network Network) Network {
	if network == "" {
		return NetworkNone
	}
	return network
}

func hasCredentialMount(mounts []Mount) bool {
	for _, mount := range mounts {
		if mount.Credential {
			return true
		}
	}
	return false
}

func secretEnvName(name string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(name, "-", "_"))
	switch normalized {
	case "gh_token", "github_token", "github_pat", "gitlab_token", "forge_token", "ssh_auth_sock":
		return true
	}
	needles := []string{
		"token",
		"secret",
		"password",
		"passwd",
		"api_key",
		"apikey",
		"access_key",
		"private_key",
		"ssh_key",
		"credential",
		"authorization",
	}
	for _, needle := range needles {
		if strings.Contains(normalized, needle) {
			return true
		}
	}
	return false
}

func runtimeSocketPath(path string) bool {
	if path == "" {
		return false
	}
	normalized := strings.ToLower(filepath.ToSlash(filepath.Clean(path)))
	base := filepath.Base(normalized)
	switch base {
	case "docker.sock", "podman.sock", "containerd.sock", "crio.sock", "cri-dockerd.sock":
		return true
	}
	return strings.Contains(normalized, "/docker.sock") ||
		strings.Contains(normalized, "/podman.sock") ||
		strings.Contains(normalized, "/containerd.sock") ||
		strings.Contains(normalized, "/crio.sock")
}
