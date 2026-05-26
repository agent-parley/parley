package executor

import (
	"fmt"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/agent-parley/parley/internal/containers"
	"github.com/agent-parley/parley/internal/pathsafe"
)

type InvocationValidation struct {
	AllowedExecutables []string
	AllowedProfiles    []string
	AllowedHostRoots   []string
	AllowedEnvPrefixes []string
}

func ValidateInvocation(invocation containers.Invocation, rules InvocationValidation) error {
	if len(invocation.Command) == 0 || strings.TrimSpace(invocation.Command[0]) == "" {
		return fmt.Errorf("invocation command is required")
	}
	if !stringInList(filepath.Base(invocation.Command[0]), rules.AllowedExecutables) {
		return fmt.Errorf("executable %q is not allowed", invocation.Command[0])
	}
	if invocation.Privileged {
		return fmt.Errorf("privileged invocation is not allowed")
	}
	if strings.TrimSpace(invocation.Profile) == "" {
		return fmt.Errorf("agent profile is required")
	}
	if !stringInList(invocation.Profile, rules.AllowedProfiles) {
		return fmt.Errorf("agent profile %q is not allowed", invocation.Profile)
	}
	network := strings.TrimSpace(invocation.Network)
	if network != "" && network != "none" {
		return fmt.Errorf("network must be disabled")
	}
	for _, mount := range invocation.Mounts {
		if err := validateMount(mount, rules.AllowedHostRoots); err != nil {
			return err
		}
	}
	for key := range invocation.Env {
		if !validEnvKey(key) {
			return fmt.Errorf("invalid environment key %q", key)
		}
		if !hasAllowedPrefix(key, rules.AllowedEnvPrefixes) {
			return fmt.Errorf("environment key %q is not allowed", key)
		}
	}
	return nil
}

func validateMount(mount containers.Mount, allowedRoots []string) error {
	if strings.TrimSpace(mount.HostPath) == "" {
		return fmt.Errorf("mount host path is required")
	}
	if !filepath.IsAbs(mount.HostPath) {
		return fmt.Errorf("mount host path must be absolute")
	}
	if strings.TrimSpace(mount.ContainerPath) == "" || !strings.HasPrefix(mount.ContainerPath, "/") {
		return fmt.Errorf("mount container path must be absolute")
	}
	if mount.Mode != "" && mount.Mode != "ro" && mount.Mode != "rw" {
		return fmt.Errorf("mount mode %q is not allowed", mount.Mode)
	}
	for _, root := range allowedRoots {
		ok, err := pathWithin(root, mount.HostPath)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return fmt.Errorf("mount host path %q is outside allowed roots", mount.HostPath)
}

func pathWithin(root, candidate string) (bool, error) {
	resolvedRoot, err := pathsafe.ResolvedExistingPath(root)
	if err != nil {
		return false, err
	}
	resolvedCandidate, err := pathsafe.ResolvedExistingPath(candidate)
	if err != nil {
		return false, err
	}
	return pathsafe.Within(resolvedRoot, resolvedCandidate), nil
}

func validEnvKey(key string) bool {
	if key == "" {
		return false
	}
	for i, r := range key {
		if i == 0 && !(unicode.IsLetter(r) || r == '_') {
			return false
		}
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			return false
		}
	}
	return true
}

func hasAllowedPrefix(value string, prefixes []string) bool {
	if len(prefixes) == 0 {
		return false
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func stringInList(value string, allowed []string) bool {
	for _, item := range allowed {
		if value == item {
			return true
		}
	}
	return false
}
