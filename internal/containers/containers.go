// Package containers defines the local container runtime boundary.
package containers

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/agent-parley/parley/internal/pathsafe"
)

type Mount struct {
	HostPath      string
	ContainerPath string
	Mode          string
}

type Invocation struct {
	Image      string
	Command    []string
	Env        map[string]string
	Mounts     []Mount
	Network    string
	Privileged bool
	WorkDir    string
	OutputDir  string
	Profile    string
}

type Result struct {
	ExitCode   int
	StdoutPath string
	StderrPath string
}

type Runtime interface {
	Run(ctx context.Context, invocation Invocation) (Result, error)
}

type PodmanRuntime struct {
	binary string
}

func NewPodmanRuntime(binary string) PodmanRuntime {
	if strings.TrimSpace(binary) == "" {
		binary = "podman"
	}
	return PodmanRuntime{binary: binary}
}

func (r PodmanRuntime) Preflight(ctx context.Context, image string) error {
	if strings.TrimSpace(image) == "" {
		return fmt.Errorf("container image is required")
	}
	binary := r.binary
	if strings.TrimSpace(binary) == "" {
		binary = "podman"
	}
	if _, err := exec.LookPath(binary); err != nil {
		return fmt.Errorf("podman binary %q is unavailable: %w", binary, err)
	}
	cmd := exec.CommandContext(ctx, binary, "image", "exists", image)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("container image %q is not available locally", image)
	}
	return nil
}

func (r PodmanRuntime) Run(ctx context.Context, invocation Invocation) (Result, error) {
	if strings.TrimSpace(invocation.Image) == "" {
		return Result{}, fmt.Errorf("container image is required")
	}
	if len(invocation.Command) == 0 || strings.TrimSpace(invocation.Command[0]) == "" {
		return Result{}, fmt.Errorf("container command is required")
	}
	if invocation.Privileged {
		return Result{}, fmt.Errorf("privileged containers are not allowed")
	}
	network := strings.TrimSpace(invocation.Network)
	if network == "" {
		network = "none"
	}
	if network != "none" {
		return Result{}, fmt.Errorf("container network must be disabled")
	}
	if strings.TrimSpace(invocation.OutputDir) == "" {
		return Result{}, fmt.Errorf("container output directory is required")
	}
	if !filepath.IsAbs(invocation.OutputDir) {
		return Result{}, fmt.Errorf("container output directory must be absolute")
	}
	if err := pathsafe.MkdirAllNoSymlink(invocation.OutputDir, 0o700); err != nil {
		return Result{}, err
	}
	stdoutPath := filepath.Join(invocation.OutputDir, "stdout.txt")
	stderrPath := filepath.Join(invocation.OutputDir, "stderr.txt")
	stdout, err := pathsafe.CreateFileNoFollow(stdoutPath, 0o600)
	if err != nil {
		return Result{}, err
	}
	defer stdout.Close()
	stderr, err := pathsafe.CreateFileNoFollow(stderrPath, 0o600)
	if err != nil {
		return Result{}, err
	}
	defer stderr.Close()

	args, err := podmanArgs(invocation, network)
	if err != nil {
		return Result{}, err
	}
	cmd := exec.CommandContext(ctx, r.binary, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err = cmd.Run()
	result := Result{ExitCode: 0, StdoutPath: stdoutPath, StderrPath: stderrPath}
	if err == nil {
		return result, nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		result.ExitCode = exitErr.ExitCode()
		return result, nil
	}
	return Result{}, err
}

func podmanArgs(invocation Invocation, network string) ([]string, error) {
	args := []string{"run", "--rm", "--network", network}
	if invocation.WorkDir != "" {
		args = append(args, "--workdir", invocation.WorkDir)
	}
	mounts := append([]Mount(nil), invocation.Mounts...)
	sort.Slice(mounts, func(i, j int) bool {
		return mounts[i].ContainerPath < mounts[j].ContainerPath
	})
	for _, mount := range mounts {
		if strings.TrimSpace(mount.HostPath) == "" || strings.TrimSpace(mount.ContainerPath) == "" {
			return nil, fmt.Errorf("mount host and container paths are required")
		}
		mode := mount.Mode
		if mode == "" {
			mode = "ro"
		}
		if mode != "ro" && mode != "rw" {
			return nil, fmt.Errorf("unsupported mount mode %q", mount.Mode)
		}
		args = append(args, "--volume", fmt.Sprintf("%s:%s:%s", mount.HostPath, mount.ContainerPath, mode))
	}
	keys := make([]string, 0, len(invocation.Env))
	for key := range invocation.Env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "--env", key+"="+invocation.Env[key])
	}
	args = append(args, invocation.Image)
	args = append(args, invocation.Command...)
	return args, nil
}
