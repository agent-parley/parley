package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/agents"
	"github.com/agent-parley/parley/internal/models"
)

const (
	ModeAllInOne = models.ModeAllInOne
	ModeManager  = models.ModeManager
	ModeExecutor = models.ModeExecutor
)

const (
	ExecutionModeDryRun  = "dry-run"
	ExecutionModeLocalPi = "local-pi"
)

const (
	RuntimeProviderPodman = "podman"
	RuntimeProviderDocker = "docker"
)

const (
	DefaultAgentIdleRetentionMinutes = 0
	DefaultMaxIdleAgents             = 1
)

type Config struct {
	BindAddr                  string
	DataRoot                  string
	Mode                      string
	ExecutionMode             string
	RuntimeProvider           string
	AppContainer              bool
	TrustedLAN                bool
	AgentIdleRetentionMinutes int
	MaxIdleAgents             int
}

func DefaultDataRoot() string {
	if value := os.Getenv("PARLEY_DATA_ROOT"); value != "" {
		return value
	}

	switch runtime.GOOS {
	case "windows":
		if value := os.Getenv("LOCALAPPDATA"); value != "" {
			return filepath.Join(value, "Parley")
		}
		if value := os.Getenv("APPDATA"); value != "" {
			return filepath.Join(value, "Parley")
		}
	case "darwin":
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			return filepath.Join(home, "Library", "Application Support", "Parley")
		}
	default:
		if value := os.Getenv("XDG_DATA_HOME"); value != "" {
			return filepath.Join(value, "parley")
		}
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			return filepath.Join(home, ".local", "share", "parley")
		}
	}

	return filepath.Join(".", ".parley-data")
}

func (c Config) Validate() error {
	if c.BindAddr == "" {
		return errors.New("bind address is required")
	}
	if c.DataRoot == "" {
		return errors.New("data root is required")
	}
	if c.Mode == "" {
		c.Mode = models.ModeAllInOne
	}
	if c.Mode != models.ModeAllInOne && c.Mode != models.ModeManager && c.Mode != models.ModeExecutor {
		return fmt.Errorf("unsupported mode %q", c.Mode)
	}
	if c.Mode != models.ModeAllInOne {
		return fmt.Errorf("mode %q is planned but disabled until authenticated runner APIs exist; use %q", c.Mode, models.ModeAllInOne)
	}
	if c.ExecutionMode == "" {
		c.ExecutionMode = ExecutionModeDryRun
	}
	if c.ExecutionMode != ExecutionModeDryRun && c.ExecutionMode != ExecutionModeLocalPi {
		return fmt.Errorf("unsupported execution mode %q", c.ExecutionMode)
	}
	if c.ExecutionMode == ExecutionModeLocalPi && runtime.GOOS != "linux" {
		return fmt.Errorf("execution mode %q is experimental and currently supported only on Linux", c.ExecutionMode)
	}
	if c.RuntimeProvider == "" {
		c.RuntimeProvider = RuntimeProviderPodman
	}
	if c.RuntimeProvider != RuntimeProviderPodman && c.RuntimeProvider != RuntimeProviderDocker {
		return fmt.Errorf("unsupported runtime provider %q", c.RuntimeProvider)
	}
	if c.RuntimeProvider == RuntimeProviderDocker {
		return fmt.Errorf("runtime provider %q is planned but unsupported; use %q", c.RuntimeProvider, RuntimeProviderPodman)
	}
	if c.AppContainer && c.ExecutionMode == ExecutionModeLocalPi {
		return fmt.Errorf("app-container mode currently supports %q only; run experimental %q as a local process", ExecutionModeDryRun, ExecutionModeLocalPi)
	}
	if c.AgentIdleRetentionMinutes < 0 {
		return errors.New("agent idle retention minutes must be non-negative")
	}
	if c.MaxIdleAgents < 0 {
		return errors.New("max idle agents must be non-negative")
	}
	if c.TrustedLAN {
		return errors.New("trusted LAN exposure is disabled until authentication is implemented")
	}
	if host, _, err := net.SplitHostPort(c.BindAddr); err == nil {
		if !isLocalBindHost(host) && !(c.AppContainer && isWildcardBindHost(host)) {
			return fmt.Errorf("refusing non-localhost bind %q until authentication is implemented", c.BindAddr)
		}
	} else if !strings.HasPrefix(c.BindAddr, "127.0.0.1:") {
		return fmt.Errorf("refusing bind %q until authentication is implemented", c.BindAddr)
	}
	return nil
}

func isLocalBindHost(host string) bool {
	return host == "127.0.0.1" || host == "localhost" || host == "::1"
}

func isWildcardBindHost(host string) bool {
	return host == "0.0.0.0" || host == "::" || host == ""
}

func (c Config) AgentPolicy() agents.Policy {
	return agents.Policy{IdleRetention: time.Duration(c.AgentIdleRetentionMinutes) * time.Minute, MaxIdleAgents: c.MaxIdleAgents}
}

func (c Config) EnsureDirs() error {
	paths := []string{
		c.DataRoot,
		filepath.Join(c.DataRoot, "projects"),
		filepath.Join(c.DataRoot, "managers"),
		filepath.Join(c.DataRoot, "executors"),
		filepath.Join(c.DataRoot, "worktrees"),
	}
	for _, path := range paths {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
	}
	return nil
}
