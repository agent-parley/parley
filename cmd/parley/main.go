package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/agent-parley/parley/internal/adapters/pi"
	"github.com/agent-parley/parley/internal/app"
	"github.com/agent-parley/parley/internal/artifacts"
	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/containers"
	"github.com/agent-parley/parley/internal/dispatcher"
	"github.com/agent-parley/parley/internal/executor"
	"github.com/agent-parley/parley/internal/manager"
	plannerexec "github.com/agent-parley/parley/internal/planner"
	"github.com/agent-parley/parley/internal/store"
	"github.com/agent-parley/parley/internal/worktrees"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	idleRetentionDefault, err := envIntStrict("PARLEY_AGENT_IDLE_RETENTION_MINUTES", config.DefaultAgentIdleRetentionMinutes)
	if err != nil {
		logger.Error("invalid config", "error", err)
		os.Exit(1)
	}
	maxIdleDefault, err := envIntStrict("PARLEY_MAX_IDLE_AGENTS", config.DefaultMaxIdleAgents)
	if err != nil {
		logger.Error("invalid config", "error", err)
		os.Exit(1)
	}

	var cfg config.Config
	flag.StringVar(&cfg.BindAddr, "bind", envDefault("PARLEY_BIND", "127.0.0.1:7345"), "HTTP bind address")
	flag.StringVar(&cfg.DataRoot, "data-root", envDefault("PARLEY_DATA_ROOT", config.DefaultDataRoot()), "Parley data root")
	flag.StringVar(&cfg.Mode, "mode", envDefault("PARLEY_MODE", config.ModeAllInOne), "run mode: all-in-one only; manager/executor modes are planned but disabled")
	flag.StringVar(&cfg.ExecutionMode, "execution-mode", envDefault("PARLEY_EXECUTION_MODE", config.ExecutionModeDryRun), "attempt execution mode: dry-run or explicit experimental local-pi")
	flag.StringVar(&cfg.RuntimeProvider, "runtime-provider", envDefault("PARLEY_RUNTIME_PROVIDER", config.RuntimeProviderPodman), "local container runtime provider: podman now; docker is planned but unsupported")
	flag.BoolVar(&cfg.AppContainer, "app-container", envBool("PARLEY_APP_CONTAINER"), "serve from an app container; wildcard binds must be constrained by loopback host-port publishing")
	flag.IntVar(&cfg.AgentIdleRetentionMinutes, "agent-idle-retention-minutes", idleRetentionDefault, "local agent idle retention in minutes; 0 closes agents after each task/step")
	flag.IntVar(&cfg.MaxIdleAgents, "max-idle-agents", maxIdleDefault, "maximum local idle agent sessions to retain when idle retention is enabled")
	flag.BoolVar(&cfg.TrustedLAN, "trusted-lan", envBool("PARLEY_TRUSTED_LAN"), "disabled until authentication is implemented; non-localhost binds are refused")
	flag.Parse()

	if err := cfg.Validate(); err != nil {
		logger.Error("invalid config", "error", err)
		os.Exit(1)
	}
	if err := cfg.EnsureDirs(); err != nil {
		logger.Error("failed to create data root", "error", err)
		os.Exit(1)
	}

	st, err := store.Open(cfg.DataRoot)
	if err != nil {
		logger.Error("failed to open store", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := st.Close(); err != nil {
			logger.Error("failed to close store", "error", err)
		}
	}()

	_ = cfg.AgentPolicy() // Policy settings are parsed for checkpoint/session planning; live agent reuse remains disabled.
	logger.Info("agent session policy", "idle_retention_minutes", cfg.AgentIdleRetentionMinutes, "max_idle_agents", cfg.MaxIdleAgents, "live_reuse", false)
	attemptRunner := attemptRunnerForConfig(cfg)
	plannerRunner := plannerRunnerForConfig(cfg)
	artifactWriter := artifacts.NewWriter(st)
	workflow := manager.NewWorkflowService(st, attemptRunner, artifactWriter, logger)
	attemptDispatcher := dispatcher.New(st, workflow, attemptRunner, logger, 1)
	if err := attemptDispatcher.Recover(context.Background()); err != nil {
		logger.Error("failed to recover queued attempts", "error", err)
		os.Exit(1)
	}
	handler := app.New(app.Dependencies{Config: cfg, Store: st, Logger: logger, Dispatcher: attemptDispatcher, PlannerRunner: plannerRunner})
	server := &http.Server{
		Addr:              cfg.BindAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("parley listening", "addr", cfg.BindAddr, "data_root", cfg.DataRoot, "mode", cfg.Mode, "execution_mode", cfg.ExecutionMode, "runtime_provider", cfg.RuntimeProvider, "app_container", cfg.AppContainer)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown failed", "error", err)
		os.Exit(1)
	}
	fmt.Println("parley stopped")
}

func attemptRunnerForConfig(cfg config.Config) executor.Runner {
	if cfg.ExecutionMode == config.ExecutionModeLocalPi {
		return executor.NewLocalPiRunner(worktrees.NewLocalManager(cfg.DataRoot), containers.NewRuntime(cfg.RuntimeProvider), pi.Adapter{})
	}
	return executor.NewDryRunRunner()
}

func plannerRunnerForConfig(cfg config.Config) plannerexec.Runner {
	if cfg.ExecutionMode == config.ExecutionModeLocalPi {
		return plannerexec.NewLocalPiRunner(worktrees.NewLocalManager(cfg.DataRoot), containers.NewRuntime(cfg.RuntimeProvider), pi.Adapter{})
	}
	return plannerexec.NewDryRunRunner()
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envBool(name string) bool {
	value := os.Getenv(name)
	return value == "1" || value == "true" || value == "TRUE" || value == "yes"
}

func envIntStrict(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: must be an integer", name)
	}
	return parsed, nil
}
