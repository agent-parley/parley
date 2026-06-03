package manager

import (
	"context"
	"fmt"
	"os"
	"time"

	managerhttp "github.com/agent-parley/parley/internal/manager/http"
	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/runnerclient"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
)

type Config struct {
	Addr      string
	DataDir   string
	RunnerBin string
	Adapter   string
}

type App struct {
	cfg    Config
	store  *store.Store
	runner *runnerclient.Client
	http   *managerhttp.Server
}

func New(ctx context.Context, cfg Config) (*App, error) {
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:8080"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = ".parley-data"
	}
	if cfg.Adapter == "" {
		cfg.Adapter = "noop"
	}

	st, err := store.Open(ctx, cfg.DataDir)
	if err != nil {
		return nil, err
	}
	renderer, err := web.NewRenderer()
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	hub := managerhttp.NewHub()
	runner, err := runnerclient.StartChildWithEnv(ctx, cfg.RunnerBin, []string{
		"PARLEY_ADAPTER=" + cfg.Adapter,
		"PARLEY_DATA_DIR=" + cfg.DataDir,
	})
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	if err := st.UpsertRunner(ctx, runner.Ready().RunnerID, "connected", runner.Ready().Capabilities); err != nil {
		_ = runner.Close(context.Background())
		_ = st.Close()
		return nil, err
	}
	engine := orchestrator.NewEngineWithOptions(st, runner, renderer, hub, orchestrator.EngineOptions{
		ImplementationAdapter: cfg.Adapter,
		ValidationAdapter:     "validation",
		DataRoot:              cfg.DataDir,
		ProjectID:             getenv("PARLEY_PROJECT_ID", "default"),
	})
	runner.SetHandlers(engine.HandleRunnerEvent, engine.HandleRunnerArtifact, engine.HandleRunnerReport, engine.HandleRunnerResult, engine.HandleRunnerLog)
	httpServer := managerhttp.NewServer(cfg.Addr, st, engine, hub, renderer)
	return &App{cfg: cfg, store: st, runner: runner, http: httpServer}, nil
}

func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() { errCh <- a.http.Run(ctx) }()
	select {
	case err := <-errCh:
		_ = a.close(context.Background())
		if err != nil {
			return fmt.Errorf("manager http: %w", err)
		}
		return nil
	case <-ctx.Done():
		err := <-errCh
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = a.close(shutdownCtx)
		return err
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func (a *App) close(ctx context.Context) error {
	if a.runner != nil {
		_ = a.runner.Close(ctx)
	}
	if a.store != nil {
		return a.store.Close()
	}
	return nil
}
