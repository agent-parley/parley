package manager

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/agent-parley/parley/internal/manager/runnerclient"
)

type Config struct {
	Addr      string
	DataDir   string
	RunnerBin string
}

type App struct {
	cfg    Config
	runner *runnerclient.Client
	server *http.Server
}

func New(ctx context.Context, cfg Config) (*App, error) {
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:8080"
	}
	runner, err := runnerclient.StartChild(ctx, cfg.RunnerBin)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Parley manager ready\n"))
	})
	return &App{cfg: cfg, runner: runner, server: &http.Server{Addr: cfg.Addr, Handler: mux}}, nil
}

func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		err := a.server.ListenAndServe()
		if err == http.ErrServerClosed {
			err = nil
		}
		errCh <- err
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = a.server.Shutdown(shutdownCtx)
		if a.runner != nil {
			_ = a.runner.Close(shutdownCtx)
		}
		return <-errCh
	case err := <-errCh:
		if a.runner != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = a.runner.Close(shutdownCtx)
			cancel()
		}
		if err != nil {
			return fmt.Errorf("manager server: %w", err)
		}
		return nil
	}
}
