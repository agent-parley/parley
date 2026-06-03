package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/agent-parley/parley/internal/manager"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := manager.Config{
		Addr:      getenv("PARLEY_ADDR", "127.0.0.1:8080"),
		DataDir:   getenv("PARLEY_DATA_DIR", ".parley-data"),
		RunnerBin: os.Getenv("PARLEY_RUNNER_BIN"),
	}
	app, err := manager.New(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parley: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Parley listening on http://%s\n", cfg.Addr)
	if err := app.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "parley: %v\n", err)
		os.Exit(1)
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
