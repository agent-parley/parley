package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	runnerbootstrap "github.com/agent-parley/parley/internal/runner/bootstrap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runnerbootstrap.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
