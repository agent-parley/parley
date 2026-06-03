package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	runnersession "github.com/agent-parley/parley/internal/runner/session"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv, url, err := runnersession.Listen()
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner listen: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("READY %s\n", url)
	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "runner serve: %v\n", err)
		os.Exit(1)
	}
}
