package managerhttp

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
)

type RunController interface {
	StartRun(context.Context, string) (string, error)
	StartQueuedRun(context.Context, string) error
	CancelRun(context.Context, string) error
	QueueState(context.Context) (orchestrator.QueueState, error)
}

type Server struct {
	addr     string
	store    *store.Store
	engine   RunController
	hub      *Hub
	renderer web.Renderer
	security *security
	http     *http.Server
}

func NewServer(addr string, st *store.Store, engine RunController, hub *Hub, renderer web.Renderer) *Server {
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	s := &Server{addr: addr, store: st, engine: engine, hub: hub, renderer: renderer, security: newSecurity(addr)}
	s.http = &http.Server{Addr: addr, Handler: s.routes()}
	return s
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		err := s.http.ListenAndServe()
		if err == http.ErrServerClosed {
			err = nil
		}
		errCh <- err
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.http.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown manager http: %w", err)
		}
		return <-errCh
	case err := <-errCh:
		return err
	}
}
