package managerhttp

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/secrets"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

type RunController interface {
	StartProjectRun(context.Context, string, string) (string, error)
	StartProjectRunInput(context.Context, string, contract.TaskInput) (string, error)
	SubmitConversationMessage(context.Context, string, string) (store.Message, error)
	StartQueuedRun(context.Context, string) error
	CancelRun(context.Context, string) error
	ReRunStage(context.Context, string, string, event.Actor) (store.Attempt, error)
	SubmitHumanReview(context.Context, string, string, orchestrator.HumanReviewSubmission) (report.Report, error)
	QueueState(context.Context) (orchestrator.QueueState, error)
}

type Server struct {
	addr     string
	store    *store.Store
	engine   RunController
	hub      *Hub
	renderer web.Renderer
	security *security
	secrets  *secrets.Service
	http     *http.Server
}

func NewServer(addr string, st *store.Store, engine RunController, hub *Hub, renderer web.Renderer, secretService *secrets.Service) *Server {
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	s := &Server{addr: addr, store: st, engine: engine, hub: hub, renderer: renderer, security: newSecurity(addr), secrets: secretService}
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
