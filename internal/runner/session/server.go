package session

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/agent-parley/parley/internal/runner"
	"github.com/agent-parley/parley/internal/runner/adapter"
	"github.com/agent-parley/parley/internal/shared/protocol"
)

type Server struct {
	server   *http.Server
	listener net.Listener
	adapters []adapter.AgentAdapter

	mu     sync.Mutex
	active bool
}

type Option func(*Server)

func WithAdapters(adapters ...adapter.AgentAdapter) Option {
	return func(s *Server) {
		s.adapters = append(s.adapters, adapters...)
	}
}

func Listen(opts ...Option) (*Server, string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", fmt.Errorf("listen runner session: %w", err)
	}
	s := &Server{listener: ln}
	for _, opt := range opts {
		opt(s)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/session", s.handleSession)
	s.server = &http.Server{Handler: mux}
	url := "ws://" + ln.Addr().String() + "/session"
	return s, url, nil
}

func (s *Server) Serve(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		err := s.server.Serve(s.listener)
		if err == http.ErrServerClosed {
			err = nil
		}
		errCh <- err
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown runner session server: %w", err)
		}
		return <-errCh
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	if !s.reserveSession() {
		http.Error(w, "runner session already active", http.StatusConflict)
		return
	}
	defer s.releaseSession()

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"localhost:*", "127.0.0.1:*"}})
	if err != nil {
		return
	}
	sess := protocol.NewSession(conn)
	run := runner.New(sess)
	for _, a := range s.adapters {
		run.Register(a)
	}
	sess.Start(context.Background())
	<-sess.Done()
}

func (s *Server) reserveSession() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active {
		return false
	}
	s.active = true
	return true
}

func (s *Server) releaseSession() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = false
}
