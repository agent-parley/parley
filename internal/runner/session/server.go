package session

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/agent-parley/parley/internal/runner"
	"github.com/agent-parley/parley/internal/shared/protocol"
)

type Server struct {
	server   *http.Server
	listener net.Listener
}

func Listen() (*Server, string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", fmt.Errorf("listen runner session: %w", err)
	}
	s := &Server{listener: ln}
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
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"localhost:*", "127.0.0.1:*"}})
	if err != nil {
		return
	}
	sess := protocol.NewSession(conn)
	_ = runner.New(sess)
	sess.Start(context.Background())
	<-sess.Done()
}
