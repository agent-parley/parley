package session

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/agent-parley/parley/internal/manager/runnerclient"
	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestServerAllowsOnlyOneActiveSessionReservation(t *testing.T) {
	s := &Server{}
	if !s.reserveSession() {
		t.Fatal("first reservation should succeed")
	}
	if s.reserveSession() {
		t.Fatal("second reservation should fail while active")
	}
	s.releaseSession()
	if !s.reserveSession() {
		t.Fatal("reservation after release should succeed")
	}
}

func TestListenBuildsURLAndServeStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv, url, err := Listen(WithAdapters(readyOnlyAdapter{name: "unit_ready"}))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if srv.server == nil || srv.listener == nil || !strings.HasPrefix(url, "ws://127.0.0.1:") || !strings.HasSuffix(url, "/session") {
		t.Fatalf("Listen returned server=%v listener=%v url=%q", srv.server, srv.listener, url)
	}
	if len(srv.adapters) != 1 || srv.adapters[0].Name() != "unit_ready" {
		t.Fatalf("adapters = %+v, want injected adapter", srv.adapters)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()
	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve after cancel error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not stop after context cancel")
	}
}

func TestServeReturnsListenerError(t *testing.T) {
	srv, _, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if err := srv.listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	if err := srv.Serve(context.Background()); err == nil {
		t.Fatal("Serve returned nil after listener was closed")
	}
}

func TestHandleSessionRegistersInjectedAdaptersInReadyMessage(t *testing.T) {
	serverCtx, stop, _, url := startTestSessionServer(t, WithAdapters(readyOnlyAdapter{name: "unit_ready"}))
	defer stop()
	client, err := runnerclient.Dial(serverCtx, url, "runner_session_unit")
	if err != nil {
		t.Fatalf("dial runner: %v", err)
	}
	ready := client.Ready()
	if ready.RunnerID != "runner_session_unit" || !hasAdapter(ready.Capabilities.Adapters, "noop") || !hasAdapter(ready.Capabilities.Adapters, "unit_ready") {
		t.Fatalf("ready = %+v, want noop and injected adapter", ready)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close client: %v", err)
	}
	stop()
}

func TestHandleSessionRejectsConcurrentConnectionAndReleasesAfterDisconnect(t *testing.T) {
	serverCtx, stop, _, url := startTestSessionServer(t)
	defer stop()
	client, err := runnerclient.Dial(serverCtx, url, "runner_first")
	if err != nil {
		t.Fatalf("dial first runner: %v", err)
	}

	_, resp, err := websocket.Dial(serverCtx, url, nil)
	if err == nil {
		t.Fatal("second websocket dial succeeded while first session was active")
	}
	if resp == nil || resp.StatusCode != http.StatusConflict {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("second websocket status = %d, want %d (err=%v)", status, http.StatusConflict, err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close first client: %v", err)
	}

	var reconnected *runnerclient.Client
	deadline := time.After(5 * time.Second)
	for reconnected == nil {
		select {
		case <-deadline:
			t.Fatal("session did not release active slot after disconnect")
		default:
		}
		reconnected, err = runnerclient.Dial(serverCtx, url, "runner_second")
		if err != nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if reconnected.Ready().RunnerID != "runner_second" {
		t.Fatalf("reconnected ready = %+v", reconnected.Ready())
	}
	if err := reconnected.Close(context.Background()); err != nil {
		t.Fatalf("close reconnected client: %v", err)
	}
	stop()
}

type readyOnlyAdapter struct{ name string }

func (a readyOnlyAdapter) Name() string { return a.name }

func (readyOnlyAdapter) Run(context.Context, contract.Dispatch, runnerio.Sink) (report.Report, error) {
	return report.Report{}, nil
}

func startTestSessionServer(t *testing.T, opts ...Option) (context.Context, context.CancelFunc, <-chan error, string) {
	t.Helper()
	serverCtx, stop := context.WithCancel(context.Background())
	srv, url, err := Listen(opts...)
	if err != nil {
		stop()
		t.Fatalf("listen runner: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(serverCtx) }()
	t.Cleanup(func() {
		stop()
		waitServeStopped(t, serveErr)
	})
	ctx, cancel := context.WithTimeout(serverCtx, 5*time.Second)
	t.Cleanup(cancel)
	return ctx, stop, serveErr, url
}

func waitServeStopped(t *testing.T, serveErr <-chan error) {
	t.Helper()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func hasAdapter(adapters []string, want string) bool {
	for _, adapter := range adapters {
		if adapter == want {
			return true
		}
	}
	return false
}
