package app

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/testsupport"
)

func TestHealthzAllowsContainerLocalhostHostHeader(t *testing.T) {
	st := testsupport.OpenStore(t)
	s := &Server{cfg: config.Config{BindAddr: "0.0.0.0:7345", AppContainer: true}, store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
	handler := withSecurityHeaders(withHostAllowlist(withRequestSafety(http.HandlerFunc(s.healthz), s.csrfToken), s.cfg.BindAddr))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Host = "127.0.0.1:7345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Fatalf("unexpected health response code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestAppContainerHostAllowlistRejectsNonLocalHostAsDefenseInDepth(t *testing.T) {
	s := &Server{cfg: config.Config{BindAddr: "0.0.0.0:7345", AppContainer: true}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
	handler := withHostAllowlist(http.HandlerFunc(s.healthz), s.cfg.BindAddr)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Host = "parley.internal:7345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected non-local host header to be rejected, got %d", rec.Code)
	}
}

func TestHealthzRejectsUnsafeMethod(t *testing.T) {
	s := &Server{cfg: config.Config{BindAddr: "127.0.0.1:7345"}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	rec := httptest.NewRecorder()
	s.healthz(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected method not allowed, got %d", rec.Code)
	}
}
