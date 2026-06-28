package managerhttp

import (
	"net/http/httptest"
	"testing"
)

func TestSecurityHostAndOriginAllowlistUsesConfiguredPort(t *testing.T) {
	sec := newSecurity("127.0.0.1:8080")
	for _, host := range []string{"127.0.0.1:8080", "localhost:8080"} {
		if !sec.allowedHost(host) {
			t.Fatalf("expected host %s to be allowed", host)
		}
	}
	for _, host := range []string{"127.0.0.1:9999", "localhost:9999", "example.com:8080"} {
		if sec.allowedHost(host) {
			t.Fatalf("expected host %s to be denied", host)
		}
	}

	req := httptest.NewRequest("POST", "http://127.0.0.1:8080/runs", nil)
	req.Header.Set("Origin", "http://localhost:8080")
	if !sec.allowedOrigin(req) {
		t.Fatal("expected localhost origin on configured port to be allowed")
	}
	req.Header.Set("Origin", "http://127.0.0.1:9999")
	if sec.allowedOrigin(req) {
		t.Fatal("expected origin on different port to be denied")
	}
}

func TestSecurityWildcardBindAllowsLoopbackOnBoundPort(t *testing.T) {
	sec := newSecurity("0.0.0.0:8080")
	for _, host := range []string{"0.0.0.0:8080", "127.0.0.1:8080", "localhost:8080", "[::1]:8080"} {
		if !sec.allowedHost(host) {
			t.Fatalf("expected loopback host %s to be allowed", host)
		}
	}
	for _, host := range []string{"127.0.0.1:18080", "192.0.2.10:8080", "example.com:8080"} {
		if sec.allowedHost(host) {
			t.Fatalf("expected host %s to be denied", host)
		}
	}

	req := httptest.NewRequest("POST", "http://127.0.0.1:8080/runs", nil)
	req.Header.Set("Origin", "http://localhost:8080")
	if !sec.allowedOrigin(req) {
		t.Fatal("expected loopback origin on bound port to be allowed")
	}
	req.Header.Set("Origin", "http://127.0.0.1:18080")
	if sec.allowedOrigin(req) {
		t.Fatal("expected loopback origin on a different port to be denied")
	}
	req.Header.Set("Origin", "http://example.com:8080")
	if sec.allowedOrigin(req) {
		t.Fatal("expected non-loopback origin to be denied")
	}
}
