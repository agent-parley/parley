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
