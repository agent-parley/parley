package managerhttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/manager/web"
)

func TestPrototypeRouteRendersSingleLinearDesignFromSeedData(t *testing.T) {
	renderer, err := web.NewRenderer()
	if err != nil {
		t.Fatalf("new renderer: %v", err)
	}
	srv := NewServer("127.0.0.1:8080", nil, nil, NewHub(), renderer)
	for _, path := range []string{"/prototype", "/prototype?tab=review", "/prototype?view=runners", "/prototype?run=run_proto_completed&tab=review"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080"+path, nil)
		srv.routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d body=%s", path, rec.Code, rec.Body.String())
		}
		body := rec.Body.String()
		if !strings.Contains(body, "Operator prototype") || !strings.Contains(body, "run_proto_running") {
			t.Fatalf("%s did not render seeded prototype body", path)
		}
		if strings.Contains(body, "?v=") || strings.Contains(body, "Command center") {
			t.Fatalf("%s rendered retired variation UI", path)
		}
	}
}
