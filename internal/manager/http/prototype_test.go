package managerhttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/manager/web"
)

func TestPrototypeRouteRendersVariationsFromSeedData(t *testing.T) {
	renderer, err := web.NewRenderer()
	if err != nil {
		t.Fatalf("new renderer: %v", err)
	}
	srv := NewServer("127.0.0.1:8080", nil, nil, NewHub(), renderer)
	for _, variation := range []string{"1", "2", "3"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/prototype?v="+variation, nil)
		srv.routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("variation %s status = %d body=%s", variation, rec.Code, rec.Body.String())
		}
		body := rec.Body.String()
		if !strings.Contains(body, "Parley operator UI prototype") || !strings.Contains(body, "run_proto_running") {
			t.Fatalf("variation %s did not render seeded prototype body", variation)
		}
	}
}
