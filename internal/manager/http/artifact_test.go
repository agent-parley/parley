package managerhttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/manager/store"
)

func TestArtifactHandlerServesByIDOnlyAndEscapesPreview(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "artifact test")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	artifact, err := st.SaveArtifact(ctx, wr.Run.ID, "adapter_output", "text/plain", []byte("<script>alert(1)</script>"), ".txt")
	if err != nil {
		t.Fatalf("save artifact: %v", err)
	}

	srv := NewServer("127.0.0.1:8080", st, nil, NewHub(), nil)
	body := requestArtifact(t, srv, "/artifacts/"+artifact.ID, http.StatusOK)
	if strings.Contains(body, "<script>") {
		t.Fatalf("artifact preview was not escaped: %s", body)
	}
	if !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Fatalf("escaped artifact missing from body: %s", body)
	}

	requestArtifact(t, srv, "/artifacts/"+artifact.ID+"?path=/tmp/x", http.StatusNotFound)
	requestArtifact(t, srv, "/artifacts/"+artifact.ID+"/raw", http.StatusNotFound)
}

func TestArtifactHandlerDownloadsRawHTML(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "html artifact test")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	artifact, err := st.SaveArtifact(ctx, wr.Run.ID, "adapter_output", "text/html", []byte("<h1>raw</h1>"), ".html")
	if err != nil {
		t.Fatalf("save artifact: %v", err)
	}

	srv := NewServer("127.0.0.1:8080", st, nil, NewHub(), nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/artifacts/"+artifact.ID, nil)
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Disposition"); !strings.HasPrefix(got, "attachment;") {
		t.Fatalf("Content-Disposition = %q", got)
	}
	if rec.Body.String() != "<h1>raw</h1>" {
		t.Fatalf("raw html body = %q", rec.Body.String())
	}
}

func requestArtifact(t *testing.T, srv *Server, path string, want int) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080"+path, nil)
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("GET %s status = %d want %d body=%s", path, rec.Code, want, rec.Body.String())
	}
	return rec.Body.String()
}
