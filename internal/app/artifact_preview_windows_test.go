//go:build windows

package app

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agent-parley/parley/internal/artifacts"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/testsupport"
)

func TestArtifactPreviewOnWindowsReturnsUserFacingForbidden(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	artifact, err := artifacts.NewWriter(st).WriteForAttempt(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "summary.md", models.ArtifactKindSummary, "visible")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{store: st}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/"+artifact.ID, nil)
	rec := httptest.NewRecorder()
	s.artifact(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected Windows artifact preview to return 403, got %d body=%q", rec.Code, rec.Body.String())
	}
}
