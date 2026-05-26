package app

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/artifacts"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/testsupport"
)

func TestBuildHandoffExcludesInternalArtifactsAndManagerDatabaseSidecars(t *testing.T) {
	st := testsupport.OpenStore(t)
	project, run, task := testsupport.ProjectAndTask(t, st)
	writer := artifacts.NewWriter(st)
	if _, err := writer.WriteForAttempt(run.ID, task.ID, 1, st.AttemptDir(project.ID, run.ID, task.ID, 1), "summary.md", models.ArtifactKindSummary, "visible"); err != nil { t.Fatal(err) }
	if _, err := writer.WriteForAttemptWithSensitivity(run.ID, task.ID, 1, st.AttemptDir(project.ID, run.ID, task.ID, 1), "runtime/stdout.txt", models.ArtifactKindWorkerOutput, models.SensitivityInternal, "raw"); err != nil { t.Fatal(err) }
	if _, err := writer.WriteForAttemptWithSensitivity(run.ID, task.ID, 1, st.AttemptDir(project.ID, run.ID, task.ID, 1), "checkpoints/worker.json", models.ArtifactKindCheckpoint, models.SensitivityInternal, "{}"); err != nil { t.Fatal(err) }

	s := &Server{store: st}
	handoff := s.buildHandoff(project, run, task, models.LocalExecutorID, "homelab")
	if len(handoff.Included) != 1 || handoff.Included[0].Name != "summary.md" {
		t.Fatalf("internal artifacts should be excluded: %+v", handoff.Included)
	}
	wantExclusions := map[string]bool{"parley.db": false, "parley.db-wal": false, "parley.db-shm": false}
	for _, exclusion := range handoff.Excluded {
		if _, ok := wantExclusions[exclusion.RelativePath]; ok {
			wantExclusions[exclusion.RelativePath] = true
		}
	}
	for path, seen := range wantExclusions {
		if !seen {
			t.Fatalf("missing handoff exclusion for %s: %+v", path, handoff.Excluded)
		}
	}
}

func TestHandoffStartExplainsMockPreviewOnly(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)
	s := &Server{store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+task.ID+"/handoff", nil)
	rec := httptest.NewRecorder()
	s.taskRoutes(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status=%d body=%q", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{"Preview mock runner handoff", "No files are copied", "no remote runner is contacted", "task assignment does not change"} {
		if !strings.Contains(body, want) {
			t.Fatalf("handoff start missing %q in body:\n%s", want, body)
		}
	}
}

func TestHandoffApprovalExplainsNoRealSync(t *testing.T) {
	st := testsupport.OpenStore(t)
	project, run, task := testsupport.ProjectAndTask(t, st)
	s := &Server{store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
	handoff := s.buildHandoff(project, run, task, models.LocalExecutorID, "homelab")
	saved, err := st.SaveHandoff(handoff)
	if err != nil {
		t.Fatal(err)
	}
	saved.ManifestPreview = s.handoffManifestPreview(saved)
	if err := st.UpdateHandoff(saved); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+task.ID+"/handoff/"+saved.ID, nil)
	rec := httptest.NewRecorder()
	s.taskRoutes(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status=%d body=%q", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{"Mock safe handoff preview", "does not copy files", "contact a remote runner", "change task assignment"} {
		if !strings.Contains(body, want) {
			t.Fatalf("handoff approval missing %q in body:\n%s", want, body)
		}
	}
}
