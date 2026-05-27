package app

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/agent-parley/parley/internal/artifacts"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/testsupport"
)

func TestArtifactPreviewAllowsNormalArtifactUnderDataRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("browser artifact reads are disabled on Windows in this prototype")
	}
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	artifact, err := artifacts.NewWriter(st).WriteForAttempt(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "summary.md", models.ArtifactKindSummary, "visible")
	if err != nil { t.Fatal(err) }
	s := &Server{store: st}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/"+artifact.ID, nil)
	rec := httptest.NewRecorder()
	s.artifact(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "visible" {
		t.Fatalf("unexpected preview response code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestArtifactPreviewRejectsInternalArtifact(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	artifact, err := artifacts.NewWriter(st).WriteForAttemptWithSensitivity(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "runtime/stderr.txt", models.ArtifactKindWorkerOutput, models.SensitivityInternal, "secret stderr")
	if err != nil { t.Fatal(err) }
	s := &Server{store: st}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/"+artifact.ID, nil)
	rec := httptest.NewRecorder()
	s.artifact(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d body=%q", rec.Code, rec.Body.String())
	}
	if rec.Body.String() == "secret stderr" {
		t.Fatalf("internal artifact body leaked")
	}
}

func TestTaskDiagnosticsPreviewAllowsInternalArtifactUnderTask(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("browser artifact reads are disabled on Windows in this prototype")
	}
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	artifact, err := artifacts.NewWriter(st).WriteForAttemptWithSensitivity(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "runtime/stderr.txt", models.ArtifactKindWorkerOutput, models.SensitivityInternal, "secret stderr")
	if err != nil { t.Fatal(err) }
	s := &Server{store: st}
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+task.ID+"/diagnostics/"+artifact.ID, nil)
	rec := httptest.NewRecorder()
	s.taskRoutes(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "secret stderr" {
		t.Fatalf("unexpected diagnostics response code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestTaskDiagnosticsPreviewRejectsNormalArtifact(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	artifact, err := artifacts.NewWriter(st).WriteForAttempt(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "summary.md", models.ArtifactKindSummary, "visible")
	if err != nil { t.Fatal(err) }
	s := &Server{store: st}
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+task.ID+"/diagnostics/"+artifact.ID, nil)
	rec := httptest.NewRecorder()
	s.taskRoutes(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected forbidden for normal artifact on diagnostics route, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestTaskDiagnosticsPreviewRejectsNonDiagnosticInternalArtifact(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	artifact, err := artifacts.NewWriter(st).WriteForAttemptWithSensitivity(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "internal/notes.txt", models.ArtifactKindFindings, models.SensitivityInternal, "private notes")
	if err != nil { t.Fatal(err) }
	s := &Server{store: st}
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+task.ID+"/diagnostics/"+artifact.ID, nil)
	rec := httptest.NewRecorder()
	s.taskRoutes(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected forbidden for non-diagnostic internal artifact, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestArtifactPreviewRejectsOutsideDataRoot(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil { t.Fatal(err) }
	artifact := models.Artifact{RunID: run.ID, TaskID: task.ID, AttemptNumber: 1, Kind: models.ArtifactKindSummary, Path: outside, Sensitivity: models.SensitivityNormal}
	if err := st.AddArtifact(artifact); err != nil { t.Fatal(err) }
	stored := st.ArtifactsForTask(task.ID)[0]
	s := &Server{store: st}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/"+stored.ID, nil)
	rec := httptest.NewRecorder()
	s.artifact(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected forbidden for outside path, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestNormalArtifactsForTaskExcludesInternal(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	writer := artifacts.NewWriter(st)
	if _, err := writer.WriteForAttempt(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "summary.md", models.ArtifactKindSummary, "visible"); err != nil { t.Fatal(err) }
	if _, err := writer.WriteForAttemptWithSensitivity(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "runtime/stdout.txt", models.ArtifactKindWorkerOutput, models.SensitivityInternal, "raw"); err != nil { t.Fatal(err) }
	s := &Server{store: st}
	visible := s.normalArtifactsForTask(task.ID)
	if len(visible) != 1 || visible[0].Sensitivity != models.SensitivityNormal {
		t.Fatalf("unexpected visible artifacts: %+v", visible)
	}
}

func TestNormalArtifactsForTaskExcludesSecretClassifiedArtifacts(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	writer := artifacts.NewWriter(st)
	if _, err := writer.WriteForAttempt(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "summary.md", models.ArtifactKindSummary, "visible"); err != nil { t.Fatal(err) }
	if _, err := writer.WriteForAttempt(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "leaky-summary.md", models.ArtifactKindSummary, "Authorization: Bearer abcdefghijklmnopqrstuvwxyz"); err != nil { t.Fatal(err) }
	s := &Server{store: st}
	visible := s.normalArtifactsForTask(task.ID)
	if len(visible) != 1 || artifactName(visible[0].Path) != "summary.md" || visible[0].Sensitivity != models.SensitivityNormal {
		t.Fatalf("unexpected visible artifacts: %+v", visible)
	}
}
