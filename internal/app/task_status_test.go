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
	"github.com/agent-parley/parley/internal/store"
	"github.com/agent-parley/parley/internal/testsupport"
)

func TestQueuedStatusLabelAndTemplateConstant(t *testing.T) {
	if got := statusLabel(models.TaskStatusQueued); got != "Queued" {
		t.Fatalf("queued status label = %q", got)
	}
	if templateConstants()["TaskStatusQueued"] != models.TaskStatusQueued {
		t.Fatalf("TaskStatusQueued template constant missing")
	}
}

func TestTaskPageRendersDraftQueueAction(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)

	body := renderTaskPage(t, st, task.ID)
	assertContains(t, body, "Draft ready")
	assertContains(t, body, "Approve and queue attempt")
	assertNotContains(t, body, "Attempt queued")
	assertNotContains(t, body, "Queue fix attempt")
}

func TestTaskPageRendersQueuedStateWithoutDraftOrFixActions(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)
	if _, _, _, err := st.QueueAttempt(task.ID); err != nil {
		t.Fatal(err)
	}

	body := renderTaskPage(t, st, task.ID)
	assertContains(t, body, "Queued")
	assertContains(t, body, "Attempt queued")
	assertNotContains(t, body, "Approve and queue attempt")
	assertNotContains(t, body, "Queue fix attempt")
}

func TestTaskPageRendersNeedsFixQueueAction(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)
	started, err := st.BeginAttempt(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.FailAttempt(task.ID, started.Attempt.ID, started.Lease.ID, "failed"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.RequestFix(task.ID); err != nil {
		t.Fatal(err)
	}

	body := renderTaskPage(t, st, task.ID)
	assertContains(t, body, "Fix requested")
	assertContains(t, body, "Queue fix attempt")
	assertNotContains(t, body, "Attempt queued")
	assertNotContains(t, body, "Approve and queue attempt")
}

func TestTaskPageRendersRealTabControls(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)

	body := renderTaskPage(t, st, task.ID)
	assertContains(t, body, `role="tablist"`)
	assertContains(t, body, `role="tab"`)
	assertContains(t, body, `role="tabpanel"`)
	assertContains(t, body, `data-tab-target="panel-diff"`)
	assertNotContains(t, body, `href="#diff"`)
}

func TestTaskPageActivitySuppressesDuplicateSummary(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	if _, err := st.AppendEvent(models.Event{RunID: run.ID, TaskID: task.ID, Type: models.EventLeaseGranted, ActorKind: models.ActorKindManager, ActorID: models.ActorKindManager, Summary: "Runner slot reserved"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendEvent(models.Event{RunID: run.ID, TaskID: task.ID, Type: models.EventTaskStateChanged, ActorKind: models.ActorKindManager, ActorID: models.ActorKindManager, Summary: "Attempt accepted by durable dispatcher"}); err != nil {
		t.Fatal(err)
	}

	body := renderTaskPage(t, st, task.ID)
	if count := strings.Count(body, "Runner slot reserved"); count != 1 {
		t.Fatalf("expected duplicate-looking event summary to be suppressed, count=%d body:\n%s", count, body)
	}
	assertContains(t, body, "Attempt accepted by durable dispatcher")
}

func TestTaskPageDiffPanelExplainsEmptyDiff(t *testing.T) {
	st := testsupport.OpenStore(t)
	project, run, task := testsupport.ProjectAndTask(t, st)
	writer := artifacts.NewWriter(st)
	attemptDir := st.AttemptDir(project.ID, run.ID, task.ID, 1)
	if _, err := writer.WriteForAttempt(run.ID, task.ID, 1, attemptDir, "diff.patch", models.ArtifactKindDiff, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteForAttempt(run.ID, task.ID, 1, attemptDir, "changed-files.txt", models.ArtifactKindChangedFiles, ""); err != nil {
		t.Fatal(err)
	}

	body := renderTaskPage(t, st, task.ID)
	assertContains(t, body, "No diff was produced for this attempt.")
	assertNotContains(t, body, "diff.patch")
}

func TestTaskPageRendersDiagnosticsTabWithoutLeakingBody(t *testing.T) {
	st := testsupport.OpenStore(t)
	project, run, task := testsupport.ProjectAndTask(t, st)
	artifact, err := artifacts.NewWriter(st).WriteForAttemptWithSensitivity(run.ID, task.ID, 1, st.AttemptDir(project.ID, run.ID, task.ID, 1), "runtime/stderr.txt", models.ArtifactKindWorkerOutput, models.SensitivityInternal, "raw stderr body")
	if err != nil {
		t.Fatal(err)
	}

	body := renderTaskPage(t, st, task.ID)
	assertContains(t, body, "Diagnostics (1)")
	assertContains(t, body, "/tasks/"+task.ID+"/diagnostics/"+artifact.ID)
	assertContains(t, body, "stderr.txt")
	assertContains(t, body, "excluded from normal artifact preview and handoff manifests")
	assertNotContains(t, body, "raw stderr body")
}

func renderTaskPage(t *testing.T, st *store.Store, taskID string) string {
	t.Helper()
	s := &Server{store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+taskID, nil)
	rec := httptest.NewRecorder()
	s.taskRoutes(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected task page status=%d body=%q", rec.Code, rec.Body.String())
	}
	return rec.Body.String()
}

func assertContains(t *testing.T, body, text string) {
	t.Helper()
	if !strings.Contains(body, text) {
		t.Fatalf("rendered task page missing %q in body:\n%s", text, body)
	}
}

func assertNotContains(t *testing.T, body, text string) {
	t.Helper()
	if strings.Contains(body, text) {
		t.Fatalf("rendered task page unexpectedly contained %q in body:\n%s", text, body)
	}
}
