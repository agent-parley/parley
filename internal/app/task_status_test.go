package app

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/artifacts"
	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/dispatcher"
	"github.com/agent-parley/parley/internal/executor"
	"github.com/agent-parley/parley/internal/manager"
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
	assertContains(t, body, `id="task-activity"`)
	assertContains(t, body, "/tasks/"+task.ID+"/activity")
	assertContains(t, body, "Activity updates paused; retrying")
	assertNotContains(t, body, "Approve and queue attempt")
	assertNotContains(t, body, "Queue fix attempt")
}

func TestTaskPageShowsLocalPiWarningAtQueueActions(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)

	body := renderTaskPageWithConfig(t, st, task.ID, config.Config{ExecutionMode: config.ExecutionModeLocalPi})
	assertContains(t, body, "Experimental local-pi mode")
	assertContains(t, body, "runs real local worker/reviewer agents")
	warning := localPiWarningBlock(t, body)
	assertNotContains(t, warning, "Docker")
	assertNotContains(t, warning, "remote runner")
}

func TestTaskActivityIsScopedToCurrentTask(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	if _, err := st.AppendEvent(models.Event{RunID: run.ID, TaskID: task.ID, Type: models.EventAttemptWorkerStarted, ActorKind: models.ActorKindRunner, ActorID: models.LocalExecutorID, Summary: "current task event"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendEvent(models.Event{RunID: run.ID, TaskID: "sibling-task", Type: models.EventAttemptWorkerStarted, ActorKind: models.ActorKindRunner, ActorID: models.LocalExecutorID, Summary: "sibling task event"}); err != nil {
		t.Fatal(err)
	}
	body := renderTaskPage(t, st, task.ID)
	assertContains(t, body, "current task event")
	assertNotContains(t, body, "sibling task event")
}

func TestTaskActivityFragmentRendersLiveStatus(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	if _, _, _, err := st.QueueAttempt(task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendEvent(models.Event{RunID: run.ID, TaskID: task.ID, Type: models.EventAttemptWorkerStarted, ActorKind: models.ActorKindRunner, ActorID: models.LocalExecutorID, Summary: "Worker started", Data: map[string]any{"attempt": 2, "status": "running", "exit_code": 7}}); err != nil {
		t.Fatal(err)
	}
	s := &Server{store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+task.ID+"/activity", nil)
	rec := httptest.NewRecorder()
	s.taskRoutes(rec, req)
	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected activity status=%d body=%q", rec.Code, body)
	}
	assertContains(t, body, `data-task-active="true"`)
	assertContains(t, body, "Worker started")
	assertContains(t, body, "Attempt 2")
	assertContains(t, body, "running")
	assertContains(t, body, "exit 7")
}

func TestTaskPageRendersNeedsFixQueueAction(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)
	moveTaskToAwaitingReview(t, st, task.ID)
	if _, _, _, err := st.RequestFix(task.ID); err != nil {
		t.Fatal(err)
	}

	body := renderTaskPage(t, st, task.ID)
	assertContains(t, body, "Fix requested")
	assertContains(t, body, "Queue fix attempt")
	assertNotContains(t, body, "Attempt queued")
	assertNotContains(t, body, "Approve and queue attempt")
}

func TestRequestFixManualPolicyDoesNotQueueAttempt(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)
	moveTaskToAwaitingReview(t, st, task.ID)
	s := &Server{store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
	postTaskAction(t, s, task.ID, "request-fix")
	updated, _ := st.GetTask(task.ID)
	if updated.Status != models.TaskStatusNeedsFix {
		t.Fatalf("manual request-fix should leave task needs-fix, got %+v", updated)
	}
	for _, attempt := range st.AttemptsForTask(task.ID) {
		if attempt.Status == models.AttemptStatusQueued {
			t.Fatalf("manual request-fix must not queue attempt: %+v", st.AttemptsForTask(task.ID))
		}
	}
}

func TestRequestFixAutoPolicyQueuesDurableAttempt(t *testing.T) {
	st := testsupport.OpenStore(t)
	project, _, blockingTask := testsupport.ProjectAndTask(t, st)
	project.QueuePolicy = models.QueuePolicyAutoWhenReady
	if err := st.UpdateProjectSettings(project); err != nil {
		t.Fatal(err)
	}
	_, targetTask, err := st.CreateManualRunTask(project, "Needs fix", "fix it", "", "", "done")
	if err != nil {
		t.Fatal(err)
	}
	moveTaskToAwaitingReview(t, st, targetTask.ID)
	runner := &blockingTaskRunner{started: make(chan struct{}, 1), block: make(chan struct{})}
	t.Cleanup(func() { close(runner.block) })
	d := newBlockingDispatcher(t, st, runner)
	if err := d.Enqueue(context.Background(), blockingTask.ID); err != nil {
		t.Fatalf("enqueue blocking task: %v", err)
	}
	select {
	case <-runner.started:
	case <-time.After(2 * time.Second):
		t.Fatalf("blocking task did not start")
	}
	s := &Server{store: st, dispatcher: d, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
	postTaskAction(t, s, targetTask.ID, "request-fix")
	updated, _ := st.GetTask(targetTask.ID)
	if updated.Status != models.TaskStatusQueued {
		t.Fatalf("auto request-fix should queue task, got %+v", updated)
	}
	attempts := st.AttemptsForTask(targetTask.ID)
	if len(attempts) != 2 || attempts[1].Status != models.AttemptStatusQueued || attempts[1].Kind != models.AttemptKindFix {
		t.Fatalf("expected durable queued fix attempt, got %+v", attempts)
	}
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

type blockingTaskRunner struct {
	started chan struct{}
	block   chan struct{}
}

func (r *blockingTaskRunner) RunAttempt(ctx context.Context, input executor.AttemptInput) (executor.AttemptResult, error) {
	if r.started != nil {
		select {
		case r.started <- struct{}{}:
		default:
		}
	}
	if r.block != nil {
		<-r.block
	}
	return executor.AttemptResult{Summary: "done", Files: []executor.OutputFile{{Name: "summary.md", Kind: models.ArtifactKindSummary, Body: "done"}}}, nil
}

func newBlockingDispatcher(t *testing.T, st *store.Store, runner *blockingTaskRunner) *dispatcher.Dispatcher {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	wf := manager.NewWorkflowService(st, runner, artifacts.NewWriter(st), logger)
	return dispatcher.New(st, wf, runner, logger, 1)
}

func moveTaskToAwaitingReview(t *testing.T, st *store.Store, taskID string) {
	t.Helper()
	started, err := st.BeginAttempt(taskID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.CompleteAttempt(taskID, started.Attempt.ID, started.Lease.ID, "ready for review"); err != nil {
		t.Fatal(err)
	}
}

func postTaskAction(t *testing.T, s *Server, taskID, action string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/tasks/"+taskID+"/"+action, strings.NewReader("csrf_token=test"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.taskRoutes(rec, req)
	if rec.Code != http.StatusSeeOther && rec.Code != http.StatusOK {
		t.Fatalf("unexpected %s status=%d body=%q", action, rec.Code, rec.Body.String())
	}
}

func localPiWarningBlock(t *testing.T, body string) string {
	t.Helper()
	start := strings.Index(body, "Experimental local-pi mode")
	if start < 0 {
		t.Fatalf("missing local-pi warning in body:\n%s", body)
	}
	end := strings.Index(body[start:], "</div>")
	if end < 0 {
		t.Fatalf("missing local-pi warning close in body:\n%s", body[start:])
	}
	return body[start : start+end]
}

func renderTaskPage(t *testing.T, st *store.Store, taskID string) string {
	t.Helper()
	return renderTaskPageWithConfig(t, st, taskID, config.Config{})
}

func renderTaskPageWithConfig(t *testing.T, st *store.Store, taskID string, cfg config.Config) string {
	t.Helper()
	s := &Server{cfg: cfg, store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), csrfToken: "test"}
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
