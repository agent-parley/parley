package manager_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/artifacts"
	"github.com/agent-parley/parley/internal/executor"
	"github.com/agent-parley/parley/internal/manager"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/store"
	"github.com/agent-parley/parley/internal/testsupport"
)

type workflowRunner struct {
	result executor.AttemptResult
	err    error
	panicRun bool
	inputs *[]executor.AttemptInput
	run    func(executor.AttemptInput) (executor.AttemptResult, error)
}

func (r workflowRunner) RunAttempt(ctx context.Context, input executor.AttemptInput) (executor.AttemptResult, error) {
	if r.inputs != nil {
		*r.inputs = append(*r.inputs, input)
	}
	if r.panicRun {
		panic("secret panic /tmp/private")
	}
	if r.run != nil {
		return r.run(input)
	}
	return r.result, r.err
}

func newWorkflow(t *testing.T, st *store.Store, runner workflowRunner) *manager.WorkflowService {
	t.Helper()
	return manager.NewWorkflowService(st, runner, artifacts.NewWriter(st), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func absoluteArtifactName() string {
	if runtime.GOOS == "windows" {
		return `C:\absolute-output.md`
	}
	return "/absolute-output.md"
}

func TestStartAttemptSuccessWritesArtifactsAndReleasesLease(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	wf := newWorkflow(t, st, workflowRunner{result: executor.AttemptResult{Summary: "ready", Files: []executor.OutputFile{{Name: "worker-output.md", Kind: models.ArtifactKindWorkerOutput, Body: "worker summary"}}}})

	if err := wf.StartAttempt(context.Background(), task.ID); err != nil {
		t.Fatalf("start attempt: %v", err)
	}
	gotTask, _ := st.GetTask(task.ID)
	if gotTask.Status != models.TaskStatusAwaitingReview || gotTask.LeaseID != "" {
		t.Fatalf("unexpected task after success: %+v", gotTask)
	}
	artifacts := st.ArtifactsForTask(task.ID)
	if len(artifacts) != 1 || artifacts[0].Sensitivity != models.SensitivityNormal || artifacts[0].Kind != models.ArtifactKindWorkerOutput {
		t.Fatalf("unexpected artifacts: %+v", artifacts)
	}
	events := st.EventsForRun(run.ID)
	for _, summary := range []string{"Runner slot reserved", "Worker attempt started using selected runner record", "Attempt outputs saved", "Reviewer step completed", "Runner slot released"} {
		if !testsupport.HasEventSummary(events, summary) {
			t.Fatalf("missing %q in events: %+v", summary, events)
		}
	}
}

func TestStartAttemptFailureWritesDiagnosticsAndEmitsFailureLifecycle(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	wf := newWorkflow(t, st, workflowRunner{
		result: executor.AttemptResult{Summary: "worker failed safely", Files: []executor.OutputFile{{Name: "runtime/stderr.txt", Kind: models.ArtifactKindWorkerOutput, Body: "raw stderr", Sensitivity: models.SensitivityInternal}}},
		err:    errors.New("worker failed"),
	})

	if err := wf.StartAttempt(context.Background(), task.ID); err == nil {
		t.Fatalf("expected runner failure")
	}
	gotTask, _ := st.GetTask(task.ID)
	if gotTask.Status != models.TaskStatusFailed || gotTask.LeaseID != "" {
		t.Fatalf("unexpected failed task: %+v", gotTask)
	}
	attempts := st.AttemptsForTask(task.ID)
	if len(attempts) != 1 || attempts[0].Status != models.AttemptStatusFailed || attempts[0].Summary != "worker failed safely" {
		t.Fatalf("unexpected failed attempt: %+v", attempts)
	}
	artifacts := st.ArtifactsForTask(task.ID)
	if len(artifacts) != 1 || artifacts[0].Sensitivity != models.SensitivityInternal {
		t.Fatalf("expected internal diagnostic artifact, got %+v", artifacts)
	}
	events := st.EventsForRun(run.ID)
	for _, summary := range []string{"Attempt failure diagnostics saved", "Attempt failed", "Runner slot released after failed attempt"} {
		if !testsupport.HasEventSummary(events, summary) {
			t.Fatalf("missing %q in events: %+v", summary, events)
		}
	}
}

func TestStartAttemptDoesNotWriteAfterAttemptNoLongerRunning(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	wf := newWorkflow(t, st, workflowRunner{run: func(input executor.AttemptInput) (executor.AttemptResult, error) {
		if _, _, _, err := st.FailAttempt(input.Task.ID, input.Attempt.ID, input.Lease.ID, "attempt changed while runner was active"); err != nil {
			return executor.AttemptResult{}, err
		}
		return executor.AttemptResult{Summary: "late success", Files: []executor.OutputFile{{Name: "late.md", Kind: models.ArtifactKindSummary, Body: "should not be written"}}}, nil
	}})

	if err := wf.StartAttempt(context.Background(), task.ID); err != nil {
		t.Fatalf("stale attempt should short-circuit without surfacing runner success as an error: %v", err)
	}
	if artifacts := st.ArtifactsForTask(task.ID); len(artifacts) != 0 {
		t.Fatalf("workflow wrote artifacts after attempt stopped running: %+v", artifacts)
	}
	if testsupport.HasEventSummary(st.EventsForRun(run.ID), "Attempt outputs saved") {
		t.Fatalf("workflow emitted success artifact event after attempt stopped running: %+v", st.EventsForRun(run.ID))
	}
}

func TestStartAttemptRunnerPanicFailsAndReleasesLeaseWithoutLeakingEventData(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	wf := newWorkflow(t, st, workflowRunner{panicRun: true})

	if err := wf.StartAttempt(context.Background(), task.ID); err == nil {
		t.Fatalf("expected panic to be converted into an error")
	}
	gotTask, _ := st.GetTask(task.ID)
	if gotTask.Status != models.TaskStatusFailed || gotTask.LeaseID != "" {
		t.Fatalf("unexpected task after panic: %+v", gotTask)
	}
	for _, event := range st.EventsForRun(run.ID) {
		if event.Summary == "Attempt failed" && eventLeaksWorkflow(event, "secret", "/tmp/private") {
			t.Fatalf("panic details leaked into event: %+v", event)
		}
	}
	if !testsupport.HasEventSummary(st.EventsForRun(run.ID), "Runner slot released after failed attempt") {
		t.Fatalf("missing lease release event after panic: %+v", st.EventsForRun(run.ID))
	}
}

func eventLeaksWorkflow(event models.Event, needles ...string) bool {
	text := event.Summary
	for key, value := range event.Data {
		text += key + " " + fmt.Sprint(value)
	}
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func TestStartAttemptPassesPriorCheckpointMetadataToRunner(t *testing.T) {
	st := testsupport.OpenStore(t)
	project, run, task := testsupport.ProjectAndTask(t, st)
	writer := artifacts.NewWriter(st)
	if _, err := writer.WriteForAttemptWithSensitivity(run.ID, task.ID, 1, st.AttemptDir(project.ID, run.ID, task.ID, 1), "checkpoints/worker.json", models.ArtifactKindCheckpoint, models.SensitivityInternal, `{"summary":"prior worker checkpoint","profile":"pi-standard"}`); err != nil {
		t.Fatal(err)
	}
	task.Attempts = 1
	task.Status = models.TaskStatusNeedsFix
	if err := st.UpdateTask(task); err != nil {
		t.Fatal(err)
	}
	var inputs []executor.AttemptInput
	wf := newWorkflow(t, st, workflowRunner{inputs: &inputs, result: executor.AttemptResult{Summary: "ready", Files: []executor.OutputFile{{Name: "worker-output.md", Kind: models.ArtifactKindWorkerOutput, Body: "ok"}}}})

	if err := wf.StartAttempt(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}
	if len(inputs) != 1 || len(inputs[0].ResumeCheckpoints) != 1 {
		t.Fatalf("expected prior checkpoint metadata in runner input: %+v", inputs)
	}
	checkpoint := inputs[0].ResumeCheckpoints[0]
	if checkpoint.AttemptNumber != 1 || checkpoint.Step != "worker" || checkpoint.Summary != "prior worker checkpoint" || checkpoint.Profile != "pi-standard" {
		t.Fatalf("unexpected checkpoint metadata: %+v", inputs[0].ResumeCheckpoints)
	}
	if strings.HasPrefix(checkpoint.Path, "/") || strings.Contains(checkpoint.Path, st.DataRoot()) {
		t.Fatalf("checkpoint reference leaked absolute data-root path: %+v", checkpoint)
	}
}

func TestStartAttemptArtifactWriteFailureStillEmitsFailureLifecycle(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	wf := newWorkflow(t, st, workflowRunner{result: executor.AttemptResult{Summary: "should fail writing", Files: []executor.OutputFile{{Name: absoluteArtifactName(), Kind: models.ArtifactKindWorkerOutput, Body: "bad"}}}})

	if err := wf.StartAttempt(context.Background(), task.ID); err == nil {
		t.Fatalf("expected artifact write failure")
	}
	gotTask, _ := st.GetTask(task.ID)
	if gotTask.Status != models.TaskStatusFailed || gotTask.LeaseID != "" {
		t.Fatalf("unexpected task after artifact write failure: %+v", gotTask)
	}
	events := st.EventsForRun(run.ID)
	if !testsupport.HasEventSummary(events, "Attempt failed") || !testsupport.HasEventSummary(events, "Runner slot released after failed attempt") {
		t.Fatalf("missing failure lifecycle events: %+v", events)
	}
}
