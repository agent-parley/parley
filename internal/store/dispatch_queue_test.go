package store_test

import (
	"testing"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/store"
	"github.com/agent-parley/parley/internal/testsupport"
)

func TestQueueAttemptPersistsIntentAndBeginAttemptReusesIt(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)

	_, queuedTask, queuedAttempt, err := st.QueueAttempt(task.ID)
	if err != nil {
		t.Fatalf("queue attempt: %v", err)
	}
	if queuedTask.Status != models.TaskStatusQueued || queuedAttempt.Status != models.AttemptStatusQueued || queuedAttempt.Number != 1 {
		t.Fatalf("unexpected queued state task=%+v attempt=%+v", queuedTask, queuedAttempt)
	}
	started, err := st.BeginAttempt(task.ID)
	if err != nil {
		t.Fatalf("begin queued attempt: %v", err)
	}
	if started.Attempt.ID != queuedAttempt.ID || started.Attempt.Status != models.AttemptStatusRunning {
		t.Fatalf("queued attempt was not reused: queued=%+v started=%+v", queuedAttempt, started.Attempt)
	}
}

func TestQueueAttemptRejectsDuplicateWithoutCreatingAttempt(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)
	if _, _, _, err := st.QueueAttempt(task.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.QueueAttempt(task.ID); err == nil {
		t.Fatalf("expected duplicate queue rejection")
	}
	attempts := st.AttemptsForTask(task.ID)
	if len(attempts) != 1 || attempts[0].Status != models.AttemptStatusQueued {
		t.Fatalf("duplicate queue created attempts: %+v", attempts)
	}
}

func TestQueuedFixPlaceholderWaitsForResumeBeforeDispatch(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)
	started, err := st.BeginAttempt(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.FailAttempt(task.ID, started.Attempt.ID, started.Lease.ID, "failed"); err != nil {
		t.Fatal(err)
	}
	if _, _, requested, err := st.RequestFix(task.ID); err != nil {
		t.Fatal(err)
	} else if requested.Status != models.AttemptStatusRequested || requested.Kind != models.AttemptKindFix {
		t.Fatalf("request fix should create requested placeholder: %+v", requested)
	}
	queued, err := st.RecoverDispatchState()
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 0 {
		t.Fatalf("fix placeholder should not dispatch until resume-fix queues it: %+v", queued)
	}
	if _, queuedTask, queuedAttempt, err := st.QueueAttempt(task.ID); err != nil {
		t.Fatalf("resume-fix queue should promote fix placeholder: %v", err)
	} else if queuedTask.Status != models.TaskStatusQueued || queuedAttempt.Status != models.AttemptStatusQueued || queuedAttempt.Kind != models.AttemptKindFix || queuedAttempt.Number != 2 {
		t.Fatalf("unexpected queued fix after resume task=%+v attempt=%+v", queuedTask, queuedAttempt)
	}
}

func TestQueueAttemptRejectsPersistedBacklogLimit(t *testing.T) {
	st := testsupport.OpenStore(t)
	project, _, first := testsupport.ProjectAndTask(t, st)
	if _, _, _, err := st.QueueAttempt(first.ID); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 100; i++ {
		_, task, err := st.CreateManualRunTask(project, "task", "task", "", "", "done")
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := st.QueueAttempt(task.ID); err != nil {
			t.Fatalf("queue attempt %d: %v", i, err)
		}
	}
	_, overflow, err := st.CreateManualRunTask(project, "overflow", "overflow", "", "", "done")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.QueueAttempt(overflow.ID); err == nil {
		t.Fatalf("expected backlog limit rejection")
	}
}

func TestQueuedAttemptSurvivesSQLiteReopen(t *testing.T) {
	root := t.TempDir()
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	_, _, task := testsupport.ProjectAndTask(t, st)
	if _, _, _, err := st.QueueAttempt(task.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	queued := reopened.QueuedAttempts()
	if len(queued) != 1 || queued[0].TaskID != task.ID || queued[0].Status != models.AttemptStatusQueued {
		t.Fatalf("queued attempt did not survive reopen: %+v", queued)
	}
}

func TestRecoverDispatchStateFailsInterruptedRunningAttempt(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	started, err := st.BeginAttempt(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := st.RecoverDispatchState()
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 0 {
		t.Fatalf("running attempt should not be queued for rerun: %+v", queued)
	}
	gotTask, _ := st.GetTask(task.ID)
	if gotTask.Status != models.TaskStatusFailed || gotTask.LeaseID != "" {
		t.Fatalf("interrupted task not failed/released: %+v", gotTask)
	}
	attempts := st.AttemptsForTask(task.ID)
	if len(attempts) != 1 || attempts[0].ID != started.Attempt.ID || attempts[0].Status != models.AttemptStatusFailed {
		t.Fatalf("interrupted attempt not failed: %+v", attempts)
	}
	if active := st.ActiveLeaseCountByExecutor()[models.LocalExecutorID]; active != 0 {
		t.Fatalf("expected interrupted lease released, active=%d", active)
	}
	if !testsupport.HasEventSummary(st.EventsForRun(run.ID), "Attempt interrupted by manager restart") {
		t.Fatalf("missing interrupted event: %+v", st.EventsForRun(run.ID))
	}
}
