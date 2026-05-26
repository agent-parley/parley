package store_test

import (
	"testing"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/store"
	"github.com/agent-parley/parley/internal/testsupport"
)

func TestAttemptLifecycleTransitionsAndPersists(t *testing.T) {
	root := t.TempDir()
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	project, run, task := testsupport.ProjectAndTask(t, st)

	started, err := st.BeginAttempt(task.ID)
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	if started.Attempt.Number != 1 || started.Attempt.Status != models.AttemptStatusRunning {
		t.Fatalf("unexpected started attempt: %+v", started.Attempt)
	}
	if _, _, _, err := st.CompleteAttempt(task.ID, started.Attempt.ID, started.Lease.ID, "ready for review"); err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	if active := st.ActiveLeaseCountByExecutor()[models.LocalExecutorID]; active != 0 {
		t.Fatalf("expected completed attempt to release lease, active count=%d", active)
	}
	if _, _, _, err := st.CompleteAttempt(task.ID, started.Attempt.ID, started.Lease.ID, "ready for review"); err != nil {
		t.Fatalf("completed attempt transition should be idempotent: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	gotTask, ok := reopened.GetTask(task.ID)
	if !ok {
		t.Fatalf("task %s not persisted", task.ID)
	}
	if gotTask.Status != models.TaskStatusAwaitingReview || gotTask.Attempts != 1 || gotTask.LeaseID != "" {
		t.Fatalf("unexpected persisted task: %+v", gotTask)
	}
	gotRun, _ := reopened.GetRun(run.ID)
	if gotRun.Status != models.RunStatusAwaitingReview {
		t.Fatalf("unexpected persisted run: %+v", gotRun)
	}
	attempts := reopened.AttemptsForTask(task.ID)
	if len(attempts) != 1 || attempts[0].Status != models.AttemptStatusReviewed || attempts[0].Summary != "ready for review" {
		t.Fatalf("unexpected attempts: %+v", attempts)
	}
	_ = project
}

func TestFailedTaskCanRequestFixAndRetry(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)

	started, err := st.BeginAttempt(task.ID)
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	if _, _, _, err := st.FailAttempt(task.ID, started.Attempt.ID, started.Lease.ID, "worker failed"); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	failedTask, _ := st.GetTask(task.ID)
	if failedTask.Status != models.TaskStatusFailed || failedTask.LeaseID != "" {
		t.Fatalf("unexpected failed task: %+v", failedTask)
	}
	if active := st.ActiveLeaseCountByExecutor()[models.LocalExecutorID]; active != 0 {
		t.Fatalf("expected failed attempt to release lease, active count=%d", active)
	}
	if _, _, _, err := st.FailAttempt(task.ID, started.Attempt.ID, started.Lease.ID, "worker failed again"); err != nil {
		t.Fatalf("failed attempt transition should be idempotent: %v", err)
	}

	_, needsFix, requested, err := st.RequestFix(task.ID)
	if err != nil {
		t.Fatalf("request fix: %v", err)
	}
	if needsFix.Status != models.TaskStatusNeedsFix || requested.Status != models.AttemptStatusRequested || requested.Kind != models.AttemptKindFix {
		t.Fatalf("unexpected fix transition task=%+v attempt=%+v", needsFix, requested)
	}
	_, queuedTask, queued, err := st.QueueAttempt(task.ID)
	if err != nil {
		t.Fatalf("queue fix attempt: %v", err)
	}
	if queuedTask.Status != models.TaskStatusQueued || queued.ID != requested.ID || queued.Status != models.AttemptStatusQueued {
		t.Fatalf("unexpected queued fix transition task=%+v attempt=%+v", queuedTask, queued)
	}
	second, err := st.BeginAttempt(task.ID)
	if err != nil {
		t.Fatalf("begin fix attempt: %v", err)
	}
	if second.Attempt.Number != 2 || second.Attempt.Kind != models.AttemptKindFix || second.Attempt.Status != models.AttemptStatusRunning {
		t.Fatalf("unexpected second attempt: %+v", second.Attempt)
	}
}

func TestRequestFixNormalizesLegacyQueuedFixPlaceholder(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)
	started, err := st.BeginAttempt(task.ID)
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	if _, _, _, err := st.FailAttempt(task.ID, started.Attempt.ID, started.Lease.ID, "failed"); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	task, _ = st.GetTask(task.ID)
	task.Status = models.TaskStatusNeedsFix
	if err := st.UpdateTask(task); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateAttempt(task.ProjectID, task.RunID, task.ID, 2, models.AttemptKindFix, models.AttemptStatusQueued, "legacy queued placeholder"); err != nil {
		t.Fatal(err)
	}
	_, _, normalized, err := st.RequestFix(task.ID)
	if err != nil {
		t.Fatalf("request fix: %v", err)
	}
	if normalized.Status != models.AttemptStatusRequested || normalized.Summary != "Fix requested; queue the next attempt when ready." {
		t.Fatalf("legacy placeholder was not normalized: %+v", normalized)
	}
}

func TestRequestFixOnFailedTaskBumpsRetryLimit(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, _, task := testsupport.ProjectAndTask(t, st)
	task.MaxAttempts = 1
	if err := st.UpdateTask(task); err != nil {
		t.Fatal(err)
	}
	started, err := st.BeginAttempt(task.ID)
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	if _, _, _, err := st.FailAttempt(task.ID, started.Attempt.ID, started.Lease.ID, "failed"); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	_, needsFix, _, err := st.RequestFix(task.ID)
	if err != nil {
		t.Fatalf("request fix from failed task should bump retry limit: %v", err)
	}
	if needsFix.MaxAttempts != 2 {
		t.Fatalf("expected max attempts bumped to 2, got %d", needsFix.MaxAttempts)
	}
}

func TestEventSequencesPersistAndReplayInOrder(t *testing.T) {
	root := t.TempDir()
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	_, run, task := testsupport.ProjectAndTask(t, st)
	for _, summary := range []string{"first", "second", "third"} {
		if _, err := st.AppendEvent(models.Event{RunID: run.ID, TaskID: task.ID, Type: models.EventTaskStateChanged, ActorKind: models.ActorKindManager, ActorID: models.ActorKindManager, Summary: summary}); err != nil {
			t.Fatalf("append event: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	events := reopened.EventsForRunAfter(run.ID, 1)
	if len(events) != 2 || events[0].Summary != "second" || events[1].Summary != "third" {
		t.Fatalf("unexpected replay events: %+v", events)
	}
	if events[0].Sequence != 2 || events[1].Sequence != 3 {
		t.Fatalf("unexpected sequences: %+v", events)
	}
}
