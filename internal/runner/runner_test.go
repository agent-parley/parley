package runner

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/protocol"
	"github.com/agent-parley/parley/internal/shared/report"
)

type runnerHarness struct {
	ctx     context.Context
	cancel  context.CancelFunc
	server  *httptest.Server
	manager *protocol.Session
	runner  *Runner
	reports chan report.Report
	results chan protocol.ResultPayload
}

func newRunnerHarness(t *testing.T, adapters ...*fakeAdapter) *runnerHarness {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	runnerCh := make(chan *Runner, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := websocket.Accept(w, req, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		sess := protocol.NewSession(conn)
		r := New(sess)
		for _, adapter := range adapters {
			r.Register(adapter)
		}
		runnerCh <- r
		sess.Start(context.Background())
		<-sess.Done()
	}))

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		cancel()
		server.Close()
		t.Fatalf("dial runner session: %v", err)
	}
	manager := protocol.NewSession(conn)
	reports := make(chan report.Report, 32)
	results := make(chan protocol.ResultPayload, 32)
	manager.Handle(protocol.TypeReport, func(_ context.Context, msg protocol.Message) error {
		rep, err := protocol.DecodePayload[report.Report](msg)
		if err != nil {
			return err
		}
		reports <- rep
		return nil
	})
	manager.Handle(protocol.TypeResult, func(_ context.Context, msg protocol.Message) error {
		result, err := protocol.DecodePayload[protocol.ResultPayload](msg)
		if err != nil {
			return err
		}
		results <- result
		return nil
	})
	manager.Start(context.Background())

	var r *Runner
	select {
	case r = <-runnerCh:
	case <-ctx.Done():
		cancel()
		_ = manager.Close(websocket.StatusNormalClosure, "test setup timeout")
		server.Close()
		t.Fatal("runner session was not accepted")
	}

	h := &runnerHarness{
		ctx:     ctx,
		cancel:  cancel,
		server:  server,
		manager: manager,
		runner:  r,
		reports: reports,
		results: results,
	}
	t.Cleanup(func() {
		cancel()
		_ = manager.Close(websocket.StatusNormalClosure, "test done")
		server.Close()
	})
	return h
}

func (h *runnerHarness) handleDispatch(t *testing.T, disp contract.Dispatch) {
	t.Helper()
	if err := h.runner.handleDispatch(h.ctx, protocol.MustMessage(protocol.TypeDispatch, disp)); err != nil {
		t.Fatalf("handle dispatch: %v", err)
	}
}

func (h *runnerHarness) handleEvictWarmSession(t *testing.T, key string) {
	t.Helper()
	msg := protocol.MustMessage(protocol.TypeEvictWarmSession, protocol.EvictWarmSessionPayload{WarmSessionKey: key})
	if err := h.runner.handleEvictWarmSession(h.ctx, msg); err != nil {
		t.Fatalf("handle evict warm session: %v", err)
	}
}

func (h *runnerHarness) handleCancel(t *testing.T, payload protocol.CancelPayload) {
	t.Helper()
	if err := h.runner.handleCancel(h.ctx, protocol.MustMessage(protocol.TypeCancel, payload)); err != nil {
		t.Fatalf("handle cancel: %v", err)
	}
}

func (h *runnerHarness) nextReport(t *testing.T) report.Report {
	t.Helper()
	select {
	case rep := <-h.reports:
		return rep
	case <-h.ctx.Done():
		t.Fatal("timed out waiting for report")
		return report.Report{}
	}
}

func (h *runnerHarness) nextResult(t *testing.T) protocol.ResultPayload {
	t.Helper()
	select {
	case result := <-h.results:
		return result
	case <-h.ctx.Done():
		t.Fatal("timed out waiting for result")
		return protocol.ResultPayload{}
	}
}

func (h *runnerHarness) waitForResults(t *testing.T, count int) {
	t.Helper()
	for range count {
		_ = h.nextResult(t)
	}
}

type fakeAdapter struct {
	name string
	run  func(context.Context, contract.Dispatch, runnerio.Sink) (report.Report, error)

	mu           sync.Mutex
	evicted      []string
	evictStarted chan string
	releaseEvict chan struct{}
}

func (f *fakeAdapter) Name() string {
	if f.name == "" {
		return "fake"
	}
	return f.name
}

func (f *fakeAdapter) Run(ctx context.Context, disp contract.Dispatch, sink runnerio.Sink) (report.Report, error) {
	if f.run != nil {
		return f.run(ctx, disp, sink)
	}
	return validReport(disp, f.Name()), nil
}

func (f *fakeAdapter) EvictWarmSession(ctx context.Context, key string) error {
	f.mu.Lock()
	f.evicted = append(f.evicted, key)
	f.mu.Unlock()
	if f.evictStarted != nil {
		f.evictStarted <- key
	}
	if f.releaseEvict != nil {
		select {
		case <-f.releaseEvict:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (f *fakeAdapter) evictCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.evicted)
}

func (f *fakeAdapter) evictedKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.evicted...)
}

func TestHandleDispatchWrapsInvalidAdapterReportAsHarnessInvalid(t *testing.T) {
	adapter := &fakeAdapter{name: "fake"}
	adapter.run = func(_ context.Context, disp contract.Dispatch, _ runnerio.Sink) (report.Report, error) {
		rep := validReport(disp, adapter.Name())
		rep.Summary = ""
		rep.Payload = map[string]any{"original": "payload"}
		return rep, nil
	}
	h := newRunnerHarness(t, adapter)

	disp := testDispatch("run_invalid", "task_1", "attempt_1", adapter.Name())
	h.handleDispatch(t, disp)

	rep := h.nextReport(t)
	result := h.nextResult(t)

	if rep.Status != report.StatusInvalid {
		t.Fatalf("report status = %q, want %q", rep.Status, report.StatusInvalid)
	}
	if rep.Actor.Kind != report.ActorKindHarness || rep.Actor.ID != "runner" {
		t.Fatalf("report actor = %+v, want harness runner", rep.Actor)
	}
	if result.TerminalStatus != "failed" {
		t.Fatalf("terminal status = %q, want failed", result.TerminalStatus)
	}
	invalid, ok := rep.Payload["invalid_report"].(map[string]any)
	if !ok {
		t.Fatalf("payload.invalid_report = %#v, want object", rep.Payload["invalid_report"])
	}
	if invalid["status"] != report.StatusCompleted {
		t.Fatalf("invalid_report.status = %#v, want %q", invalid["status"], report.StatusCompleted)
	}
	if invalid["summary"] != "" {
		t.Fatalf("invalid_report.summary = %#v, want empty original summary", invalid["summary"])
	}
	actor, ok := invalid["actor"].(map[string]any)
	if !ok {
		t.Fatalf("invalid_report.actor = %#v, want object", invalid["actor"])
	}
	if actor["kind"] != report.ActorKindAgent || actor["id"] != adapter.Name() {
		t.Fatalf("invalid_report.actor = %#v, want original agent actor", actor)
	}
	payload, ok := invalid["payload"].(map[string]any)
	if !ok || payload["original"] != "payload" {
		t.Fatalf("invalid_report.payload = %#v, want original payload", invalid["payload"])
	}
	if len(rep.Errors) == 0 || !strings.Contains(rep.Errors[0], "summary is required") {
		t.Fatalf("errors = %#v, want validation error for missing summary", rep.Errors)
	}
}

func TestHandleDispatchEmitsFailedReportForAdapterRunError(t *testing.T) {
	adapter := &fakeAdapter{name: "fake"}
	adapter.run = func(context.Context, contract.Dispatch, runnerio.Sink) (report.Report, error) {
		return report.Report{}, errors.New("adapter exploded")
	}
	h := newRunnerHarness(t, adapter)

	disp := testDispatch("run_error", "task_1", "attempt_1", adapter.Name())
	h.handleDispatch(t, disp)

	rep := h.nextReport(t)
	result := h.nextResult(t)

	if rep.Status != report.StatusFailed {
		t.Fatalf("report status = %q, want %q", rep.Status, report.StatusFailed)
	}
	if rep.Actor.Kind != report.ActorKindAgent || rep.Actor.ID != adapter.Name() {
		t.Fatalf("report actor = %+v, want adapter actor", rep.Actor)
	}
	if rep.Summary != "adapter failed" {
		t.Fatalf("summary = %q, want adapter failed", rep.Summary)
	}
	if len(rep.Errors) != 1 || rep.Errors[0] != "adapter exploded" {
		t.Fatalf("errors = %#v, want adapter error", rep.Errors)
	}
	if result.TerminalStatus != "failed" {
		t.Fatalf("terminal status = %q, want failed", result.TerminalStatus)
	}
}

func TestHandleDispatchEmitsFailedReportForUnknownAdapter(t *testing.T) {
	h := newRunnerHarness(t)

	disp := testDispatch("run_unknown", "task_1", "attempt_1", "missing")
	h.handleDispatch(t, disp)

	rep := h.nextReport(t)
	result := h.nextResult(t)

	if rep.Status != report.StatusFailed {
		t.Fatalf("report status = %q, want %q", rep.Status, report.StatusFailed)
	}
	if rep.Actor.Kind != report.ActorKindAgent || rep.Actor.ID != "missing" {
		t.Fatalf("report actor = %+v, want missing adapter actor", rep.Actor)
	}
	if len(rep.Errors) != 1 || !strings.Contains(rep.Errors[0], `adapter "missing" not registered`) {
		t.Fatalf("errors = %#v, want not-registered error", rep.Errors)
	}
	if result.TerminalStatus != "failed" {
		t.Fatalf("terminal status = %q, want failed", result.TerminalStatus)
	}
}

func TestHandleEvictWarmSessionNoopsWhileActiveThenEvictsIdleOnce(t *testing.T) {
	started := make(chan contract.Dispatch, 1)
	release := make(chan struct{})
	adapter := &fakeAdapter{name: "warm"}
	adapter.run = func(ctx context.Context, disp contract.Dispatch, _ runnerio.Sink) (report.Report, error) {
		started <- disp
		select {
		case <-release:
			return validReport(disp, adapter.Name()), nil
		case <-ctx.Done():
			return report.Report{}, ctx.Err()
		}
	}
	h := newRunnerHarness(t, adapter)

	const warmKey = "conversation-1"
	disp := testDispatch("run_warm", "task_1", "attempt_1", adapter.Name())
	disp.WarmSessionKey = warmKey
	h.handleDispatch(t, disp)
	waitStarted(t, h.ctx, started)

	h.handleEvictWarmSession(t, warmKey)
	if got := adapter.evictCount(); got != 0 {
		t.Fatalf("evict count while dispatch active = %d, want 0", got)
	}

	close(release)
	_ = h.nextReport(t)
	_ = h.nextResult(t)

	h.handleEvictWarmSession(t, warmKey)
	if got := adapter.evictCount(); got != 1 {
		t.Fatalf("evict count after idle eviction = %d, want 1", got)
	}
	if keys := adapter.evictedKeys(); len(keys) != 1 || keys[0] != warmKey {
		t.Fatalf("evicted keys = %#v, want [%q]", keys, warmKey)
	}
	assertNoWarmSession(t, h.runner, warmKey)
	assertNoWarmSessionLock(t, h.runner, warmKey)

	h.handleEvictWarmSession(t, warmKey)
	if got := adapter.evictCount(); got != 1 {
		t.Fatalf("evict count after repeated idle eviction = %d, want still 1", got)
	}
}

func TestWarmSessionMapsReturnToBaselineAfterChurn(t *testing.T) {
	adapter := &fakeAdapter{name: "warm"}
	h := newRunnerHarness(t, adapter)

	const count = 25
	for i := range count {
		warmKey := fmt.Sprintf("conversation-%d", i)
		disp := testDispatch("run_churn", "task_1", fmt.Sprintf("attempt_%d", i), adapter.Name())
		disp.WarmSessionKey = warmKey
		h.handleDispatch(t, disp)
		_ = h.nextReport(t)
		_ = h.nextResult(t)

		h.handleEvictWarmSession(t, warmKey)
		assertNoWarmSession(t, h.runner, warmKey)
		assertNoWarmSessionLock(t, h.runner, warmKey)
	}
	if got := adapter.evictCount(); got != count {
		t.Fatalf("evict count after churn = %d, want %d", got, count)
	}
	assertNoWarmState(t, h.runner)
}

func TestHandleEvictWarmSessionSerializesWithRacingDispatch(t *testing.T) {
	evictStarted := make(chan string, 1)
	releaseEvict := make(chan struct{})
	runStarted := make(chan contract.Dispatch, 1)
	releaseRun := make(chan struct{})
	adapter := &fakeAdapter{name: "warm", evictStarted: evictStarted, releaseEvict: releaseEvict}
	adapter.run = func(ctx context.Context, disp contract.Dispatch, _ runnerio.Sink) (report.Report, error) {
		runStarted <- disp
		select {
		case <-releaseRun:
			return validReport(disp, adapter.Name()), nil
		case <-ctx.Done():
			return report.Report{}, ctx.Err()
		}
	}
	h := newRunnerHarness(t, adapter)

	const warmKey = "conversation-race"
	h.runner.mu.Lock()
	h.runner.warm[warmKey] = &runnerWarmSession{adapter: adapter.Name()}
	h.runner.mu.Unlock()

	evictDone := make(chan error, 1)
	go func() {
		evictDone <- h.runner.handleEvictWarmSession(h.ctx, protocol.MustMessage(protocol.TypeEvictWarmSession, protocol.EvictWarmSessionPayload{WarmSessionKey: warmKey}))
	}()
	select {
	case key := <-evictStarted:
		if key != warmKey {
			t.Fatalf("evicted key = %q, want %q", key, warmKey)
		}
	case <-h.ctx.Done():
		t.Fatal("timed out waiting for eviction to start")
	}

	disp := testDispatch("run_race", "task_1", "attempt_1", adapter.Name())
	disp.WarmSessionKey = warmKey
	dispatchDone := make(chan error, 1)
	go func() {
		dispatchDone <- h.runner.handleDispatch(h.ctx, protocol.MustMessage(protocol.TypeDispatch, disp))
	}()

	select {
	case <-runStarted:
		t.Fatal("dispatch started before idle eviction released the warm-session lock")
	case err := <-dispatchDone:
		t.Fatalf("dispatch returned before idle eviction released the warm-session lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseEvict)
	select {
	case err := <-evictDone:
		if err != nil {
			t.Fatalf("evict warm session: %v", err)
		}
	case <-h.ctx.Done():
		t.Fatal("timed out waiting for eviction to finish")
	}
	select {
	case err := <-dispatchDone:
		if err != nil {
			t.Fatalf("dispatch after eviction release: %v", err)
		}
	case <-h.ctx.Done():
		t.Fatal("timed out waiting for dispatch to return")
	}
	waitStarted(t, h.ctx, runStarted)
	close(releaseRun)
	_ = h.nextReport(t)
	_ = h.nextResult(t)
	if got := adapter.evictCount(); got != 1 {
		t.Fatalf("evict count = %d, want 1", got)
	}

	h.handleEvictWarmSession(t, warmKey)
	if got := adapter.evictCount(); got != 2 {
		t.Fatalf("evict count after reaped warm session = %d, want 2", got)
	}
	assertNoWarmSession(t, h.runner, warmKey)
	assertNoWarmSessionLock(t, h.runner, warmKey)
}

func TestHandleCancelRoutesByAttemptAndTaskPrefix(t *testing.T) {
	t.Run("exact attempt cancels only that attempt", func(t *testing.T) {
		started := make(chan string, 3)
		canceled := make(chan string, 3)
		release := make(chan struct{})
		adapter := blockingAdapter("fake", started, canceled, release)
		h := newRunnerHarness(t, adapter)

		for _, disp := range []contract.Dispatch{
			testDispatch("run_cancel", "task_1", "attempt_1", adapter.Name()),
			testDispatch("run_cancel", "task_1", "attempt_2", adapter.Name()),
			testDispatch("run_cancel", "task_2", "attempt_3", adapter.Name()),
		} {
			h.handleDispatch(t, disp)
		}
		waitStartedKeys(t, h.ctx, started, []string{
			activeKey("run_cancel", "task_1", "attempt_1"),
			activeKey("run_cancel", "task_1", "attempt_2"),
			activeKey("run_cancel", "task_2", "attempt_3"),
		})

		h.handleCancel(t, protocol.CancelPayload{RunID: "run_cancel", TaskID: "task_1", AttemptID: "attempt_2"})
		waitCanceledKeys(t, h.ctx, canceled, []string{activeKey("run_cancel", "task_1", "attempt_2")})
		assertNoCanceled(t, canceled)

		close(release)
		h.waitForResults(t, 3)
	})

	t.Run("empty attempt cancels all attempts under run task prefix", func(t *testing.T) {
		started := make(chan string, 4)
		canceled := make(chan string, 4)
		release := make(chan struct{})
		adapter := blockingAdapter("fake", started, canceled, release)
		h := newRunnerHarness(t, adapter)

		for _, disp := range []contract.Dispatch{
			testDispatch("run_prefix", "task_1", "attempt_1", adapter.Name()),
			testDispatch("run_prefix", "task_1", "attempt_2", adapter.Name()),
			testDispatch("run_prefix", "task_2", "attempt_3", adapter.Name()),
			testDispatch("run_other", "task_1", "attempt_4", adapter.Name()),
		} {
			h.handleDispatch(t, disp)
		}
		waitStartedKeys(t, h.ctx, started, []string{
			activeKey("run_prefix", "task_1", "attempt_1"),
			activeKey("run_prefix", "task_1", "attempt_2"),
			activeKey("run_prefix", "task_2", "attempt_3"),
			activeKey("run_other", "task_1", "attempt_4"),
		})

		h.handleCancel(t, protocol.CancelPayload{RunID: "run_prefix", TaskID: "task_1"})
		waitCanceledKeys(t, h.ctx, canceled, []string{
			activeKey("run_prefix", "task_1", "attempt_1"),
			activeKey("run_prefix", "task_1", "attempt_2"),
		})
		assertNoCanceled(t, canceled)

		close(release)
		h.waitForResults(t, 4)
	})
}

func TestSameWarmSessionKeyDispatchesSerialize(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan string, 2)
	adapter := &fakeAdapter{name: "warm"}
	adapter.run = func(ctx context.Context, disp contract.Dispatch, _ runnerio.Sink) (report.Report, error) {
		started <- disp.AttemptID
		select {
		case attempt := <-release:
			if attempt != disp.AttemptID {
				return report.Report{}, errors.New("released wrong attempt")
			}
			return validReport(disp, adapter.Name()), nil
		case <-ctx.Done():
			return report.Report{}, ctx.Err()
		}
	}
	h := newRunnerHarness(t, adapter)

	first := testDispatch("run_serial", "task_1", "attempt_1", adapter.Name())
	first.WarmSessionKey = "conversation-serial"
	second := testDispatch("run_serial", "task_1", "attempt_2", adapter.Name())
	second.WarmSessionKey = first.WarmSessionKey

	h.handleDispatch(t, first)
	waitStartedAttempt(t, h.ctx, started, "attempt_1")

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- h.runner.handleDispatch(h.ctx, protocol.MustMessage(protocol.TypeDispatch, second))
	}()

	select {
	case attempt := <-started:
		t.Fatalf("adapter started %s before first same-key dispatch completed", attempt)
	case err := <-secondDone:
		t.Fatalf("second dispatch returned before first same-key dispatch completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	release <- "attempt_1"
	_ = h.nextReport(t)
	_ = h.nextResult(t)

	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second dispatch after first completed: %v", err)
		}
	case <-h.ctx.Done():
		t.Fatal("timed out waiting for second dispatch to return")
	}
	waitStartedAttempt(t, h.ctx, started, "attempt_2")
	release <- "attempt_2"
	_ = h.nextReport(t)
	_ = h.nextResult(t)
}

func blockingAdapter(name string, started, canceled chan<- string, release <-chan struct{}) *fakeAdapter {
	adapter := &fakeAdapter{name: name}
	adapter.run = func(ctx context.Context, disp contract.Dispatch, _ runnerio.Sink) (report.Report, error) {
		key := activeKey(disp.RunID, disp.TaskID, disp.AttemptID)
		started <- key
		select {
		case <-ctx.Done():
			canceled <- key
			return report.Report{}, ctx.Err()
		case <-release:
			return validReport(disp, adapter.Name()), nil
		}
	}
	return adapter
}

func testDispatch(runID, taskID, attemptID, adapter string) contract.Dispatch {
	return contract.Dispatch{
		ProjectID: "project_1",
		RunID:     runID,
		TaskID:    taskID,
		AttemptID: attemptID,
		StageID:   "stage_1",
		StageType: contract.StageTypeImplementation,
		Adapter:   adapter,
		Input:     map[string]any{"prompt": "test"},
	}
}

func validReport(disp contract.Dispatch, adapter string) report.Report {
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: adapter},
		Status:        report.StatusCompleted,
		Summary:       "completed",
		Payload:       map[string]any{"ok": true},
	}
}

func waitStarted[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal("timed out waiting for adapter run to start")
		var zero T
		return zero
	}
}

func waitStartedAttempt(t *testing.T, ctx context.Context, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("started attempt = %q, want %q", got, want)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s to start", want)
	}
}

func waitStartedKeys(t *testing.T, ctx context.Context, ch <-chan string, want []string) {
	t.Helper()
	waitKeys(t, ctx, "started", ch, want)
}

func waitCanceledKeys(t *testing.T, ctx context.Context, ch <-chan string, want []string) {
	t.Helper()
	waitKeys(t, ctx, "canceled", ch, want)
}

func waitKeys(t *testing.T, ctx context.Context, label string, ch <-chan string, want []string) {
	t.Helper()
	remaining := map[string]bool{}
	for _, key := range want {
		remaining[key] = true
	}
	for len(remaining) > 0 {
		select {
		case got := <-ch:
			if !remaining[got] {
				t.Fatalf("unexpected %s key %q; remaining want %#v", label, got, remaining)
			}
			delete(remaining, got)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s keys; remaining %#v", label, remaining)
		}
	}
}

func assertNoCanceled(t *testing.T, ch <-chan string) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("unexpected canceled attempt %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func assertNoWarmSession(t *testing.T, r *Runner, key string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if session := r.warm[key]; session != nil {
		t.Fatalf("warm session %q still present: %+v", key, session)
	}
}

func assertNoWarmSessionLock(t *testing.T, r *Runner, key string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if lock := r.warmLocks[key]; lock != nil {
		t.Fatalf("warm session lock %q still present: %+v", key, lock)
	}
}

func assertNoWarmState(t *testing.T, r *Runner) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.warm) != 0 {
		t.Fatalf("warm sessions still present: %#v", r.warm)
	}
	if len(r.warmLocks) != 0 {
		t.Fatalf("warm session locks still present: %#v", r.warmLocks)
	}
}
