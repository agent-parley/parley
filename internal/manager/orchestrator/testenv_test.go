package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/event"
)

// testWaitTimeout is a generous safety net, not a tuned deadline: waits are driven by
// engine broadcasts (see eventRecorder), so they complete the instant the awaited state
// is reached. The timeout only fires if the engine never makes progress, which is a real
// failure worth surfacing rather than a load-sensitive flake.
const testWaitTimeout = 30 * time.Second

// eventRecorder is the Broadcaster used in tests. It records every broadcast so tests can
// assert on emitted events, and signals waiters so status waits are event-driven instead
// of time-sliced polling. The engine calls Broadcast after every persisted event, so any
// state change a test waits on is always accompanied by a wakeup.
type eventRecorder struct {
	mu     sync.Mutex
	events []event.Event
	notify chan struct{}
}

func newEventRecorder() *eventRecorder {
	return &eventRecorder{notify: make(chan struct{}, 1)}
}

func (r *eventRecorder) Broadcast(_ string, ev event.Event, _ string) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

// snapshot returns a copy of the broadcast events recorded so far.
func (r *eventRecorder) snapshot() []event.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]event.Event, len(r.events))
	copy(out, r.events)
	return out
}

// waitUntil blocks until pred is true, re-checking on every broadcast (the fast path) and
// on a coarse backstop ticker (insurance against any state change that skips a broadcast),
// failing the test after testWaitTimeout. pred reads the store, so it stays authoritative.
func (r *eventRecorder) waitUntil(t *testing.T, pred func() bool) {
	t.Helper()
	deadline := time.NewTimer(testWaitTimeout)
	defer deadline.Stop()
	backstop := time.NewTicker(250 * time.Millisecond)
	defer backstop.Stop()
	for {
		if pred() {
			return
		}
		select {
		case <-r.notify:
		case <-backstop.C:
		case <-deadline.C:
			if pred() {
				return
			}
			t.Fatalf("timed out after %s waiting for engine state", testWaitTimeout)
		}
	}
}

// testEnv bundles a store, engine, and event recorder with ordered teardown registered on
// t.Cleanup. Because t.Cleanup runs LIFO and t.TempDir registers its RemoveAll first, the
// teardown order is engine.Shutdown -> store.Close -> TempDir removal: no engine goroutine
// can touch a closed store or a deleted data dir. This is the structural fix for the
// recurring "leaked engine goroutines race t.TempDir cleanup" flake.
type testEnv struct {
	t        *testing.T
	ctx      context.Context
	store    *store.Store
	engine   *Engine
	recorder *eventRecorder
}

// testRecorders lets the (t, st)-signature wait helpers find the recorder for a store
// without threading it through every call site. Keyed by the unique per-test *store.Store.
var testRecorders sync.Map // *store.Store -> *eventRecorder

func newTestEnv(t *testing.T, runner Runner, opts EngineOptions) *testEnv {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	eng := newRecordingEngine(t, st, runner, opts)
	return &testEnv{t: t, ctx: ctx, store: st, engine: eng, recorder: recorderForStore(t, st)}
}

// newRecordingEngine builds an engine wired to an eventRecorder broadcaster and registers
// ordered teardown (engine.Shutdown then store.Close, both before any t.TempDir removal).
// It is the drop-in replacement for NewEngineWithOptions(st, runner, fakeFragmentRenderer{},
// fakeBroadcaster{}, opts) in tests; drop the test's own `defer st.Close()` when using it.
func newRecordingEngine(t *testing.T, st *store.Store, runner Runner, opts EngineOptions) *Engine {
	t.Helper()
	if opts.DataRoot == "" {
		opts.DataRoot = t.TempDir()
	}
	rec := newEventRecorder()
	eng := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, rec, opts)
	testRecorders.Store(st, rec)
	t.Cleanup(func() { testRecorders.Delete(st) })
	registerEngineTeardown(t, eng, st)
	return eng
}

// registerEngineTeardown registers ordered teardown so engine.Shutdown drains goroutines
// before store.Close, both before any t.TempDir removal (t.Cleanup is LIFO). Use it for an
// engine built with a custom broadcaster where newRecordingEngine's recorder isn't wanted.
func registerEngineTeardown(t *testing.T, eng *Engine, st *store.Store) {
	t.Helper()
	t.Cleanup(func() { st.Close() })
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
		defer cancel()
		if err := eng.Shutdown(shutdownCtx); err != nil {
			t.Errorf("engine shutdown: %v", err)
		}
	})
}

func lookupRecorder(st *store.Store) (*eventRecorder, bool) {
	if v, ok := testRecorders.Load(st); ok {
		return v.(*eventRecorder), true
	}
	return nil, false
}

func recorderForStore(t *testing.T, st *store.Store) *eventRecorder {
	t.Helper()
	if rec, ok := lookupRecorder(st); ok {
		return rec
	}
	t.Fatalf("no event recorder registered for store; build the engine via newRecordingEngine/newTestEnv")
	return nil
}

func waitForRunStatus(t *testing.T, st *store.Store, runID, want string) {
	t.Helper()
	recorderForStore(t, st).waitUntil(t, func() bool {
		run, err := st.GetRun(context.Background(), runID)
		return err == nil && run.Status == want
	})
}

func freezeRunWorkflowSnapshot(t *testing.T, engine *Engine, st *store.Store, runID string) {
	t.Helper()
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingWorkflowAdjustment)
	if err := engine.FreezeRunWorkflowSnapshot(context.Background(), runID, event.Actor{Kind: event.ActorKindOperator, ID: "test"}); err != nil {
		t.Fatalf("FreezeRunWorkflowSnapshot() error = %v", err)
	}
}

func waitForNotRunStatus(t *testing.T, st *store.Store, runID, status string) {
	t.Helper()
	recorderForStore(t, st).waitUntil(t, func() bool {
		run, err := st.GetRun(context.Background(), runID)
		return err == nil && run.Status != status
	})
}

func waitForEventType(t *testing.T, st *store.Store, runID, typ string) {
	t.Helper()
	recorderForStore(t, st).waitUntil(t, func() bool {
		events, err := st.ListEvents(context.Background(), runID)
		return err == nil && hasEventType(events, typ)
	})
}

func waitForWorkflowStageAwaiting(t *testing.T, st *store.Store, runID, workflowStageID string) {
	t.Helper()
	recorderForStore(t, st).waitUntil(t, func() bool {
		run, err := st.GetRun(context.Background(), runID)
		if err != nil || run.Status != store.RunStatusAwaitingHuman {
			return false
		}
		stage := stageByWorkflowID(t, st, runID, workflowStageID)
		return stage.Status == store.StageStatusRunning
	})
}
