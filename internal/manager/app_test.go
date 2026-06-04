package manager

import (
	"context"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/runnerclient"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/protocol"
)

func TestHeartbeatRecoveredMarksRunnerConnectedAndEmitsReconnected(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if err := st.UpsertRunnerWithOrigin(ctx, "runner_a", store.RunnerStatusSuspect, store.RunnerOriginSpawned, protocol.Capabilities{}); err != nil {
		t.Fatalf("upsert runner: %v", err)
	}
	app := &App{store: st}
	app.handleHeartbeatRecovered(ctx, "runner_a")
	runner, err := st.GetRunner(ctx, "runner_a")
	if err != nil {
		t.Fatalf("get runner: %v", err)
	}
	if runner.Status != store.RunnerStatusConnected || runner.MissedHeartbeats != 0 {
		t.Fatalf("runner = %+v, want connected with zero misses", runner)
	}
	events, err := st.ListRunnerEvents(ctx, "runner_a")
	if err != nil {
		t.Fatalf("list runner events: %v", err)
	}
	if len(events) != 1 || events[0].Type != "runner.reconnected" {
		t.Fatalf("events = %#v, want runner.reconnected", events)
	}
}

func TestRunnerProxyWaitsForRestartClient(t *testing.T) {
	proxy := newRunnerProxy()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got := make(chan *runnerclient.Client, 1)
	errCh := make(chan error, 1)
	go func() {
		client, err := proxy.waitClient(ctx)
		if err != nil {
			errCh <- err
			return
		}
		got <- client
	}()
	select {
	case <-got:
		t.Fatal("waitClient returned before client was set")
	case <-time.After(25 * time.Millisecond):
	}
	client := &runnerclient.Client{}
	proxy.Set(client)
	select {
	case err := <-errCh:
		t.Fatalf("waitClient error: %v", err)
	case gotClient := <-got:
		if gotClient != client {
			t.Fatal("waitClient returned wrong client")
		}
	case <-ctx.Done():
		t.Fatal("waitClient did not unblock after Set")
	}
}

func TestCrashLoopGuardStopsRestartAfterCap(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	app := &App{store: st}
	for i := 0; i < spawnedRunnerRestartCap; i++ {
		if !app.allowRestart("runner_a") {
			t.Fatalf("allowRestart(%d) = false before cap", i)
		}
	}
	if app.allowRestart("runner_a") {
		t.Fatal("allowRestart after cap = true, want false")
	}
	events, err := st.ListRunnerEvents(ctx, "runner_a")
	if err != nil {
		t.Fatalf("list runner events: %v", err)
	}
	if len(events) != 1 || events[0].Type != "runner.down" || events[0].Data["reason"] != "crash_loop" {
		t.Fatalf("events = %#v, want crash-loop runner.down", events)
	}
}
