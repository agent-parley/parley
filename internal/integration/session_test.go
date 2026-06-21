package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/runnerclient"
	"github.com/agent-parley/parley/internal/runner/runnerio"
	runnersession "github.com/agent-parley/parley/internal/runner/session"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/ids"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestLocalhostHandshakePingAndCancelMessage(t *testing.T) {
	serverCtx, stop := context.WithCancel(context.Background())
	defer stop()
	srv, url, err := runnersession.Listen()
	if err != nil {
		t.Fatalf("listen runner: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(serverCtx) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := runnerclient.Dial(ctx, url, ids.New("runner"))
	if err != nil {
		t.Fatalf("dial runner: %v", err)
	}
	if got := client.Ready(); got.RunnerID == "" || len(got.Capabilities.Adapters) != 1 || got.Capabilities.Adapters[0] != "noop" {
		t.Fatalf("unexpected ready: %+v", got)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	disp := contract.Dispatch{RunID: ids.New("run"), TaskID: ids.New("task"), AttemptID: ids.New("attempt"), StageID: ids.New("stage"), StageType: contract.StageTypeImplementation, Adapter: "noop", Input: map[string]any{"idea": "cancel test", "sleep_ms": 500}}
	reportCh := make(chan report.Report, 1)
	errCh := make(chan error, 1)
	go func() {
		rep, err := client.Dispatch(ctx, disp)
		reportCh <- rep
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := client.Cancel(ctx, disp.RunID, disp.TaskID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case rep := <-reportCh:
		if err := <-errCh; err != nil {
			t.Fatalf("dispatch returned error: %v", err)
		}
		if rep.Status != report.StatusFailed {
			t.Fatalf("cancel did not produce failed report: %+v", rep)
		}
	case <-ctx.Done():
		t.Fatal("dispatch timeout")
	}
	_ = client.Close(context.Background())
	stop()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func TestRunnerReadyIncludesAdditionalSessionAdapters(t *testing.T) {
	serverCtx, stop := context.WithCancel(context.Background())
	defer stop()
	srv, url, err := runnersession.Listen(runnersession.WithAdapters(readyOnlyAdapter{}))
	if err != nil {
		t.Fatalf("listen runner: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(serverCtx) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := runnerclient.Dial(ctx, url, ids.New("runner"))
	if err != nil {
		t.Fatalf("dial runner: %v", err)
	}
	ready := client.Ready()
	if !hasAdapter(ready.Capabilities.Adapters, "noop") || !hasAdapter(ready.Capabilities.Adapters, "ready_only") {
		t.Fatalf("ready adapters = %+v, want noop and ready_only", ready.Capabilities.Adapters)
	}

	_ = client.Close(context.Background())
	stop()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

type readyOnlyAdapter struct{}

func (readyOnlyAdapter) Name() string { return "ready_only" }

func (readyOnlyAdapter) Run(context.Context, contract.Dispatch, runnerio.Sink) (report.Report, error) {
	return report.Report{}, nil
}

func hasAdapter(adapters []string, want string) bool {
	for _, adapter := range adapters {
		if adapter == want {
			return true
		}
	}
	return false
}

func TestCancelAttemptDoesNotCancelSiblingAttemptForSameTask(t *testing.T) {
	serverCtx, stop := context.WithCancel(context.Background())
	defer stop()
	srv, url, err := runnersession.Listen()
	if err != nil {
		t.Fatalf("listen runner: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(serverCtx) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := runnerclient.Dial(ctx, url, ids.New("runner"))
	if err != nil {
		t.Fatalf("dial runner: %v", err)
	}

	runID := ids.New("run")
	taskID := ids.New("task")
	disp1 := contract.Dispatch{RunID: runID, TaskID: taskID, AttemptID: ids.New("attempt"), StageID: ids.New("stage"), StageType: contract.StageTypeImplementation, Adapter: "noop", Input: map[string]any{"idea": "cancel first", "sleep_ms": 500}}
	disp2 := contract.Dispatch{RunID: runID, TaskID: taskID, AttemptID: ids.New("attempt"), StageID: ids.New("stage"), StageType: contract.StageTypeImplementation, Adapter: "noop", Input: map[string]any{"idea": "let second finish", "sleep_ms": 120}}

	reportCh1 := make(chan report.Report, 1)
	errCh1 := make(chan error, 1)
	go func() {
		rep, err := client.Dispatch(ctx, disp1)
		reportCh1 <- rep
		errCh1 <- err
	}()
	reportCh2 := make(chan report.Report, 1)
	errCh2 := make(chan error, 1)
	go func() {
		rep, err := client.Dispatch(ctx, disp2)
		reportCh2 <- rep
		errCh2 <- err
	}()

	time.Sleep(50 * time.Millisecond)
	if err := client.CancelAttempt(ctx, disp1.RunID, disp1.TaskID, disp1.AttemptID); err != nil {
		t.Fatalf("cancel attempt: %v", err)
	}

	var rep1, rep2 report.Report
	select {
	case rep1 = <-reportCh1:
		if err := <-errCh1; err != nil {
			t.Fatalf("first dispatch returned error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("first dispatch timeout")
	}
	select {
	case rep2 = <-reportCh2:
		if err := <-errCh2; err != nil {
			t.Fatalf("second dispatch returned error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("second dispatch timeout")
	}
	if rep1.Status != report.StatusFailed {
		t.Fatalf("first attempt status = %s, want failed", rep1.Status)
	}
	if rep2.Status != report.StatusCompleted {
		t.Fatalf("second attempt status = %s, want completed", rep2.Status)
	}

	_ = client.Close(context.Background())
	stop()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}
