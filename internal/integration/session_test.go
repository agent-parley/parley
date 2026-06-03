package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/runnerclient"
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
	case <-time.After(5 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}
