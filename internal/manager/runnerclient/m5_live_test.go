//go:build integration

package runnerclient

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestM5LiveRunnerKillReportsDown(t *testing.T) {
	if os.Getenv("PARLEY_M5_LIVE") != "1" {
		t.Skip("set PARLEY_M5_LIVE=1 and PARLEY_RUNNER_BIN to run guarded host runner-kill test")
	}
	runnerBin := os.Getenv("PARLEY_RUNNER_BIN")
	if runnerBin == "" {
		t.Skip("PARLEY_RUNNER_BIN not set")
	}
	if _, err := exec.LookPath(runnerBin); err != nil {
		if _, statErr := os.Stat(runnerBin); statErr != nil {
			t.Skipf("runner binary not available: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := StartChildWithEnv(ctx, runnerBin, []string{"PARLEY_ADAPTER=noop"})
	if err != nil {
		t.Fatalf("start child: %v", err)
	}
	down := make(chan string, 1)
	client.SetLifecycleHandlers(nil, nil, func(_ context.Context, _ string, reason string, _ error) {
		down <- reason
	})
	if client.cmd == nil || client.cmd.Process == nil {
		t.Fatal("child process unavailable")
	}
	if err := client.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	select {
	case reason := <-down:
		if reason != "process_exit" && reason != "session_done" {
			t.Fatalf("down reason = %s", reason)
		}
	case <-ctx.Done():
		t.Fatal("runner kill did not surface down")
	}
}
