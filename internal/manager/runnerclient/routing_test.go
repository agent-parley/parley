package runnerclient

import (
	"context"
	"testing"

	"github.com/agent-parley/parley/internal/shared/protocol"
)

func TestHandleResultRoutesByRunTaskAttempt(t *testing.T) {
	first := &dispatchWaiter{resultCh: make(chan protocol.ResultPayload, 1)}
	second := &dispatchWaiter{resultCh: make(chan protocol.ResultPayload, 1)}
	client := &Client{resultWaiters: map[string]*dispatchWaiter{
		resultWaiterKey("run_a", "task_a", "attempt_a"): first,
		resultWaiterKey("run_b", "task_b", "attempt_b"): second,
	}}
	msg := protocol.MustMessage(protocol.TypeResult, protocol.ResultPayload{RunID: "run_b", TaskID: "task_b", AttemptID: "attempt_b", TerminalStatus: "completed"})
	if err := client.handleResult(context.Background(), msg); err != nil {
		t.Fatalf("handle result: %v", err)
	}
	select {
	case <-first.resultCh:
		t.Fatal("result routed to wrong waiter")
	default:
	}
	select {
	case got := <-second.resultCh:
		if got.RunID != "run_b" {
			t.Fatalf("unexpected result: %+v", got)
		}
	default:
		t.Fatal("result was not routed to matching waiter")
	}
}
