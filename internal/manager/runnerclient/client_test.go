package runnerclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/protocol"
	"github.com/agent-parley/parley/internal/shared/report"
)

type runnerClientHarness struct {
	ctx    context.Context
	client *Client
	server *httptest.Server
}

const runnerClientHarnessTimeout = 30 * time.Second

func newRunnerClientHarness(t *testing.T, configure func(*protocol.Session)) *runnerClientHarness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), runnerClientHarnessTimeout)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		sess := protocol.NewSession(conn)
		sess.Handle(protocol.TypeHello, func(ctx context.Context, msg protocol.Message) error {
			hello, err := protocol.DecodePayload[protocol.HelloPayload](msg)
			if err != nil {
				return err
			}
			return sess.Send(ctx, protocol.MustMessage(protocol.TypeReady, protocol.ReadyPayload{
				RunnerID:     hello.RunnerID,
				Capabilities: protocol.Capabilities{Adapters: []string{"noop"}},
			}))
		})
		sess.Handle(protocol.TypePing, func(ctx context.Context, _ protocol.Message) error {
			return sess.Send(ctx, protocol.MustMessage(protocol.TypePong, map[string]any{}))
		})
		if configure != nil {
			configure(sess)
		}
		sess.Start(context.Background())
		<-sess.Done()
	}))
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	client, err := Dial(ctx, url, "runner_unit")
	if err != nil {
		cancel()
		server.Close()
		t.Fatalf("dial runner client: %v", err)
	}
	h := &runnerClientHarness{ctx: ctx, client: client, server: server}
	t.Cleanup(func() {
		_ = client.Close(context.Background())
		server.Close()
		cancel()
	})
	return h
}

func TestDispatchRoundTripReturnsReportAndResult(t *testing.T) {
	disp := baseDispatchForClientTest()
	dispatchSeen := make(chan contract.Dispatch, 1)
	h := newRunnerClientHarness(t, func(sess *protocol.Session) {
		sess.Handle(protocol.TypeDispatch, func(ctx context.Context, msg protocol.Message) error {
			got, err := protocol.DecodePayload[contract.Dispatch](msg)
			if err != nil {
				return err
			}
			dispatchSeen <- got
			if err := sess.Send(ctx, protocol.MustMessage(protocol.TypeReport, baseReportForClientTest(got, report.StatusCompleted))); err != nil {
				return err
			}
			return sess.Send(ctx, protocol.MustMessage(protocol.TypeResult, protocol.ResultPayload{RunID: got.RunID, TaskID: got.TaskID, AttemptID: got.AttemptID, TerminalStatus: "completed"}))
		})
	})

	rep, err := h.client.Dispatch(h.ctx, disp)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if rep.StageID != disp.StageID || rep.Status != report.StatusCompleted {
		t.Fatalf("report = %+v, want completed report for stage %s", rep, disp.StageID)
	}
	select {
	case got := <-dispatchSeen:
		if got.RunID != disp.RunID || got.Input["idea"] != "unit dispatch" {
			t.Fatalf("dispatch payload = %+v, want original payload", got)
		}
	case <-h.ctx.Done():
		t.Fatal("server did not receive dispatch")
	}
	if h.client.RunnerID() != "runner_unit" || h.client.Ready().RunnerID != "runner_unit" {
		t.Fatalf("client ready/runner id = %+v/%q", h.client.Ready(), h.client.RunnerID())
	}
}

func TestDispatchReturnsErrDispatchFailedOnFailedTerminalResult(t *testing.T) {
	disp := baseDispatchForClientTest()
	h := newRunnerClientHarness(t, func(sess *protocol.Session) {
		sess.Handle(protocol.TypeDispatch, func(ctx context.Context, msg protocol.Message) error {
			got, err := protocol.DecodePayload[contract.Dispatch](msg)
			if err != nil {
				return err
			}
			if err := sess.Send(ctx, protocol.MustMessage(protocol.TypeReport, baseReportForClientTest(got, report.StatusCompleted))); err != nil {
				return err
			}
			return sess.Send(ctx, protocol.MustMessage(protocol.TypeResult, protocol.ResultPayload{RunID: got.RunID, TaskID: got.TaskID, AttemptID: got.AttemptID, TerminalStatus: "failed"}))
		})
	})

	rep, err := h.client.Dispatch(h.ctx, disp)
	if !errors.Is(err, ErrDispatchFailed) {
		t.Fatalf("dispatch error = %v, want ErrDispatchFailed", err)
	}
	if rep.Status != report.StatusCompleted {
		t.Fatalf("report status = %s, want completed report returned with failure", rep.Status)
	}
}

func TestDispatchContextCancelSendsCancelAttempt(t *testing.T) {
	disp := baseDispatchForClientTest()
	dispatchSeen := make(chan struct{}, 1)
	cancelSeen := make(chan protocol.CancelPayload, 1)
	h := newRunnerClientHarness(t, func(sess *protocol.Session) {
		sess.Handle(protocol.TypeDispatch, func(ctx context.Context, msg protocol.Message) error {
			if _, err := protocol.DecodePayload[contract.Dispatch](msg); err != nil {
				return err
			}
			dispatchSeen <- struct{}{}
			return nil
		})
		sess.Handle(protocol.TypeCancel, func(ctx context.Context, msg protocol.Message) error {
			payload, err := protocol.DecodePayload[protocol.CancelPayload](msg)
			if err != nil {
				return err
			}
			cancelSeen <- payload
			return nil
		})
	})

	dispatchCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := h.client.Dispatch(dispatchCtx, disp)
		errCh <- err
	}()
	select {
	case <-dispatchSeen:
	case <-h.ctx.Done():
		t.Fatal("server did not receive dispatch before timeout")
	}
	cancel()
	dispatchReturnDeadline := time.After(2 * time.Minute)
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("dispatch error = %v, want context.Canceled", err)
		}
	case <-dispatchReturnDeadline:
		t.Fatal("dispatch did not return after cancellation")
	}
	cancelSeenDeadline := time.After(2 * time.Minute)
	select {
	case got := <-cancelSeen:
		if got.RunID != disp.RunID || got.TaskID != disp.TaskID || got.AttemptID != disp.AttemptID {
			t.Fatalf("cancel payload = %+v, want dispatch attempt", got)
		}
	case <-cancelSeenDeadline:
		t.Fatal("server did not receive cancel attempt")
	}
}

func TestDispatchSessionDoneAfterContextCancelReturnsContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	err := (&Client{}).dispatchSessionDoneErr(ctx, func() { attempts++ })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dispatch session-done error = %v, want context.Canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("cancel attempts = %d, want 1", attempts)
	}
}

func TestClientPingCancelEvictAndClose(t *testing.T) {
	cancelSeen := make(chan protocol.CancelPayload, 1)
	evictSeen := make(chan protocol.EvictWarmSessionPayload, 1)
	h := newRunnerClientHarness(t, func(sess *protocol.Session) {
		sess.Handle(protocol.TypePing, func(ctx context.Context, _ protocol.Message) error {
			return sess.Send(ctx, protocol.MustMessage(protocol.TypePong, map[string]any{}))
		})
		sess.Handle(protocol.TypeCancel, func(ctx context.Context, msg protocol.Message) error {
			payload, err := protocol.DecodePayload[protocol.CancelPayload](msg)
			if err != nil {
				return err
			}
			cancelSeen <- payload
			return nil
		})
		sess.Handle(protocol.TypeEvictWarmSession, func(ctx context.Context, msg protocol.Message) error {
			payload, err := protocol.DecodePayload[protocol.EvictWarmSessionPayload](msg)
			if err != nil {
				return err
			}
			evictSeen <- payload
			return nil
		})
	})

	if err := h.client.Ping(h.ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := h.client.Cancel(h.ctx, "run_cancel", "task_cancel"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case got := <-cancelSeen:
		if got.AttemptID != "" || got.RunID != "run_cancel" || got.TaskID != "task_cancel" {
			t.Fatalf("cancel payload = %+v", got)
		}
	case <-h.ctx.Done():
		t.Fatal("server did not receive cancel")
	}
	if err := h.client.EvictWarmSession(h.ctx, ""); err != nil {
		t.Fatalf("empty evict: %v", err)
	}
	select {
	case got := <-evictSeen:
		t.Fatalf("empty evict sent payload %+v", got)
	default:
	}
	if err := h.client.EvictWarmSession(h.ctx, "warm-1"); err != nil {
		t.Fatalf("evict: %v", err)
	}
	select {
	case got := <-evictSeen:
		if got.WarmSessionKey != "warm-1" {
			t.Fatalf("evict payload = %+v", got)
		}
	case <-h.ctx.Done():
		t.Fatal("server did not receive evict")
	}
	if err := h.client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !h.client.isClosing() {
		t.Fatal("client is not marked closing after Close")
	}
}

func TestHandlersDecodeRouteAndPropagateErrors(t *testing.T) {
	client := &Client{
		runnerID:      "runner_handlers",
		reportWaiters: map[string]*dispatchWaiter{},
		resultWaiters: map[string]*dispatchWaiter{},
	}
	reportWaiter := &dispatchWaiter{reportCh: make(chan report.Report, 1), resultCh: make(chan protocol.ResultPayload, 1)}
	resultWaiter := &dispatchWaiter{reportCh: make(chan report.Report, 1), resultCh: make(chan protocol.ResultPayload, 1)}
	client.reportWaiters["stage_handlers"] = reportWaiter
	client.resultWaiters[resultWaiterKey("run_handlers", "task_handlers", "attempt_handlers")] = resultWaiter

	var gotEvent event.Event
	var gotReport report.Report
	var gotResult protocol.ResultPayload
	var gotLog protocol.LogPayload
	client.SetHandlers(
		func(_ context.Context, ev event.Event) error { gotEvent = ev; return nil },
		nil,
		func(_ context.Context, rep report.Report) error { gotReport = rep; return nil },
		func(_ context.Context, result protocol.ResultPayload) error { gotResult = result; return nil },
		func(_ context.Context, log protocol.LogPayload) error { gotLog = log; return nil },
	)
	ev := event.Event{SchemaVersion: event.SchemaVersion, Type: "runner.ready", Actor: event.Actor{Kind: event.ActorKindAdapter, ID: "runner_handlers"}, Summary: "ready"}
	if err := client.handleEvent(context.Background(), protocol.MustMessage(protocol.TypeEvent, ev)); err != nil {
		t.Fatalf("handle event: %v", err)
	}
	if gotEvent.Type != ev.Type {
		t.Fatalf("event handler got %+v, want %+v", gotEvent, ev)
	}
	rep := report.Report{SchemaVersion: report.SchemaVersion, RunID: "run_handlers", TaskID: "task_handlers", AttemptID: "attempt_handlers", StageID: "stage_handlers", StageType: contract.StageTypeImplementation, Actor: report.Actor{Kind: report.ActorKindAgent, ID: "noop"}, Status: report.StatusCompleted, Summary: "done", Payload: map[string]any{}}
	if err := client.handleReport(context.Background(), protocol.MustMessage(protocol.TypeReport, rep)); err != nil {
		t.Fatalf("handle report: %v", err)
	}
	if gotReport.StageID != rep.StageID {
		t.Fatalf("report handler got %+v, want %+v", gotReport, rep)
	}
	select {
	case queued := <-reportWaiter.reportCh:
		if queued.StageID != rep.StageID {
			t.Fatalf("queued report = %+v, want stage %s", queued, rep.StageID)
		}
	default:
		t.Fatal("report was not routed to waiter")
	}
	result := protocol.ResultPayload{RunID: rep.RunID, TaskID: rep.TaskID, AttemptID: rep.AttemptID, TerminalStatus: "completed"}
	if err := client.handleResult(context.Background(), protocol.MustMessage(protocol.TypeResult, result)); err != nil {
		t.Fatalf("handle result: %v", err)
	}
	if gotResult.RunID != result.RunID {
		t.Fatalf("result handler got %+v, want %+v", gotResult, result)
	}
	select {
	case queued := <-resultWaiter.resultCh:
		if queued.AttemptID != result.AttemptID {
			t.Fatalf("queued result = %+v, want attempt %s", queued, result.AttemptID)
		}
	default:
		t.Fatal("result was not routed to waiter")
	}
	logPayload := protocol.LogPayload{Level: "info", Message: "hello"}
	if err := client.handleLog(context.Background(), protocol.MustMessage(protocol.TypeLog, logPayload)); err != nil {
		t.Fatalf("handle log: %v", err)
	}
	if gotLog.Message != logPayload.Message {
		t.Fatalf("log handler got %+v, want %+v", gotLog, logPayload)
	}

	if err := client.handleLog(context.Background(), protocol.Message{Type: protocol.TypeLog, Payload: []byte("{")}); err == nil {
		t.Fatal("handleLog accepted malformed payload")
	}
	sentinel := errors.New("handler failed")
	client.SetHandlers(func(context.Context, event.Event) error { return sentinel }, nil, nil, nil, nil)
	if err := client.handleEvent(context.Background(), protocol.MustMessage(protocol.TypeEvent, ev)); !errors.Is(err, sentinel) {
		t.Fatalf("handler error = %v, want sentinel", err)
	}
}

func TestLifecycleHandlersAndReadyFramingHelpers(t *testing.T) {
	client := &Client{runnerID: "runner_lifecycle", artifacts: newArtifactReassembler()}
	defer client.cleanupArtifacts()
	missedCh := make(chan int, 1)
	recoveredCh := make(chan string, 1)
	downCh := make(chan string, 2)
	client.SetLifecycleHandlers(
		func(_ context.Context, _ string, missed int, _ error) { missedCh <- missed },
		func(_ context.Context, runnerID string) { recoveredCh <- runnerID },
		func(_ context.Context, _ string, reason string, _ error) { downCh <- reason },
	)
	client.notifyHeartbeatMissed(2, errors.New("timeout"))
	client.notifyHeartbeatRecovered()
	client.markDown("session_done", protocol.ErrSessionClosed)
	client.markDown("process_exit", errors.New("already down"))
	select {
	case missed := <-missedCh:
		if missed != 2 {
			t.Fatalf("missed = %d, want 2", missed)
		}
	default:
		t.Fatal("heartbeat missed handler was not called")
	}
	select {
	case runnerID := <-recoveredCh:
		if runnerID != "runner_lifecycle" {
			t.Fatalf("recovered runner = %q", runnerID)
		}
	default:
		t.Fatal("heartbeat recovered handler was not called")
	}
	select {
	case reason := <-downCh:
		if reason != "session_done" {
			t.Fatalf("down reason = %q, want session_done", reason)
		}
	default:
		t.Fatal("down handler was not called")
	}
	select {
	case reason := <-downCh:
		t.Fatalf("down handler called more than once with %q", reason)
	default:
	}
	client.setClosing()
	if !client.isClosing() {
		t.Fatal("setClosing did not mark client closing")
	}

	line, err := readReadyLine(context.Background(), strings.NewReader("READY ws://127.0.0.1:9999/session\n"))
	if err != nil {
		t.Fatalf("read ready line: %v", err)
	}
	url, err := parseReady(line)
	if err != nil {
		t.Fatalf("parse ready: %v", err)
	}
	if url != "ws://127.0.0.1:9999/session" {
		t.Fatalf("ready URL = %q", url)
	}
	if _, err := readReadyLine(context.Background(), strings.NewReader("")); err == nil || !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("empty ready line error = %v, want EOF", err)
	}
	for _, line := range []string{"NOTREADY ws://127.0.0.1:9999/session", "READY ws://example.test/session", "READY ws://127.0.0.1:9999/not-session"} {
		if _, err := parseReady(line); err == nil {
			t.Fatalf("parseReady(%q) succeeded, want error", line)
		}
	}
	var prefixed strings.Builder
	copyPrefixed(&prefixed, strings.NewReader("one\ntwo\n"), "runner: ")
	if got := prefixed.String(); got != "runner: one\nrunner: two\n" {
		t.Fatalf("copyPrefixed output = %q", got)
	}
}

func baseDispatchForClientTest() contract.Dispatch {
	return contract.Dispatch{
		ProjectID: "project_test",
		RunID:     "run_test",
		TaskID:    "task_test",
		AttemptID: "attempt_test",
		StageID:   "stage_test",
		StageType: contract.StageTypeImplementation,
		Adapter:   "noop",
		Input:     map[string]any{"idea": "unit dispatch"},
	}
}

func baseReportForClientTest(disp contract.Dispatch, status string) report.Report {
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        status,
		Summary:       fmt.Sprintf("%s report", status),
		Payload:       map[string]any{},
	}
}
