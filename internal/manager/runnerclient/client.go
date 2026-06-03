package runnerclient

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/ids"
	"github.com/agent-parley/parley/internal/shared/protocol"
	"github.com/agent-parley/parley/internal/shared/report"
)

var ErrDispatchFailed = errors.New("dispatch failed")

type EventHandler func(context.Context, event.Event) error
type ArtifactHandler func(context.Context, protocol.ArtifactPayload) error
type ReportHandler func(context.Context, report.Report) error
type ResultHandler func(context.Context, protocol.ResultPayload) error
type LogHandler func(context.Context, protocol.LogPayload) error

type Client struct {
	session  *protocol.Session
	cmd      *exec.Cmd
	runnerID string
	ready    protocol.ReadyPayload
	pongCh   chan struct{}

	mu            sync.RWMutex
	onEvent       EventHandler
	onArtifact    ArtifactHandler
	onReport      ReportHandler
	onResult      ResultHandler
	onLog         LogHandler
	reportWaiters map[string]*dispatchWaiter
	resultWaiters map[string]*dispatchWaiter
}

type dispatchWaiter struct {
	reportCh chan report.Report
	resultCh chan protocol.ResultPayload
}

func ResolveRunnerBinary() (string, error) {
	if override := os.Getenv("PARLEY_RUNNER_BIN"); override != "" {
		return override, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	name := "parley-runner"
	return filepath.Join(filepath.Dir(exe), name), nil
}

func StartChild(ctx context.Context, runnerBin string) (*Client, error) {
	if runnerBin == "" {
		var err error
		runnerBin, err = ResolveRunnerBinary()
		if err != nil {
			return nil, err
		}
	}
	cmd := exec.Command(runnerBin)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("runner stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("runner stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start runner: %w", err)
	}
	go copyPrefixed(os.Stderr, stderr, "runner: ")

	readyLine, err := readReadyLine(ctx, stdout)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	}
	url, err := parseReady(readyLine)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	}
	client, err := Dial(ctx, url, ids.New("runner"))
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	}
	client.cmd = cmd
	return client, nil
}

func Dial(ctx context.Context, url, runnerID string) (*Client, error) {
	conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: http.DefaultClient})
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("dial runner websocket (%s): %w", resp.Status, err)
		}
		return nil, fmt.Errorf("dial runner websocket: %w", err)
	}
	c := &Client{session: protocol.NewSession(conn), runnerID: runnerID, pongCh: make(chan struct{}, 1), reportWaiters: map[string]*dispatchWaiter{}, resultWaiters: map[string]*dispatchWaiter{}}
	readyCh := make(chan protocol.ReadyPayload, 1)
	c.session.Handle(protocol.TypeReady, func(ctx context.Context, msg protocol.Message) error {
		ready, err := protocol.DecodePayload[protocol.ReadyPayload](msg)
		if err != nil {
			return err
		}
		select {
		case readyCh <- ready:
		default:
		}
		return nil
	})
	c.session.Handle(protocol.TypePong, func(context.Context, protocol.Message) error {
		select {
		case c.pongCh <- struct{}{}:
		default:
		}
		return nil
	})
	c.session.Handle(protocol.TypeEvent, c.handleEvent)
	c.session.Handle(protocol.TypeArtifact, c.handleArtifact)
	c.session.Handle(protocol.TypeReport, c.handleReport)
	c.session.Handle(protocol.TypeResult, c.handleResult)
	c.session.Handle(protocol.TypeLog, c.handleLog)
	c.session.Start(context.Background())

	if err := c.send(ctx, protocol.TypeHello, protocol.HelloPayload{RunnerID: runnerID}); err != nil {
		return nil, err
	}
	select {
	case ready := <-readyCh:
		c.ready = ready
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("runner ready timeout")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	go c.heartbeat()
	return c, nil
}

func (c *Client) Ready() protocol.ReadyPayload { return c.ready }

func (c *Client) SetHandlers(onEvent EventHandler, onArtifact ArtifactHandler, onReport ReportHandler, onResult ResultHandler, onLog LogHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onEvent = onEvent
	c.onArtifact = onArtifact
	c.onReport = onReport
	c.onResult = onResult
	c.onLog = onLog
}

func (c *Client) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	waiter := &dispatchWaiter{reportCh: make(chan report.Report, 1), resultCh: make(chan protocol.ResultPayload, 1)}
	reportKey := disp.StageID
	resultKey := resultWaiterKey(disp.RunID, disp.TaskID, disp.AttemptID)
	c.mu.Lock()
	c.reportWaiters[reportKey] = waiter
	c.resultWaiters[resultKey] = waiter
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.reportWaiters, reportKey)
		delete(c.resultWaiters, resultKey)
		c.mu.Unlock()
	}()

	if err := c.send(ctx, protocol.TypeDispatch, disp); err != nil {
		return report.Report{}, err
	}

	var rep report.Report
	select {
	case rep = <-waiter.reportCh:
	case <-ctx.Done():
		_ = c.Cancel(context.Background(), disp.RunID, disp.TaskID)
		return report.Report{}, ctx.Err()
	case <-c.session.Done():
		return report.Report{}, protocol.ErrSessionClosed
	}

	select {
	case result := <-waiter.resultCh:
		if result.TerminalStatus == "failed" && rep.Status == report.StatusCompleted {
			return rep, fmt.Errorf("%w: runner returned failed result", ErrDispatchFailed)
		}
	case <-time.After(2 * time.Second):
		return rep, nil
	case <-ctx.Done():
		return rep, ctx.Err()
	case <-c.session.Done():
		return rep, protocol.ErrSessionClosed
	}
	return rep, nil
}

func (c *Client) Cancel(ctx context.Context, runID, taskID string) error {
	return c.send(ctx, protocol.TypeCancel, protocol.CancelPayload{RunID: runID, TaskID: taskID})
}

func (c *Client) Ping(ctx context.Context) error {
	for {
		select {
		case <-c.pongCh:
			continue
		default:
		}
		if err := c.send(ctx, protocol.TypePing, map[string]any{}); err != nil {
			return err
		}
		select {
		case <-c.pongCh:
			return nil
		case <-c.session.Done():
			return protocol.ErrSessionClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Client) Close(ctx context.Context) error {
	_ = c.session.Close(websocket.StatusNormalClosure, "manager shutdown")
	if c.cmd == nil || c.cmd.Process == nil {
		return nil
	}
	_ = c.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- c.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") {
			return fmt.Errorf("wait runner: %w", err)
		}
		return nil
	case <-time.After(3 * time.Second):
		_ = c.cmd.Process.Kill()
		return <-done
	case <-ctx.Done():
		_ = c.cmd.Process.Kill()
		return ctx.Err()
	}
}

func (c *Client) handleEvent(ctx context.Context, msg protocol.Message) error {
	ev, err := protocol.DecodePayload[event.Event](msg)
	if err != nil {
		return err
	}
	c.mu.RLock()
	handler := c.onEvent
	c.mu.RUnlock()
	if handler != nil {
		return handler(ctx, ev)
	}
	return nil
}

func (c *Client) handleArtifact(ctx context.Context, msg protocol.Message) error {
	art, err := protocol.DecodePayload[protocol.ArtifactPayload](msg)
	if err != nil {
		return err
	}
	c.mu.RLock()
	handler := c.onArtifact
	c.mu.RUnlock()
	if handler != nil {
		return handler(ctx, art)
	}
	return nil
}

func (c *Client) handleReport(ctx context.Context, msg protocol.Message) error {
	rep, err := protocol.DecodePayload[report.Report](msg)
	if err != nil {
		return err
	}
	c.mu.RLock()
	waiter := c.reportWaiters[rep.StageID]
	handler := c.onReport
	c.mu.RUnlock()
	if waiter != nil {
		select {
		case waiter.reportCh <- rep:
		default:
		}
	}
	if handler != nil {
		return handler(ctx, rep)
	}
	return nil
}

func (c *Client) handleResult(ctx context.Context, msg protocol.Message) error {
	result, err := protocol.DecodePayload[protocol.ResultPayload](msg)
	if err != nil {
		return err
	}
	c.mu.RLock()
	waiter := c.resultWaiters[resultWaiterKey(result.RunID, result.TaskID, result.AttemptID)]
	handler := c.onResult
	c.mu.RUnlock()
	if waiter != nil {
		select {
		case waiter.resultCh <- result:
		default:
		}
	}
	if handler != nil {
		return handler(ctx, result)
	}
	return nil
}

func (c *Client) handleLog(ctx context.Context, msg protocol.Message) error {
	logPayload, err := protocol.DecodePayload[protocol.LogPayload](msg)
	if err != nil {
		return err
	}
	c.mu.RLock()
	handler := c.onLog
	c.mu.RUnlock()
	if handler != nil {
		return handler(ctx, logPayload)
	}
	return nil
}

func (c *Client) send(ctx context.Context, typ string, payload any) error {
	msg, err := protocol.NewMessage(typ, payload)
	if err != nil {
		return err
	}
	return c.session.Send(ctx, msg)
}

func (c *Client) heartbeat() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = c.Ping(ctx)
			cancel()
		case <-c.session.Done():
			return
		}
	}
}

func readReadyLine(ctx context.Context, r io.Reader) (string, error) {
	lineCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(r)
		if scanner.Scan() {
			lineCh <- scanner.Text()
			return
		}
		if err := scanner.Err(); err != nil {
			errCh <- err
			return
		}
		errCh <- io.EOF
	}()
	select {
	case line := <-lineCh:
		return line, nil
	case err := <-errCh:
		return "", fmt.Errorf("read runner READY line: %w", err)
	case <-time.After(10 * time.Second):
		return "", fmt.Errorf("read runner READY line: timeout")
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func parseReady(line string) (string, error) {
	const prefix = "READY "
	if !strings.HasPrefix(line, prefix) {
		return "", fmt.Errorf("runner first line %q does not start with READY", line)
	}
	url := strings.TrimSpace(strings.TrimPrefix(line, prefix))
	if !strings.HasPrefix(url, "ws://127.0.0.1:") || !strings.HasSuffix(url, "/session") {
		return "", fmt.Errorf("runner READY url %q is not a localhost session URL", url)
	}
	return url, nil
}

func resultWaiterKey(runID, taskID, attemptID string) string {
	return runID + "/" + taskID + "/" + attemptID
}

func copyPrefixed(dst io.Writer, src io.Reader, prefix string) {
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		fmt.Fprintf(dst, "%s%s\n", prefix, scanner.Text())
	}
}
