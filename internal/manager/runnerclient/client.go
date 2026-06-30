package runnerclient

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
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
type HeartbeatMissedHandler func(context.Context, string, int, error)
type HeartbeatRecoveredHandler func(context.Context, string)
type DownHandler func(context.Context, string, string, error)

type Client struct {
	session  *protocol.Session
	cmd      *exec.Cmd
	runnerID string
	ready    protocol.ReadyPayload
	pongCh   chan struct{}

	downOnce sync.Once

	mu                   sync.RWMutex
	onEvent              EventHandler
	onArtifact           ArtifactHandler
	onReport             ReportHandler
	onResult             ResultHandler
	onLog                LogHandler
	onHeartbeatMissed    HeartbeatMissedHandler
	onHeartbeatRecovered HeartbeatRecoveredHandler
	onDown               DownHandler
	reportWaiters        map[string]*dispatchWaiter
	resultWaiters        map[string]*dispatchWaiter
	artifacts            *artifactReassembler
	closing              bool
	cmdDone              chan struct{}
	cmdErr               error
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
	sibling := filepath.Join(filepath.Dir(exe), name)
	if _, err := os.Stat(sibling); err == nil {
		return sibling, nil
	}
	return exe, nil
}

func StartChild(ctx context.Context, runnerBin string) (*Client, error) {
	return StartChildWithEnv(ctx, runnerBin, nil)
}

func StartChildWithEnv(ctx context.Context, runnerBin string, env []string) (*Client, error) {
	return StartChildWithEnvAndID(ctx, runnerBin, env, ids.New("runner"))
}

func StartChildWithEnvAndID(ctx context.Context, runnerBin string, env []string, runnerID string) (*Client, error) {
	if runnerID == "" {
		runnerID = ids.New("runner")
	}
	if runnerBin == "" {
		var err error
		runnerBin, err = ResolveRunnerBinary()
		if err != nil {
			return nil, err
		}
	}
	if isCurrentExecutable(runnerBin) {
		env = append(env, "PARLEY_RUNNER_CHILD=1")
	}
	cmd := exec.Command(runnerBin)
	if len(env) > 0 {
		cmd.Env = mergeEnv(os.Environ(), env)
	}
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
	client, err := Dial(ctx, url, runnerID)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	}
	client.cmd = cmd
	client.cmdDone = make(chan struct{})
	go client.watchProcess()
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
	c := &Client{
		session:       protocol.NewSession(conn),
		runnerID:      runnerID,
		pongCh:        make(chan struct{}, 1),
		reportWaiters: map[string]*dispatchWaiter{},
		resultWaiters: map[string]*dispatchWaiter{},
		artifacts:     newArtifactReassembler(),
	}
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
	go c.watchSession()
	go c.heartbeat()
	return c, nil
}

func isCurrentExecutable(candidate string) bool {
	if candidate == "" {
		return false
	}
	exe, err := os.Executable()
	if err != nil {
		return false
	}
	exeAbs, err := filepath.Abs(exe)
	if err != nil {
		return false
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return false
	}
	exeInfo, err := os.Stat(exeAbs)
	if err != nil {
		return false
	}
	candidateInfo, err := os.Stat(candidateAbs)
	if err != nil {
		return false
	}
	return os.SameFile(exeInfo, candidateInfo)
}

func mergeEnv(base, overrides []string) []string {
	positions := map[string]int{}
	merged := append([]string{}, base...)
	for i, value := range merged {
		if key := envKey(value); key != "" {
			positions[key] = i
		}
	}
	for _, value := range overrides {
		key := envKey(value)
		if key == "" {
			continue
		}
		if pos, ok := positions[key]; ok {
			merged[pos] = value
			continue
		}
		positions[key] = len(merged)
		merged = append(merged, value)
	}
	return merged
}

func envKey(value string) string {
	key, _, ok := strings.Cut(value, "=")
	if !ok {
		return ""
	}
	return key
}

func (c *Client) Ready() protocol.ReadyPayload { return c.ready }

func (c *Client) RunnerID() string { return c.runnerID }

func (c *Client) ChildPID() (int, bool) {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return 0, false
	}
	return c.cmd.Process.Pid, true
}

func (c *Client) SetHandlers(onEvent EventHandler, onArtifact ArtifactHandler, onReport ReportHandler, onResult ResultHandler, onLog LogHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onEvent = onEvent
	c.onArtifact = onArtifact
	c.onReport = onReport
	c.onResult = onResult
	c.onLog = onLog
}

func (c *Client) SetLifecycleHandlers(onHeartbeatMissed HeartbeatMissedHandler, onHeartbeatRecovered HeartbeatRecoveredHandler, onDown DownHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onHeartbeatMissed = onHeartbeatMissed
	c.onHeartbeatRecovered = onHeartbeatRecovered
	c.onDown = onDown
}

func (c *Client) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	cancelAttempt := func() {
		// Best effort, but do not silently drop the cancel on a transient send
		// failure under load: the runner must learn the attempt was cancelled.
		// The dispatch context is already cancelled, so use a detached context
		// and retry until the cancel lands or the session is gone. A send that
		// returns an error did not deliver, so a retry cannot duplicate a cancel
		// the runner already received.
		for attempt := 1; attempt <= 3; attempt++ {
			select {
			case <-c.session.Done():
				return
			default:
			}
			cancelCtx, cancelTimeout := context.WithTimeout(context.Background(), 5*time.Second)
			err := c.CancelAttempt(cancelCtx, disp.RunID, disp.TaskID, disp.AttemptID)
			cancelTimeout()
			if err == nil {
				return
			}
			log.Printf("runnerclient: cancel attempt %d/3 for run %s attempt %s failed: %v", attempt, disp.RunID, disp.AttemptID, err)
		}
	}
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
		if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, protocol.ErrSessionClosed) {
			cancelAttempt()
			return report.Report{}, ctxErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			cancelAttempt()
		}
		return report.Report{}, err
	}

	var rep report.Report
	select {
	case rep = <-waiter.reportCh:
	case <-ctx.Done():
		cancelAttempt()
		return report.Report{}, ctx.Err()
	case <-c.session.Done():
		return report.Report{}, c.dispatchSessionDoneErr(ctx, cancelAttempt)
	}

	select {
	case result := <-waiter.resultCh:
		if result.TerminalStatus == "failed" && rep.Status == report.StatusCompleted {
			return rep, fmt.Errorf("%w: runner returned failed result", ErrDispatchFailed)
		}
	case <-time.After(2 * time.Second):
		return rep, nil
	case <-ctx.Done():
		cancelAttempt()
		return rep, ctx.Err()
	case <-c.session.Done():
		return rep, c.dispatchSessionDoneErr(ctx, cancelAttempt)
	}
	return rep, nil
}

func (c *Client) dispatchSessionDoneErr(ctx context.Context, cancelAttempt func()) error {
	// Dispatch cancellation is caller intent. If the protocol session closes in
	// the same scheduling window, report the caller's cancellation deterministically
	// while still making the best-effort cancel attempt before returning.
	if err := ctx.Err(); err != nil {
		cancelAttempt()
		return err
	}
	return protocol.ErrSessionClosed
}

func (c *Client) Cancel(ctx context.Context, runID, taskID string) error {
	return c.CancelAttempt(ctx, runID, taskID, "")
}

func (c *Client) CancelAttempt(ctx context.Context, runID, taskID, attemptID string) error {
	return c.send(ctx, protocol.TypeCancel, protocol.CancelPayload{RunID: runID, TaskID: taskID, AttemptID: attemptID})
}

func (c *Client) EvictWarmSession(ctx context.Context, warmSessionKey string) error {
	if strings.TrimSpace(warmSessionKey) == "" {
		return nil
	}
	return c.send(ctx, protocol.TypeEvictWarmSession, protocol.EvictWarmSessionPayload{WarmSessionKey: warmSessionKey})
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
	c.setClosing()
	_ = c.session.Close(websocket.StatusNormalClosure, "manager shutdown")
	c.cleanupArtifacts()
	if c.cmd == nil || c.cmd.Process == nil {
		return nil
	}
	_ = c.cmd.Process.Signal(os.Interrupt)
	cmdDone := c.commandDone()
	if cmdDone == nil {
		return nil
	}
	select {
	case <-cmdDone:
		err := c.commandErr()
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") {
			return fmt.Errorf("wait runner: %w", err)
		}
		return nil
	case <-time.After(3 * time.Second):
		_ = c.cmd.Process.Kill()
		<-cmdDone
		return c.commandErr()
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
	assembler := c.artifactReassembler()
	complete, ready, err := assembler.Accept(art)
	if err != nil {
		_ = assembler.Close()
		return err
	}
	if !ready {
		return nil
	}
	c.mu.RLock()
	handler := c.onArtifact
	c.mu.RUnlock()
	if handler != nil {
		return handler(ctx, complete)
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
	missed := 0
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := c.Ping(ctx)
			cancel()
			if err != nil {
				missed++
				c.notifyHeartbeatMissed(missed, err)
				if missed >= 3 {
					c.markDown("heartbeat_timeout", err)
					return
				}
				continue
			}
			if missed > 0 {
				missed = 0
				c.notifyHeartbeatRecovered()
			}
		case <-c.session.Done():
			return
		}
	}
}

func (c *Client) watchSession() {
	<-c.session.Done()
	c.cleanupArtifacts()
	if c.isClosing() {
		return
	}
	c.markDown("session_done", protocol.ErrSessionClosed)
}

func (c *Client) watchProcess() {
	err := c.cmd.Wait()
	c.mu.Lock()
	c.cmdErr = err
	if c.cmdDone != nil {
		close(c.cmdDone)
	}
	closing := c.closing
	c.mu.Unlock()
	if closing {
		return
	}
	c.markDown("process_exit", err)
}

func (c *Client) markDown(reason string, err error) {
	c.cleanupArtifacts()
	c.downOnce.Do(func() {
		c.mu.RLock()
		handler := c.onDown
		runnerID := c.runnerID
		c.mu.RUnlock()
		if handler != nil {
			handler(context.Background(), runnerID, reason, err)
		}
	})
}

func (c *Client) notifyHeartbeatMissed(missed int, err error) {
	c.mu.RLock()
	handler := c.onHeartbeatMissed
	runnerID := c.runnerID
	c.mu.RUnlock()
	if handler != nil {
		handler(context.Background(), runnerID, missed, err)
	}
}

func (c *Client) notifyHeartbeatRecovered() {
	c.mu.RLock()
	handler := c.onHeartbeatRecovered
	runnerID := c.runnerID
	c.mu.RUnlock()
	if handler != nil {
		handler(context.Background(), runnerID)
	}
}

func (c *Client) setClosing() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closing = true
}

func (c *Client) isClosing() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closing
}

func (c *Client) artifactReassembler() *artifactReassembler {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.artifacts == nil {
		c.artifacts = newArtifactReassembler()
	}
	return c.artifacts
}

func (c *Client) cleanupArtifacts() {
	c.mu.RLock()
	artifacts := c.artifacts
	c.mu.RUnlock()
	if artifacts != nil {
		_ = artifacts.Close()
	}
}

func (c *Client) commandDone() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cmdDone
}

func (c *Client) commandErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cmdErr
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
