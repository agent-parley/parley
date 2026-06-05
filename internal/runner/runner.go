package runner

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/coder/websocket"

	"github.com/agent-parley/parley/internal/runner/adapter"
	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/protocol"
	"github.com/agent-parley/parley/internal/shared/report"
)

type Runner struct {
	session *protocol.Session

	mu       sync.Mutex
	runnerID string
	adapters map[string]adapter.AgentAdapter
	active   map[string]context.CancelFunc
}

func New(sess *protocol.Session) *Runner {
	r := &Runner{
		session:  sess,
		adapters: map[string]adapter.AgentAdapter{},
		active:   map[string]context.CancelFunc{},
	}
	r.Register(adapter.Noop{})
	sess.Handle(protocol.TypeHello, r.handleHello)
	sess.Handle(protocol.TypePing, r.handlePing)
	sess.Handle(protocol.TypeDispatch, r.handleDispatch)
	sess.Handle(protocol.TypeCancel, r.handleCancel)
	return r
}

func (r *Runner) Register(a adapter.AgentAdapter) {
	r.adapters[a.Name()] = a
}

func (r *Runner) handleHello(ctx context.Context, msg protocol.Message) error {
	payload, err := protocol.DecodePayload[protocol.HelloPayload](msg)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.runnerID = payload.RunnerID
	r.mu.Unlock()
	adapters := make([]string, 0, len(r.adapters))
	for name := range r.adapters {
		adapters = append(adapters, name)
	}
	return r.send(ctx, protocol.TypeReady, protocol.ReadyPayload{
		RunnerID:     payload.RunnerID,
		Capabilities: protocol.Capabilities{Adapters: adapters},
	})
}

func (r *Runner) handlePing(ctx context.Context, _ protocol.Message) error {
	return r.send(ctx, protocol.TypePong, map[string]any{})
}

func (r *Runner) handleDispatch(ctx context.Context, msg protocol.Message) error {
	disp, err := protocol.DecodePayload[contract.Dispatch](msg)
	if err != nil {
		return err
	}
	a, ok := r.adapters[disp.Adapter]
	if !ok {
		failed := failedReport(disp, fmt.Errorf("adapter %q not registered", disp.Adapter))
		if sendErr := r.send(ctx, protocol.TypeReport, failed); sendErr != nil {
			return sendErr
		}
		return r.send(ctx, protocol.TypeResult, protocol.ResultPayload{RunID: disp.RunID, TaskID: disp.TaskID, AttemptID: disp.AttemptID, TerminalStatus: "failed"})
	}

	taskCtx, cancel := context.WithCancel(context.Background())
	key := activeKey(disp.RunID, disp.TaskID, disp.AttemptID)
	r.mu.Lock()
	r.active[key] = cancel
	r.mu.Unlock()

	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.active, key)
			r.mu.Unlock()
			cancel()
		}()
		sink := sessionSink{session: r.session, disp: disp}
		rep, runErr := a.Run(taskCtx, disp, sink)
		if runErr != nil {
			rep = failedReport(disp, runErr)
		}
		if err := rep.Validate(); err != nil {
			rep = invalidReport(disp, err)
		}
		terminal := "completed"
		if rep.Status != report.StatusCompleted {
			terminal = "failed"
		}
		_ = r.send(context.Background(), protocol.TypeReport, rep)
		_ = r.send(context.Background(), protocol.TypeResult, protocol.ResultPayload{RunID: disp.RunID, TaskID: disp.TaskID, AttemptID: disp.AttemptID, TerminalStatus: terminal})
	}()
	return nil
}

func (r *Runner) handleCancel(ctx context.Context, msg protocol.Message) error {
	payload, err := protocol.DecodePayload[protocol.CancelPayload](msg)
	if err != nil {
		return err
	}
	r.mu.Lock()
	var cancels []context.CancelFunc
	if payload.AttemptID != "" {
		if cancel := r.active[activeKey(payload.RunID, payload.TaskID, payload.AttemptID)]; cancel != nil {
			cancels = append(cancels, cancel)
		}
	} else {
		prefix := activePrefix(payload.RunID, payload.TaskID)
		for key, cancel := range r.active {
			if strings.HasPrefix(key, prefix) {
				cancels = append(cancels, cancel)
			}
		}
	}
	r.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return nil
}

func (r *Runner) send(ctx context.Context, typ string, payload any) error {
	msg, err := protocol.NewMessage(typ, payload)
	if err != nil {
		return err
	}
	return r.session.Send(ctx, msg)
}

func activeKey(runID, taskID, attemptID string) string {
	return activePrefix(runID, taskID) + attemptID
}

func activePrefix(runID, taskID string) string {
	return runID + "/" + taskID + "/"
}

type sessionSink struct {
	session *protocol.Session
	disp    contract.Dispatch
}

func (s sessionSink) Emit(ctx context.Context, ev event.Event) error {
	if ev.RunID == "" {
		ev.RunID = s.disp.RunID
	}
	if ev.TaskID == "" {
		ev.TaskID = s.disp.TaskID
	}
	if ev.AttemptID == "" {
		ev.AttemptID = s.disp.AttemptID
	}
	msg, err := protocol.NewMessage(protocol.TypeEvent, ev)
	if err != nil {
		return err
	}
	return s.session.Send(ctx, msg)
}

func (s sessionSink) Artifact(ctx context.Context, art runnerio.Artifact) error {
	return protocol.SendArtifact(ctx, s.session, protocol.ArtifactPayload{
		RunID:      s.disp.RunID,
		TaskID:     s.disp.TaskID,
		AttemptID:  s.disp.AttemptID,
		ArtifactID: art.ID,
		Name:       art.Name,
		Kind:       art.Kind,
		MediaType:  art.MediaType,
		Content:    art.Content,
	})
}

func failedReport(disp contract.Dispatch, err error) report.Report {
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        report.StatusFailed,
		Summary:       "adapter failed",
		Payload:       map[string]any{},
		Errors:        []string{err.Error()},
	}
}

func invalidReport(disp contract.Dispatch, err error) report.Report {
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "runner"},
		Status:        report.StatusInvalid,
		Summary:       "adapter returned invalid report",
		Payload:       map[string]any{},
		Errors:        []string{err.Error()},
	}
}

func CloseSession(sess *protocol.Session) error {
	return sess.Close(websocket.StatusNormalClosure, "runner shutdown")
}
