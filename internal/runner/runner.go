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

	mu        sync.Mutex
	runnerID  string
	adapters  map[string]adapter.AgentAdapter
	active    map[string]context.CancelFunc
	warm      map[string]*runnerWarmSession
	warmLocks map[string]*warmSessionLock
}

type runnerWarmSession struct {
	adapter string
	active  bool
}

type warmSessionLock struct {
	mu   sync.Mutex
	refs int
}

type warmSessionLockLease struct {
	runner *Runner
	key    string
	lock   *warmSessionLock
}

func New(sess *protocol.Session) *Runner {
	r := &Runner{
		session:   sess,
		adapters:  map[string]adapter.AgentAdapter{},
		active:    map[string]context.CancelFunc{},
		warm:      map[string]*runnerWarmSession{},
		warmLocks: map[string]*warmSessionLock{},
	}
	r.Register(adapter.Noop{})
	sess.Handle(protocol.TypeHello, r.handleHello)
	sess.Handle(protocol.TypePing, r.handlePing)
	sess.Handle(protocol.TypeDispatch, r.handleDispatch)
	sess.Handle(protocol.TypeCancel, r.handleCancel)
	sess.Handle(protocol.TypeEvictWarmSession, r.handleEvictWarmSession)
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
	var warmLock *warmSessionLockLease
	if disp.WarmSessionKey != "" {
		warmLock = r.acquireWarmSessionLock(disp.WarmSessionKey)
	}
	r.mu.Lock()
	r.active[key] = cancel
	if disp.WarmSessionKey != "" {
		session := r.warm[disp.WarmSessionKey]
		if session == nil {
			session = &runnerWarmSession{}
			r.warm[disp.WarmSessionKey] = session
		}
		session.adapter = disp.Adapter
		session.active = true
	}
	r.mu.Unlock()

	go func() {
		var idleOnce sync.Once
		markDispatchIdle := func() {
			idleOnce.Do(func() {
				r.mu.Lock()
				delete(r.active, key)
				if disp.WarmSessionKey != "" {
					if session := r.warm[disp.WarmSessionKey]; session != nil {
						session.active = false
					}
				}
				r.mu.Unlock()
			})
		}
		defer func() {
			markDispatchIdle()
			if warmLock != nil {
				warmLock.Unlock()
			}
			cancel()
		}()
		sink := sessionSink{session: r.session, disp: disp}
		rep, runErr := a.Run(taskCtx, disp, sink)
		if runErr != nil {
			rep = failedReport(disp, runErr)
		}
		if err := rep.Validate(); err != nil {
			rep = invalidReport(disp, rep, err)
		}
		terminal := "completed"
		if rep.Status == report.StatusFailed || rep.Status == report.StatusInvalid {
			terminal = "failed"
		}
		// A manager can evict a warm session as soon as it receives the
		// terminal result, so publish the idle state before the result is sent.
		markDispatchIdle()
		_ = r.send(context.Background(), protocol.TypeReport, rep)
		_ = r.send(context.Background(), protocol.TypeResult, protocol.ResultPayload{RunID: disp.RunID, TaskID: disp.TaskID, AttemptID: disp.AttemptID, TerminalStatus: terminal})
	}()
	return nil
}

func (r *Runner) handleEvictWarmSession(ctx context.Context, msg protocol.Message) error {
	payload, err := protocol.DecodePayload[protocol.EvictWarmSessionPayload](msg)
	if err != nil {
		return err
	}
	if payload.WarmSessionKey == "" {
		return nil
	}
	r.mu.Lock()
	session := r.warm[payload.WarmSessionKey]
	if session == nil || session.active {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	lock := r.acquireWarmSessionLock(payload.WarmSessionKey)
	defer lock.Unlock()

	r.mu.Lock()
	session = r.warm[payload.WarmSessionKey]
	if session == nil || session.active {
		r.mu.Unlock()
		return nil
	}
	adapterName := session.adapter
	delete(r.warm, payload.WarmSessionKey)
	// The per-key mutex is reaped when this eviction releases it and no
	// dispatch is queued on the same key; deleting it here would let a racing
	// dispatch create a second mutex and bypass dispatch-vs-evict serialization.
	a := r.adapters[adapterName]
	r.mu.Unlock()
	if evicter, ok := a.(adapter.WarmSessionEvicter); ok {
		return evicter.EvictWarmSession(ctx, payload.WarmSessionKey)
	}
	return nil
}

func (r *Runner) acquireWarmSessionLock(key string) *warmSessionLockLease {
	r.mu.Lock()
	lock := r.warmLocks[key]
	if lock == nil {
		lock = &warmSessionLock{}
		r.warmLocks[key] = lock
	}
	lock.refs++
	r.mu.Unlock()

	lock.mu.Lock()
	return &warmSessionLockLease{runner: r, key: key, lock: lock}
}

func (l *warmSessionLockLease) Unlock() {
	// Drop the reference before releasing the per-key mutex. Otherwise a waiting
	// evict can acquire the mutex, finish, and observe another lease's stale ref
	// before that lease has had a chance to decrement it.
	l.runner.releaseWarmSessionLock(l.key, l.lock)
	l.lock.mu.Unlock()
}

func (r *Runner) releaseWarmSessionLock(key string, lock *warmSessionLock) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lock.refs > 0 {
		lock.refs--
	}
	if lock.refs == 0 && r.warm[key] == nil && r.warmLocks[key] == lock {
		delete(r.warmLocks, key)
	}
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

func invalidReport(disp contract.Dispatch, candidate report.Report, err error) report.Report {
	invalidCandidate := map[string]any{
		"schema_version": candidate.SchemaVersion,
		"run_id":         candidate.RunID,
		"task_id":        candidate.TaskID,
		"attempt_id":     candidate.AttemptID,
		"stage_id":       candidate.StageID,
		"stage_type":     candidate.StageType,
		"actor":          map[string]any{"kind": candidate.Actor.Kind, "id": candidate.Actor.ID},
		"status":         candidate.Status,
		"summary":        candidate.Summary,
		"evidence_refs":  append([]string{}, candidate.EvidenceRefs...),
		"payload":        candidate.Payload,
		"errors":         append([]string{}, candidate.Errors...),
	}
	if candidate.Verdict != nil {
		invalidCandidate["verdict"] = string(*candidate.Verdict)
	}
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
		Payload:       map[string]any{"invalid_report": invalidCandidate},
		Errors:        []string{err.Error()},
	}
}

func CloseSession(sess *protocol.Session) error {
	return sess.Close(websocket.StatusNormalClosure, "runner shutdown")
}
