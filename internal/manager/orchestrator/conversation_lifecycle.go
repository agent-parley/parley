package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/agent-parley/parley/internal/shared/event"
)

var errConversationTurnDeadline = errors.New("conversation turn execution deadline exceeded")

type ConversationTurnState struct {
	ConversationID string
	Status         string
	InFlight       bool
	Queued         int
	Cancellable    bool
}

type conversationTurn struct {
	projectID        string
	conversationID   string
	triggerMessageID string
}

type conversationSession struct {
	conversationID string
	projectID      string

	dispatchMu sync.Mutex
	queue      []conversationTurn
	running    bool
	warm       bool
	ready      bool
	cancel     context.CancelFunc

	idleSince time.Time
	idleTimer *time.Timer
}

type reservedConversationTurn struct {
	session   *conversationSession
	turn      conversationTurn
	ctx       context.Context
	cancel    context.CancelFunc
	coldStart bool
}

type warmSessionEvicter interface {
	EvictWarmSession(context.Context, string) error
}

func (e *Engine) enqueueConversationTurn(ctx context.Context, projectID, conversationID, triggerMessageID string) {
	e.conversationMu.Lock()
	session := e.conversationSessionLocked(projectID, conversationID)
	session.queue = append(session.queue, conversationTurn{projectID: projectID, conversationID: conversationID, triggerMessageID: triggerMessageID})
	e.markConversationReadyLocked(session)
	queued := len(session.queue)
	running := session.running
	e.conversationMu.Unlock()

	_ = e.emitConversationLifecycleEvent(ctx, projectID, conversationID, triggerMessageID, "conversation.turn_queued", "conversation turn queued", map[string]any{
		"queued":    queued,
		"in_flight": running,
	})
	e.scheduleConversationTurns()
}

func (e *Engine) ConversationTurnState(conversationID string) ConversationTurnState {
	e.conversationMu.Lock()
	defer e.conversationMu.Unlock()
	session := e.conversationSessions[conversationID]
	state := ConversationTurnState{ConversationID: conversationID, Status: "idle"}
	if session == nil {
		return state
	}
	state.InFlight = session.running
	state.Queued = len(session.queue)
	state.Cancellable = state.InFlight || state.Queued > 0
	if state.InFlight {
		state.Status = "thinking"
	} else if state.Queued > 0 {
		state.Status = "queued"
	}
	return state
}

func (e *Engine) CancelProjectConversationTurn(ctx context.Context, projectID string) error {
	project, err := e.store.GetProject(ctx, projectID)
	if err != nil {
		return err
	}
	conversation, err := e.store.EnsureProjectConversation(ctx, project.ID)
	if err != nil {
		return err
	}
	return e.CancelConversationTurn(ctx, conversation.ID)
}

func (e *Engine) CancelConversationTurn(ctx context.Context, conversationID string) error {
	e.conversationMu.Lock()
	session := e.conversationSessions[conversationID]
	if session == nil {
		e.conversationMu.Unlock()
		return nil
	}
	queued := len(session.queue)
	session.queue = nil
	session.ready = false
	e.removeConversationReadyLocked(conversationID)
	cancel := session.cancel
	projectID := session.projectID
	running := session.running
	if !running && session.warm {
		e.scheduleConversationIdleEvictionLocked(session, time.Now().UTC())
	}
	e.conversationMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if running || queued > 0 {
		_ = e.emitConversationLifecycleEvent(ctx, projectID, conversationID, "", "conversation.turn_cancelled", "conversation turn cancelled", map[string]any{
			"queued_cancelled": queued,
			"in_flight":        running,
		})
	}
	e.scheduleConversationTurns()
	return nil
}

func (e *Engine) conversationSessionLocked(projectID, conversationID string) *conversationSession {
	session := e.conversationSessions[conversationID]
	if session == nil {
		session = &conversationSession{conversationID: conversationID, projectID: projectID}
		e.conversationSessions[conversationID] = session
	} else if projectID != "" {
		session.projectID = projectID
	}
	return session
}

func (e *Engine) markConversationReadyLocked(session *conversationSession) {
	if session == nil || session.running || len(session.queue) == 0 || session.ready {
		return
	}
	session.ready = true
	e.conversationReady = append(e.conversationReady, session.conversationID)
}

func (e *Engine) removeConversationReadyLocked(conversationID string) {
	if len(e.conversationReady) == 0 {
		return
	}
	out := e.conversationReady[:0]
	for _, id := range e.conversationReady {
		if id != conversationID {
			out = append(out, id)
		}
	}
	e.conversationReady = out
}

func (e *Engine) scheduleConversationTurns() {
	for {
		evictions, work := e.reserveConversationTurn()
		if len(evictions) == 0 && work == nil {
			return
		}
		for _, session := range evictions {
			e.evictConversationWarmSession(e.rootCtx, session, "budget_lru")
		}
		if work == nil {
			continue
		}
		if !e.spawn(func() { e.runReservedConversationTurn(work) }) {
			work.cancel()
			e.finishReservedConversationTurn(work)
		}
	}
}

func (e *Engine) reserveConversationTurn() ([]*conversationSession, *reservedConversationTurn) {
	e.conversationMu.Lock()
	defer e.conversationMu.Unlock()
	if len(e.conversationReady) == 0 {
		return nil, nil
	}

	ready := e.conversationReady
	e.conversationReady = nil
	skipped := make([]string, 0, len(ready))
	for i, conversationID := range ready {
		session := e.conversationSessions[conversationID]
		if session == nil {
			continue
		}
		session.ready = false
		if session.running || len(session.queue) == 0 {
			continue
		}
		if _, ok := e.tryReserveGlobalTurn(); !ok {
			session.ready = true
			skipped = append(skipped, conversationID)
			continue
		}
		evictions, ok := e.reserveConversationBudgetLocked(session)
		if !ok {
			e.releaseGlobalTurn()
			session.ready = true
			skipped = append(skipped, conversationID)
			continue
		}

		turn := session.queue[0]
		session.queue = session.queue[1:]
		session.running = true
		var turnCtx context.Context
		var cancel context.CancelFunc
		if e.conversationTurnDeadline > 0 {
			turnCtx, cancel = context.WithTimeoutCause(e.rootCtx, e.conversationTurnDeadline, errConversationTurnDeadline)
		} else {
			turnCtx, cancel = context.WithCancel(e.rootCtx)
		}
		session.cancel = cancel
		coldStart := !session.warm
		session.warm = true
		session.idleSince = time.Time{}
		if session.idleTimer != nil {
			session.idleTimer.Stop()
			session.idleTimer = nil
		}
		if len(session.queue) > 0 {
			// The remaining turns for this Conversation become ready only after
			// the current turn finishes; per-Conversation serialization is the
			// invariant this queue preserves.
			session.ready = false
		}
		for _, id := range skipped {
			if skippedSession := e.conversationSessions[id]; skippedSession != nil {
				skippedSession.ready = true
			}
		}
		for _, id := range ready[i+1:] {
			if remainingSession := e.conversationSessions[id]; remainingSession != nil {
				remainingSession.ready = true
			}
		}
		e.conversationReady = append(append(e.conversationReady, skipped...), ready[i+1:]...)
		return evictions, &reservedConversationTurn{session: session, turn: turn, ctx: turnCtx, cancel: cancel, coldStart: coldStart}
	}
	e.conversationReady = skipped
	return nil, nil
}

func (e *Engine) reserveConversationBudgetLocked(session *conversationSession) ([]*conversationSession, bool) {
	if session.warm {
		return nil, true
	}
	if e.conversationWarmCountLocked() < e.conversationBudget {
		return nil, true
	}
	candidate := e.lruIdleConversationSessionLocked(session.conversationID)
	if candidate == nil {
		return nil, false
	}
	candidate.warm = false
	candidate.idleSince = time.Time{}
	if candidate.idleTimer != nil {
		candidate.idleTimer.Stop()
		candidate.idleTimer = nil
	}
	return []*conversationSession{candidate}, true
}

func (e *Engine) conversationWarmCountLocked() int {
	count := 0
	for _, session := range e.conversationSessions {
		if session.warm {
			count++
		}
	}
	return count
}

func (e *Engine) lruIdleConversationSessionLocked(excludeConversationID string) *conversationSession {
	var candidate *conversationSession
	for _, session := range e.conversationSessions {
		if session.conversationID == excludeConversationID || !session.warm || session.running || len(session.queue) > 0 {
			continue
		}
		if candidate == nil || session.idleSince.Before(candidate.idleSince) {
			candidate = session
		}
	}
	return candidate
}

func (e *Engine) runReservedConversationTurn(work *reservedConversationTurn) {
	_ = e.emitConversationLifecycleEvent(work.ctx, work.turn.projectID, work.turn.conversationID, work.turn.triggerMessageID, "conversation.turn_started", "conversation turn started", map[string]any{
		"cold_start": work.coldStart,
	})

	func() {
		work.session.dispatchMu.Lock()
		defer work.session.dispatchMu.Unlock()

		project, err := e.store.GetProject(work.ctx, work.turn.projectID)
		if err != nil {
			e.persistConversationTurnFailure(work.ctx, work.turn, fmt.Errorf("load project: %w", err))
			return
		}
		conversation, err := e.store.GetConversation(work.ctx, work.turn.conversationID)
		if err != nil {
			e.persistConversationTurnFailure(work.ctx, work.turn, fmt.Errorf("load conversation: %w", err))
			return
		}
		input, err := e.conversationDispatchInput(work.ctx, project, conversation, work.turn.triggerMessageID)
		if err != nil {
			e.persistConversationTurnFailure(work.ctx, work.turn, err)
			return
		}
		e.dispatchConversationReply(work.ctx, project.ID, conversation.ID, work.turn.triggerMessageID, input)
	}()
	e.finishReservedConversationTurn(work)
}

func (e *Engine) persistConversationTurnFailure(ctx context.Context, turn conversationTurn, err error) {
	if ctx.Err() != nil {
		return
	}
	_, _ = e.persistConversationAssistantMessage(ctx, turn.projectID, turn.conversationID, turn.triggerMessageID, "The conversational agent could not complete this turn: "+err.Error(), event.Actor{Kind: event.ActorKindAdapter, ID: e.conversationAdapter}, "conversation agent failed")
}

func (e *Engine) finishReservedConversationTurn(work *reservedConversationTurn) {
	cause := context.Cause(work.ctx)
	timedOut := errors.Is(cause, errConversationTurnDeadline)
	wasCanceled := cause != nil
	work.cancel()
	e.conversationMu.Lock()
	session := work.session
	if session.running {
		session.running = false
	}
	session.cancel = nil
	if timedOut {
		session.warm = false
		session.idleSince = time.Time{}
		if session.idleTimer != nil {
			session.idleTimer.Stop()
			session.idleTimer = nil
		}
	}
	queued := len(session.queue)
	if queued > 0 {
		e.markConversationReadyLocked(session)
	} else if session.warm {
		e.scheduleConversationIdleEvictionLocked(session, time.Now().UTC())
	}
	e.conversationMu.Unlock()
	e.releaseGlobalTurn()
	e.kickDispatchPending()

	if timedOut {
		background := context.Background()
		_ = e.emitConversationLifecycleEvent(background, work.turn.projectID, work.turn.conversationID, work.turn.triggerMessageID, "conversation.turn_timed_out", "conversation turn timed out", map[string]any{
			"deadline_seconds": e.conversationTurnDeadline.Seconds(),
			"queued":           queued,
			"cold_start":       work.coldStart,
		})
		_, _ = e.persistConversationAssistantMessage(background, work.turn.projectID, work.turn.conversationID, work.turn.triggerMessageID, conversationTurnDeadlineMessage(e.conversationTurnDeadline), event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "conversation turn timed out")
		e.evictConversationWarmSession(background, session, "turn_deadline")
		e.scheduleConversationTurns()
		return
	}

	typ := "conversation.turn_completed"
	summary := "conversation turn completed"
	if wasCanceled {
		typ = "conversation.turn_cancelled"
		summary = "conversation turn cancelled"
	}
	_ = e.emitConversationLifecycleEvent(context.Background(), work.turn.projectID, work.turn.conversationID, work.turn.triggerMessageID, typ, summary, map[string]any{
		"queued":     queued,
		"cold_start": work.coldStart,
	})
	e.scheduleConversationTurns()
}

func conversationTurnDeadlineMessage(deadline time.Duration) string {
	label := deadline.String()
	if deadline > 0 && deadline%time.Minute == 0 {
		label = fmt.Sprintf("%d-minute", deadline/time.Minute)
	}
	return fmt.Sprintf("This turn hit the %s execution deadline and was stopped. The conversation is intact - send a message to try again.", label)
}

func (e *Engine) scheduleConversationIdleEvictionLocked(session *conversationSession, idleSince time.Time) {
	if e.conversationIdleTTL <= 0 {
		return
	}
	session.idleSince = idleSince
	if session.idleTimer != nil {
		session.idleTimer.Stop()
	}
	conversationID := session.conversationID
	session.idleTimer = time.AfterFunc(e.conversationIdleTTL, func() {
		e.evictIdleConversationSession(conversationID, idleSince)
	})
}

func (e *Engine) evictIdleConversationSession(conversationID string, idleSince time.Time) {
	if !e.spawn(func() {
		e.conversationMu.Lock()
		session := e.conversationSessions[conversationID]
		if session == nil || !session.warm || session.running || len(session.queue) > 0 || !session.idleSince.Equal(idleSince) {
			e.conversationMu.Unlock()
			return
		}
		session.warm = false
		session.idleSince = time.Time{}
		if session.idleTimer != nil {
			session.idleTimer.Stop()
			session.idleTimer = nil
		}
		e.conversationMu.Unlock()

		e.evictConversationWarmSession(e.rootCtx, session, "idle_ttl")
		e.scheduleConversationTurns()
	}) {
		return
	}
}

func (e *Engine) evictConversationWarmSession(ctx context.Context, session *conversationSession, reason string) {
	if session == nil || ctx.Err() != nil {
		return
	}
	session.dispatchMu.Lock()
	defer session.dispatchMu.Unlock()
	if evicter, ok := e.runner.(warmSessionEvicter); ok {
		_ = evicter.EvictWarmSession(ctx, session.conversationID)
	}
	_ = e.emitConversationLifecycleEvent(ctx, session.projectID, session.conversationID, "", "conversation.session_evicted", "conversation warm session evicted", map[string]any{"reason": reason})
	e.pruneDormantConversationSession(session)
}

func (e *Engine) pruneDormantConversationSession(session *conversationSession) bool {
	e.conversationMu.Lock()
	defer e.conversationMu.Unlock()
	return e.pruneDormantConversationSessionLocked(session)
}

func (e *Engine) pruneDormantConversationSessionLocked(session *conversationSession) bool {
	if session == nil || e.conversationSessions[session.conversationID] != session {
		return false
	}
	if session.running || len(session.queue) > 0 || session.warm || session.ready || e.conversationReadyContainsLocked(session.conversationID) {
		return false
	}
	delete(e.conversationSessions, session.conversationID)
	return true
}

func (e *Engine) conversationReadyContainsLocked(conversationID string) bool {
	for _, id := range e.conversationReady {
		if id == conversationID {
			return true
		}
	}
	return false
}

func (e *Engine) emitConversationLifecycleEvent(ctx context.Context, projectID, conversationID, triggerMessageID, typ, summary string, data map[string]any) error {
	if data == nil {
		data = map[string]any{}
	}
	data["conversation_id"] = conversationID
	if triggerMessageID != "" {
		data["trigger_message_id"] = triggerMessageID
	}
	_, err := e.emit(ctx, event.Event{
		SchemaVersion: event.SchemaVersion,
		ProjectID:     projectID,
		Type:          typ,
		Actor:         event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"},
		Summary:       summary,
		Data:          data,
	})
	return err
}
