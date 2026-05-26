package agents

import (
	"sort"
	"time"
)

type Decision string

const (
	CloseNow Decision = "close_now"
	KeepIdle Decision = "keep_idle"
)

// Policy records local idle-session guardrails for future explicit reuse.
// Current execution writes internal checkpoints and closes live agents/containers after each step.
type Policy struct {
	IdleRetention time.Duration
	MaxIdleAgents int
}

type Session struct {
	ID         string
	LastActive time.Time
	IdleUntil  time.Time
}

type Manager struct {
	policy Policy
	idle   []Session
}

func NewManager(policy Policy) *Manager {
	return &Manager{policy: policy}
}

func (m *Manager) Release(session Session, now time.Time) Decision {
	if m == nil || m.policy.IdleRetention <= 0 || m.policy.MaxIdleAgents <= 0 {
		return CloseNow
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	session.LastActive = now
	session.IdleUntil = now.Add(m.policy.IdleRetention)
	m.idle = append(m.idle, session)
	m.pruneExpired(now)
	m.enforceMaxIdle()
	for _, idle := range m.idle {
		if idle.ID == session.ID {
			return KeepIdle
		}
	}
	return CloseNow
}

func (m *Manager) Prune(now time.Time) []Session {
	if m == nil {
		return nil
	}
	return m.pruneExpired(now)
}

func (m *Manager) IdleCount() int {
	if m == nil {
		return 0
	}
	return len(m.idle)
}

func (m *Manager) pruneExpired(now time.Time) []Session {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	kept := m.idle[:0]
	var closed []Session
	for _, session := range m.idle {
		if !session.IdleUntil.IsZero() && !now.Before(session.IdleUntil) {
			closed = append(closed, session)
			continue
		}
		kept = append(kept, session)
	}
	m.idle = kept
	return closed
}

func (m *Manager) enforceMaxIdle() {
	if m.policy.MaxIdleAgents < 0 || len(m.idle) <= m.policy.MaxIdleAgents {
		return
	}
	sort.Slice(m.idle, func(i, j int) bool { return m.idle[i].LastActive.Before(m.idle[j].LastActive) })
	m.idle = append([]Session(nil), m.idle[len(m.idle)-m.policy.MaxIdleAgents:]...)
}
