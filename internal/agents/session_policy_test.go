package agents_test

import (
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/agents"
)

func TestReleaseClosesWhenRetentionZero(t *testing.T) {
	manager := agents.NewManager(agents.Policy{IdleRetention: 0, MaxIdleAgents: 1})
	if got := manager.Release(agents.Session{ID: "one"}, time.Now()); got != agents.CloseNow {
		t.Fatalf("Release()=%s, want close_now", got)
	}
	if manager.IdleCount() != 0 {
		t.Fatalf("idle count=%d", manager.IdleCount())
	}
}

func TestReleaseKeepsUntilRetention(t *testing.T) {
	now := time.Now()
	manager := agents.NewManager(agents.Policy{IdleRetention: time.Minute, MaxIdleAgents: 1})
	if got := manager.Release(agents.Session{ID: "one"}, now); got != agents.KeepIdle {
		t.Fatalf("Release()=%s, want keep_idle", got)
	}
	if manager.IdleCount() != 1 {
		t.Fatalf("idle count=%d", manager.IdleCount())
	}
	closed := manager.Prune(now.Add(time.Minute))
	if len(closed) != 1 || closed[0].ID != "one" || manager.IdleCount() != 0 {
		t.Fatalf("unexpected prune closed=%+v idle=%d", closed, manager.IdleCount())
	}
}

func TestMaxIdleEvictsOldest(t *testing.T) {
	now := time.Now()
	manager := agents.NewManager(agents.Policy{IdleRetention: time.Hour, MaxIdleAgents: 1})
	if got := manager.Release(agents.Session{ID: "old"}, now); got != agents.KeepIdle {
		t.Fatalf("old release=%s", got)
	}
	if got := manager.Release(agents.Session{ID: "new"}, now.Add(time.Second)); got != agents.KeepIdle {
		t.Fatalf("new release=%s", got)
	}
	if manager.IdleCount() != 1 {
		t.Fatalf("idle count=%d", manager.IdleCount())
	}
}

func TestMaxIdleZeroDisablesRetention(t *testing.T) {
	manager := agents.NewManager(agents.Policy{IdleRetention: time.Minute, MaxIdleAgents: 0})
	if got := manager.Release(agents.Session{ID: "one"}, time.Now()); got != agents.CloseNow {
		t.Fatalf("Release()=%s, want close_now", got)
	}
}
