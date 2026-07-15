package orchestrator

import "context"

type globalRunCapacityError struct {
	snapshot globalCapacitySnapshot
}

func (e *globalRunCapacityError) Error() string { return "global concurrency cap reached" }
func (e *globalRunCapacityError) Unwrap() error { return ErrNoRunnerSlots }

// globalCapacitySnapshot is a point-in-time view of the shared run/turn
// execution umbrella. Its mutex is deliberately independent from queueMu and
// conversationMu; callers use these methods only as short leaf operations.
type globalCapacitySnapshot struct {
	runsInflight        int
	turnsInflight       int
	globalMaxConcurrent int
	interactiveReserve  int
}

func (e *Engine) reserveRunAdmission(ctx context.Context) error {
	available, err := e.availableDispatchSlots(ctx)
	if err != nil {
		return err
	}
	if available <= 0 {
		return ErrNoRunnerSlots
	}
	snapshot, ok := e.tryReserveGlobalRun()
	if !ok {
		return &globalRunCapacityError{snapshot: snapshot}
	}
	return nil
}

func (e *Engine) tryReserveGlobalRun() (globalCapacitySnapshot, bool) {
	e.globalMu.Lock()
	defer e.globalMu.Unlock()

	if e.globalMaxConcurrent > 0 && e.runsInflight+e.turnsInflight >= e.globalMaxConcurrent-e.interactiveReserve {
		return e.globalCapacitySnapshotLocked(), false
	}
	e.runsInflight++
	return e.globalCapacitySnapshotLocked(), true
}

func (e *Engine) releaseGlobalRun() {
	e.globalMu.Lock()
	defer e.globalMu.Unlock()
	if e.runsInflight <= 0 {
		panic("orchestrator: global run reservation underflow")
	}
	e.runsInflight--
}

func (e *Engine) tryReserveGlobalTurn() (globalCapacitySnapshot, bool) {
	e.globalMu.Lock()
	defer e.globalMu.Unlock()

	if e.globalMaxConcurrent > 0 && e.runsInflight+e.turnsInflight >= e.globalMaxConcurrent {
		return e.globalCapacitySnapshotLocked(), false
	}
	e.turnsInflight++
	return e.globalCapacitySnapshotLocked(), true
}

func (e *Engine) releaseGlobalTurn() {
	e.globalMu.Lock()
	defer e.globalMu.Unlock()
	if e.turnsInflight <= 0 {
		panic("orchestrator: global turn reservation underflow")
	}
	e.turnsInflight--
}

func (e *Engine) globalCapacitySnapshot() globalCapacitySnapshot {
	e.globalMu.Lock()
	defer e.globalMu.Unlock()
	return e.globalCapacitySnapshotLocked()
}

func (e *Engine) globalCapacitySnapshotLocked() globalCapacitySnapshot {
	return globalCapacitySnapshot{
		runsInflight:        e.runsInflight,
		turnsInflight:       e.turnsInflight,
		globalMaxConcurrent: e.globalMaxConcurrent,
		interactiveReserve:  e.interactiveReserve,
	}
}

func (s globalCapacitySnapshot) eventData() map[string]any {
	return map[string]any{
		"runs_inflight":  s.runsInflight,
		"turns_inflight": s.turnsInflight,
		"global_max":     s.globalMaxConcurrent,
		"reserve":        s.interactiveReserve,
	}
}
