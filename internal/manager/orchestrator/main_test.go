package orchestrator

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs the orchestrator suite under goleak so any goroutine that outlives a test
// — the recurring "leaked engine goroutines race t.TempDir cleanup" flake class — becomes a
// hard, deterministic failure instead of a load-dependent flake. Engine-backed tests must
// stop the engine via newRecordingEngine / registerEngineTeardown (which call Engine.Shutdown
// before the store is closed and the temp dir removed); store-only tests must close the store.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
