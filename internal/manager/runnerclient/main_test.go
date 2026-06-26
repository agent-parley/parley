package runnerclient

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs the runnerclient suite under goleak so session, heartbeat, child-process,
// and artifact-transfer goroutines cannot outlive their tests.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
