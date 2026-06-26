package session

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs the runner session suite under goleak so websocket server/session
// goroutines must be drained before tests finish.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
