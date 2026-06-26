package protocol

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs the protocol suite under goleak so websocket reader/writer goroutines
// must stop with their Session before tests finish.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
