package runner

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs the runner suite under goleak so dispatch and cancellation goroutines
// must be drained before tests finish.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
