package managerhttp

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs the manager HTTP suite under goleak so server and async request
// goroutines must be drained before tests finish.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
