package manager

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs the manager suite under goleak so App/HTTP/runner child lifecycle
// goroutines must be fully stopped before package tests complete.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
