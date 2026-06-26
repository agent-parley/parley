package provider

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs the provider suite under goleak so process wait/output goroutines
// must exit before package tests complete.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
