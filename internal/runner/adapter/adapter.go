package adapter

import (
	"context"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

// EventSink is the adapter's reporting channel back to the Manager through the Runner session.
type EventSink interface {
	Emit(ctx context.Context, ev event.Event) error
}

// AgentAdapter is the narrow seam where real execution providers plug in later.
type AgentAdapter interface {
	Name() string
	Run(ctx context.Context, disp contract.Dispatch, sink EventSink) (report.Report, error)
}
