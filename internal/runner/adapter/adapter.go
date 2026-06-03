package adapter

import (
	"context"

	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

// AgentAdapter is the narrow seam where real execution providers plug in later.
type AgentAdapter interface {
	Name() string
	Run(ctx context.Context, disp contract.Dispatch, sink runnerio.Sink) (report.Report, error)
}
