// Package runnerio defines the runner-side reporting channel shared by adapters
// and sandbox providers. It lives in its own package so both can depend on it
// without adapter and provider importing each other.
package runnerio

import (
	"context"

	"github.com/agent-parley/parley/internal/shared/event"
)

// Artifact is a durable output produced during a run. The runner transfers it
// to the Manager over the session as a first-class artifact (never inlined in a
// report payload). Run/task/attempt identity is filled in by the runner from
// the dispatch, so adapters and providers only supply the content and metadata.
type Artifact struct {
	ID        string
	Name      string
	Kind      string
	MediaType string
	Content   []byte
}

// Sink is the adapter/provider reporting channel back to the Manager through the
// runner session. Emit streams events; Artifact transfers durable outputs.
type Sink interface {
	Emit(ctx context.Context, ev event.Event) error
	Artifact(ctx context.Context, art Artifact) error
}
