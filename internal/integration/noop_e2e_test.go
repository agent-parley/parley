package integration_test

import (
	"context"
	"testing"
	"time"

	managerhttp "github.com/agent-parley/parley/internal/manager/http"
	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/runnerclient"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	runnersession "github.com/agent-parley/parley/internal/runner/session"
	"github.com/agent-parley/parley/internal/shared/ids"
)

func TestNoopEndToEndRun(t *testing.T) {
	serverCtx, stop := context.WithCancel(context.Background())
	defer stop()
	srv, url, err := runnersession.Listen()
	if err != nil {
		t.Fatalf("listen runner: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(serverCtx) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := runnerclient.Dial(ctx, url, ids.New("runner"))
	if err != nil {
		t.Fatalf("dial runner: %v", err)
	}
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	renderer, err := web.NewRenderer()
	if err != nil {
		t.Fatalf("renderer: %v", err)
	}
	hub := managerhttp.NewHub()
	engine := orchestrator.NewEngine(st, client, renderer, hub)
	client.SetHandlers(engine.HandleRunnerEvent, engine.HandleRunnerArtifact, engine.HandleRunnerReport, engine.HandleRunnerResult, engine.HandleRunnerLog)

	runID, err := engine.StartRun(ctx, "add a local-first harness")
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	var bundle store.RunBundle
	for {
		bundle, err = st.RunBundle(ctx, runID)
		if err != nil {
			t.Fatalf("bundle: %v", err)
		}
		if bundle.Run.Status == store.RunStatusCompleted {
			break
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("run did not complete; last status=%s", bundle.Run.Status)
		}
	}
	if len(bundle.Stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(bundle.Stages))
	}
	for _, stage := range bundle.Stages {
		if stage.Status != store.RunStatusCompleted {
			t.Fatalf("stage not completed: %+v", stage)
		}
	}
	var reportArtifacts, adapterArtifacts int
	for _, artifact := range bundle.Artifacts {
		switch artifact.Kind {
		case "report":
			reportArtifacts++
		case "adapter_output":
			adapterArtifacts++
		}
	}
	if reportArtifacts != 2 {
		t.Fatalf("expected 2 report artifacts, got %d", reportArtifacts)
	}
	// D3: the noop adapter's artifact must arrive as a first-class transfer over
	// the session and be stored, not inlined in the report payload.
	if adapterArtifacts != 1 {
		t.Fatalf("expected 1 adapter_output artifact transferred over the session, got %d", adapterArtifacts)
	}
	if len(bundle.Events) < 7 {
		t.Fatalf("expected live workflow events, got %d", len(bundle.Events))
	}
	_ = client.Close(context.Background())
	stop()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}
