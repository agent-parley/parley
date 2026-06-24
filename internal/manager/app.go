package manager

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	managerhttp "github.com/agent-parley/parley/internal/manager/http"
	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/runnerclient"
	"github.com/agent-parley/parley/internal/manager/settings"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

const (
	spawnedRunnerRestartCap    = 3
	spawnedRunnerRestartWindow = time.Minute
	spawnedRunnerRestartDelay  = 250 * time.Millisecond
)

type Config struct {
	Addr       string
	DataDir    string
	RunnerBin  string
	Adapter    string
	ProjectID  string
	SourceRepo string
	Settings   settings.Settings
}

type App struct {
	cfg       Config
	store     *store.Store
	runner    *runnerProxy
	engine    *orchestrator.Engine
	http      *managerhttp.Server
	runnerEnv []string

	mu             sync.Mutex
	runnerID       string
	restartHistory []time.Time
	closing        bool
}

func New(ctx context.Context, cfg Config) (*App, error) {
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:8080"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = ".parley-data"
	}
	if cfg.Adapter == "" {
		cfg.Adapter = "noop"
	}
	cfg.Settings = settings.ResolveDefaults(cfg.Settings)
	if err := settings.Validate(cfg.Settings); err != nil {
		return nil, err
	}

	st, err := store.Open(ctx, cfg.DataDir)
	if err != nil {
		return nil, err
	}
	projectID := cfg.ProjectID
	if projectID == "" {
		projectID = getenv("PARLEY_PROJECT_ID", store.DefaultProjectID)
	}
	sourceRepo := cfg.SourceRepo
	if sourceRepo == "" {
		sourceRepo = os.Getenv("PARLEY_SOURCE_REPO")
	}
	if sourceRepo == "" {
		if cwd, getErr := os.Getwd(); getErr == nil {
			sourceRepo = cwd
		}
	}
	project, err := st.EnsureProject(ctx, store.ProjectSpec{
		ID:                 projectID,
		Name:               "Default project",
		RepositoryPath:     sourceRepo,
		QueueAutoWhenReady: cfg.Settings.Queue.AutoWhenReady,
		QueueMaxConcurrent: cfg.Settings.Queue.MaxConcurrent,
		QueueBacklogCap:    cfg.Settings.Queue.BacklogCap,
	})
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	renderer, err := web.NewRenderer()
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	hub := managerhttp.NewHub()
	runner := newRunnerProxy()
	queuePolicy := orchestrator.QueuePolicy{
		AutoWhenReady: project.QueueAutoWhenReady,
		MaxConcurrent: project.QueueMaxConcurrent,
		BacklogCap:    project.QueueBacklogCap,
	}
	notificationSink := managerhttp.NewInAppNotificationSink(st, hub, renderer)
	engine := orchestrator.NewEngineWithOptions(st, runner, renderer, hub, orchestrator.EngineOptions{
		ImplementationAdapter: cfg.Adapter,
		PlanningAdapter:       cfg.Adapter,
		ConversationAdapter:   cfg.Adapter,
		ValidationAdapter:     "validation",
		DataRoot:              cfg.DataDir,
		ProjectID:             project.ID,
		QueuePolicy:           &queuePolicy,
		RunnerSlots:           1,
		NotificationSinks:     []orchestrator.NotificationSink{notificationSink},
	})
	app := &App{
		cfg:    cfg,
		store:  st,
		runner: runner,
		engine: engine,
		runnerEnv: []string{
			"PARLEY_ADAPTER=" + cfg.Adapter,
			"PARLEY_DATA_DIR=" + cfg.DataDir,
			"PARLEY_PROJECT_ID=" + project.ID,
			"PARLEY_SOURCE_REPO=" + sourceRepo,
			"PARLEY_WORKSPACE_ROOT=" + project.WorkspacePath,
		},
	}
	if err := app.startRunnerChild(ctx, false); err != nil {
		_ = st.Close()
		return nil, err
	}
	engineRunner := engine
	httpServer := managerhttp.NewServer(cfg.Addr, st, engineRunner, hub, renderer)
	app.http = httpServer
	if err := engine.RecoverAndDispatch(ctx); err != nil {
		_ = app.close(context.Background())
		return nil, err
	}
	return app, nil
}

func (a *App) startRunnerChild(ctx context.Context, restarted bool) error {
	a.mu.Lock()
	runnerID := a.runnerID
	a.mu.Unlock()
	client, err := runnerclient.StartChildWithEnvAndID(ctx, a.cfg.RunnerBin, a.runnerEnv, runnerID)
	if err != nil {
		return err
	}
	if runnerID == "" {
		runnerID = client.RunnerID()
		a.mu.Lock()
		a.runnerID = runnerID
		a.mu.Unlock()
	}
	client.SetHandlers(a.engine.HandleRunnerEvent, a.engine.HandleRunnerArtifact, a.engine.HandleRunnerReport, a.engine.HandleRunnerResult, a.engine.HandleRunnerLog)
	client.SetLifecycleHandlers(a.handleHeartbeatMissed, a.handleHeartbeatRecovered, a.handleRunnerDown)
	if err := a.store.UpsertRunnerWithOrigin(ctx, client.Ready().RunnerID, store.RunnerStatusConnected, store.RunnerOriginSpawned, client.Ready().Capabilities); err != nil {
		_ = client.Close(context.Background())
		return err
	}
	a.runner.Set(client)
	if !restarted {
		if err := a.appendRunnerEvent(ctx, runnerID, "runner.registered", "spawned runner registered", map[string]any{"status": store.RunnerStatusConnected}); err != nil {
			return err
		}
		return a.appendRunnerEvent(ctx, runnerID, "runner.ready", "spawned runner ready", map[string]any{"status": store.RunnerStatusConnected, "capabilities": client.Ready().Capabilities})
	}
	if err := a.appendRunnerEvent(ctx, runnerID, "runner.reconnected", "spawned runner reconnected", map[string]any{"status": store.RunnerStatusConnected}); err != nil {
		return err
	}
	go func() {
		if err := a.engine.DispatchPending(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "parley: dispatch pending after runner reconnect: %v\n", err)
		}
	}()
	return nil
}

func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() { errCh <- a.http.Run(ctx) }()
	select {
	case err := <-errCh:
		_ = a.close(context.Background())
		if err != nil {
			return fmt.Errorf("manager http: %w", err)
		}
		return nil
	case <-ctx.Done():
		err := <-errCh
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = a.close(shutdownCtx)
		return err
	}
}

func (a *App) handleHeartbeatMissed(ctx context.Context, runnerID string, missed int, err error) {
	if a.isClosing() {
		return
	}
	_ = a.store.UpdateRunnerHealth(ctx, runnerID, store.RunnerStatusSuspect, missed)
	_ = a.appendRunnerEvent(ctx, runnerID, "runner.heartbeat_missed", "runner heartbeat missed", map[string]any{"status": store.RunnerStatusSuspect, "missed_heartbeats": missed, "error": errString(err)})
}

func (a *App) handleHeartbeatRecovered(ctx context.Context, runnerID string) {
	if a.isClosing() {
		return
	}
	_ = a.store.UpdateRunnerHealth(ctx, runnerID, store.RunnerStatusConnected, 0)
	_ = a.appendRunnerEvent(ctx, runnerID, "runner.reconnected", "runner heartbeat recovered", map[string]any{"status": store.RunnerStatusConnected})
}

func (a *App) handleRunnerDown(ctx context.Context, runnerID, reason string, err error) {
	if a.isClosing() {
		return
	}
	oldClient := a.runner.current()
	a.runner.ClearForRestart()
	if oldClient != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = oldClient.Close(shutdownCtx)
		cancel()
	}
	_ = a.store.UpdateRunnerHealth(ctx, runnerID, store.RunnerStatusDown, 0)
	_ = a.appendRunnerEvent(ctx, runnerID, "runner.down", "runner down", map[string]any{"status": store.RunnerStatusDown, "reason": reason, "error": errString(err)})
	_ = a.engine.HandleRunnerDown(context.Background(), runnerID, reason)
	runner, getErr := a.store.GetRunner(ctx, runnerID)
	if getErr != nil || runner.Origin != store.RunnerOriginSpawned {
		a.runner.MarkDown(fmt.Errorf("runner %s down: %s", runnerID, reason))
		return
	}
	if !a.allowRestart(runnerID) {
		a.runner.MarkDown(fmt.Errorf("runner %s down: crash loop", runnerID))
		return
	}
	go func() {
		time.Sleep(spawnedRunnerRestartDelay)
		if a.isClosing() {
			return
		}
		if err := a.startRunnerChild(context.Background(), true); err != nil {
			_ = a.store.UpdateRunnerHealth(context.Background(), runnerID, store.RunnerStatusDown, 0)
			_ = a.appendRunnerEvent(context.Background(), runnerID, "runner.down", "runner restart failed", map[string]any{"status": store.RunnerStatusDown, "reason": "restart_failed", "error": err.Error()})
			a.runner.MarkDown(err)
		}
	}()
}

func (a *App) allowRestart(runnerID string) bool {
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	cutoff := now.Add(-spawnedRunnerRestartWindow)
	kept := a.restartHistory[:0]
	for _, t := range a.restartHistory {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	a.restartHistory = kept
	if len(a.restartHistory) >= spawnedRunnerRestartCap {
		_ = a.appendRunnerEvent(context.Background(), runnerID, "runner.down", "runner crash-loop guard tripped", map[string]any{"status": store.RunnerStatusDown, "reason": "crash_loop", "restart_cap": spawnedRunnerRestartCap, "window_seconds": int(spawnedRunnerRestartWindow.Seconds())})
		return false
	}
	a.restartHistory = append(a.restartHistory, now)
	return true
}

func (a *App) appendRunnerEvent(ctx context.Context, runnerID, typ, summary string, data map[string]any) error {
	if data == nil {
		data = map[string]any{}
	}
	data["runner_id"] = runnerID
	if _, ok := data["origin"]; !ok {
		data["origin"] = store.RunnerOriginSpawned
	}
	_, err := a.store.AppendEvent(ctx, event.Event{
		SchemaVersion: event.SchemaVersion,
		Type:          typ,
		Actor:         event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"},
		Summary:       summary,
		Data:          data,
	})
	return err
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func (a *App) close(ctx context.Context) error {
	a.mu.Lock()
	a.closing = true
	a.mu.Unlock()
	if a.runner != nil {
		_ = a.runner.Close(ctx)
	}
	if a.store != nil {
		return a.store.Close()
	}
	return nil
}

func (a *App) isClosing() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closing
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type runnerProxy struct {
	mu      sync.Mutex
	client  *runnerclient.Client
	ready   chan struct{}
	downErr error
}

func newRunnerProxy() *runnerProxy {
	return &runnerProxy{ready: make(chan struct{})}
}

func (p *runnerProxy) Set(client *runnerclient.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.client = client
	p.downErr = nil
	if p.ready != nil {
		close(p.ready)
		p.ready = nil
	}
}

func (p *runnerProxy) ClearForRestart() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.client = nil
	p.downErr = nil
	p.ready = make(chan struct{})
}

func (p *runnerProxy) MarkDown(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.client = nil
	p.downErr = err
	if p.ready != nil {
		close(p.ready)
		p.ready = nil
	}
}

func (p *runnerProxy) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	client, err := p.waitClient(ctx)
	if err != nil {
		return report.Report{}, err
	}
	return client.Dispatch(ctx, disp)
}

func (p *runnerProxy) CancelAttempt(ctx context.Context, runID, taskID, attemptID string) error {
	client, err := p.waitClient(ctx)
	if err != nil {
		return err
	}
	return client.CancelAttempt(ctx, runID, taskID, attemptID)
}

func (p *runnerProxy) Close(ctx context.Context) error {
	client := p.current()
	if client == nil {
		return nil
	}
	return client.Close(ctx)
}

func (p *runnerProxy) waitClient(ctx context.Context) (*runnerclient.Client, error) {
	for {
		p.mu.Lock()
		client := p.client
		if client != nil {
			p.mu.Unlock()
			return client, nil
		}
		if p.downErr != nil {
			err := p.downErr
			p.mu.Unlock()
			return nil, fmt.Errorf("runner unavailable: %w", err)
		}
		ready := p.ready
		if ready == nil {
			ready = make(chan struct{})
			p.ready = ready
		}
		p.mu.Unlock()
		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *runnerProxy) Ready() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client != nil && p.downErr == nil
}

func (p *runnerProxy) current() *runnerclient.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client
}
