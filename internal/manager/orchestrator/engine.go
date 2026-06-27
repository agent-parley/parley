package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/agent-parley/parley/internal/manager/contextpack"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/runner/worktree"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/protocol"
	"github.com/agent-parley/parley/internal/shared/report"
)

type Runner interface {
	Dispatch(context.Context, contract.Dispatch) (report.Report, error)
	CancelAttempt(context.Context, string, string, string) error
}

type FragmentRenderer interface {
	RenderRunFragments(store.RunBundle) (string, error)
}

type Broadcaster interface {
	Broadcast(runID string, ev event.Event, fragment string)
}

type NotificationSink interface {
	Notify(context.Context, store.Notification) error
}

type QueuePolicy struct {
	AutoWhenReady bool
	MaxConcurrent int
	BacklogCap    int
}

type QueueState struct {
	Policy                 QueuePolicy
	Pending                int
	Running                int
	RunnerSlots            int
	ReadyRunnerSlots       int
	EffectiveMaxConcurrent int
}

type QueueBacklogFullError struct {
	Pending int
	Cap     int
}

func (e QueueBacklogFullError) Error() string {
	return fmt.Sprintf("queue backlog full: %d pending runs at cap %d", e.Pending, e.Cap)
}

var (
	ErrNoRunnerSlots = errors.New("no runner slot available")
	ErrRunNotPending = errors.New("run is not pending")
	ErrRunHeld       = errors.New("run held by dispatch gate")
)

const taskContractArtifactKind = "task_contract"

type Engine struct {
	store             *store.Store
	runner            Runner
	renderer          FragmentRenderer
	broadcast         Broadcaster
	notificationSinks []NotificationSink
	graph             Graph

	mu          sync.Mutex
	activeRuns  map[string]context.CancelFunc
	activeStage map[string]store.Stage
	cancelling  map[string]bool
	runnerDown  map[string]string
	closed      bool
	rootCtx     context.Context
	rootCancel  context.CancelFunc
	wg          sync.WaitGroup
	queueMu     sync.Mutex

	queuePolicy           QueuePolicy
	runnerSlots           int
	conversationBudget    int
	conversationIdleTTL   time.Duration
	conversationMu        sync.Mutex
	conversationSessions  map[string]*conversationSession
	conversationReady     []string
	gate                  func(context.Context, store.Run) (bool, error)
	implementationAdapter string
	planningAdapter       string
	conversationAdapter   string
	validationAdapter     string
	dataRoot              string
	projectID             string
	gitAuthorName         string
	gitAuthorEmail        string
	gitExecutable         string
	contextAssembler      *contextpack.Assembler
}

type EngineOptions struct {
	ImplementationAdapter string
	PlanningAdapter       string
	ConversationAdapter   string
	ValidationAdapter     string
	DataRoot              string
	ProjectID             string
	GitAuthorName         string
	GitAuthorEmail        string
	GitExecutable         string
	QueuePolicy           *QueuePolicy
	RunnerSlots           int
	ConversationBudget    int
	ConversationIdleTTL   time.Duration
	ContextAssembler      *contextpack.Assembler
	NotificationSinks     []NotificationSink
}

type workerSnapshot struct {
	WorktreePath   string
	BaseSHA        string
	BaseTreeSHA    string
	WorkerTreeSHA  string
	DiffArtifactID string
}

func NewEngine(st *store.Store, runner Runner, renderer FragmentRenderer, broadcast Broadcaster) *Engine {
	return NewEngineWithOptions(st, runner, renderer, broadcast, EngineOptions{})
}

func defaultQueuePolicy() QueuePolicy {
	return QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}
}

func normalizeQueuePolicy(policy QueuePolicy) QueuePolicy {
	defaults := defaultQueuePolicy()
	if policy.MaxConcurrent < 1 {
		policy.MaxConcurrent = defaults.MaxConcurrent
	}
	if policy.BacklogCap < 1 {
		policy.BacklogCap = defaults.BacklogCap
	}
	return policy
}

func NewEngineWithOptions(st *store.Store, runner Runner, renderer FragmentRenderer, broadcast Broadcaster, opts EngineOptions) *Engine {
	implementationAdapter := opts.ImplementationAdapter
	if implementationAdapter == "" {
		implementationAdapter = "noop"
	}
	planningAdapter := opts.PlanningAdapter
	if planningAdapter == "" {
		planningAdapter = implementationAdapter
	}
	conversationAdapter := opts.ConversationAdapter
	if conversationAdapter == "" {
		conversationAdapter = planningAdapter
	}
	validationAdapter := opts.ValidationAdapter
	if validationAdapter == "" {
		validationAdapter = "validation"
	}
	dataRoot := opts.DataRoot
	if dataRoot == "" {
		dataRoot = ".parley-data"
	}
	projectID := opts.ProjectID
	if projectID == "" {
		projectID = store.DefaultProjectID
	}
	queuePolicy := defaultQueuePolicy()
	if opts.QueuePolicy != nil {
		queuePolicy = normalizeQueuePolicy(*opts.QueuePolicy)
	}
	runnerSlots := opts.RunnerSlots
	if runnerSlots <= 0 {
		runnerSlots = 1
	}
	conversationBudget := opts.ConversationBudget
	if conversationBudget <= 0 {
		conversationBudget = 1
	}
	conversationIdleTTL := opts.ConversationIdleTTL
	if conversationIdleTTL <= 0 {
		conversationIdleTTL = 15 * time.Minute
	}
	contextAssembler := opts.ContextAssembler
	if contextAssembler == nil {
		contextAssembler = contextpack.NewAssembler(contextpack.Options{
			Providers: []contextpack.SourceProvider{
				contextpack.RepoEvidenceProvider{Git: opts.GitExecutable},
			},
		})
	}
	rootCtx, rootCancel := context.WithCancel(context.Background())
	return &Engine{
		store:                 st,
		runner:                runner,
		renderer:              renderer,
		broadcast:             broadcast,
		notificationSinks:     opts.NotificationSinks,
		graph:                 NewGraph(),
		activeRuns:            map[string]context.CancelFunc{},
		activeStage:           map[string]store.Stage{},
		cancelling:            map[string]bool{},
		runnerDown:            map[string]string{},
		queuePolicy:           queuePolicy,
		runnerSlots:           runnerSlots,
		conversationBudget:    conversationBudget,
		conversationIdleTTL:   conversationIdleTTL,
		conversationSessions:  map[string]*conversationSession{},
		implementationAdapter: implementationAdapter,
		planningAdapter:       planningAdapter,
		conversationAdapter:   conversationAdapter,
		validationAdapter:     validationAdapter,
		dataRoot:              dataRoot,
		projectID:             projectID,
		gitAuthorName:         opts.GitAuthorName,
		gitAuthorEmail:        opts.GitAuthorEmail,
		gitExecutable:         opts.GitExecutable,
		contextAssembler:      contextAssembler,
		rootCtx:               rootCtx,
		rootCancel:            rootCancel,
	}
}

func (e *Engine) StartRun(ctx context.Context, idea string) (string, error) {
	return e.StartProjectRun(ctx, e.projectID, idea)
}

func (e *Engine) StartProjectRun(ctx context.Context, projectID, idea string) (string, error) {
	return e.StartProjectRunInput(ctx, projectID, contract.TaskInput{Idea: idea})
}

func (e *Engine) StartRunInput(ctx context.Context, input contract.TaskInput) (string, error) {
	return e.StartProjectRunInput(ctx, e.projectID, input)
}

func (e *Engine) StartProjectRunInput(ctx context.Context, projectID string, input contract.TaskInput) (string, error) {
	if projectID == "" {
		projectID = e.projectID
	}
	input.RefinementLevel = contract.NormalizeRefinementLevel(input.RefinementLevel)
	if err := contract.ValidateRefinementLevel(input.RefinementLevel); err != nil {
		return "", err
	}
	e.queueMu.Lock()
	pending, err := e.store.CountRunsByProjectStatus(ctx, projectID, store.RunStatusPending)
	if err != nil {
		e.queueMu.Unlock()
		return "", err
	}
	if pending >= e.queuePolicy.BacklogCap {
		_ = e.emitQueueEvent(ctx, projectID, "queue.rejected_backlog_full", "queue backlog full", map[string]any{"pending": pending, "backlog_cap": e.queuePolicy.BacklogCap})
		e.queueMu.Unlock()
		return "", QueueBacklogFullError{Pending: pending, Cap: e.queuePolicy.BacklogCap}
	}
	wr, err := e.createQueuedRun(ctx, projectID, input)
	if err != nil {
		e.queueMu.Unlock()
		return "", err
	}
	data := map[string]any{"idea": input.Idea, "refinement_level": wr.Run.RefinementLevel, "workflow_template_id": wr.Run.WorkflowTemplateID}
	if wr.Task.ConversationID != "" {
		data["conversation_id"] = wr.Task.ConversationID
	}
	if _, err := e.emit(ctx, runEvent(wr, "run.created", event.Actor{Kind: event.ActorKindUser, ID: "local"}, "run created", data)); err != nil {
		e.queueMu.Unlock()
		return "", err
	}
	if err := e.emitQueueEvent(ctx, projectID, "queue.enqueued", "run enqueued", map[string]any{"project_id": projectID, "run_id": wr.Run.ID, "pending": pending + 1, "backlog_cap": e.queuePolicy.BacklogCap}); err != nil {
		e.queueMu.Unlock()
		return "", err
	}
	e.queueMu.Unlock()
	if e.queuePolicy.AutoWhenReady {
		e.spawn(func() {
			if err := e.DispatchPending(e.rootCtx); err != nil {
				log.Printf("dispatch pending failed: %v", err)
			}
		})
	}
	return wr.Run.ID, nil
}

func (e *Engine) createQueuedRun(ctx context.Context, projectID string, input contract.TaskInput) (store.WorkflowRun, error) {
	if _, err := e.store.GetProject(ctx, projectID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return store.WorkflowRun{}, err
		}
		if _, err := e.store.EnsureProject(ctx, store.ProjectSpec{ID: projectID, Name: projectID, QueueAutoWhenReady: e.queuePolicy.AutoWhenReady, QueueMaxConcurrent: e.queuePolicy.MaxConcurrent, QueueBacklogCap: e.queuePolicy.BacklogCap}); err != nil {
			return store.WorkflowRun{}, err
		}
	}
	wr, err := e.store.CreateWorkflowRunForProjectInput(ctx, projectID, input)
	if err != nil {
		return store.WorkflowRun{}, err
	}
	template, err := e.store.GetWorkflowTemplate(ctx, wr.Run.WorkflowTemplateID)
	if err != nil {
		return store.WorkflowRun{}, err
	}
	wr, err = e.configureRuntimeStageAdapters(ctx, wr, template)
	if err != nil {
		return store.WorkflowRun{}, err
	}
	if err := e.store.SaveWorkflowSnapshot(ctx, wr.Run.ID, e.workflowSnapshot(wr, template, "", "", true)); err != nil {
		return store.WorkflowRun{}, err
	}
	return wr, nil
}

func (e *Engine) configureRuntimeStageAdapters(ctx context.Context, wr store.WorkflowRun, template workflow.Template) (store.WorkflowRun, error) {
	stages, err := e.store.ListStagesForAttempt(ctx, wr.Run.ID, wr.Attempt.ID)
	if err != nil {
		return store.WorkflowRun{}, err
	}
	byWorkflowStageID := map[string]store.Stage{}
	for _, stage := range stages {
		byWorkflowStageID[stage.WorkflowStageID] = stage
	}
	for _, templateStage := range template.Stages {
		stage, ok := byWorkflowStageID[templateStage.ID]
		if !ok {
			continue
		}
		desired := e.adapterForTemplateStage(templateStage)
		if stage.Adapter == desired {
			continue
		}
		if err := e.store.UpdateStageAdapter(ctx, stage.ID, desired); err != nil {
			return store.WorkflowRun{}, err
		}
	}
	return e.store.GetWorkflowRun(ctx, wr.Run.ID)
}

func (e *Engine) adapterForTemplateStage(stage workflow.StageTemplate) string {
	if stage.Type == workflow.StageTypeValidation {
		return e.validationAdapter
	}
	if stage.Type == workflow.StageTypeMemoryUpdate {
		return ""
	}
	if stage.Actor == workflow.ActorAgent {
		return e.implementationAdapter
	}
	return ""
}

func (e *Engine) StartQueuedRun(ctx context.Context, runID string) error {
	e.queueMu.Lock()
	defer e.queueMu.Unlock()
	return e.dispatchRunLocked(ctx, runID, "manual")
}

func (e *Engine) CancelRun(ctx context.Context, runID string) error {
	e.queueMu.Lock()
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		e.queueMu.Unlock()
		return err
	}
	if store.RunStatusIsTerminal(wr.Run.Status) {
		e.queueMu.Unlock()
		return nil
	}
	if wr.Run.Status == store.RunStatusPending {
		err := e.cancelQueuedRunLocked(ctx, wr)
		e.queueMu.Unlock()
		return err
	}
	if wr.Run.Status == store.RunStatusAwaitingHuman {
		e.queueMu.Unlock()
		return e.cancelRunTerminal(ctx, wr, "workflow cancelled while awaiting human input")
	}
	e.queueMu.Unlock()
	e.markCancelling(runID)
	if cancel := e.activeCancel(runID); cancel != nil {
		cancel()
	}
	if e.runner == nil {
		return nil
	}
	if err := e.runner.CancelAttempt(ctx, wr.Run.ID, wr.Task.ID, wr.Attempt.ID); err != nil && !errors.Is(err, protocol.ErrSessionClosed) {
		return err
	}
	return nil
}

func (e *Engine) cancelQueuedRunLocked(ctx context.Context, wr store.WorkflowRun) error {
	persisted, changed, err := e.store.UpdateRunStatusFromAndAppendEvent(ctx, wr.Run.ID, store.RunStatusPending, store.RunStatusCancelled, runEvent(wr, "run.cancelled", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "queued run cancelled", map[string]any{"terminal_status": store.RunStatusCancelled}))
	if err != nil {
		return err
	}
	if !changed {
		return ErrRunNotPending
	}
	_, err = e.publishEvent(ctx, persisted)
	return err
}

func (e *Engine) QueueState(ctx context.Context) (QueueState, error) {
	pending, err := e.store.CountRunsByProjectStatus(ctx, e.projectID, store.RunStatusPending)
	if err != nil {
		return QueueState{}, err
	}
	running, err := e.store.CountRunsByStatus(ctx, store.RunStatusRunning)
	if err != nil {
		return QueueState{}, err
	}
	readySlots := e.readyRunnerSlots()
	return QueueState{
		Policy:                 e.queuePolicy,
		Pending:                pending,
		Running:                running,
		RunnerSlots:            e.runnerSlots,
		ReadyRunnerSlots:       readySlots,
		EffectiveMaxConcurrent: minInt(e.queuePolicy.MaxConcurrent, readySlots),
	}, nil
}

func (e *Engine) RecoverAndDispatch(ctx context.Context) error {
	if err := e.failInterruptedRunning(ctx); err != nil {
		return err
	}
	return e.DispatchPending(ctx)
}

func (e *Engine) failInterruptedRunning(ctx context.Context) error {
	runs, err := e.store.ListRunsByStatus(ctx, store.RunStatusRunning, 0)
	if err != nil {
		return err
	}
	var joined []error
	for _, run := range runs {
		wr, err := e.store.GetWorkflowRun(ctx, run.ID)
		if err != nil {
			joined = append(joined, err)
			continue
		}
		persisted, changed, err := e.store.UpdateRunStatusFromAndAppendEvent(ctx, run.ID, store.RunStatusRunning, store.RunStatusFailed, runEvent(wr, "run.failed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run failed during manager restart recovery", map[string]any{
			"terminal_status": store.RunStatusFailed,
			"reason":          "manager_restarted",
			"retryable":       true,
		}))
		if err != nil {
			joined = append(joined, err)
			continue
		}
		if !changed {
			continue
		}
		if _, err := e.publishEvent(ctx, persisted); err != nil {
			joined = append(joined, err)
		}
	}
	return errors.Join(joined...)
}

func (e *Engine) DispatchPending(ctx context.Context) error {
	if !e.queuePolicy.AutoWhenReady {
		return nil
	}
	e.queueMu.Lock()
	defer e.queueMu.Unlock()
	return e.dispatchPendingLocked(ctx)
}

func (e *Engine) dispatchPendingLocked(ctx context.Context) error {
	// Walk the pending backlog once, oldest first (FIFO). Iterating a finite
	// snapshot — rather than repeatedly re-selecting the oldest pending run —
	// means a gate-held run can never make the dispatcher spin under queueMu,
	// and it is skipped instead of blocking younger runs (no head-of-line block).
	pending, err := e.store.ListRunsByStatus(ctx, store.RunStatusPending, 0)
	if err != nil {
		return err
	}
	for _, p := range pending {
		available, err := e.availableDispatchSlots(ctx)
		if err != nil {
			return err
		}
		if available <= 0 {
			return nil
		}
		switch err := e.dispatchRunLocked(ctx, p.ID, "auto"); {
		case err == nil:
		case errors.Is(err, ErrRunHeld), errors.Is(err, ErrRunNotPending):
			// Held by a future approval gate, or already claimed by another
			// path; skip it so the rest of the backlog still dispatches.
			continue
		case errors.Is(err, ErrNoRunnerSlots):
			return nil
		default:
			return err
		}
	}
	return nil
}

func (e *Engine) dispatchRunLocked(ctx context.Context, runID, trigger string) error {
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status != store.RunStatusPending {
		return ErrRunNotPending
	}
	allowed, err := e.dispatchGateAllowsRun(ctx, run)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrRunHeld
	}
	available, err := e.availableDispatchSlots(ctx)
	if err != nil {
		return err
	}
	if available <= 0 {
		return ErrNoRunnerSlots
	}
	queueEvent := newQueueEvent("queue.dispatched", "queued run dispatched", map[string]any{"project_id": run.ProjectID, "run_id": run.ID, "trigger": trigger, "max_concurrent": e.queuePolicy.MaxConcurrent, "runner_slots": e.runnerSlots})
	queueEvent.ProjectID = run.ProjectID
	persisted, changed, err := e.store.UpdateRunStatusFromAndAppendSystemEvent(ctx, run.ID, store.RunStatusPending, store.RunStatusRunning, queueEvent)
	if err != nil {
		return err
	}
	if !changed {
		return ErrRunNotPending
	}
	e.broadcast.Broadcast("", persisted, "")
	runCtx, cancel := context.WithCancel(e.rootCtx)
	e.registerActiveRun(run.ID, cancel)
	if !e.spawn(func() { e.executeRun(runCtx, run.ID) }) {
		cancel()
		e.unregisterActiveRun(run.ID)
	}
	return nil
}

func (e *Engine) dispatchGateAllowsRun(ctx context.Context, run store.Run) (bool, error) {
	// Gate-aware seam: auto-queue must reach execution through this dispatch boundary.
	// Track B approval gates can return false here to hold a pending run without
	// changing queue policy or bypassing the existing deterministic stage graph.
	// A held run (false) is skipped by the dispatcher and reported as ErrRunHeld by
	// manual start — it is never silently treated as dispatched.
	if e.gate != nil {
		return e.gate(ctx, run)
	}
	return true, nil
}

func (e *Engine) availableDispatchSlots(ctx context.Context) (int, error) {
	readySlots := e.readyRunnerSlots()
	if readySlots <= 0 {
		return 0, nil
	}
	capacity := minInt(e.queuePolicy.MaxConcurrent, readySlots)
	running, err := e.store.CountRunsByStatus(ctx, store.RunStatusRunning)
	if err != nil {
		return 0, err
	}
	if running >= capacity {
		return 0, nil
	}
	return capacity - running, nil
}

func (e *Engine) readyRunnerSlots() int {
	if e.runner == nil {
		return 0
	}
	if ready, ok := e.runner.(interface{ Ready() bool }); ok && !ready.Ready() {
		return 0
	}
	return e.runnerSlots
}

func (e *Engine) emitQueueEvent(ctx context.Context, projectID, typ, summary string, data map[string]any) error {
	ev := newQueueEvent(typ, summary, data)
	ev.ProjectID = projectID
	if ev.Data != nil && projectID != "" {
		ev.Data["project_id"] = projectID
	}
	_, err := e.emit(ctx, ev)
	return err
}

func newQueueEvent(typ, summary string, data map[string]any) event.Event {
	if data == nil {
		data = map[string]any{}
	}
	return event.Event{
		SchemaVersion: event.SchemaVersion,
		Type:          typ,
		Actor:         event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"},
		Summary:       summary,
		Data:          data,
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (e *Engine) registerActiveRun(runID string, cancel context.CancelFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.activeRuns[runID] = cancel
}

func (e *Engine) unregisterActiveRun(runID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.activeRuns, runID)
	delete(e.activeStage, runID)
	delete(e.cancelling, runID)
	delete(e.runnerDown, runID)
}

func (e *Engine) activeCancel(runID string) context.CancelFunc {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.activeRuns[runID]
}

// spawn runs fn in a goroutine tracked by the engine's WaitGroup. Once Shutdown
// has begun it is a no-op (returns false), so the auto-dispatch chain cannot keep
// spawning work past teardown. wg.Add happens under the same mutex that Shutdown
// uses to set closed, so there is no Add-after-Wait race.
func (e *Engine) spawn(fn func()) bool {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return false
	}
	e.wg.Add(1)
	e.mu.Unlock()
	go func() {
		defer e.wg.Done()
		fn()
	}()
	return true
}

// Shutdown stops the engine: it suppresses new spawns, cancels the root context
// (so ctx-aware in-flight work unwinds), and waits for tracked goroutines to drain
// or for ctx to expire. After it returns nil, no engine goroutine is running, so
// callers (notably tests) can safely close the store and remove the data dir.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	if !e.closed {
		e.closed = true
		e.mu.Unlock()
		e.rootCancel()
	} else {
		e.mu.Unlock()
	}
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("engine shutdown timed out with goroutines still running: %w", ctx.Err())
	}
}

func (e *Engine) markCancelling(runID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cancelling[runID] = true
}

func (e *Engine) isCancelling(runID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cancelling[runID]
}

// markRunnerDown records that runID's in-flight work failed because its runner
// disconnected. It is set both when a stage dispatch fails with ErrSessionClosed
// (the stage path itself observed the runner vanish) and from HandleRunnerDown,
// so whichever path finalizes the run, run.failed carries reason=runner_disconnected
// rather than a generic dispatch-failure summary.
func (e *Engine) markRunnerDown(runID, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.runnerDown[runID] = reason
}

func (e *Engine) runnerDownReason(runID string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	reason, ok := e.runnerDown[runID]
	return reason, ok
}

func (e *Engine) setActiveStage(runID string, stage store.Stage) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.activeStage[runID] = stage
}

func (e *Engine) clearActiveStage(runID string, stageID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.activeStage[runID].ID == stageID {
		delete(e.activeStage, runID)
	}
}

func (e *Engine) HandleRunnerEvent(ctx context.Context, ev event.Event) error {
	_, err := e.emit(ctx, ev)
	return err
}

func (e *Engine) HandleRunnerReport(context.Context, report.Report) error { return nil }

func (e *Engine) HandleRunnerResult(context.Context, protocol.ResultPayload) error { return nil }

func (e *Engine) HandleRunnerLog(context.Context, protocol.LogPayload) error { return nil }

func (e *Engine) HandleRunnerDown(ctx context.Context, runnerID, reason string) error {
	e.mu.Lock()
	active := make([]string, 0, len(e.activeRuns))
	cancels := make([]context.CancelFunc, 0, len(e.activeRuns))
	for runID, cancel := range e.activeRuns {
		active = append(active, runID)
		cancels = append(cancels, cancel)
	}
	e.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	var joined []error
	for _, runID := range active {
		wr, err := e.store.GetWorkflowRun(ctx, runID)
		if err != nil {
			joined = append(joined, err)
			continue
		}
		e.markRunnerDown(runID, "runner_disconnected")
		if e.isCancelling(runID) {
			if err := e.cancelRunTerminal(ctx, wr, "workflow cancelled while runner disconnected"); err != nil {
				joined = append(joined, err)
			}
			continue
		}
		persisted, changed, err := e.store.UpdateRunStatusIfOpenAndAppendEvent(ctx, runID, store.RunStatusFailed, runEvent(wr, "run.failed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "runner disconnected", map[string]any{
			"terminal_status": store.RunStatusFailed,
			"reason":          "runner_disconnected",
			"runner_id":       runnerID,
			"signal":          reason,
		}))
		if err != nil {
			joined = append(joined, err)
			continue
		}
		if !changed {
			continue
		}
		if _, err := e.publishEvent(ctx, persisted); err != nil {
			joined = append(joined, err)
		}
	}
	return errors.Join(joined...)
}

func (e *Engine) executeRun(ctx context.Context, runID string) {
	e.executeRunAfter(ctx, runID, "")
}

func (e *Engine) executeRunAfter(ctx context.Context, runID, resumeAfterWorkflowStageID string) {
	e.executeRunWithCleanup(ctx, runID, func() error {
		return e.executeRunFrom(ctx, runID, resumeAfterWorkflowStageID)
	})
}

func (e *Engine) executeRunWithCleanup(ctx context.Context, runID string, execute func() error) {
	defer func() {
		e.unregisterActiveRun(runID)
		if e.queuePolicy.AutoWhenReady {
			e.spawn(func() {
				if err := e.DispatchPending(e.rootCtx); err != nil {
					log.Printf("dispatch pending failed: %v", err)
				}
			})
		}
	}()
	if err := execute(); err != nil {
		if errors.Is(err, errRunAwaitingHuman) {
			return
		}
		if e.isCancelling(runID) {
			if wr, getErr := e.store.GetWorkflowRun(context.Background(), runID); getErr == nil {
				if cancelErr := e.cancelRunTerminal(context.Background(), wr, "workflow cancelled"); cancelErr != nil {
					log.Printf("workflow %s cancel terminal failed: %v", runID, cancelErr)
				}
				return
			}
		}
		log.Printf("workflow %s failed: %v", runID, err)
	}
}

func (e *Engine) executeRunErr(ctx context.Context, runID string) error {
	return e.executeRunFrom(ctx, runID, "")
}

type executeRunOptions struct {
	resumeAfterWorkflowStageID string
	startWorkflowStageID       string
	seedState                  *executionState
}

func (e *Engine) executeRunFrom(ctx context.Context, runID, resumeAfterWorkflowStageID string) error {
	return e.executeRunWithOptions(ctx, runID, executeRunOptions{resumeAfterWorkflowStageID: resumeAfterWorkflowStageID})
}

func (e *Engine) executeRunFromStage(ctx context.Context, runID, startWorkflowStageID string, seed executionState) error {
	return e.executeRunWithOptions(ctx, runID, executeRunOptions{startWorkflowStageID: startWorkflowStageID, seedState: &seed})
}

func (e *Engine) executeRunWithOptions(ctx context.Context, runID string, opts executeRunOptions) error {
	wr, err := e.store.GetWorkflowRun(context.Background(), runID)
	if err != nil {
		return err
	}
	if wr.Run.Status != store.RunStatusRunning {
		return nil
	}
	if opts.resumeAfterWorkflowStageID != "" && opts.startWorkflowStageID != "" {
		return fmt.Errorf("cannot resume after and start at workflow stages in one execution")
	}
	if opts.resumeAfterWorkflowStageID == "" && opts.startWorkflowStageID == "" {
		started, err := e.hasRunEventType(context.Background(), runID, "run.started")
		if err != nil {
			return err
		}
		if !started {
			if _, err := e.emit(context.Background(), runEvent(wr, "run.started", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run started", map[string]any{"status": store.RunStatusRunning})); err != nil {
				return err
			}
		}
	}

	runtime, err := e.loadRuntimeWorkflow(context.Background(), wr)
	if err != nil {
		return err
	}
	currentID := runtime.Graph.Start()
	maxTransitions := maxInt(16, len(runtime.Stages)*(maxFixLoops(runtime.Template, workflow.StageTemplate{})+2)*4)
	var lastReport report.Report
	var lastValidationReport report.Report
	var lastDeliveryReport report.Report
	var snapshot workerSnapshot
	var snapshotErr error
	if opts.seedState != nil {
		lastReport = opts.seedState.lastReport
		lastValidationReport = opts.seedState.lastValidationReport
		lastDeliveryReport = opts.seedState.lastDeliveryReport
		snapshot = opts.seedState.snapshot
		snapshotErr = opts.seedState.snapshotErr
	}

	if opts.startWorkflowStageID != "" {
		if _, ok := runtime.ByID[opts.startWorkflowStageID]; !ok {
			return fmt.Errorf("start workflow stage %q not found in frozen snapshot", opts.startWorkflowStageID)
		}
		currentID = opts.startWorkflowStageID
	}

	if opts.resumeAfterWorkflowStageID != "" {
		resumeStage, ok := runtime.ByID[opts.resumeAfterWorkflowStageID]
		if !ok {
			return fmt.Errorf("resume workflow stage %q not found in frozen snapshot", opts.resumeAfterWorkflowStageID)
		}
		state, err := e.reconstructExecutionState(context.Background(), wr, runtime, opts.resumeAfterWorkflowStageID)
		if err != nil {
			return err
		}
		lastReport = state.lastReport
		lastValidationReport = state.lastValidationReport
		lastDeliveryReport = state.lastDeliveryReport
		snapshot = state.snapshot
		snapshotErr = state.snapshotErr
		nextID, ok := runtime.Graph.Next(opts.resumeAfterWorkflowStageID, routingOutcome(resumeStage.Template, lastReport))
		if !ok {
			return e.stopRun(context.Background(), wr, lastReport.Status, "workflow stopped after "+opts.resumeAfterWorkflowStageID)
		}
		if isFixLoopTransition(runtime, resumeStage.Template, lastReport, nextID) {
			newWR, newRuntime, err := e.startFixLoopAttempt(context.Background(), wr, runtime, resumeStage, lastReport, nextID)
			if err != nil {
				return err
			}
			wr = newWR
			runtime = newRuntime
			lastValidationReport = report.Report{}
			snapshot = workerSnapshot{}
			snapshotErr = nil
		}
		currentID = nextID
	}

	for step := 0; step < maxTransitions; step++ {
		runtimeStage, ok := runtime.ByID[currentID]
		if !ok {
			return fmt.Errorf("workflow stage %q not found in frozen snapshot", currentID)
		}
		rep, err := e.runWorkflowStage(ctx, wr, runtime, runtimeStage, lastReport, lastValidationReport, snapshot, snapshotErr)
		if err != nil {
			return err
		}
		if runtimeStage.Template.Type == workflow.StageTypeImplementation && rep.Status == report.StatusCompleted {
			snapshot, snapshotErr = e.snapshotWorktree(ctx, wr, rep)
		}
		if runtimeStage.Template.Type == workflow.StageTypeValidation {
			lastValidationReport = rep
		}
		if reportCarriesDeliveryPayload(rep) {
			lastDeliveryReport = rep
		}
		if runtimeStage.Template.Type == workflow.StageTypeStopReport {
			return e.finishRunFromStopReport(context.Background(), wr, rep)
		}
		if e.isCancelling(wr.Run.ID) {
			return e.cancelRunTerminal(context.Background(), wr, "workflow cancelled after "+runtimeStage.Template.ID)
		}
		nextID, ok := runtime.Graph.Next(runtimeStage.Template.ID, routingOutcome(runtimeStage.Template, rep))
		if !ok {
			return e.stopRun(context.Background(), wr, rep.Status, "workflow stopped after "+runtimeStage.Template.ID)
		}
		if isFixLoopTransition(runtime, runtimeStage.Template, rep, nextID) {
			if fixLoopExhausted, err := e.fixLoopExhausted(context.Background(), wr, runtime.Template, runtimeStage.Template); err != nil {
				return err
			} else if fixLoopExhausted {
				rep = withFixLoopExhaustion(rep, maxFixLoops(runtime.Template, runtimeStage.Template))
				nextID = runtime.Graph.StopID()
			} else {
				newWR, newRuntime, err := e.startFixLoopAttempt(context.Background(), wr, runtime, runtimeStage, rep, nextID)
				if err != nil {
					return err
				}
				wr = newWR
				runtime = newRuntime
				lastReport = rep
				lastValidationReport = report.Report{}
				snapshot = workerSnapshot{}
				snapshotErr = nil
				currentID = nextID
				continue
			}
		}
		if nextID == runtime.Graph.StopID() && !reportCarriesDeliveryPayload(rep) && lastDeliveryReport.StageID != "" {
			rep = withDeliveryPayload(rep, lastDeliveryReport)
		}
		lastReport = rep
		currentID = nextID
	}
	return fmt.Errorf("workflow exceeded %d transitions; possible unbounded loop in frozen template %s", maxTransitions, runtime.Template.ID)
}

func (e *Engine) loadRuntimeWorkflow(ctx context.Context, wr store.WorkflowRun) (runtimeWorkflow, error) {
	template, err := e.store.LatestWorkflowTemplateSnapshot(ctx, wr.Run.ID)
	if err != nil {
		template, err = e.store.GetWorkflowTemplate(ctx, wr.Run.WorkflowTemplateID)
		if err != nil {
			return runtimeWorkflow{}, err
		}
	}
	stages, err := e.store.ListStagesForAttempt(ctx, wr.Run.ID, wr.Attempt.ID)
	if err != nil {
		return runtimeWorkflow{}, err
	}
	return newRuntimeWorkflow(template, stages)
}

func (e *Engine) runWorkflowStage(ctx context.Context, wr store.WorkflowRun, runtime runtimeWorkflow, runtimeStage runtimeStage, lastReport, lastValidationReport report.Report, snapshot workerSnapshot, snapshotErr error) (report.Report, error) {
	stage := runtimeStage.Stage
	templateStage := runtimeStage.Template
	switch templateStage.Type {
	case workflow.StageTypeIdeaRefinement, contract.StageTypeIdeaIntake:
		return e.runIdeaIntakeStage(ctx, wr, stage, runtime.Template)
	case workflow.StageTypeImplementation:
		return e.dispatchStage(ctx, wr, stage, stage.Adapter, templateStage.Type, e.stageDispatchInput(runtime, templateStage, implementationInput(wr, lastReport)))
	case workflow.StageTypeValidation:
		return e.dispatchStage(ctx, wr, stage, stage.Adapter, templateStage.Type, e.stageDispatchInput(runtime, templateStage, map[string]any{"idea": wr.Run.Idea}))
	case workflow.StageTypeReview:
		if templateStage.Actor == workflow.ActorHuman {
			return e.runHumanStage(ctx, wr, stage, templateStage, snapshot, snapshotErr)
		}
		return e.runReviewStage(ctx, wr, runtime, runtimeStage, lastReport, lastValidationReport, snapshot, snapshotErr)
	case workflow.StageTypeMemoryUpdate:
		if templateStage.Actor == workflow.ActorHuman {
			return e.runHumanStage(ctx, wr, stage, templateStage, snapshot, snapshotErr)
		}
		return e.runMemoryUpdateStage(ctx, wr, runtime, runtimeStage, lastReport)
	case workflow.StageTypeCommit:
		reportForCommit := lastValidationReport
		if reportForCommit.StageID == "" {
			reportForCommit = lastReport
		}
		return e.runCommitStage(ctx, wr, stage, runtime.Template, templateStage, reportForCommit, snapshot, snapshotErr)
	case workflow.StageTypePRCreation, contract.StageTypePRReady:
		return e.runPRReadyStage(ctx, wr, stage, lastReport, runtime.Template, templateStage)
	case workflow.StageTypeStopReport:
		return e.runStopReport(ctx, wr, stage, lastReport)
	default:
		return report.Report{}, fmt.Errorf("unsupported workflow stage type %q", templateStage.Type)
	}
}

func (e *Engine) stageDispatchInput(runtime runtimeWorkflow, stage workflow.StageTemplate, input map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range input {
		out[key] = value
	}
	out["workflow_template_id"] = runtime.Template.ID
	out["workflow_template_settings"] = runtime.Template.Settings
	out["workflow_stage_id"] = stage.ID
	out["workflow_stage_type"] = stage.Type
	out["workflow_stage_label"] = stage.Label
	out["workflow_stage_actor"] = stage.Actor
	out["workflow_stage_target"] = stage.Target
	out["workflow_stage_settings"] = stage.Settings
	return out
}

func (e *Engine) runIdeaIntake(ctx context.Context, wr store.WorkflowRun) (report.Report, error) {
	stage := wr.IdeaIntakeStage
	template, err := e.store.LatestWorkflowTemplateSnapshot(ctx, wr.Run.ID)
	if err != nil {
		template, err = e.store.GetWorkflowTemplate(ctx, wr.Run.WorkflowTemplateID)
		if err != nil {
			return report.Report{}, err
		}
	}
	return e.runIdeaIntakeStage(ctx, wr, stage, template)
}

func (e *Engine) runIdeaIntakeStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, template workflow.Template) (report.Report, error) {
	briefMarkdown, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	contractMarkdown, contractArtifact, err := e.taskContractArtifact(ctx, wr)
	if err != nil {
		return report.Report{}, err
	}
	switch contract.NormalizeRefinementLevel(wr.Run.RefinementLevel) {
	case contract.RefinementLevelStandard:
		return e.runStandardIdeaPlanningStage(ctx, wr, stage, template, contractMarkdown, contractArtifact.ID, briefMarkdown, briefArtifact.ID)
	}
	if err := e.startStage(ctx, wr, stage, stage.StageType+" stage started"); err != nil {
		return report.Report{}, err
	}
	return e.completeIdeaIntakeWithPlan(ctx, wr, stage, template, contractArtifact.ID, taskPlanMarkdown(wr), report.Actor{Kind: report.ActorKindHarness, ID: "idea_intake"}, "idea refined into a task plan and frozen workflow snapshot")
}

func (e *Engine) runStandardIdeaPlanningStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, template workflow.Template, contractMarkdown, contractArtifactID, briefMarkdown, briefArtifactID string) (report.Report, error) {
	if err := e.store.UpdateStageAdapter(ctx, stage.ID, e.planningAdapter); err != nil {
		return report.Report{}, err
	}
	stage.Adapter = e.planningAdapter
	e.setActiveStage(wr.Run.ID, stage)
	defer e.clearActiveStage(wr.Run.ID, stage.ID)
	if err := e.startStage(ctx, wr, stage, stage.StageType+" stage started"); err != nil {
		return report.Report{}, err
	}
	input := e.stageDispatchInput(runtimeWorkflow{Template: template}, plannerTemplateStage(template, stage), map[string]any{
		"input_mode":                contract.AdapterInputModePlanning,
		"idea":                      wr.Run.Idea,
		"refinement_level":          contract.RefinementLevelStandard,
		"contract_markdown":         contractMarkdown,
		"task_contract_artifact_id": contractArtifactID,
	})
	input = withStageBriefInput(input, briefMarkdown, briefArtifactID)
	disp := contract.Dispatch{
		ProjectID:    wr.Run.ProjectID,
		RepositoryID: wr.Task.RepositoryID,
		RunID:        wr.Run.ID,
		TaskID:       wr.Task.ID,
		AttemptID:    wr.Attempt.ID,
		StageID:      stage.ID,
		StageType:    stage.StageType,
		Adapter:      e.planningAdapter,
		Input:        input,
	}
	plannerReport, err := e.dispatchWithReportRepair(ctx, wr, stage, disp, reportRepairOptions{
		AdapterName:   e.planningAdapter,
		StageType:     stage.StageType,
		EmitLifecycle: e.planningAdapter != "",
		LifecycleData: map[string]any{"adapter": e.planningAdapter, "input_mode": contract.AdapterInputModePlanning},
		Validator:     planningReportValidator(wr, stage, e.planningAdapter),
	})
	if err != nil {
		return report.Report{}, err
	}
	if plannerReport.Status != report.StatusCompleted {
		if err := e.completeStage(context.Background(), wr, stage, plannerReport); err != nil {
			return report.Report{}, err
		}
		return plannerReport, nil
	}
	actor := plannerReport.Actor
	if actor.Kind == "" {
		actor = report.Actor{Kind: report.ActorKindAgent, ID: e.planningAdapter}
	}
	return e.completeIdeaIntakeWithPlan(ctx, wr, stage, template, contractArtifactID, normalizePlannerTaskPlan(payloadString(plannerReport.Payload, "task_plan_markdown")), actor, "planner agent refined the idea into a task plan and frozen workflow snapshot")
}

func plannerTemplateStage(template workflow.Template, stage store.Stage) workflow.StageTemplate {
	for _, templateStage := range template.Stages {
		if templateStage.ID == stage.WorkflowStageID {
			templateStage.Actor = workflow.ActorAgent
			if templateStage.Target == "" {
				templateStage.Target = workflow.TargetPlan
			}
			return templateStage
		}
	}
	return workflow.StageTemplate{ID: stage.WorkflowStageID, Type: stage.StageType, Label: "Idea refinement", Actor: workflow.ActorAgent, Target: workflow.TargetPlan}
}

func planningReportValidator(wr store.WorkflowRun, stage store.Stage, adapterName string) reportRepairValidator {
	base := baseReportValidator(wr, stage, stage.StageType, adapterName)
	return func(rep report.Report) (report.Report, error) {
		validated, err := base(rep)
		if err != nil {
			return validated, err
		}
		if validated.Status == report.StatusNeedsInput {
			err := fmt.Errorf("standard idea intake is single-shot and must not return needs_input")
			return invalidAdapterReport(wr, stage, adapterName, "planner returned invalid task plan", validated, err), err
		}
		if validated.Status != report.StatusCompleted {
			return validated, nil
		}
		plan := payloadString(validated.Payload, "task_plan_markdown")
		if err := validatePlannerTaskPlan(plan); err != nil {
			return invalidAdapterReport(wr, stage, adapterName, "planner returned invalid task plan", validated, err), err
		}
		return validated, nil
	}
}

func validatePlannerTaskPlan(plan string) error {
	plan = strings.TrimSpace(plan)
	if plan == "" {
		return fmt.Errorf("payload.task_plan_markdown is required")
	}
	for _, required := range []string{"# Task Plan", "This artifact is a task plan, not a workflow definition.", "## Assumptions", "## Open Questions"} {
		if !strings.Contains(plan, required) {
			return fmt.Errorf("payload.task_plan_markdown must include %q", required)
		}
	}
	return nil
}

func normalizePlannerTaskPlan(plan string) string {
	plan = strings.TrimSpace(plan)
	if plan == "" {
		return plan
	}
	return plan + "\n"
}

func (e *Engine) completeIdeaIntakeWithPlan(ctx context.Context, wr store.WorkflowRun, stage store.Stage, template workflow.Template, contractArtifactID, planMarkdown string, actor report.Actor, summary string) (report.Report, error) {
	planArtifact, err := e.store.SaveArtifact(ctx, wr.Run.ID, "task_plan", "text/markdown", []byte(planMarkdown), ".md")
	if err != nil {
		return report.Report{}, err
	}
	if err := e.store.UpdateStageTaskPlanArtifactID(ctx, stage.ID, planArtifact.ID); err != nil {
		return report.Report{}, err
	}
	stage.TaskPlanArtifactID = planArtifact.ID
	if err := e.store.SaveWorkflowSnapshot(ctx, wr.Run.ID, e.workflowSnapshot(wr, template, contractArtifactID, planArtifact.ID, true)); err != nil {
		return report.Report{}, err
	}
	rep := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         actor,
		Status:        report.StatusCompleted,
		Summary:       summary,
		EvidenceRefs:  []string{contractArtifactID, planArtifact.ID},
		Payload: map[string]any{
			"idea_verbatim":             wr.Run.Idea,
			"refinement_level":          wr.Run.RefinementLevel,
			"task_contract_artifact_id": contractArtifactID,
			"task_plan_artifact_id":     planArtifact.ID,
			"workflow_snapshot_frozen":  true,
			"implementation_stage_id":   wr.ImplementationStage.ID,
			"validation_stage_id":       wr.ValidationStage.ID,
			"commit_stage_id":           wr.CommitStage.ID,
			"pr_ready_stage_id":         wr.PRReadyStage.ID,
		},
		Errors: []string{},
	}
	if err := e.completeStage(context.Background(), wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

func (e *Engine) dispatchStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, adapterName, stageType string, input map[string]any) (report.Report, error) {
	briefMarkdown, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	e.setActiveStage(wr.Run.ID, stage)
	defer e.clearActiveStage(wr.Run.ID, stage.ID)
	if err := e.startStage(ctx, wr, stage, stage.StageType+" stage started"); err != nil {
		return report.Report{}, err
	}
	input = withStageBriefInput(input, briefMarkdown, briefArtifact.ID)
	disp := contract.Dispatch{
		ProjectID:    wr.Run.ProjectID,
		RepositoryID: wr.Task.RepositoryID,
		RunID:        wr.Run.ID,
		TaskID:       wr.Task.ID,
		AttemptID:    wr.Attempt.ID,
		StageID:      stage.ID,
		StageType:    stageType,
		Adapter:      adapterName,
		Input:        input,
	}
	rep, err := e.dispatchWithReportRepair(ctx, wr, stage, disp, reportRepairOptions{
		AdapterName:   adapterName,
		StageType:     stageType,
		EmitLifecycle: adapterName != "" && stageType != contract.StageTypeValidation,
		LifecycleData: map[string]any{"adapter": adapterName},
		Validator:     baseReportValidator(wr, stage, stageType, adapterName),
	})
	if err != nil {
		return report.Report{}, err
	}
	if err := e.completeStage(context.Background(), wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

func (e *Engine) runCommit(ctx context.Context, wr store.WorkflowRun, validationReport report.Report, snapshot workerSnapshot, snapshotErr error) (report.Report, error) {
	return e.runCommitStage(ctx, wr, wr.CommitStage, workflow.Template{}, workflow.StageTemplate{}, validationReport, snapshot, snapshotErr)
}

func (e *Engine) runCommitStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, template workflow.Template, templateStage workflow.StageTemplate, validationReport report.Report, snapshot workerSnapshot, snapshotErr error) (report.Report, error) {
	_, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	if err := e.startStage(ctx, wr, stage, "commit stage started"); err != nil {
		return report.Report{}, err
	}
	if snapshotErr != nil {
		rep := commitFailureReport(wr, stage, snapshotErr, snapshot)
		return rep, e.completeStage(context.Background(), wr, stage, rep)
	}
	branchPolicy := settingString(templateStage.Settings, "branch_policy")
	if branchPolicy == "" {
		branchPolicy = settingString(template.Settings, "branch_policy")
	}
	targetBranch := settingString(templateStage.Settings, "target_branch")
	if targetBranch == "" {
		targetBranch = settingString(template.Settings, "target_branch")
	}
	result, err := commitWorktree(ctx, commitOptions{
		WorktreePath:   snapshot.WorktreePath,
		BaseSHA:        snapshot.BaseSHA,
		BaseTreeSHA:    snapshot.BaseTreeSHA,
		WorkerTreeSHA:  snapshot.WorkerTreeSHA,
		RunID:          wr.Run.ID,
		TaskID:         wr.Task.ID,
		Idea:           wr.Run.Idea,
		ReportSummary:  validationReport.Summary,
		DiffArtifactID: snapshot.DiffArtifactID,
		Git:            e.gitExecutable,
		AuthorName:     e.gitAuthorName,
		AuthorEmail:    e.gitAuthorEmail,
		BranchPolicy:   branchPolicy,
		TargetBranch:   targetBranch,
	})
	if err != nil {
		rep := commitFailureReport(wr, stage, err, snapshot)
		return rep, e.completeStage(context.Background(), wr, stage, rep)
	}
	rep := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "commit"},
		Status:        report.StatusCompleted,
		Summary:       "committed worktree to " + result.Branch,
		EvidenceRefs:  []string{snapshot.DiffArtifactID},
		Payload: map[string]any{
			"branch":           result.Branch,
			"branch_policy":    result.BranchPolicy,
			"commit_sha":       result.CommitSHA,
			"diff_artifact_id": snapshot.DiffArtifactID,
			"no_verify":        true,
			"hooks_disabled":   true,
			"author_name":      result.AuthorName,
			"author_email":     result.AuthorEmail,
		},
		Errors: []string{},
	}
	if err := e.completeStage(context.Background(), wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

func (e *Engine) snapshotWorktree(ctx context.Context, wr store.WorkflowRun, implementationReport report.Report) (workerSnapshot, error) {
	snapshot := workerSnapshot{DiffArtifactID: payloadString(implementationReport.Payload, "diff_artifact_id")}
	worktreePath, err := worktree.Locate(e.dataRoot, wr.Run.ProjectID, wr.Run.ID, wr.Task.ID, wr.Attempt.ID)
	if err != nil {
		return snapshot, err
	}
	snapshot.WorktreePath = worktreePath
	baseSHA, baseTreeSHA, workerTreeSHA, err := snapshotGitWorktree(ctx, e.gitExecutable, worktreePath)
	if err != nil {
		return snapshot, err
	}
	snapshot.BaseSHA = baseSHA
	snapshot.BaseTreeSHA = baseTreeSHA
	snapshot.WorkerTreeSHA = workerTreeSHA
	return snapshot, nil
}

func (e *Engine) runPRReady(ctx context.Context, wr store.WorkflowRun, commitReport report.Report) (report.Report, error) {
	return e.runPRReadyStage(ctx, wr, wr.PRReadyStage, commitReport, workflow.Template{}, workflow.StageTemplate{})
}

func (e *Engine) runPRReadyStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, commitReport report.Report, template workflow.Template, templateStage workflow.StageTemplate) (report.Report, error) {
	_, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	if err := e.startStage(ctx, wr, stage, stage.StageType+" stage started"); err != nil {
		return report.Report{}, err
	}
	branch := payloadString(commitReport.Payload, "branch")
	commitSHA := payloadString(commitReport.Payload, "commit_sha")
	diffID := payloadString(commitReport.Payload, "diff_artifact_id")
	rep := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "pr_ready"},
		Status:        report.StatusCompleted,
		Summary:       "PR-ready handoff reached",
		EvidenceRefs:  []string{diffID},
		Payload: map[string]any{
			"branch":           branch,
			"commit_sha":       commitSHA,
			"diff_artifact_id": diffID,
			"push_performed":   false,
			"pr_created":       false,
			"pr_behavior":      settingString(template.Settings, "pr_behavior"),
		},
		Errors: []string{},
	}
	if err := e.completeStage(context.Background(), wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

func (e *Engine) runHumanStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, templateStage workflow.StageTemplate, snapshot workerSnapshot, snapshotErr error) (report.Report, error) {
	_, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	if err := e.startStage(ctx, wr, stage, stage.StageType+" human stage started"); err != nil {
		return report.Report{}, err
	}
	if err := e.suspendForHumanReview(context.Background(), wr, stage, templateStage, briefArtifact, snapshot, snapshotErr); err != nil {
		return report.Report{}, err
	}
	return report.Report{}, errRunAwaitingHuman
}

func (e *Engine) runStopReport(ctx context.Context, wr store.WorkflowRun, stage store.Stage, previous report.Report) (report.Report, error) {
	_, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	if err := e.startStage(ctx, wr, stage, "stop/report stage started"); err != nil {
		return report.Report{}, err
	}
	status := report.StatusCompleted
	if previous.Status != "" {
		status = previous.Status
	}
	summary := "workflow completed"
	if status != report.StatusCompleted && previous.Summary != "" {
		summary = "workflow stopped: " + previous.Summary
	}
	errors := append([]string{}, previous.Errors...)
	if (status == report.StatusFailed || status == report.StatusInvalid) && len(errors) == 0 {
		errors = []string{summary}
	}
	rep := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "stop_report"},
		Status:        status,
		Summary:       summary,
		Payload: map[string]any{
			"previous_stage_id":   previous.StageID,
			"previous_stage_type": previous.StageType,
			"previous_status":     previous.Status,
			"previous_verdict":    verdictString(previous.Verdict),
		},
		Errors: errors,
	}
	copyPayloadStrings(rep.Payload, previous.Payload, "branch", "commit_sha", "diff_artifact_id")
	if err := e.completeStage(context.Background(), wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

func (e *Engine) prepareStageBrief(ctx context.Context, wr store.WorkflowRun, stage store.Stage) (string, store.Artifact, error) {
	bundle, err := e.store.RunBundle(ctx, wr.Run.ID)
	if err != nil {
		return "", store.Artifact{}, err
	}
	repositoryPath, repositoryWarnings := e.repositoryPathForStage(ctx, wr, stage)
	brief, err := e.contextAssembler.Assemble(ctx, contextpack.Request{
		Project:            bundle.Project,
		Run:                bundle.Run,
		Task:               bundle.Task,
		Attempt:            bundle.Attempt,
		Stages:             bundle.Stages,
		Events:             bundle.Events,
		Artifacts:          bundle.Artifacts,
		CurrentStage:       stage,
		RepositoryPath:     repositoryPath,
		RepositoryWarnings: repositoryWarnings,
		ReadArtifact: func(ctx context.Context, artifactID string) ([]byte, error) {
			_, content, err := e.store.GetArtifact(ctx, artifactID)
			return content, err
		},
	})
	if err != nil {
		return "", store.Artifact{}, err
	}
	markdown := contextpack.Markdown(brief)
	artifact, err := e.store.SaveArtifact(ctx, wr.Run.ID, "stage_brief", "text/markdown", []byte(markdown), ".md")
	if err != nil {
		return "", store.Artifact{}, err
	}
	if err := e.store.UpdateStageBriefArtifactID(ctx, stage.ID, artifact.ID); err != nil {
		return "", store.Artifact{}, err
	}
	return markdown, artifact, nil
}

func (e *Engine) repositoryPathForStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage) (string, []string) {
	var warnings []string
	if stage.StageType != contract.StageTypeImplementation && e.dataRoot != "" {
		worktreePath, err := worktree.Locate(e.dataRoot, wr.Run.ProjectID, wr.Run.ID, wr.Task.ID, wr.Attempt.ID)
		if err == nil {
			return worktreePath, nil
		}
		warnings = append(warnings, "worktree repo evidence unavailable: "+err.Error())
	}
	if wr.Task.RepositoryID == "" {
		return "", warnings
	}
	repo, err := e.store.GetRepository(ctx, wr.Task.RepositoryID)
	if err != nil {
		warnings = append(warnings, "repository metadata unavailable: "+err.Error())
		return "", warnings
	}
	return repo.Path, warnings
}

func withStageBriefInput(input map[string]any, markdown, artifactID string) map[string]any {
	out := map[string]any{}
	for key, value := range input {
		out[key] = value
	}
	out["stage_brief_artifact_id"] = artifactID
	out["stage_brief_markdown"] = markdown
	return out
}

func (e *Engine) startStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, summary string) error {
	e.setActiveStage(wr.Run.ID, stage)
	if err := e.store.UpdateStageStatus(ctx, stage.ID, store.StageStatusRunning); err != nil {
		return err
	}
	data := map[string]any{}
	if stage.WorkflowStageID != "" {
		data["workflow_stage_id"] = stage.WorkflowStageID
	}
	if stage.StageBriefArtifactID != "" {
		data["stage_brief_artifact_id"] = stage.StageBriefArtifactID
	}
	if stage.TaskPlanArtifactID != "" {
		data["task_plan_artifact_id"] = stage.TaskPlanArtifactID
	}
	_, err := e.emit(ctx, stageEvent(wr, stage, "stage.started", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, summary, data))
	return err
}

func (e *Engine) completeStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, rep report.Report) error {
	defer e.clearActiveStage(wr.Run.ID, stage.ID)
	if err := rep.Validate(); err != nil {
		rep = report.Report{
			SchemaVersion: report.SchemaVersion,
			RunID:         wr.Run.ID,
			TaskID:        wr.Task.ID,
			AttemptID:     wr.Attempt.ID,
			StageID:       stage.ID,
			StageType:     stage.StageType,
			Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "manager"},
			Status:        report.StatusInvalid,
			Summary:       "stage returned invalid report",
			Payload:       map[string]any{},
			Errors:        []string{err.Error()},
		}
	}
	artifact, err := e.store.SaveReportArtifact(ctx, rep)
	if err != nil {
		return err
	}
	if err := e.store.UpdateStageStatus(ctx, stage.ID, rep.Status); err != nil {
		return err
	}
	data := map[string]any{
		"stage_id":           stage.ID,
		"stage_type":         stage.StageType,
		"status":             rep.Status,
		"report_artifact_id": artifact.ID,
	}
	if rep.Verdict != nil {
		data["verdict"] = string(*rep.Verdict)
	}
	if stage.WorkflowStageID != "" {
		data["workflow_stage_id"] = stage.WorkflowStageID
	}
	if stage.StageBriefArtifactID != "" {
		data["stage_brief_artifact_id"] = stage.StageBriefArtifactID
	}
	if stage.TaskPlanArtifactID != "" {
		data["task_plan_artifact_id"] = stage.TaskPlanArtifactID
	}
	copyPayloadStrings(data, rep.Payload, "branch", "branch_policy", "commit_sha", "diff_artifact_id", "task_contract_artifact_id", "task_plan_artifact_id")
	if typ := completionEventType(rep); typ != "" {
		if _, err := e.emit(ctx, stageEvent(wr, stage, typ, reportActor(rep.Actor, stage), rep.Summary, data)); err != nil {
			return err
		}
	}
	_, err = e.emit(ctx, stageEvent(wr, stage, stageTerminalEventType(rep.Status), event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, stageTerminalSummary(stage, rep), data))
	return err
}

// HandleRunnerArtifact persists an artifact transferred over the runner session.
// Artifacts are first-class (0066/0068): they arrive before the report that
// references them in evidence_refs, never inlined in the report payload.
func (e *Engine) HandleRunnerArtifact(ctx context.Context, art protocol.ArtifactPayload) error {
	kind := art.Kind
	if kind == "" {
		kind = "adapter_output"
	}
	mediaType := art.MediaType
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	ext := filepath.Ext(art.Name)
	if ext == "" {
		ext = ".txt"
	}
	_, err := e.store.SaveArtifactWithID(ctx, art.ArtifactID, art.RunID, kind, mediaType, art.Content, ext)
	return err
}

func (e *Engine) finishRunFromStopReport(ctx context.Context, wr store.WorkflowRun, rep report.Report) error {
	if rep.Status != report.StatusCompleted {
		return e.stopRun(ctx, wr, rep.Status, rep.Summary)
	}
	persisted, changed, err := e.store.UpdateRunStatusIfOpenAndAppendEvent(ctx, wr.Run.ID, store.RunStatusCompleted, runEvent(wr, "run.completed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "workflow reached stop/report", map[string]any{
		"terminal_status":  store.RunStatusCompleted,
		"branch":           payloadString(rep.Payload, "branch"),
		"commit_sha":       payloadString(rep.Payload, "commit_sha"),
		"diff_artifact_id": payloadString(rep.Payload, "diff_artifact_id"),
	}))
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	_, err = e.publishEvent(ctx, persisted)
	return err
}

func (e *Engine) stopRun(ctx context.Context, wr store.WorkflowRun, status, summary string) error {
	if e.isCancelling(wr.Run.ID) {
		return e.cancelRunTerminal(ctx, wr, summary)
	}
	// Map to the documented run.* terminal taxonomy (event.schema.md): failed and
	// invalid are failure terminals. A stage that completes with needs_input outside
	// a durable suspend/resume path still terminates as abandoned.
	runStatus := store.RunStatusFailed
	eventType := "run.failed"
	switch status {
	case report.StatusInvalid:
		runStatus = store.RunStatusInvalid
		eventType = "run.failed"
	case report.StatusNeedsInput:
		runStatus = store.RunStatusNeedsInput
		eventType = "run.abandoned"
	}
	data := map[string]any{"terminal_status": runStatus}
	if reason, ok := e.runnerDownReason(wr.Run.ID); ok && runStatus == store.RunStatusFailed {
		data["reason"] = reason
		summary = "runner disconnected"
	}
	persisted, changed, err := e.store.UpdateRunStatusIfOpenAndAppendEvent(ctx, wr.Run.ID, runStatus, runEvent(wr, eventType, event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, summary, data))
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	_, err = e.publishEvent(ctx, persisted)
	return err
}

func (e *Engine) cancelRunTerminal(ctx context.Context, wr store.WorkflowRun, summary string) error {
	persisted, changed, err := e.store.UpdateRunStatusIfOpenAndAppendEvent(ctx, wr.Run.ID, store.RunStatusCancelled, runEvent(wr, "run.cancelled", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, summary, map[string]any{"terminal_status": store.RunStatusCancelled}))
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	_, err = e.publishEvent(ctx, persisted)
	return err
}

func (e *Engine) emit(ctx context.Context, ev event.Event) (event.Event, error) {
	persisted, err := e.store.AppendEvent(ctx, ev)
	if err != nil {
		return event.Event{}, err
	}
	return e.publishEvent(ctx, persisted)
}

func (e *Engine) publishEvent(ctx context.Context, persisted event.Event) (event.Event, error) {
	if persisted.RunID == "" {
		e.broadcast.Broadcast("", persisted, "")
		return persisted, nil
	}
	bundle, err := e.store.RunBundle(ctx, persisted.RunID)
	if err != nil {
		return event.Event{}, err
	}
	fragment, err := e.renderer.RenderRunFragments(bundle)
	if err != nil {
		return event.Event{}, fmt.Errorf("render run fragments: %w", err)
	}
	e.broadcast.Broadcast(persisted.RunID, persisted, fragment)
	e.dispatchNotification(ctx, persisted, bundle)
	return persisted, nil
}

func (e *Engine) dispatchNotification(ctx context.Context, ev event.Event, bundle store.RunBundle) {
	class, ok := notificationClassForEvent(ev)
	if !ok {
		return
	}
	prefs, err := e.store.GetProjectNotificationPreferences(ctx, ev.ProjectID)
	if err != nil {
		log.Printf("notification prefs unavailable for project %s: %v", ev.ProjectID, err)
		return
	}
	if class == store.NotificationClassNeedsYou && !prefs.OnlyWhenNeeded {
		return
	}
	if class == store.NotificationClassFinished && !prefs.WhenFinished {
		return
	}
	notification, err := e.store.InsertNotification(ctx, store.NotificationInput{
		ProjectID: ev.ProjectID,
		RunID:     ev.RunID,
		Class:     class,
		Title:     notificationTitle(ev, bundle),
	})
	if err != nil {
		log.Printf("notification insert failed for event %s: %v", ev.ID, err)
		return
	}
	if !e.spawn(func() { e.deliverNotification(e.rootCtx, notification) }) {
		log.Printf("notification delivery skipped during shutdown for %s", notification.ID)
	}
}

func (e *Engine) deliverNotification(ctx context.Context, notification store.Notification) {
	for _, sink := range e.notificationSinks {
		if sink == nil {
			continue
		}
		if err := sink.Notify(ctx, notification); err != nil {
			log.Printf("notification sink failed for %s: %v", notification.ID, err)
		}
	}
}

func notificationClassForEvent(ev event.Event) (string, bool) {
	switch ev.Type {
	case "stage.awaiting_human":
		stageType, _ := ev.Data["stage_type"].(string)
		if stageType != "" && stageType != contract.StageTypeReview {
			return "", false
		}
		return store.NotificationClassNeedsYou, true
	case "run.completed":
		return store.NotificationClassFinished, true
	case "run.failed":
		if terminal, _ := ev.Data["terminal_status"].(string); terminal == store.RunStatusCancelled || terminal == store.RunStatusNeedsInput {
			return "", false
		}
		return store.NotificationClassFinished, true
	default:
		return "", false
	}
}

func notificationTitle(ev event.Event, bundle store.RunBundle) string {
	idea := strings.TrimSpace(bundle.Run.Idea)
	if idea == "" {
		idea = ev.RunID
	}
	if runes := []rune(idea); len(runes) > 96 {
		idea = strings.TrimSpace(string(runes[:96])) + "…"
	}
	switch ev.Type {
	case "stage.awaiting_human":
		return "Review needed: " + idea
	case "run.completed":
		return "Run completed: " + idea
	case "run.failed":
		if terminal, _ := ev.Data["terminal_status"].(string); terminal == store.RunStatusInvalid {
			return "Run invalid: " + idea
		}
		return "Run failed: " + idea
	default:
		return ev.Summary
	}
}

func (e *Engine) workflowSnapshot(wr store.WorkflowRun, template workflow.Template, taskContractArtifactID, taskPlanArtifactID string, frozen bool) map[string]any {
	stages, err := e.store.ListStagesForAttempt(context.Background(), wr.Run.ID, wr.Attempt.ID)
	if err != nil || len(stages) == 0 {
		stages = []store.Stage{wr.IdeaIntakeStage, wr.ImplementationStage, wr.ValidationStage, wr.CommitStage, wr.PRReadyStage}
	}
	return map[string]any{
		"schema_version":             1,
		"project_id":                 wr.Run.ProjectID,
		"workspace_path":             wr.Project.WorkspacePath,
		"repository_id":              wr.Task.RepositoryID,
		"conversation_id":            wr.Task.ConversationID,
		"workflow_template_id":       wr.Run.WorkflowTemplateID,
		"workflow_template_snapshot": template,
		"workflow_template_frozen":   true,
		"run_id":                     wr.Run.ID,
		"task_id":                    wr.Task.ID,
		"attempt_id":                 wr.Attempt.ID,
		"idea_verbatim":              wr.Run.Idea,
		"refinement_level":           wr.Run.RefinementLevel,
		"frozen":                     frozen,
		"task_contract_artifact_id":  taskContractArtifactID,
		"task_plan_artifact_id":      taskPlanArtifactID,
		"graph":                      "from frozen workflow_template_snapshot",
		"edges":                      template.Edges,
		"stages":                     stageSnapshotsForWorkflow(stages, template),
	}
}

func stageSnapshotsForWorkflow(stages []store.Stage, template workflow.Template) []map[string]any {
	byWorkflowID := map[string]workflow.StageTemplate{}
	for _, stage := range template.Stages {
		byWorkflowID[stage.ID] = stage
	}
	out := make([]map[string]any, 0, len(stages))
	for _, stage := range stages {
		out = append(out, stageSnapshot(stage, byWorkflowID[stage.WorkflowStageID]))
	}
	return out
}

func stageSnapshot(stage store.Stage, templateStage workflow.StageTemplate) map[string]any {
	out := map[string]any{"id": stage.ID, "type": stage.StageType, "adapter": stage.Adapter}
	if stage.WorkflowStageID != "" {
		out["workflow_stage_id"] = stage.WorkflowStageID
	}
	if templateStage.ID != "" {
		out["template_stage"] = templateStage
	}
	return out
}

func (e *Engine) hasRunEventType(ctx context.Context, runID, eventType string) (bool, error) {
	events, err := e.store.ListEvents(ctx, runID)
	if err != nil {
		return false, err
	}
	for _, ev := range events {
		if ev.Type == eventType {
			return true, nil
		}
	}
	return false, nil
}

func (e *Engine) taskContractArtifact(ctx context.Context, wr store.WorkflowRun) (string, store.Artifact, error) {
	artifacts, err := e.store.ListArtifacts(ctx, wr.Run.ID)
	if err != nil {
		return "", store.Artifact{}, err
	}
	for _, artifact := range artifacts {
		if artifact.Kind != taskContractArtifactKind {
			continue
		}
		artifact, content, err := e.store.GetArtifact(ctx, artifact.ID)
		if err != nil {
			return "", store.Artifact{}, err
		}
		return string(content), artifact, nil
	}
	markdown := taskContractMarkdown(wr)
	artifact, err := e.store.SaveArtifact(ctx, wr.Run.ID, taskContractArtifactKind, "text/markdown", []byte(markdown), ".md")
	if err != nil {
		return "", store.Artifact{}, err
	}
	return markdown, artifact, nil
}

func taskContractMarkdown(wr store.WorkflowRun) string {
	return fmt.Sprintf("# Parley Task Contract\n\nProject ID: `%s`\nRun ID: `%s`\nTask ID: `%s`\nAttempt ID: `%s`\nRefinement level: `%s`\n\n## User idea (verbatim)\n\n%s\n", wr.Run.ProjectID, wr.Run.ID, wr.Task.ID, wr.Attempt.ID, wr.Run.RefinementLevel, wr.Run.Idea)
}

func taskPlanMarkdown(wr store.WorkflowRun) string {
	level := contract.NormalizeRefinementLevel(wr.Run.RefinementLevel)
	var b strings.Builder
	fmt.Fprintf(&b, "# Task Plan\n\n")
	fmt.Fprintf(&b, "Project ID: `%s`\n", wr.Run.ProjectID)
	fmt.Fprintf(&b, "Run ID: `%s`\n", wr.Run.ID)
	fmt.Fprintf(&b, "Task ID: `%s`\n", wr.Task.ID)
	fmt.Fprintf(&b, "Attempt ID: `%s`\n", wr.Attempt.ID)
	fmt.Fprintf(&b, "Refinement level: `%s`\n\n", level)
	b.WriteString("## User Idea\n\n")
	b.WriteString(wr.Run.Idea)
	if !strings.HasSuffix(wr.Run.Idea, "\n") {
		b.WriteString("\n")
	}
	b.WriteString("\n## Plan Boundary\n\n")
	b.WriteString("This artifact is a task plan, not a workflow definition. It does not choose, add, remove, or reorder workflow stages.\n\n")
	b.WriteString("## Objective\n\n")
	b.WriteString("Deliver the submitted idea while preserving the repository's current behavior outside the requested change.\n\n")
	switch level {
	case contract.RefinementLevelDirect:
		b.WriteString("## Direct Plan\n\n")
		b.WriteString("- Preserve the idea as the implementation prompt.\n")
		b.WriteString("- Make the smallest coherent code and test changes needed for the request.\n")
		b.WriteString("- Validate the touched path with the narrowest meaningful build or test command.\n")
	default:
		b.WriteString("## Standard Plan\n\n")
		b.WriteString("### Scope\n\n")
		b.WriteString("- Implement the submitted idea without changing unrelated workflow policy.\n")
		b.WriteString("- Keep output artifacts private to the project workspace unless the user explicitly promotes them.\n\n")
		b.WriteString("### Implementation Approach\n\n")
		b.WriteString("- Read the code and tests for the affected path.\n")
		b.WriteString("- Make the smallest end-to-end change that satisfies the task.\n")
		b.WriteString("- Persist any new run output as an artifact rather than repository content.\n\n")
		b.WriteString("### Validation\n\n")
		b.WriteString("- Run focused tests for changed packages.\n")
		b.WriteString("- Run build and vet before the work is handed off.\n\n")
		b.WriteString("### Open Questions\n\n")
		b.WriteString("- No clarification is required before starting; any material limitation should be recorded in the handoff.\n")
	}
	b.WriteString("\n")
	return b.String()
}

func implementationInput(wr store.WorkflowRun, previous report.Report) map[string]any {
	input := map[string]any{
		"idea":                      wr.Run.Idea,
		"contract_markdown":         taskContractMarkdown(wr),
		"task_contract_artifact_id": payloadString(previous.Payload, "task_contract_artifact_id"),
		"workflow_snapshot_frozen":  true,
	}
	if previous.StageID != "" {
		input["previous_stage_report"] = reportInput(previous)
	}
	if previous.StageType == contract.StageTypeReview && previous.Status == report.StatusChangesRequested {
		accepted := acceptedFindings(previous.Payload)
		input["accepted_findings"] = accepted
		input["fix_loop_context"] = map[string]any{
			"source":            "review_arbitration",
			"trigger_stage_id":  previous.StageID,
			"trigger_status":    previous.Status,
			"review_verdict":    verdictString(previous.Verdict),
			"accepted_findings": accepted,
		}
	}
	if previous.StageType == contract.StageTypeValidation && previous.Status == report.StatusFailed {
		input["fix_loop_context"] = map[string]any{
			"source":           "validation_failure",
			"trigger_stage_id": previous.StageID,
			"trigger_status":   previous.Status,
			"summary":          previous.Summary,
			"errors":           append([]string{}, previous.Errors...),
		}
	}
	return input
}

func dispatchFailedReport(wr store.WorkflowRun, stage store.Stage, adapterName string, err error) report.Report {
	actor := report.Actor{Kind: report.ActorKindAgent, ID: adapterName}
	if stage.StageType == contract.StageTypeValidation {
		actor = report.Actor{Kind: report.ActorKindHarness, ID: adapterName}
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         actor,
		Status:        report.StatusFailed,
		Summary:       "dispatch failed",
		Payload:       map[string]any{},
		Errors:        []string{err.Error()},
	}
}

func commitFailureReport(wr store.WorkflowRun, stage store.Stage, err error, snapshot workerSnapshot) report.Report {
	summary := "commit failed"
	payload := map[string]any{
		"diff_artifact_id": snapshot.DiffArtifactID,
	}
	if snapshot.BaseSHA != "" {
		payload["base_sha"] = snapshot.BaseSHA
	}
	if snapshot.BaseTreeSHA != "" {
		payload["base_tree_sha"] = snapshot.BaseTreeSHA
	}
	if snapshot.WorkerTreeSHA != "" {
		payload["worker_tree_sha"] = snapshot.WorkerTreeSHA
	}
	var lost worktreeLostOnResumeError
	if errors.As(err, &lost) {
		summary = worktreeLostOnResumeSummary
		payload["failure_reason"] = "worktree_lost_on_restart"
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "commit"},
		Status:        report.StatusFailed,
		Summary:       summary,
		Payload:       payload,
		Errors:        []string{err.Error()},
	}
}

func runEvent(wr store.WorkflowRun, typ string, actor event.Actor, summary string, data map[string]any) event.Event {
	return event.Event{SchemaVersion: event.SchemaVersion, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, Type: typ, Actor: actor, Summary: summary, Data: data}
}

func stageEvent(wr store.WorkflowRun, stage store.Stage, typ string, actor event.Actor, summary string, data map[string]any) event.Event {
	if data == nil {
		data = map[string]any{}
	}
	data["stage_id"] = stage.ID
	data["stage_type"] = stage.StageType
	return event.Event{SchemaVersion: event.SchemaVersion, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, Type: typ, Actor: actor, Summary: summary, Data: data}
}

// completionEventType maps a stage report to the performer-family detail event
// from event.schema.md. Unknown/human actors do not emit task.* in the skeleton;
// the stage terminal event records their lifecycle outcome.
func completionEventType(rep report.Report) string {
	failed := rep.Status == report.StatusFailed || rep.Status == report.StatusInvalid
	switch rep.Actor.Kind {
	case report.ActorKindAgent:
		if failed {
			return "adapter.failed"
		}
		return "adapter.completed"
	case report.ActorKindHarness:
		if failed {
			return "harness.failed"
		}
		return "harness.completed"
	default:
		return ""
	}
}

func stageTerminalEventType(status string) string {
	switch status {
	case report.StatusCompleted, report.StatusApproved, report.StatusChangesRequested:
		return "stage.completed"
	case report.StatusInvalid:
		return "stage.invalid"
	default:
		return "stage.failed"
	}
}

func stageTerminalSummary(stage store.Stage, rep report.Report) string {
	switch rep.Status {
	case report.StatusCompleted, report.StatusApproved, report.StatusChangesRequested:
		return stage.StageType + " stage completed"
	case report.StatusInvalid:
		return stage.StageType + " stage returned invalid report"
	default:
		return stage.StageType + " stage failed"
	}
}

func reportActor(actor report.Actor, stage store.Stage) event.Actor {
	switch actor.Kind {
	case report.ActorKindAgent:
		return event.Actor{Kind: event.ActorKindAdapter, ID: actor.ID}
	case report.ActorKindHarness:
		return event.Actor{Kind: event.ActorKindHarness, ID: actor.ID}
	case report.ActorKindHuman:
		return event.Actor{Kind: event.ActorKindUser, ID: actor.ID}
	default:
		return event.Actor{Kind: event.ActorKindWorkflowEngine, ID: stage.StageType}
	}
}

func reportCarriesDeliveryPayload(rep report.Report) bool {
	return payloadString(rep.Payload, "branch") != "" || payloadString(rep.Payload, "commit_sha") != "" || payloadString(rep.Payload, "diff_artifact_id") != ""
}

func withDeliveryPayload(rep report.Report, delivery report.Report) report.Report {
	if rep.Payload == nil {
		rep.Payload = map[string]any{}
	}
	copyPayloadStrings(rep.Payload, delivery.Payload, "branch", "branch_policy", "commit_sha", "diff_artifact_id")
	return rep
}

func settingString(settings map[string]any, key string) string {
	return payloadString(settings, key)
}

func payloadString(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func copyPayloadStrings(dest map[string]any, payload map[string]any, keys ...string) {
	for _, key := range keys {
		if value := payloadString(payload, key); value != "" {
			dest[key] = value
		}
	}
}
