package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sync"

	"github.com/agent-parley/parley/internal/manager/contextpack"
	"github.com/agent-parley/parley/internal/manager/store"
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

type Engine struct {
	store     *store.Store
	runner    Runner
	renderer  FragmentRenderer
	broadcast Broadcaster
	graph     Graph

	mu          sync.Mutex
	activeRuns  map[string]context.CancelFunc
	activeStage map[string]store.Stage
	cancelling  map[string]bool
	runnerDown  map[string]string
	queueMu     sync.Mutex

	queuePolicy           QueuePolicy
	runnerSlots           int
	gate                  func(context.Context, store.Run) (bool, error)
	implementationAdapter string
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
	ValidationAdapter     string
	DataRoot              string
	ProjectID             string
	GitAuthorName         string
	GitAuthorEmail        string
	GitExecutable         string
	QueuePolicy           *QueuePolicy
	RunnerSlots           int
	ContextAssembler      *contextpack.Assembler
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
	contextAssembler := opts.ContextAssembler
	if contextAssembler == nil {
		contextAssembler = contextpack.NewAssembler(contextpack.Options{})
	}
	return &Engine{
		store:                 st,
		runner:                runner,
		renderer:              renderer,
		broadcast:             broadcast,
		graph:                 NewGraph(),
		activeRuns:            map[string]context.CancelFunc{},
		activeStage:           map[string]store.Stage{},
		cancelling:            map[string]bool{},
		runnerDown:            map[string]string{},
		queuePolicy:           queuePolicy,
		runnerSlots:           runnerSlots,
		implementationAdapter: implementationAdapter,
		validationAdapter:     validationAdapter,
		dataRoot:              dataRoot,
		projectID:             projectID,
		gitAuthorName:         opts.GitAuthorName,
		gitAuthorEmail:        opts.GitAuthorEmail,
		gitExecutable:         opts.GitExecutable,
		contextAssembler:      contextAssembler,
	}
}

func (e *Engine) StartRun(ctx context.Context, idea string) (string, error) {
	return e.StartProjectRun(ctx, e.projectID, idea)
}

func (e *Engine) StartProjectRun(ctx context.Context, projectID, idea string) (string, error) {
	if projectID == "" {
		projectID = e.projectID
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
	wr, err := e.createQueuedRun(ctx, projectID, idea)
	if err != nil {
		e.queueMu.Unlock()
		return "", err
	}
	if _, err := e.emit(ctx, runEvent(wr, "run.created", event.Actor{Kind: event.ActorKindUser, ID: "local"}, "run created", map[string]any{"idea": idea})); err != nil {
		e.queueMu.Unlock()
		return "", err
	}
	if err := e.emitQueueEvent(ctx, projectID, "queue.enqueued", "run enqueued", map[string]any{"project_id": projectID, "run_id": wr.Run.ID, "pending": pending + 1, "backlog_cap": e.queuePolicy.BacklogCap}); err != nil {
		e.queueMu.Unlock()
		return "", err
	}
	e.queueMu.Unlock()
	if e.queuePolicy.AutoWhenReady {
		go func() {
			if err := e.DispatchPending(context.Background()); err != nil {
				log.Printf("dispatch pending failed: %v", err)
			}
		}()
	}
	return wr.Run.ID, nil
}

func (e *Engine) createQueuedRun(ctx context.Context, projectID, idea string) (store.WorkflowRun, error) {
	if _, err := e.store.GetProject(ctx, projectID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return store.WorkflowRun{}, err
		}
		if _, err := e.store.EnsureProject(ctx, store.ProjectSpec{ID: projectID, Name: projectID, QueueAutoWhenReady: e.queuePolicy.AutoWhenReady, QueueMaxConcurrent: e.queuePolicy.MaxConcurrent, QueueBacklogCap: e.queuePolicy.BacklogCap}); err != nil {
			return store.WorkflowRun{}, err
		}
	}
	wr, err := e.store.CreateWorkflowRunForProject(ctx, projectID, idea)
	if err != nil {
		return store.WorkflowRun{}, err
	}
	wr.ImplementationStage.Adapter = e.implementationAdapter
	wr.ValidationStage.Adapter = e.validationAdapter
	if err := e.store.UpdateStageAdapter(ctx, wr.ImplementationStage.ID, e.implementationAdapter); err != nil {
		return store.WorkflowRun{}, err
	}
	if err := e.store.UpdateStageAdapter(ctx, wr.ValidationStage.ID, e.validationAdapter); err != nil {
		return store.WorkflowRun{}, err
	}
	if err := e.store.SaveWorkflowSnapshot(ctx, wr.Run.ID, e.workflowSnapshot(wr, "", false)); err != nil {
		return store.WorkflowRun{}, err
	}
	return wr, nil
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
	changed, err := e.store.UpdateRunStatusFrom(ctx, wr.Run.ID, store.RunStatusPending, store.RunStatusCancelled)
	if err != nil {
		return err
	}
	if !changed {
		return ErrRunNotPending
	}
	_, err = e.emit(ctx, runEvent(wr, "run.cancelled", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "queued run cancelled", map[string]any{"terminal_status": store.RunStatusCancelled}))
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
		changed, err := e.store.UpdateRunStatusFrom(ctx, run.ID, store.RunStatusRunning, store.RunStatusFailed)
		if err != nil {
			joined = append(joined, err)
			continue
		}
		if !changed {
			continue
		}
		_, err = e.emit(ctx, runEvent(wr, "run.failed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run failed during manager restart recovery", map[string]any{
			"terminal_status": store.RunStatusFailed,
			"reason":          "manager_restarted",
			"retryable":       true,
		}))
		if err != nil {
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
	runCtx, cancel := context.WithCancel(context.Background())
	e.registerActiveRun(run.ID, cancel)
	go e.executeRun(runCtx, run.ID)
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
		changed, err := e.store.UpdateRunStatusIfOpen(ctx, runID, store.RunStatusFailed)
		if err != nil {
			joined = append(joined, err)
			continue
		}
		if !changed {
			continue
		}
		_, err = e.emit(ctx, runEvent(wr, "run.failed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "runner disconnected", map[string]any{
			"terminal_status": store.RunStatusFailed,
			"reason":          "runner_disconnected",
			"runner_id":       runnerID,
			"signal":          reason,
		}))
		if err != nil {
			joined = append(joined, err)
		}
	}
	return errors.Join(joined...)
}

func (e *Engine) executeRun(ctx context.Context, runID string) {
	defer func() {
		e.unregisterActiveRun(runID)
		if e.queuePolicy.AutoWhenReady {
			go func() {
				if err := e.DispatchPending(context.Background()); err != nil {
					log.Printf("dispatch pending failed: %v", err)
				}
			}()
		}
	}()
	if err := e.executeRunErr(ctx, runID); err != nil {
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
	wr, err := e.store.GetWorkflowRun(context.Background(), runID)
	if err != nil {
		return err
	}
	if wr.Run.Status != store.RunStatusRunning {
		return nil
	}
	if _, err := e.emit(context.Background(), runEvent(wr, "run.started", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run started", map[string]any{"status": store.RunStatusRunning})); err != nil {
		return err
	}

	ideaReport, err := e.runIdeaIntake(ctx, wr)
	if err != nil {
		return err
	}
	next, err := e.graph.Next(contract.StageTypeIdeaIntake, ideaReport.Status)
	if err != nil {
		return err
	}
	if next == NodeStopReport {
		return e.stopRun(context.Background(), wr, ideaReport.Status, "workflow stopped after idea intake")
	}
	if e.isCancelling(wr.Run.ID) {
		return e.cancelRunTerminal(context.Background(), wr, "workflow cancelled after idea intake")
	}

	implementationReport, err := e.dispatchStage(ctx, wr, wr.ImplementationStage, e.implementationAdapter, contract.StageTypeImplementation, implementationInput(wr, ideaReport))
	if err != nil {
		return err
	}
	next, err = e.graph.Next(contract.StageTypeImplementation, implementationReport.Status)
	if err != nil {
		return err
	}
	if next == NodeStopReport {
		return e.stopRun(context.Background(), wr, implementationReport.Status, "workflow stopped after implementation")
	}
	if e.isCancelling(wr.Run.ID) {
		return e.cancelRunTerminal(context.Background(), wr, "workflow cancelled after implementation")
	}

	workerSnapshot, snapshotErr := e.snapshotWorktree(ctx, wr, implementationReport)

	validationReport, err := e.dispatchStage(ctx, wr, wr.ValidationStage, e.validationAdapter, contract.StageTypeValidation, map[string]any{"idea": wr.Run.Idea})
	if err != nil {
		return err
	}
	next, err = e.graph.Next(contract.StageTypeValidation, validationReport.Status)
	if err != nil {
		return err
	}
	if next == NodeStopReport {
		return e.stopRun(context.Background(), wr, validationReport.Status, "workflow stopped after validation")
	}
	if e.isCancelling(wr.Run.ID) {
		return e.cancelRunTerminal(context.Background(), wr, "workflow cancelled after validation")
	}

	commitReport, err := e.runCommit(ctx, wr, validationReport, workerSnapshot, snapshotErr)
	if err != nil {
		return err
	}
	next, err = e.graph.Next(contract.StageTypeCommit, commitReport.Status)
	if err != nil {
		return err
	}
	if next == NodeStopReport {
		return e.stopRun(context.Background(), wr, commitReport.Status, "workflow stopped after commit")
	}
	if e.isCancelling(wr.Run.ID) {
		return e.cancelRunTerminal(context.Background(), wr, "workflow cancelled after commit")
	}

	prReadyReport, err := e.runPRReady(ctx, wr, commitReport)
	if err != nil {
		return err
	}
	next, err = e.graph.Next(contract.StageTypePRReady, prReadyReport.Status)
	if err != nil {
		return err
	}
	if next != NodeDone {
		return e.stopRun(context.Background(), wr, prReadyReport.Status, "workflow stopped after pr_ready")
	}
	if e.isCancelling(wr.Run.ID) {
		return e.cancelRunTerminal(context.Background(), wr, "workflow cancelled after pr_ready")
	}
	changed, err := e.store.UpdateRunStatusIfOpen(context.Background(), wr.Run.ID, store.RunStatusCompleted)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	_, err = e.emit(context.Background(), runEvent(wr, "run.completed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run reached PR-ready human stop", map[string]any{
		"terminal_status":  store.RunStatusCompleted,
		"branch":           payloadString(prReadyReport.Payload, "branch"),
		"commit_sha":       payloadString(prReadyReport.Payload, "commit_sha"),
		"diff_artifact_id": payloadString(prReadyReport.Payload, "diff_artifact_id"),
	}))
	return err
}

func (e *Engine) runIdeaIntake(ctx context.Context, wr store.WorkflowRun) (report.Report, error) {
	stage := wr.IdeaIntakeStage
	_, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	if err := e.startStage(ctx, wr, stage, "idea_intake stage started"); err != nil {
		return report.Report{}, err
	}
	contractMarkdown := taskContractMarkdown(wr)
	contractArtifact, err := e.store.SaveArtifact(ctx, wr.Run.ID, "task_contract", "text/markdown", []byte(contractMarkdown), ".md")
	if err != nil {
		return report.Report{}, err
	}
	if err := e.store.SaveWorkflowSnapshot(ctx, wr.Run.ID, e.workflowSnapshot(wr, contractArtifact.ID, true)); err != nil {
		return report.Report{}, err
	}
	rep := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "idea_intake"},
		Status:        report.StatusCompleted,
		Summary:       "idea frozen into task contract and workflow snapshot",
		EvidenceRefs:  []string{contractArtifact.ID},
		Payload: map[string]any{
			"idea_verbatim":             wr.Run.Idea,
			"task_contract_artifact_id": contractArtifact.ID,
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
	brief, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	e.setActiveStage(wr.Run.ID, stage)
	defer e.clearActiveStage(wr.Run.ID, stage.ID)
	if err := e.startStage(ctx, wr, stage, stage.StageType+" stage started"); err != nil {
		return report.Report{}, err
	}
	input = withStageBriefInput(input, brief, briefArtifact.ID)
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
	if stageType == contract.StageTypeImplementation {
		if _, err := e.emit(ctx, stageEvent(wr, stage, "adapter.invocation_prepared", event.Actor{Kind: event.ActorKindAdapter, ID: adapterName}, "adapter invocation prepared", map[string]any{"adapter": adapterName})); err != nil {
			return report.Report{}, err
		}
		if _, err := e.emit(ctx, stageEvent(wr, stage, "adapter.started", event.Actor{Kind: event.ActorKindAdapter, ID: adapterName}, "adapter started", map[string]any{"adapter": adapterName})); err != nil {
			return report.Report{}, err
		}
	}
	if e.runner == nil {
		rep := dispatchFailedReport(wr, stage, adapterName, fmt.Errorf("runner unavailable"))
		return rep, e.completeStage(context.Background(), wr, stage, rep)
	}
	rep, err := e.runner.Dispatch(ctx, disp)
	if err != nil {
		if errors.Is(err, protocol.ErrSessionClosed) {
			// The dispatch failed because the runner vanished mid-stage — classify
			// it so the run terminal reads runner_disconnected regardless of whether
			// this path or HandleRunnerDown finalizes the run first.
			e.markRunnerDown(wr.Run.ID, "runner_disconnected")
		}
		rep = dispatchFailedReport(wr, stage, adapterName, err)
	}
	if err := e.completeStage(context.Background(), wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

func (e *Engine) runCommit(ctx context.Context, wr store.WorkflowRun, validationReport report.Report, snapshot workerSnapshot, snapshotErr error) (report.Report, error) {
	stage := wr.CommitStage
	_, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	if err := e.startStage(ctx, wr, stage, "commit stage started"); err != nil {
		return report.Report{}, err
	}
	if snapshotErr != nil {
		rep := commitFailureReport(wr, stage, snapshotErr, snapshot.DiffArtifactID)
		return rep, e.completeStage(context.Background(), wr, stage, rep)
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
	})
	if err != nil {
		rep := commitFailureReport(wr, stage, err, snapshot.DiffArtifactID)
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
	stage := wr.PRReadyStage
	_, briefArtifact, err := e.prepareStageBrief(ctx, wr, stage)
	if err != nil {
		return report.Report{}, err
	}
	stage.StageBriefArtifactID = briefArtifact.ID
	if err := e.startStage(ctx, wr, stage, "pr_ready stage started"); err != nil {
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
		Summary:       "PR-ready human stop reached",
		EvidenceRefs:  []string{diffID},
		Payload: map[string]any{
			"branch":           branch,
			"commit_sha":       commitSHA,
			"diff_artifact_id": diffID,
			"push_performed":   false,
			"pr_created":       false,
		},
		Errors: []string{},
	}
	if err := e.completeStage(context.Background(), wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

func (e *Engine) prepareStageBrief(ctx context.Context, wr store.WorkflowRun, stage store.Stage) (contextpack.StageBrief, store.Artifact, error) {
	bundle, err := e.store.RunBundle(ctx, wr.Run.ID)
	if err != nil {
		return contextpack.StageBrief{}, store.Artifact{}, err
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
		return contextpack.StageBrief{}, store.Artifact{}, err
	}
	artifact, err := e.store.SaveArtifact(ctx, wr.Run.ID, "stage_brief", "text/markdown", []byte(contextpack.Markdown(brief)), ".md")
	if err != nil {
		return contextpack.StageBrief{}, store.Artifact{}, err
	}
	if err := e.store.UpdateStageBriefArtifactID(ctx, stage.ID, artifact.ID); err != nil {
		return contextpack.StageBrief{}, store.Artifact{}, err
	}
	return brief, artifact, nil
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

func withStageBriefInput(input map[string]any, brief contextpack.StageBrief, artifactID string) map[string]any {
	out := map[string]any{}
	for key, value := range input {
		out[key] = value
	}
	markdown := contextpack.Markdown(brief)
	out["stage_brief_artifact_id"] = artifactID
	out["stage_brief"] = brief
	out["stage_brief_markdown"] = markdown
	out["curated_context"] = markdown
	return out
}

func (e *Engine) startStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, summary string) error {
	e.setActiveStage(wr.Run.ID, stage)
	if err := e.store.UpdateStageStatus(ctx, stage.ID, store.StageStatusRunning); err != nil {
		return err
	}
	data := map[string]any{}
	if stage.StageBriefArtifactID != "" {
		data["stage_brief_artifact_id"] = stage.StageBriefArtifactID
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
	if stage.StageBriefArtifactID != "" {
		data["stage_brief_artifact_id"] = stage.StageBriefArtifactID
	}
	copyPayloadStrings(data, rep.Payload, "branch", "commit_sha", "diff_artifact_id", "task_contract_artifact_id")
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

func (e *Engine) stopRun(ctx context.Context, wr store.WorkflowRun, status, summary string) error {
	if e.isCancelling(wr.Run.ID) {
		return e.cancelRunTerminal(ctx, wr, summary)
	}
	// Map to the documented run.* terminal taxonomy (event.schema.md): failed and
	// invalid are failure terminals; needs_input has no resume path in the skeleton
	// so it terminates as abandoned.
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
	changed, err := e.store.UpdateRunStatusIfOpen(ctx, wr.Run.ID, runStatus)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	data := map[string]any{"terminal_status": runStatus}
	if reason, ok := e.runnerDownReason(wr.Run.ID); ok && runStatus == store.RunStatusFailed {
		data["reason"] = reason
		summary = "runner disconnected"
	}
	_, err = e.emit(ctx, runEvent(wr, eventType, event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, summary, data))
	return err
}

func (e *Engine) cancelRunTerminal(ctx context.Context, wr store.WorkflowRun, summary string) error {
	changed, err := e.store.UpdateRunStatusIfOpen(ctx, wr.Run.ID, store.RunStatusCancelled)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	_, err = e.emit(ctx, runEvent(wr, "run.cancelled", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, summary, map[string]any{"terminal_status": store.RunStatusCancelled}))
	return err
}

func (e *Engine) emit(ctx context.Context, ev event.Event) (event.Event, error) {
	persisted, err := e.store.AppendEvent(ctx, ev)
	if err != nil {
		return event.Event{}, err
	}
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
	return persisted, nil
}

func (e *Engine) workflowSnapshot(wr store.WorkflowRun, taskContractArtifactID string, frozen bool) map[string]any {
	return map[string]any{
		"schema_version":            1,
		"project_id":                wr.Run.ProjectID,
		"workspace_path":            wr.Project.WorkspacePath,
		"repository_id":             wr.Task.RepositoryID,
		"run_id":                    wr.Run.ID,
		"task_id":                   wr.Task.ID,
		"attempt_id":                wr.Attempt.ID,
		"idea_verbatim":             wr.Run.Idea,
		"frozen":                    frozen,
		"task_contract_artifact_id": taskContractArtifactID,
		"graph":                     "idea_intake->implementation->validation->commit->pr_ready",
		"edges":                     e.graph.Edges(),
		"stages": []map[string]string{
			stageSnapshot(wr.IdeaIntakeStage),
			stageSnapshot(wr.ImplementationStage),
			stageSnapshot(wr.ValidationStage),
			stageSnapshot(wr.CommitStage),
			stageSnapshot(wr.PRReadyStage),
		},
	}
}

func stageSnapshot(stage store.Stage) map[string]string {
	return map[string]string{"id": stage.ID, "type": stage.StageType, "adapter": stage.Adapter}
}

func taskContractMarkdown(wr store.WorkflowRun) string {
	return fmt.Sprintf("# Parley Task Contract\n\nProject ID: `%s`\nRun ID: `%s`\nTask ID: `%s`\nAttempt ID: `%s`\n\n## User idea (verbatim)\n\n%s\n", wr.Run.ProjectID, wr.Run.ID, wr.Task.ID, wr.Attempt.ID, wr.Run.Idea)
}

func implementationInput(wr store.WorkflowRun, ideaReport report.Report) map[string]any {
	return map[string]any{
		"idea":                      wr.Run.Idea,
		"contract_markdown":         taskContractMarkdown(wr),
		"task_contract_artifact_id": payloadString(ideaReport.Payload, "task_contract_artifact_id"),
		"workflow_snapshot_frozen":  true,
	}
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

func commitFailureReport(wr store.WorkflowRun, stage store.Stage, err error, diffArtifactID string) report.Report {
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       stage.ID,
		StageType:     stage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: "commit"},
		Status:        report.StatusFailed,
		Summary:       "commit failed",
		Payload: map[string]any{
			"diff_artifact_id": diffArtifactID,
		},
		Errors: []string{err.Error()},
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
	failed := rep.Status != report.StatusCompleted
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
	case report.StatusCompleted:
		return "stage.completed"
	case report.StatusInvalid:
		return "stage.invalid"
	default:
		return "stage.failed"
	}
}

func stageTerminalSummary(stage store.Stage, rep report.Report) string {
	switch rep.Status {
	case report.StatusCompleted:
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

func copyPayloadStrings(dest map[string]any, payload map[string]any, keys ...string) {
	for _, key := range keys {
		if value := payloadString(payload, key); value != "" {
			dest[key] = value
		}
	}
}
