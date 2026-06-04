package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sync"

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

	implementationAdapter string
	validationAdapter     string
	dataRoot              string
	projectID             string
	gitAuthorName         string
	gitAuthorEmail        string
	gitExecutable         string
}

type EngineOptions struct {
	ImplementationAdapter string
	ValidationAdapter     string
	DataRoot              string
	ProjectID             string
	GitAuthorName         string
	GitAuthorEmail        string
	GitExecutable         string
}

func NewEngine(st *store.Store, runner Runner, renderer FragmentRenderer, broadcast Broadcaster) *Engine {
	return NewEngineWithOptions(st, runner, renderer, broadcast, EngineOptions{})
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
		projectID = "default"
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
		implementationAdapter: implementationAdapter,
		validationAdapter:     validationAdapter,
		dataRoot:              dataRoot,
		projectID:             projectID,
		gitAuthorName:         opts.GitAuthorName,
		gitAuthorEmail:        opts.GitAuthorEmail,
		gitExecutable:         opts.GitExecutable,
	}
}

func (e *Engine) StartRun(ctx context.Context, idea string) (string, error) {
	wr, err := e.store.CreateWorkflowRun(ctx, idea)
	if err != nil {
		return "", err
	}
	wr.ImplementationStage.Adapter = e.implementationAdapter
	wr.ValidationStage.Adapter = e.validationAdapter
	if err := e.store.UpdateStageAdapter(ctx, wr.ImplementationStage.ID, e.implementationAdapter); err != nil {
		return "", err
	}
	if err := e.store.UpdateStageAdapter(ctx, wr.ValidationStage.ID, e.validationAdapter); err != nil {
		return "", err
	}
	if err := e.store.SaveWorkflowSnapshot(ctx, wr.Run.ID, e.workflowSnapshot(wr, "", false)); err != nil {
		return "", err
	}
	if _, err := e.emit(ctx, runEvent(wr, "run.created", event.Actor{Kind: event.ActorKindUser, ID: "local"}, "run created", map[string]any{"idea": idea})); err != nil {
		return "", err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	e.registerActiveRun(wr.Run.ID, cancel)
	go e.executeRun(runCtx, wr.Run.ID)
	return wr.Run.ID, nil
}

func (e *Engine) CancelRun(ctx context.Context, runID string) error {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	if store.RunStatusIsTerminal(wr.Run.Status) {
		return nil
	}
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
	defer e.unregisterActiveRun(runID)
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
	changed, err := e.store.UpdateRunStatusIfOpen(context.Background(), wr.Run.ID, store.RunStatusRunning)
	if err != nil {
		return err
	}
	if !changed {
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

	commitReport, err := e.runCommit(ctx, wr, validationReport)
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
	changed, err = e.store.UpdateRunStatusIfOpen(context.Background(), wr.Run.ID, store.RunStatusCompleted)
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
	e.setActiveStage(wr.Run.ID, stage)
	defer e.clearActiveStage(wr.Run.ID, stage.ID)
	if err := e.startStage(ctx, wr, stage, stage.StageType+" stage started"); err != nil {
		return report.Report{}, err
	}
	disp := contract.Dispatch{
		RunID:     wr.Run.ID,
		TaskID:    wr.Task.ID,
		AttemptID: wr.Attempt.ID,
		StageID:   stage.ID,
		StageType: stageType,
		Adapter:   adapterName,
		Input:     input,
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
		rep = dispatchFailedReport(wr, stage, adapterName, err)
	}
	if err := e.completeStage(context.Background(), wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

func (e *Engine) runCommit(ctx context.Context, wr store.WorkflowRun, validationReport report.Report) (report.Report, error) {
	stage := wr.CommitStage
	if err := e.startStage(ctx, wr, stage, "commit stage started"); err != nil {
		return report.Report{}, err
	}
	worktreePath, err := worktree.Locate(e.dataRoot, e.projectID, wr.Run.ID, wr.Task.ID, wr.Attempt.ID)
	if err != nil {
		rep := commitFailureReport(wr, stage, err, validationReport)
		return rep, e.completeStage(context.Background(), wr, stage, rep)
	}
	result, err := commitWorktree(ctx, commitOptions{
		WorktreePath:   worktreePath,
		RunID:          wr.Run.ID,
		TaskID:         wr.Task.ID,
		Idea:           wr.Run.Idea,
		ReportSummary:  validationReport.Summary,
		DiffArtifactID: payloadString(validationReport.Payload, "diff_artifact_id"),
		Git:            e.gitExecutable,
		AuthorName:     e.gitAuthorName,
		AuthorEmail:    e.gitAuthorEmail,
	})
	if err != nil {
		rep := commitFailureReport(wr, stage, err, validationReport)
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
		EvidenceRefs:  []string{payloadString(validationReport.Payload, "diff_artifact_id")},
		Payload: map[string]any{
			"branch":           result.Branch,
			"commit_sha":       result.CommitSHA,
			"diff_artifact_id": payloadString(validationReport.Payload, "diff_artifact_id"),
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

func (e *Engine) runPRReady(ctx context.Context, wr store.WorkflowRun, commitReport report.Report) (report.Report, error) {
	stage := wr.PRReadyStage
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

func (e *Engine) startStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, summary string) error {
	e.setActiveStage(wr.Run.ID, stage)
	if err := e.store.UpdateStageStatus(ctx, stage.ID, store.StageStatusRunning); err != nil {
		return err
	}
	_, err := e.emit(ctx, stageEvent(wr, stage, "stage.started", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, summary, nil))
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
	_, err = e.emit(ctx, runEvent(wr, eventType, event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, summary, map[string]any{"terminal_status": runStatus}))
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
	return fmt.Sprintf("# Parley Task Contract\n\nRun ID: `%s`\nTask ID: `%s`\nAttempt ID: `%s`\n\n## User idea (verbatim)\n\n%s\n", wr.Run.ID, wr.Task.ID, wr.Attempt.ID, wr.Run.Idea)
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

func commitFailureReport(wr store.WorkflowRun, stage store.Stage, err error, validationReport report.Report) report.Report {
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
			"diff_artifact_id": payloadString(validationReport.Payload, "diff_artifact_id"),
		},
		Errors: []string{err.Error()},
	}
}

func runEvent(wr store.WorkflowRun, typ string, actor event.Actor, summary string, data map[string]any) event.Event {
	return event.Event{SchemaVersion: event.SchemaVersion, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, Type: typ, Actor: actor, Summary: summary, Data: data}
}

func stageEvent(wr store.WorkflowRun, stage store.Stage, typ string, actor event.Actor, summary string, data map[string]any) event.Event {
	if data == nil {
		data = map[string]any{}
	}
	data["stage_id"] = stage.ID
	data["stage_type"] = stage.StageType
	return event.Event{SchemaVersion: event.SchemaVersion, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, Type: typ, Actor: actor, Summary: summary, Data: data}
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
