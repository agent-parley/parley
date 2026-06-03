package orchestrator

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

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
	store                 *store.Store
	runner                Runner
	renderer              FragmentRenderer
	broadcast             Broadcaster
	graph                 Graph
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
	go e.executeRun(context.Background(), wr.Run.ID)
	return wr.Run.ID, nil
}

func (e *Engine) CancelRun(ctx context.Context, runID string) error {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	return e.runner.CancelAttempt(ctx, wr.Run.ID, wr.Task.ID, wr.Attempt.ID)
}

func (e *Engine) HandleRunnerEvent(ctx context.Context, ev event.Event) error {
	_, err := e.emit(ctx, ev)
	return err
}

func (e *Engine) HandleRunnerReport(context.Context, report.Report) error { return nil }

func (e *Engine) HandleRunnerResult(context.Context, protocol.ResultPayload) error { return nil }

func (e *Engine) HandleRunnerLog(context.Context, protocol.LogPayload) error { return nil }

func (e *Engine) executeRun(ctx context.Context, runID string) {
	if err := e.executeRunErr(ctx, runID); err != nil {
		log.Printf("workflow %s failed: %v", runID, err)
	}
}

func (e *Engine) executeRunErr(ctx context.Context, runID string) error {
	wr, err := e.store.GetWorkflowRun(ctx, runID)
	if err != nil {
		return err
	}
	if err := e.store.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusRunning); err != nil {
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
		return e.stopRun(ctx, wr, ideaReport.Status, "workflow stopped after idea intake")
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
		return e.stopRun(ctx, wr, implementationReport.Status, "workflow stopped after implementation")
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
		return e.stopRun(ctx, wr, validationReport.Status, "workflow stopped after validation")
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
		return e.stopRun(ctx, wr, commitReport.Status, "workflow stopped after commit")
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
		return e.stopRun(ctx, wr, prReadyReport.Status, "workflow stopped after pr_ready")
	}
	if err := e.store.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		return err
	}
	_, err = e.emit(ctx, runEvent(wr, "run.completed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run reached PR-ready human stop", map[string]any{
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
	if err := e.completeStage(ctx, wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

func (e *Engine) dispatchStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, adapterName, stageType string, input map[string]any) (report.Report, error) {
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
	rep, err := e.runner.Dispatch(ctx, disp)
	if err != nil {
		rep = dispatchFailedReport(wr, stage, adapterName, err)
	}
	if err := e.completeStage(ctx, wr, stage, rep); err != nil {
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
		return rep, e.completeStage(ctx, wr, stage, rep)
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
		return rep, e.completeStage(ctx, wr, stage, rep)
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
	if err := e.completeStage(ctx, wr, stage, rep); err != nil {
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
	if err := e.completeStage(ctx, wr, stage, rep); err != nil {
		return report.Report{}, err
	}
	return rep, nil
}

func (e *Engine) startStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, summary string) error {
	if err := e.store.UpdateStageStatus(ctx, stage.ID, store.StageStatusRunning); err != nil {
		return err
	}
	_, err := e.emit(ctx, stageEvent(wr, stage, "stage.started", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, summary, nil))
	return err
}

func (e *Engine) completeStage(ctx context.Context, wr store.WorkflowRun, stage store.Stage, rep report.Report) error {
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
	_, err = e.emit(ctx, stageEvent(wr, stage, completionEventType(rep), reportActor(rep.Actor, stage), rep.Summary, data))
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
	if err := e.store.UpdateRunStatus(ctx, wr.Run.ID, runStatus); err != nil {
		return err
	}
	_, err := e.emit(ctx, runEvent(wr, eventType, event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, summary, map[string]any{"terminal_status": runStatus}))
	return err
}

func (e *Engine) emit(ctx context.Context, ev event.Event) (event.Event, error) {
	persisted, err := e.store.AppendEvent(ctx, ev)
	if err != nil {
		return event.Event{}, err
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

// completionEventType maps a stage report to an outcome-faithful event type
// (0068 + event.schema.md). `invalid` is a non-success terminal, so it shares the
// `.failed` type; the precise status travels in the event's `status` data field.
// needs_input does not arise from skeleton stages; it folds into `.failed` until
// human-input stages land, which will route it to approval.requested.
// Note: event.schema.md does not yet define harness.* completion types; emitting
// them here keeps the actor and the type consistent (tracked as issue #6).
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
		if failed {
			return "task.failed"
		}
		return "task.completed"
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
