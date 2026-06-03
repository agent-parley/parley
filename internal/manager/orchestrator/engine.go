package orchestrator

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/protocol"
	"github.com/agent-parley/parley/internal/shared/report"
)

type Runner interface {
	Dispatch(context.Context, contract.Dispatch) (report.Report, error)
	Cancel(context.Context, string, string) error
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
}

func NewEngine(st *store.Store, runner Runner, renderer FragmentRenderer, broadcast Broadcaster) *Engine {
	return &Engine{store: st, runner: runner, renderer: renderer, broadcast: broadcast, graph: NewGraph()}
}

func (e *Engine) StartRun(ctx context.Context, idea string) (string, error) {
	wr, err := e.store.CreateWorkflowRun(ctx, idea)
	if err != nil {
		return "", err
	}
	if err := e.store.SaveWorkflowSnapshot(ctx, wr.Run.ID, map[string]any{"graph": "implementation->validation", "run_id": wr.Run.ID}); err != nil {
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
	return e.runner.Cancel(ctx, wr.Run.ID, wr.Task.ID)
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
	if err := e.store.UpdateStageStatus(ctx, wr.ImplementationStage.ID, store.StageStatusRunning); err != nil {
		return err
	}
	if _, err := e.emit(ctx, stageEvent(wr, wr.ImplementationStage, "stage.started", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "implementation stage started", nil)); err != nil {
		return err
	}

	disp := contract.Dispatch{
		RunID:     wr.Run.ID,
		TaskID:    wr.Task.ID,
		AttemptID: wr.Attempt.ID,
		StageID:   wr.ImplementationStage.ID,
		StageType: contract.StageTypeImplementation,
		Adapter:   "noop",
		Input:     map[string]any{"idea": wr.Run.Idea},
	}
	implementationReport, err := e.runner.Dispatch(ctx, disp)
	if err != nil {
		implementationReport = report.Report{
			SchemaVersion: report.SchemaVersion,
			RunID:         wr.Run.ID,
			TaskID:        wr.Task.ID,
			AttemptID:     wr.Attempt.ID,
			StageID:       wr.ImplementationStage.ID,
			StageType:     contract.StageTypeImplementation,
			Actor:         report.Actor{Kind: report.ActorKindAgent, ID: "noop"},
			Status:        report.StatusFailed,
			Summary:       "dispatch failed",
			Payload:       map[string]any{},
			Errors:        []string{err.Error()},
		}
	}
	if err := e.completeStage(ctx, wr, wr.ImplementationStage, implementationReport); err != nil {
		return err
	}
	next, err := e.graph.Next(contract.StageTypeImplementation, implementationReport.Status)
	if err != nil {
		return err
	}
	if next == NodeStopReport {
		return e.stopRun(ctx, wr, implementationReport.Status, "workflow stopped after implementation")
	}

	if err := e.store.UpdateStageStatus(ctx, wr.ValidationStage.ID, store.StageStatusRunning); err != nil {
		return err
	}
	if _, err := e.emit(ctx, stageEvent(wr, wr.ValidationStage, "stage.started", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "validation stage started", nil)); err != nil {
		return err
	}
	validationReport := validationReport(wr, implementationReport)
	if err := e.completeStage(ctx, wr, wr.ValidationStage, validationReport); err != nil {
		return err
	}
	next, err = e.graph.Next(contract.StageTypeValidation, validationReport.Status)
	if err != nil {
		return err
	}
	if next == NodeDone {
		if err := e.store.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
			return err
		}
		_, err := e.emit(ctx, runEvent(wr, "run.completed", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, "run completed", map[string]any{"terminal_status": store.RunStatusCompleted}))
		return err
	}
	return e.stopRun(ctx, wr, validationReport.Status, "workflow stopped after validation")
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
	if err := e.materializeAdapterArtifact(ctx, rep); err != nil {
		return err
	}
	artifact, err := e.store.SaveReportArtifact(ctx, rep)
	if err != nil {
		return err
	}
	if err := e.store.UpdateStageStatus(ctx, stage.ID, rep.Status); err != nil {
		return err
	}
	_, err = e.emit(ctx, stageEvent(wr, stage, completionEventType(rep), reportActor(rep.Actor, stage), rep.Summary, map[string]any{
		"stage_id":           stage.ID,
		"stage_type":         stage.StageType,
		"status":             rep.Status,
		"report_artifact_id": artifact.ID,
	}))
	return err
}

func (e *Engine) materializeAdapterArtifact(ctx context.Context, rep report.Report) error {
	if rep.Payload == nil {
		return nil
	}
	raw, ok := rep.Payload["artifact"]
	if !ok {
		return nil
	}
	artifactMap, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	artifactID, _ := artifactMap["id"].(string)
	content, _ := artifactMap["content"].(string)
	name, _ := artifactMap["name"].(string)
	if artifactID == "" || content == "" {
		return nil
	}
	ext := filepath.Ext(name)
	if ext == "" {
		ext = ".txt"
	}
	_, err := e.store.SaveArtifactWithID(ctx, artifactID, rep.RunID, "adapter_output", "text/plain", []byte(content), ext)
	return err
}

func (e *Engine) stopRun(ctx context.Context, wr store.WorkflowRun, status, summary string) error {
	runStatus := store.RunStatusFailed
	switch status {
	case report.StatusInvalid:
		runStatus = store.RunStatusInvalid
	case report.StatusNeedsInput:
		runStatus = store.RunStatusNeedsInput
	}
	if err := e.store.UpdateRunStatus(ctx, wr.Run.ID, runStatus); err != nil {
		return err
	}
	_, err := e.emit(ctx, runEvent(wr, "run.stopped", event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, summary, map[string]any{"terminal_status": runStatus}))
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

func completionEventType(rep report.Report) string {
	if rep.Actor.Kind == report.ActorKindAgent {
		return "adapter.completed"
	}
	return "harness.completed"
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
