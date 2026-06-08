package orchestrator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestCompletionEventType(t *testing.T) {
	cases := []struct {
		name   string
		actor  string
		status string
		want   string
	}{
		{"agent completed", report.ActorKindAgent, report.StatusCompleted, "adapter.completed"},
		{"agent failed", report.ActorKindAgent, report.StatusFailed, "adapter.failed"},
		{"agent invalid", report.ActorKindAgent, report.StatusInvalid, "adapter.failed"},
		{"harness completed", report.ActorKindHarness, report.StatusCompleted, "harness.completed"},
		{"harness failed", report.ActorKindHarness, report.StatusFailed, "harness.failed"},
		{"harness invalid", report.ActorKindHarness, report.StatusInvalid, "harness.failed"},
		{"human completed", report.ActorKindHuman, report.StatusCompleted, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := report.Report{Actor: report.Actor{Kind: tc.actor, ID: "x"}, Status: tc.status}
			if got := completionEventType(rep); got != tc.want {
				t.Fatalf("completionEventType(%s,%s) = %q, want %q", tc.actor, tc.status, got, tc.want)
			}
		})
	}
}

func TestDispatchStagePersistsStageBriefAndPassesItToRunner(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "build a bounded brief")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	runner := &capturingRunner{}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	rep, err := engine.dispatchStage(ctx, wr, wr.ImplementationStage, "capture", contract.StageTypeImplementation, implementationInput(wr, report.Report{}))
	if err != nil {
		t.Fatalf("dispatchStage() error = %v", err)
	}
	if rep.Status != report.StatusCompleted {
		t.Fatalf("status = %s", rep.Status)
	}
	briefID, _ := runner.disp.Input["stage_brief_artifact_id"].(string)
	if briefID == "" {
		t.Fatalf("dispatch input missing stage brief artifact id: %+v", runner.disp.Input)
	}
	briefText, _ := runner.disp.Input["stage_brief_markdown"].(string)
	if !strings.Contains(briefText, "# Stage brief") || !strings.Contains(briefText, "## Source: workflow_snapshot") {
		t.Fatalf("dispatch input missing source-labeled Stage brief:\n%s", briefText)
	}
	stages, err := st.ListStages(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list stages: %v", err)
	}
	for _, stage := range stages {
		if stage.ID == wr.ImplementationStage.ID {
			if stage.StageBriefArtifactID != briefID {
				t.Fatalf("stage brief ref = %s, want %s", stage.StageBriefArtifactID, briefID)
			}
			return
		}
	}
	t.Fatal("implementation stage not found")
}

func TestRuntimeGraphUsesFrozenTemplateEdges(t *testing.T) {
	template := workflow.DefaultTemplate()
	for _, candidate := range workflow.PredefinedTemplates() {
		if candidate.ID == workflow.AutonomousPRDeliveryID {
			template = candidate
			break
		}
	}
	graph, err := newRuntimeGraph(template)
	if err != nil {
		t.Fatalf("newRuntimeGraph() error = %v", err)
	}
	if next, ok := graph.Next("change_review_agent", report.StatusChangesRequested); !ok || next != "implementation" {
		t.Fatalf("changes_requested next = %q ok=%v, want implementation", next, ok)
	}
	if next, ok := graph.Next("validation", report.StatusFailed); !ok || next != "stop_report" {
		t.Fatalf("validation failed next = %q ok=%v, want stop_report", next, ok)
	}
}

func TestSelectedTemplateCreatesRuntimeStages(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "ship direct", WorkflowTemplateID: workflow.DirectCommitID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	stages, err := st.ListStages(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list stages: %v", err)
	}
	if len(stages) != len(workflow.DefaultTemplate().Stages)-1 {
		t.Fatalf("direct stage count = %d, want no PR creation stage", len(stages))
	}
	for _, stage := range stages {
		if stage.StageType == workflow.StageTypePRCreation {
			t.Fatalf("direct template persisted PR creation stage: %+v", stage)
		}
		if stage.StageType == workflow.StageTypeCommit && stage.WorkflowStageID != "commit_target_branch" {
			t.Fatalf("direct commit workflow stage id = %s, want commit_target_branch", stage.WorkflowStageID)
		}
	}
}

func TestAgentStageDispatchReceivesTemplateActorTargetSettings(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	runner := &capturingRunner{}
	policy := QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{QueuePolicy: &policy})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "review my changes", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	wr, err := st.GetWorkflowRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	runtime, err := engine.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		t.Fatalf("load runtime workflow: %v", err)
	}
	review := runtime.ByID["change_review_agent"]
	if _, err := engine.runWorkflowStage(ctx, wr, runtime, review, report.Report{}, report.Report{}, workerSnapshot{}, nil); err != nil {
		t.Fatalf("run review stage: %v", err)
	}
	if runner.disp.StageType != workflow.StageTypeReview {
		t.Fatalf("dispatch stage type = %s, want review", runner.disp.StageType)
	}
	if runner.disp.Input["workflow_stage_actor"] != workflow.ActorAgent || runner.disp.Input["workflow_stage_target"] != workflow.TargetCodeChanges {
		t.Fatalf("dispatch input missing actor/target: %+v", runner.disp.Input)
	}
	settings, ok := runner.disp.Input["workflow_stage_settings"].(map[string]any)
	if !ok || settings["profile"] != "generalist" || settings["intensity"] != "normal" {
		t.Fatalf("dispatch input settings = %#v", runner.disp.Input["workflow_stage_settings"])
	}
}

func TestStartRunFreezesSelectedWorkflowTemplateSnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	copied, err := st.CopyWorkflowTemplate(ctx, workflow.BalancedPRDeliveryID, "team_template", "Team Template")
	if err != nil {
		t.Fatalf("copy template: %v", err)
	}
	engine := NewEngineWithOptions(st, nil, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "use team template", WorkflowTemplateID: copied.ID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	var rawSnapshot string
	if err := st.DB().QueryRowContext(ctx, `SELECT snapshot_json FROM workflow_snapshots WHERE run_id = ? ORDER BY id ASC LIMIT 1`, runID).Scan(&rawSnapshot); err != nil {
		t.Fatalf("read start snapshot: %v", err)
	}
	var snapshot map[string]any
	if err := json.Unmarshal([]byte(rawSnapshot), &snapshot); err != nil {
		t.Fatalf("decode start snapshot: %v", err)
	}
	if snapshot["frozen"] != true {
		t.Fatalf("snapshot frozen = %v, want true", snapshot["frozen"])
	}
	if snapshot["workflow_template_id"] != copied.ID {
		t.Fatalf("snapshot template id = %v, want %s", snapshot["workflow_template_id"], copied.ID)
	}
	if snapshot["workflow_template_frozen"] != true {
		t.Fatalf("workflow_template_frozen = %v, want true", snapshot["workflow_template_frozen"])
	}
	templateSnapshot, ok := snapshot["workflow_template_snapshot"].(map[string]any)
	if !ok || templateSnapshot["id"] != copied.ID {
		t.Fatalf("workflow template snapshot = %+v", snapshot["workflow_template_snapshot"])
	}
}

type capturingRunner struct {
	disp contract.Dispatch
}

func (r *capturingRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.disp = disp
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        report.StatusCompleted,
		Summary:       "captured dispatch",
		Payload:       map[string]any{},
		Errors:        []string{},
	}, nil
}

func (r *capturingRunner) CancelAttempt(context.Context, string, string, string) error { return nil }
