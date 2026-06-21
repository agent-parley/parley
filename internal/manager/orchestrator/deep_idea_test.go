package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestDeepIdeaIntakeSuspendsAndResumesPlannerWithAnswers(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	templateID := createIdeaOnlyTemplate(t, st)
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "design login audit logging", RefinementLevel: contract.RefinementLevelDeep, WorkflowTemplateID: templateID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusRunning); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	plan := deepPlannerTestPlan(wr, "Use the existing security audit sink named by the operator.")
	runner := &scriptedDeepPlannerRunner{reports: []deepPlannerScript{
		{status: report.StatusNeedsInput, summary: "need login audit constraints", questions: []string{"Which audit sink should receive login failures?", "Should failed MFA attempts be included?"}},
		{status: report.StatusCompleted, summary: "deep plan produced", plan: plan},
	}}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{PlanningAdapter: "planner", DataRoot: t.TempDir(), ProjectID: "p1"})

	if err := engine.executeRunErr(ctx, wr.Run.ID); !errors.Is(err, errRunAwaitingHuman) {
		t.Fatalf("executeRunErr() error = %v, want awaiting human", err)
	}
	waitForRunStatus(t, st, wr.Run.ID, store.RunStatusAwaitingHuman)
	queue, err := engine.QueueState(ctx)
	if err != nil {
		t.Fatalf("queue state: %v", err)
	}
	if queue.Running != 0 {
		t.Fatalf("running slots = %d, want 0 after deep suspend", queue.Running)
	}
	if got := artifactKindCount(t, st, wr.Run.ID, deepIdeaQuestionsArtifactKind); got != 1 {
		t.Fatalf("question artifacts = %d, want 1", got)
	}
	stage := stageByWorkflowID(t, st, wr.Run.ID, "idea_refinement")
	if stage.Status != store.StageStatusRunning {
		t.Fatalf("idea stage status = %s, want running while awaiting answers", stage.Status)
	}

	restarted := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{PlanningAdapter: "planner", DataRoot: t.TempDir(), ProjectID: "p1"})
	receipt, err := restarted.SubmitDeepIdeaAnswers(ctx, wr.Run.ID, stage.ID, DeepIdeaAnswersSubmission{
		ActorID:    "alice",
		AnswerText: "Use the security audit sink; include MFA failures.",
	})
	if err != nil {
		t.Fatalf("SubmitDeepIdeaAnswers() error = %v", err)
	}
	if receipt.Round != 1 || receipt.ArtifactID == "" {
		t.Fatalf("receipt = %#v, want first answer artifact", receipt)
	}
	waitForRunStatus(t, st, wr.Run.ID, store.RunStatusCompleted)
	if got := artifactKindCount(t, st, wr.Run.ID, deepIdeaAnswersArtifactKind); got != 1 {
		t.Fatalf("answer artifacts = %d, want 1", got)
	}
	if _, err := restarted.SubmitDeepIdeaAnswers(ctx, wr.Run.ID, stage.ID, DeepIdeaAnswersSubmission{AnswerText: "late"}); !errors.Is(err, ErrDeepIdeaNotAwaiting) {
		t.Fatalf("late SubmitDeepIdeaAnswers() error = %v, want not awaiting", err)
	}
	disps := runner.dispatches()
	if len(disps) != 2 {
		t.Fatalf("planner dispatch count = %d, want 2", len(disps))
	}
	second := disps[1]
	if second.Input["refinement_level"] != contract.RefinementLevelDeep || second.Input["input_mode"] != contract.AdapterInputModePlanning {
		t.Fatalf("second dispatch missing deep planning input: %#v", second.Input)
	}
	history, ok := second.Input["answers_so_far"].([]map[string]any)
	if !ok || len(history) != 1 || !strings.Contains(history[0]["answer_text"].(string), "security audit sink") {
		t.Fatalf("second dispatch missing answers_so_far: %#v", second.Input["answers_so_far"])
	}
	planStage := stageByWorkflowID(t, st, wr.Run.ID, "idea_refinement")
	_, planContent, err := st.GetArtifact(ctx, planStage.TaskPlanArtifactID)
	if err != nil {
		t.Fatalf("read task plan: %v", err)
	}
	if string(planContent) != plan {
		t.Fatalf("task plan content =\n%s\nwant=\n%s", planContent, plan)
	}
}

func TestDeepIdeaIntakeQuestionBudgetFallsForwardToPlan(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	templateID := createIdeaOnlyTemplate(t, st)
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "clarify deployment hardening", RefinementLevel: contract.RefinementLevelDeep, WorkflowTemplateID: templateID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusRunning); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	runner := &scriptedDeepPlannerRunner{reports: []deepPlannerScript{
		{status: report.StatusNeedsInput, summary: "round 1", questions: []string{"What platform?"}},
		{status: report.StatusNeedsInput, summary: "round 2", questions: []string{"What threat model?"}},
		{status: report.StatusNeedsInput, summary: "round 3", questions: []string{"What rollout window?"}},
		{status: report.StatusNeedsInput, summary: "still wants more", questions: []string{"One more question that must become non-blocking."}},
	}}
	engine := NewEngineWithOptions(st, runner, fakeFragmentRenderer{}, fakeBroadcaster{}, EngineOptions{PlanningAdapter: "planner", DataRoot: t.TempDir(), ProjectID: "p1"})
	if err := engine.executeRunErr(ctx, wr.Run.ID); !errors.Is(err, errRunAwaitingHuman) {
		t.Fatalf("executeRunErr() error = %v, want awaiting human", err)
	}

	for round := 1; round <= defaultDeepIdeaMaxQuestionRounds; round++ {
		waitForRunStatus(t, st, wr.Run.ID, store.RunStatusAwaitingHuman)
		stage := stageByWorkflowID(t, st, wr.Run.ID, "idea_refinement")
		if _, err := engine.SubmitDeepIdeaAnswers(ctx, wr.Run.ID, stage.ID, DeepIdeaAnswersSubmission{AnswerText: "answer round"}); err != nil {
			t.Fatalf("submit round %d: %v", round, err)
		}
	}
	waitForRunStatus(t, st, wr.Run.ID, store.RunStatusCompleted)
	if got := artifactKindCount(t, st, wr.Run.ID, deepIdeaQuestionsArtifactKind); got != defaultDeepIdeaMaxQuestionRounds {
		t.Fatalf("question artifacts = %d, want capped %d", got, defaultDeepIdeaMaxQuestionRounds)
	}
	stage := stageByWorkflowID(t, st, wr.Run.ID, "idea_refinement")
	_, planContent, err := st.GetArtifact(ctx, stage.TaskPlanArtifactID)
	if err != nil {
		t.Fatalf("read fallback task plan: %v", err)
	}
	planText := string(planContent)
	for _, want := range []string{"# Task Plan", "Refinement level: `deep`", "## Assumptions", "One more question that must become non-blocking."} {
		if !strings.Contains(planText, want) {
			t.Fatalf("fallback plan missing %q:\n%s", want, planText)
		}
	}
	lastDisp := runner.dispatches()[defaultDeepIdeaMaxQuestionRounds]
	if lastDisp.Input["force_final_plan"] != true || lastDisp.Input["questions_remaining"] != 0 {
		t.Fatalf("final dispatch did not force final plan: %#v", lastDisp.Input)
	}
}

func createIdeaOnlyTemplate(t *testing.T, st *store.Store) string {
	t.Helper()
	template := workflow.Template{
		SchemaVersion: workflow.SchemaVersion,
		ID:            "idea_only_" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()),
		Name:          "Idea Only",
		Editable:      true,
		Stages: []workflow.StageTemplate{
			{ID: "idea_refinement", Type: workflow.StageTypeIdeaRefinement, Label: "Idea refinement", Actor: workflow.ActorHarness},
			{ID: "stop_report", Type: workflow.StageTypeStopReport, Label: "Stop/report", Actor: workflow.ActorHarness},
		},
		Edges: []workflow.Edge{
			{From: "idea_refinement", To: "stop_report", On: workflow.OnCompleted},
			{From: "idea_refinement", To: "stop_report", On: workflow.OnFailed},
			{From: "idea_refinement", To: "stop_report", On: workflow.OnInvalid},
			{From: "idea_refinement", To: "stop_report", On: workflow.OnNeedsInput},
		},
	}
	if err := st.CreateWorkflowTemplate(context.Background(), template); err != nil {
		t.Fatalf("create idea-only template: %v", err)
	}
	return template.ID
}

type deepPlannerScript struct {
	status    string
	summary   string
	questions []string
	plan      string
}

type scriptedDeepPlannerRunner struct {
	mu      sync.Mutex
	reports []deepPlannerScript
	disps   []contract.Dispatch
}

func (r *scriptedDeepPlannerRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.mu.Lock()
	idx := len(r.disps)
	r.disps = append(r.disps, disp)
	script := deepPlannerScript{status: report.StatusCompleted, summary: "deep plan produced", plan: fallbackDeepPlannerPlan(disp)}
	if idx < len(r.reports) {
		script = r.reports[idx]
	}
	r.mu.Unlock()
	payload := map[string]any{}
	if script.status == report.StatusNeedsInput {
		payload["questions"] = append([]string{}, script.questions...)
	} else {
		payload["task_plan_markdown"] = script.plan
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: disp.Adapter},
		Status:        script.status,
		Summary:       script.summary,
		Payload:       payload,
		Errors:        []string{},
	}, nil
}

func (r *scriptedDeepPlannerRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func (r *scriptedDeepPlannerRunner) dispatches() []contract.Dispatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]contract.Dispatch, len(r.disps))
	copy(out, r.disps)
	return out
}

func deepPlannerTestPlan(wr store.WorkflowRun, assumption string) string {
	return "# Task Plan\n\n" +
		"Project ID: `" + wr.Run.ProjectID + "`\n" +
		"Run ID: `" + wr.Run.ID + "`\n" +
		"Task ID: `" + wr.Task.ID + "`\n" +
		"Attempt ID: `" + wr.Attempt.ID + "`\n" +
		"Refinement level: `deep`\n\n" +
		"## User Idea\n\n" + wr.Run.Idea + "\n\n" +
		"## Plan Boundary\n\n" +
		"This artifact is a task plan, not a workflow definition. It does not choose, add, remove, or reorder workflow stages.\n\n" +
		"## Objective\n\nDesign audit logging for failed login paths using the clarified constraints.\n\n" +
		"## Repo Evidence Considered\n\n- The implementation stage should inspect authentication and logging packages.\n\n" +
		"## Implementation Approach\n\n- Add the narrow audit event and focused tests for failure paths.\n\n" +
		"## Assumptions\n\n- " + assumption + "\n\n" +
		"## Open Questions\n\n- None blocking after Deep refinement.\n\n" +
		"## Validation\n\n- Run focused authentication tests.\n"
}

func fallbackDeepPlannerPlan(disp contract.Dispatch) string {
	return "# Task Plan\n\n" +
		"Project ID: `" + disp.ProjectID + "`\n" +
		"Run ID: `" + disp.RunID + "`\n" +
		"Task ID: `" + disp.TaskID + "`\n" +
		"Attempt ID: `" + disp.AttemptID + "`\n" +
		"Refinement level: `deep`\n\n" +
		"## User Idea\n\n" + disp.Input["idea"].(string) + "\n\n" +
		"## Plan Boundary\n\n" +
		"This artifact is a task plan, not a workflow definition. It does not choose, add, remove, or reorder workflow stages.\n\n" +
		"## Objective\n\nImplement the clarified idea.\n\n" +
		"## Repo Evidence Considered\n\n- Inspect the relevant code before editing.\n\n" +
		"## Implementation Approach\n\n- Make the smallest coherent change.\n\n" +
		"## Assumptions\n\n- Answers collected so far are authoritative.\n\n" +
		"## Open Questions\n\n- None blocking.\n\n" +
		"## Validation\n\n- Run focused tests.\n"
}

func artifactKindCount(t *testing.T, st *store.Store, runID, kind string) int {
	t.Helper()
	artifacts, err := st.ListArtifacts(context.Background(), runID)
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	count := 0
	for _, artifact := range artifacts {
		if artifact.Kind == kind {
			count++
		}
	}
	return count
}
