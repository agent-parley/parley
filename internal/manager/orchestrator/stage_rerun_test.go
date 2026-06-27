package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	rworktree "github.com/agent-parley/parley/internal/runner/worktree"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

var stageReRunOperatorActor = event.Actor{Kind: event.ActorKindOperator, ID: "operator"}

func TestReRunStageCreatesNewAttemptFromTargetOverFrozenSnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	template := stageRerunTemplate("rerun_snapshot", true)
	if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create template: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "rerun from validation", WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.SaveWorkflowSnapshot(ctx, wr.Run.ID, map[string]any{"workflow_template_snapshot": template}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	updatedTemplate := stageRerunTemplate("rerun_snapshot", false)
	if err := st.UpdateWorkflowTemplate(ctx, updatedTemplate); err != nil {
		t.Fatalf("update template after snapshot: %v", err)
	}

	runner := &recordingRerunRunner{}
	engine := newRecordingEngine(t, st, runner, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	beforeAttempts, err := st.CountAttemptsForRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("count attempts before: %v", err)
	}

	attempt, err := engine.ReRunStage(ctx, wr.Run.ID, wr.ImplementationStage.ID, stageReRunOperatorActor)
	if err != nil {
		t.Fatalf("ReRunStage() error = %v", err)
	}
	waitForRunStatus(t, st, wr.Run.ID, store.RunStatusCompleted)

	afterAttempts, err := st.CountAttemptsForRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("count attempts after: %v", err)
	}
	if afterAttempts != beforeAttempts+1 {
		t.Fatalf("attempt count = %d, want %d", afterAttempts, beforeAttempts+1)
	}
	if attempt.RunID != wr.Run.ID || attempt.ID == wr.Attempt.ID {
		t.Fatalf("new attempt = %#v, previous = %s", attempt, wr.Attempt.ID)
	}

	dispatched := runner.stageTypes()
	wantDispatched := []string{contract.StageTypeImplementation, contract.StageTypeValidation, contract.StageTypeReview, contract.StageTypeReview}
	if !reflect.DeepEqual(dispatched, wantDispatched) {
		t.Fatalf("dispatched stage types = %#v, want %#v", dispatched, wantDispatched)
	}
	newStages, err := st.ListStagesForAttempt(ctx, wr.Run.ID, attempt.ID)
	if err != nil {
		t.Fatalf("list new attempt stages: %v", err)
	}
	if !hasWorkflowStage(newStages, "change_review_agent") {
		t.Fatalf("new attempt stages = %#v, want frozen snapshot review stage", newStages)
	}
	if stageForAttempt(t, st, wr.Run.ID, attempt.ID, "implementation").Status != report.StatusCompleted {
		t.Fatalf("implementation stage did not complete in new attempt")
	}
	if stageForAttempt(t, st, wr.Run.ID, attempt.ID, "validation").Status != report.StatusCompleted {
		t.Fatalf("validation stage did not complete in new attempt")
	}

	events, err := st.ListEvents(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	rerunEvent := latestEventOfType(events, "run.stage_rerun_started")
	if rerunEvent.Type == "" {
		t.Fatalf("missing run.stage_rerun_started in events %#v", eventTypes(events))
	}
	if rerunEvent.Actor != stageReRunOperatorActor {
		t.Fatalf("rerun actor = %#v, want operator", rerunEvent.Actor)
	}
	if rerunEvent.AttemptID != attempt.ID || rerunEvent.Data["requested_stage_id"] != wr.ImplementationStage.ID || rerunEvent.Data["target_workflow_stage_id"] != "implementation" || rerunEvent.Data["target_stage_type"] != contract.StageTypeImplementation {
		t.Fatalf("rerun event = %#v", rerunEvent)
	}
}

func TestReRunStageStartsAtValidationTargetWhenPrerequisitesExist(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	dataRoot := t.TempDir()
	projectID := "rerun_validation"
	sourceRepo := initCommitSourceRepo(t, ctx, map[string]string{"main.go": "package main\n\nfunc main() {}\n"})
	if _, err := st.EnsureProject(ctx, store.ProjectSpec{ID: projectID, Name: projectID, RepositoryPath: sourceRepo, QueueAutoWhenReady: true, QueueMaxConcurrent: 1, QueueBacklogCap: 10}); err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	wr, err := st.CreateWorkflowRunForProjectInput(ctx, projectID, contract.TaskInput{Idea: "rerun validation only", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	template, err := st.GetWorkflowTemplate(ctx, workflow.AutonomousPRDeliveryID)
	if err != nil {
		t.Fatalf("get template: %v", err)
	}
	if err := st.SaveWorkflowSnapshot(ctx, wr.Run.ID, map[string]any{"workflow_template_snapshot": template}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	runner := &rerunDeliveryRunner{store: st, dataRoot: dataRoot, sourceRepo: sourceRepo}
	engine := newRecordingEngine(t, st, runner, EngineOptions{DataRoot: dataRoot, ProjectID: projectID, QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	runtime, err := engine.loadRuntimeWorkflow(ctx, wr)
	if err != nil {
		t.Fatalf("load runtime: %v", err)
	}
	implStage := runtime.ByID["implementation"]
	if _, err := engine.runWorkflowStage(ctx, wr, runtime, implStage, report.Report{}, report.Report{}, workerSnapshot{}, nil); err != nil {
		t.Fatalf("run implementation prerequisite: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusFailed); err != nil {
		t.Fatalf("fail run: %v", err)
	}
	runner.reset()

	attempt, err := engine.ReRunStage(ctx, wr.Run.ID, "validation", stageReRunOperatorActor)
	if err != nil {
		t.Fatalf("ReRunStage validation error = %v", err)
	}
	waitForRunStatus(t, st, wr.Run.ID, store.RunStatusCompleted)
	got := runner.stageTypes()
	want := []string{contract.StageTypeValidation, contract.StageTypeReview, contract.StageTypeReview}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dispatched stage types = %#v, want %#v", got, want)
	}
	if stageForAttempt(t, st, wr.Run.ID, attempt.ID, "implementation").Status != store.StageStatusPending {
		t.Fatalf("implementation should remain pending when validation is the target")
	}
	if stageForAttempt(t, st, wr.Run.ID, attempt.ID, "validation").Status != report.StatusCompleted {
		t.Fatalf("validation did not run in new attempt")
	}
}

func TestReRunStageRejectsInvalidTargetsFailClosed(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "reject target", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	template, err := st.GetWorkflowTemplate(ctx, workflow.AutonomousPRDeliveryID)
	if err != nil {
		t.Fatalf("get template: %v", err)
	}
	if err := st.SaveWorkflowSnapshot(ctx, wr.Run.ID, map[string]any{"workflow_template_snapshot": template}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	engine := newRecordingEngine(t, st, &recordingRerunRunner{}, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})

	for _, target := range []string{"idea_refinement", "commit_feature_branch", "pr_creation", "memory_update", "stop_report", "missing_stage"} {
		t.Run(target, func(t *testing.T) {
			beforeAttempts, beforeStatus, beforeEvents := rerunMutationSnapshot(t, st, wr.Run.ID)
			_, err := engine.ReRunStage(ctx, wr.Run.ID, target, stageReRunOperatorActor)
			if !errors.Is(err, ErrStageReRunInvalidTarget) {
				t.Fatalf("ReRunStage(%s) error = %v, want ErrStageReRunInvalidTarget", target, err)
			}
			assertNoRerunMutation(t, st, wr.Run.ID, beforeAttempts, beforeStatus, beforeEvents)
		})
	}
}

func TestReRunStageRequiresFrozenSnapshotFailClosed(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "no frozen snapshot", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	engine := newRecordingEngine(t, st, &recordingRerunRunner{}, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
	beforeAttempts, beforeStatus, beforeEvents := rerunMutationSnapshot(t, st, wr.Run.ID)

	_, err = engine.ReRunStage(ctx, wr.Run.ID, "implementation", stageReRunOperatorActor)
	if !errors.Is(err, ErrStageReRunFrozenSnapshot) {
		t.Fatalf("ReRunStage missing snapshot error = %v, want ErrStageReRunFrozenSnapshot", err)
	}
	assertNoRerunMutation(t, st, wr.Run.ID, beforeAttempts, beforeStatus, beforeEvents)
}

func TestReRunStageRejectsTargetsWithoutCompletedPrerequisitesFailClosed(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "missing prior implementation", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	template, err := st.GetWorkflowTemplate(ctx, workflow.AutonomousPRDeliveryID)
	if err != nil {
		t.Fatalf("get template: %v", err)
	}
	if err := st.SaveWorkflowSnapshot(ctx, wr.Run.ID, map[string]any{"workflow_template_snapshot": template}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusFailed); err != nil {
		t.Fatalf("fail run: %v", err)
	}
	engine := newRecordingEngine(t, st, &recordingRerunRunner{}, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})

	for _, target := range []string{"validation", "change_review_agent"} {
		t.Run(target, func(t *testing.T) {
			beforeAttempts, beforeStatus, beforeEvents := rerunMutationSnapshot(t, st, wr.Run.ID)
			_, err := engine.ReRunStage(ctx, wr.Run.ID, target, stageReRunOperatorActor)
			if !errors.Is(err, ErrStageReRunPrerequisiteGap) {
				t.Fatalf("ReRunStage(%s) error = %v, want ErrStageReRunPrerequisiteGap", target, err)
			}
			assertNoRerunMutation(t, st, wr.Run.ID, beforeAttempts, beforeStatus, beforeEvents)
		})
	}
}

func TestReRunStageRejectsNonTerminalRunStatesFailClosed(t *testing.T) {
	ctx := context.Background()
	for _, status := range []string{store.RunStatusPending, store.RunStatusRunning, store.RunStatusAwaitingHuman, store.RunStatusNeedsInput} {
		t.Run(status, func(t *testing.T) {
			st, err := store.Open(ctx, t.TempDir())
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "reject state", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
			if err != nil {
				t.Fatalf("create run: %v", err)
			}
			if err := st.UpdateRunStatus(ctx, wr.Run.ID, status); err != nil {
				t.Fatalf("set run status: %v", err)
			}
			engine := newRecordingEngine(t, st, &recordingRerunRunner{}, EngineOptions{QueuePolicy: &QueuePolicy{AutoWhenReady: false, MaxConcurrent: 1, BacklogCap: 10}})
			beforeAttempts, beforeStatus, beforeEvents := rerunMutationSnapshot(t, st, wr.Run.ID)

			_, err = engine.ReRunStage(ctx, wr.Run.ID, "implementation", stageReRunOperatorActor)
			if !errors.Is(err, ErrStageReRunRunNotTerminal) {
				t.Fatalf("ReRunStage status %s error = %v, want ErrStageReRunRunNotTerminal", status, err)
			}
			assertNoRerunMutation(t, st, wr.Run.ID, beforeAttempts, beforeStatus, beforeEvents)
		})
	}
}

func TestReRunStageDeliveryUsesExistingBranchAndDoesNotCreatePRDuplicate(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	dataRoot := t.TempDir()
	projectID := "rerun_delivery"
	sourceRepo := initCommitSourceRepo(t, ctx, map[string]string{"main.go": "package main\n\nfunc main() {}\n"})
	if _, err := st.EnsureProject(ctx, store.ProjectSpec{ID: projectID, Name: projectID, RepositoryPath: sourceRepo, QueueAutoWhenReady: true, QueueMaxConcurrent: 1, QueueBacklogCap: 10}); err != nil {
		t.Fatalf("ensure project: %v", err)
	}
	runner := &rerunDeliveryRunner{store: st, dataRoot: dataRoot, sourceRepo: sourceRepo}
	engine := newRecordingEngine(t, st, runner, EngineOptions{DataRoot: dataRoot, ProjectID: projectID})
	runID, err := engine.StartProjectRunInput(ctx, projectID, contract.TaskInput{Idea: "update delivery branch", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: workflow.QuickFixDeliveryID})
	if err != nil {
		t.Fatalf("StartProjectRunInput() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusCompleted)
	firstWR, err := st.GetWorkflowRun(ctx, runID)
	if err != nil {
		t.Fatalf("get first workflow run: %v", err)
	}
	firstCommit := reportForAttemptWorkflowStage(t, engine, st, runID, firstWR.Attempt.ID, "commit_feature_branch")
	firstPR := reportForAttemptWorkflowStage(t, engine, st, runID, firstWR.Attempt.ID, "pr_creation")
	firstBranch := payloadString(firstCommit.Payload, "branch")
	if firstBranch == "" {
		t.Fatalf("first commit missing branch: %#v", firstCommit.Payload)
	}

	attempt, err := engine.ReRunStage(ctx, runID, "implementation", stageReRunOperatorActor)
	if err != nil {
		t.Fatalf("ReRunStage() error = %v", err)
	}
	waitForRunStatus(t, st, runID, store.RunStatusCompleted)
	secondCommit := reportForAttemptWorkflowStage(t, engine, st, runID, attempt.ID, "commit_feature_branch")
	secondPR := reportForAttemptWorkflowStage(t, engine, st, runID, attempt.ID, "pr_creation")
	secondBranch := payloadString(secondCommit.Payload, "branch")
	if secondBranch != firstBranch {
		t.Fatalf("second branch = %q, want existing branch %q", secondBranch, firstBranch)
	}
	secondCommitSHA := payloadString(secondCommit.Payload, "commit_sha")
	branchSHA := strings.TrimSpace(string(runCommitGitOutput(t, ctx, sourceRepo, "rev-parse", "refs/heads/"+firstBranch)))
	if branchSHA != secondCommitSHA {
		t.Fatalf("branch ref = %s, want second commit %s", branchSHA, secondCommitSHA)
	}
	refs := strings.Fields(string(runCommitGitOutput(t, ctx, sourceRepo, "for-each-ref", "--format=%(refname:short)", "refs/heads/"+firstBranch)))
	if len(refs) != 1 || refs[0] != firstBranch {
		t.Fatalf("delivery branch refs = %#v, want exactly %s", refs, firstBranch)
	}
	if firstPR.Payload["pr_created"] != false || secondPR.Payload["pr_created"] != false {
		t.Fatalf("pr stage should not create duplicate PRs: first=%#v second=%#v", firstPR.Payload, secondPR.Payload)
	}
}

func stageRerunTemplate(id string, includeReview bool) workflow.Template {
	stages := []workflow.StageTemplate{
		{ID: "idea_refinement", Type: workflow.StageTypeIdeaRefinement, Label: "Idea", Actor: workflow.ActorHarness},
		{ID: "implementation", Type: workflow.StageTypeImplementation, Label: "Implementation", Actor: workflow.ActorAgent},
		{ID: "validation", Type: workflow.StageTypeValidation, Label: "Validation", Actor: workflow.ActorHarness},
	}
	if includeReview {
		stages = append(stages, workflow.StageTemplate{ID: "change_review_agent", Type: workflow.StageTypeReview, Label: "Review", Actor: workflow.ActorAgent, Target: workflow.TargetCodeChanges})
	}
	stages = append(stages, workflow.StageTemplate{ID: "stop_report", Type: workflow.StageTypeStopReport, Label: "Stop", Actor: workflow.ActorHarness})
	edges := []workflow.Edge{
		{From: "idea_refinement", To: "implementation", On: workflow.OnCompleted},
		{From: "implementation", To: "validation", On: workflow.OnCompleted},
	}
	if includeReview {
		edges = append(edges,
			workflow.Edge{From: "validation", To: "change_review_agent", On: workflow.OnCompleted},
			workflow.Edge{From: "change_review_agent", To: "stop_report", On: workflow.OnCompleted},
		)
	} else {
		edges = append(edges, workflow.Edge{From: "validation", To: "stop_report", On: workflow.OnCompleted})
	}
	return workflow.Template{SchemaVersion: workflow.SchemaVersion, ID: id, Name: "Stage re-run test", Editable: true, Stages: stages, Edges: edges}
}

type recordingRerunRunner struct {
	mu    sync.Mutex
	disps []contract.Dispatch
}

func (r *recordingRerunRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	r.mu.Lock()
	r.disps = append(r.disps, disp)
	r.mu.Unlock()
	rep := validAdapterReport(disp, "rerun dispatch completed")
	if disp.StageType == contract.StageTypeReview {
		switch disp.Input["review_role"] {
		case contract.ReviewRoleCritic:
			rep.Payload = map[string]any{"raw_findings": []any{}}
		case contract.ReviewRoleArbiter:
			verdict := report.ReviewVerdictPass
			rep.Verdict = &verdict
			rep.Payload = map[string]any{
				"raw_findings":          disp.Input["raw_findings"],
				"arbitration_decisions": []any{},
				"residual_risk":         "low",
				"confidence":            "high",
			}
		}
	}
	return rep, nil
}

func (r *recordingRerunRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func (r *recordingRerunRunner) stageTypes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.disps))
	for i, disp := range r.disps {
		out[i] = disp.StageType
	}
	return out
}

type rerunDeliveryRunner struct {
	store      *store.Store
	mu         sync.Mutex
	disps      []contract.Dispatch
	dataRoot   string
	sourceRepo string
}

func (r *rerunDeliveryRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	r.mu.Lock()
	r.disps = append(r.disps, disp)
	r.mu.Unlock()
	rep := validAdapterReport(disp, "delivery dispatch completed")
	if disp.StageType == contract.StageTypeReview {
		switch disp.Input["review_role"] {
		case contract.ReviewRoleCritic:
			rep.Payload = map[string]any{"raw_findings": []any{}}
		case contract.ReviewRoleArbiter:
			verdict := report.ReviewVerdictPass
			rep.Verdict = &verdict
			rep.Payload = map[string]any{"raw_findings": disp.Input["raw_findings"], "arbitration_decisions": []any{}, "residual_risk": "low", "confidence": "high"}
		}
		return rep, nil
	}
	if disp.StageType != contract.StageTypeImplementation {
		return rep, nil
	}
	wt, err := rworktree.Create(ctx, rworktree.CreateOptions{DataRoot: r.dataRoot, ProjectID: disp.ProjectID, RunID: disp.RunID, TaskID: disp.TaskID, AttemptID: disp.AttemptID, SourceRepo: r.sourceRepo})
	if err != nil {
		rep.Status = report.StatusFailed
		rep.Summary = "create worktree failed"
		rep.Errors = []string{err.Error()}
		return rep, nil
	}
	content := "package main\n\nfunc main() { println(\"" + disp.AttemptID + "\") }\n"
	if err := os.WriteFile(filepath.Join(wt.Path, "main.go"), []byte(content), 0o600); err != nil {
		rep.Status = report.StatusFailed
		rep.Summary = "write worktree failed"
		rep.Errors = []string{err.Error()}
		return rep, nil
	}
	diffID := "diff_" + disp.AttemptID
	if r.store != nil {
		diff, err := rworktree.CaptureDiff(ctx, wt.Path, "")
		if err != nil {
			rep.Status = report.StatusFailed
			rep.Summary = "capture diff failed"
			rep.Errors = []string{err.Error()}
			return rep, nil
		}
		if _, err := r.store.SaveArtifactWithID(ctx, diffID, disp.RunID, "diff_patch", "text/x-diff", diff, ".patch"); err != nil {
			rep.Status = report.StatusFailed
			rep.Summary = "save diff failed"
			rep.Errors = []string{err.Error()}
			return rep, nil
		}
	}
	rep.Summary = "implemented delivery change"
	rep.Payload = map[string]any{"diff_artifact_id": diffID}
	return rep, nil
}

func (r *rerunDeliveryRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func (r *rerunDeliveryRunner) stageTypes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.disps))
	for i, disp := range r.disps {
		out[i] = disp.StageType
	}
	return out
}

func (r *rerunDeliveryRunner) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.disps = nil
}

func rerunMutationSnapshot(t *testing.T, st *store.Store, runID string) (int, string, int) {
	t.Helper()
	attempts, err := st.CountAttemptsForRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	run, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	events, err := st.ListEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	return attempts, run.Status, len(events)
}

func assertNoRerunMutation(t *testing.T, st *store.Store, runID string, wantAttempts int, wantStatus string, wantEvents int) {
	t.Helper()
	attempts, status, events := rerunMutationSnapshot(t, st, runID)
	if attempts != wantAttempts || status != wantStatus || events != wantEvents {
		t.Fatalf("mutation snapshot attempts/status/events = %d/%s/%d, want %d/%s/%d", attempts, status, events, wantAttempts, wantStatus, wantEvents)
	}
}

func stageForAttempt(t *testing.T, st *store.Store, runID, attemptID, workflowStageID string) store.Stage {
	t.Helper()
	stages, err := st.ListStagesForAttempt(context.Background(), runID, attemptID)
	if err != nil {
		t.Fatalf("list stages for attempt: %v", err)
	}
	for _, stage := range stages {
		if stage.WorkflowStageID == workflowStageID {
			return stage
		}
	}
	t.Fatalf("workflow stage %s not found in attempt %s", workflowStageID, attemptID)
	return store.Stage{}
}

func reportForAttemptWorkflowStage(t *testing.T, engine *Engine, st *store.Store, runID, attemptID, workflowStageID string) report.Report {
	t.Helper()
	stage := stageForAttempt(t, st, runID, attemptID, workflowStageID)
	rep, ok, err := engine.reportForStage(context.Background(), runID, stage.ID)
	if err != nil {
		t.Fatalf("report for stage %s: %v", stage.ID, err)
	}
	if !ok {
		t.Fatalf("missing report for stage %s", stage.ID)
	}
	return rep
}

func hasWorkflowStage(stages []store.Stage, workflowStageID string) bool {
	for _, stage := range stages {
		if stage.WorkflowStageID == workflowStageID {
			return true
		}
	}
	return false
}

func latestEventOfType(events []event.Event, typ string) event.Event {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == typ {
			return events[i]
		}
	}
	return event.Event{}
}
