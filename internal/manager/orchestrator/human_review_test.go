package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	rworktree "github.com/agent-parley/parley/internal/runner/worktree"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestHumanReviewSuspendsAndRestartResumeAcceptsHumanEnvelope(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	firstEngine := newRecordingEngine(t, st, &capturingRunner{}, EngineOptions{})
	runID, err := firstEngine.StartRunInput(ctx, contract.TaskInput{Idea: "needs plan review", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: workflow.BalancedPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	freezeRunWorkflowSnapshot(t, firstEngine, st, runID)
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
	queue, err := firstEngine.QueueState(ctx)
	if err != nil {
		t.Fatalf("queue state: %v", err)
	}
	if queue.Running != 0 {
		t.Fatalf("running slots = %d, want 0 after suspend", queue.Running)
	}
	if !hasArtifactKind(t, st, runID, "human_review_packet") {
		t.Fatal("missing human_review_packet artifact")
	}

	stage := stageByWorkflowID(t, st, runID, "plan_review_human")
	restartedEngine := newRecordingEngine(t, st, &capturingRunner{}, EngineOptions{})
	rep, err := restartedEngine.SubmitHumanReview(ctx, runID, stage.ID, HumanReviewSubmission{ActorID: "alice", Verdict: string(report.ReviewVerdictPass), Summary: "plan approved"})
	if err != nil {
		t.Fatalf("SubmitHumanReview() error = %v", err)
	}
	if rep.Actor.Kind != report.ActorKindHuman || rep.Actor.ID != "alice" || rep.Status != report.StatusCompleted || verdictString(rep.Verdict) != string(report.ReviewVerdictPass) {
		t.Fatalf("human report = %#v", rep)
	}
	waitForNotRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
	updatedStage := stageByWorkflowID(t, st, runID, "plan_review_human")
	if updatedStage.Status != report.StatusCompleted {
		t.Fatalf("human stage status = %s, want completed", updatedStage.Status)
	}
}

func TestAgentEscalationRoutesToWiredHumanReviewAndDoubleSubmitRejected(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	runner := &humanTestReviewRunner{verdict: report.ReviewVerdictEscalate}
	engine := newRecordingEngine(t, st, runner, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "escalate review", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: workflow.CarefulReviewID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	freezeRunWorkflowSnapshot(t, engine, st, runID)
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
	planStage := stageByWorkflowID(t, st, runID, "plan_review_human")
	if _, err := engine.SubmitHumanReview(ctx, runID, planStage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictPass), Summary: "plan ok"}); err != nil {
		t.Fatalf("submit plan review: %v", err)
	}
	waitForWorkflowStageAwaiting(t, st, runID, "change_review_human")
	changeStage := stageByWorkflowID(t, st, runID, "change_review_human")
	if _, err := engine.SubmitHumanReview(ctx, runID, changeStage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictBlocked), Summary: "blocked by operator"}); err != nil {
		t.Fatalf("submit blocked review: %v", err)
	}
	if _, err := engine.SubmitHumanReview(ctx, runID, changeStage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictPass), Summary: "late"}); err == nil {
		t.Fatal("double submit succeeded")
	}
	waitForRunStatus(t, st, runID, store.RunStatusNeedsInput)
}

func TestResumeAfterHumanCodeReviewWithLostWorktreeRematerializesAndCommits(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	fixture := startLostWorktreeResumeFixture(t, ctx)

	diff, err := rworktree.CaptureDiff(ctx, fixture.worktreePath, "")
	if err != nil {
		t.Fatalf("capture implementation diff: %v", err)
	}
	if _, err := fixture.store.SaveArtifactWithID(ctx, "implementation_diff", fixture.runID, "diff_patch", "text/x-diff", diff, ".patch"); err != nil {
		t.Fatalf("save implementation diff artifact: %v", err)
	}
	if err := os.RemoveAll(fixture.worktreePath); err != nil {
		t.Fatalf("remove worktree: %v", err)
	}

	restartedEngine := newRecordingEngine(t, fixture.store, fixture.runner, EngineOptions{DataRoot: fixture.dataRoot})
	changeStage := stageByWorkflowID(t, fixture.store, fixture.runID, "change_review_human")
	if _, err := restartedEngine.SubmitHumanReview(ctx, fixture.runID, changeStage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictPass), Summary: "code approved"}); err != nil {
		t.Fatalf("submit code review: %v", err)
	}
	waitForRunStatus(t, fixture.store, fixture.runID, store.RunStatusCompleted)

	commitStage := stageByWorkflowID(t, fixture.store, fixture.runID, "commit_feature_branch")
	commitReport, ok, err := restartedEngine.reportForStage(ctx, fixture.runID, commitStage.ID)
	if err != nil {
		t.Fatalf("read commit report: %v", err)
	}
	if !ok {
		t.Fatal("missing commit report")
	}
	if commitReport.Status != report.StatusCompleted {
		t.Fatalf("commit status = %s, want completed; report=%#v", commitReport.Status, commitReport)
	}
	commitSHA := payloadString(commitReport.Payload, "commit_sha")
	if commitSHA == "" {
		t.Fatalf("commit report missing commit_sha: %#v", commitReport.Payload)
	}
	if got := payloadString(commitReport.Payload, "diff_artifact_id"); got != "implementation_diff" {
		t.Fatalf("commit diff_artifact_id = %s, want implementation_diff", got)
	}
	parents := strings.TrimSpace(string(runCommitGitOutput(t, ctx, fixture.sourceRepo, "show", "-s", "--format=%P", commitSHA)))
	if parents != fixture.baseSHA {
		t.Fatalf("commit parent = %s, want %s", parents, fixture.baseSHA)
	}
	mainGo := string(runCommitGitOutput(t, ctx, fixture.sourceRepo, "show", commitSHA+":main.go"))
	if mainGo != "package main\n\nfunc main() { println(\"worker\") }\n" {
		t.Fatalf("committed main.go = %q", mainGo)
	}
	binaryAsset := runCommitGitOutput(t, ctx, fixture.sourceRepo, "show", commitSHA+":asset.bin")
	if !bytes.Equal(binaryAsset, []byte{0x00, 0x01, 0x02, 0xff}) {
		t.Fatalf("committed asset.bin = %v", binaryAsset)
	}
	toolEntry := strings.Fields(string(runCommitGitOutput(t, ctx, fixture.sourceRepo, "ls-tree", commitSHA, "tool.sh")))
	if len(toolEntry) < 1 || toolEntry[0] != "100755" {
		t.Fatalf("committed tool.sh entry = %#v, want executable mode", toolEntry)
	}
	branch := payloadString(commitReport.Payload, "branch")
	branchSHA := strings.TrimSpace(string(runCommitGitOutput(t, ctx, fixture.sourceRepo, "rev-parse", "refs/heads/"+branch)))
	if branchSHA != commitSHA {
		t.Fatalf("branch ref = %s, want commit %s", branchSHA, commitSHA)
	}
}

func TestResumeAfterHumanCodeReviewWithLostWorktreeCorruptDiffFailsCleanly(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	fixture := startLostWorktreeResumeFixture(t, ctx)

	if _, err := fixture.store.SaveArtifactWithID(ctx, "implementation_diff", fixture.runID, "diff_patch", "text/x-diff", []byte("not a faithful git patch\n"), ".patch"); err != nil {
		t.Fatalf("save corrupt implementation diff artifact: %v", err)
	}
	if err := os.RemoveAll(fixture.worktreePath); err != nil {
		t.Fatalf("remove worktree: %v", err)
	}
	restartedEngine := newRecordingEngine(t, fixture.store, fixture.runner, EngineOptions{DataRoot: fixture.dataRoot})
	changeStage := stageByWorkflowID(t, fixture.store, fixture.runID, "change_review_human")
	if _, err := restartedEngine.SubmitHumanReview(ctx, fixture.runID, changeStage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictPass), Summary: "code approved"}); err != nil {
		t.Fatalf("submit code review: %v", err)
	}
	waitForRunStatus(t, fixture.store, fixture.runID, store.RunStatusFailed)

	commitStage := stageByWorkflowID(t, fixture.store, fixture.runID, "commit_feature_branch")
	commitReport, ok, err := restartedEngine.reportForStage(ctx, fixture.runID, commitStage.ID)
	if err != nil {
		t.Fatalf("read commit report: %v", err)
	}
	if !ok {
		t.Fatal("missing commit report")
	}
	if commitReport.Summary != worktreeLostOnResumeSummary {
		t.Fatalf("commit summary = %q, want %q", commitReport.Summary, worktreeLostOnResumeSummary)
	}
	if got := payloadString(commitReport.Payload, "failure_reason"); got != "worktree_lost_on_restart" {
		t.Fatalf("failure_reason = %s, want worktree_lost_on_restart", got)
	}
	if got := payloadString(commitReport.Payload, "base_sha"); got != fixture.baseSHA {
		t.Fatalf("commit base_sha = %s, want %s", got, fixture.baseSHA)
	}
	if got := payloadString(commitReport.Payload, "diff_artifact_id"); got != "implementation_diff" {
		t.Fatalf("commit diff_artifact_id = %s, want implementation_diff", got)
	}
	if got := payloadString(commitReport.Payload, "commit_sha"); got != "" {
		t.Fatalf("commit_sha = %s, want empty", got)
	}
	if len(commitReport.Errors) == 0 || !strings.Contains(commitReport.Errors[0], worktreeLostOnResumeSummary) {
		t.Fatalf("commit errors = %#v, want worktree lost message", commitReport.Errors)
	}
	agentRefs := strings.TrimSpace(string(runCommitGitOutput(
		t,
		ctx,
		fixture.sourceRepo,
		"for-each-ref",
		"--format=%(refname)",
		"refs/heads/agent",
	)))
	if agentRefs != "" {
		t.Fatalf("unexpected agent branch refs after lost worktree commit:\n%s", agentRefs)
	}
}

type lostWorktreeResumeFixture struct {
	store        *store.Store
	runner       *lostWorktreeResumeRunner
	dataRoot     string
	sourceRepo   string
	runID        string
	worktreePath string
	baseSHA      string
}

func startLostWorktreeResumeFixture(t *testing.T, ctx context.Context) lostWorktreeResumeFixture {
	t.Helper()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	dataRoot := t.TempDir()
	source := initCommitSourceRepo(t, ctx, map[string]string{"main.go": "package main\n\nfunc main() {}\n"})
	baseSHA := strings.TrimSpace(string(runCommitGitOutput(t, ctx, source, "rev-parse", "HEAD")))
	spec := store.DefaultProjectSpec(st.DataDir())
	spec.RepositoryPath = source
	if _, err := st.EnsureProject(ctx, spec); err != nil {
		t.Fatalf("bind default project repository: %v", err)
	}
	runner := &lostWorktreeResumeRunner{dataRoot: dataRoot, sourceRepo: source, worktreePaths: make(chan string, 1)}
	firstEngine := newRecordingEngine(t, st, runner, EngineOptions{DataRoot: dataRoot})
	runID, err := firstEngine.StartRunInput(ctx, contract.TaskInput{
		Idea:               "exercise lost worktree resume",
		RefinementLevel:    contract.RefinementLevelDirect,
		WorkflowTemplateID: workflow.CarefulReviewID,
	})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	freezeRunWorkflowSnapshot(t, firstEngine, st, runID)
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
	planStage := stageByWorkflowID(t, st, runID, "plan_review_human")
	if _, err := firstEngine.SubmitHumanReview(ctx, runID, planStage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictPass), Summary: "plan approved"}); err != nil {
		t.Fatalf("submit plan review: %v", err)
	}
	waitForWorkflowStageAwaiting(t, st, runID, "change_review_human")
	var worktreePath string
	select {
	case worktreePath = <-runner.worktreePaths:
	case <-time.After(20 * time.Second):
		t.Fatal("implementation runner did not create a worktree")
	}

	awaitingEvent := eventByWorkflowStage(t, st, runID, "stage.awaiting_human", "change_review_human")
	snapshotPayload, ok := awaitingEvent.Data["implementation_snapshot"].(map[string]any)
	if !ok {
		t.Fatalf("stage.awaiting_human event missing implementation_snapshot: %#v", awaitingEvent.Data)
	}
	if got := payloadString(snapshotPayload, "base_sha"); got != baseSHA {
		t.Fatalf("event base_sha = %s, want %s", got, baseSHA)
	}
	if got := payloadString(snapshotPayload, "diff_artifact_id"); got != "implementation_diff" {
		t.Fatalf("event diff_artifact_id = %s, want implementation_diff", got)
	}
	packetArtifactID, _ := awaitingEvent.Data["human_review_packet_id"].(string)
	if packetArtifactID == "" {
		t.Fatalf("stage.awaiting_human event missing packet id: %#v", awaitingEvent.Data)
	}
	_, packetContent, err := st.GetArtifact(ctx, packetArtifactID)
	if err != nil {
		t.Fatalf("get human packet artifact: %v", err)
	}
	var packet map[string]any
	if err := json.Unmarshal(packetContent, &packet); err != nil {
		t.Fatalf("decode human packet: %v", err)
	}
	packetSnapshot, ok := packet["implementation_snapshot"].(map[string]any)
	if !ok {
		t.Fatalf("human packet missing implementation_snapshot: %#v", packet)
	}
	if got := payloadString(packetSnapshot, "base_sha"); got != baseSHA {
		t.Fatalf("packet base_sha = %s, want %s", got, baseSHA)
	}
	return lostWorktreeResumeFixture{store: st, runner: runner, dataRoot: dataRoot, sourceRepo: source, runID: runID, worktreePath: worktreePath, baseSHA: baseSHA}
}

func TestMalformedHumanReviewRejectedWhileAwaiting(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	engine := newRecordingEngine(t, st, &capturingRunner{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "bad human form", RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: workflow.BalancedPRDeliveryID})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	freezeRunWorkflowSnapshot(t, engine, st, runID)
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
	stage := stageByWorkflowID(t, st, runID, "plan_review_human")
	if _, err := engine.SubmitHumanReview(ctx, runID, stage.ID, HumanReviewSubmission{Verdict: "bogus", Summary: "bad"}); err == nil {
		t.Fatal("invalid verdict accepted")
	}
	run, err := st.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != store.RunStatusAwaitingHuman {
		t.Fatalf("run status = %s, want awaiting_human", run.Status)
	}
}

func TestSubmitHumanReviewRollsBackRunStatusOnPostCASFailure(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	engine := newRecordingEngine(t, st, &capturingRunner{}, EngineOptions{})
	runID, err := engine.StartRunInput(ctx, contract.TaskInput{
		Idea:               "rollback failed human resume",
		RefinementLevel:    contract.RefinementLevelDirect,
		WorkflowTemplateID: workflow.BalancedPRDeliveryID,
	})
	if err != nil {
		t.Fatalf("StartRunInput() error = %v", err)
	}
	freezeRunWorkflowSnapshot(t, engine, st, runID)
	waitForRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
	stage := stageByWorkflowID(t, st, runID, "plan_review_human")

	badWorkspace := filepath.Join(t.TempDir(), "bad-workspace")
	if _, err := st.EnsureProject(ctx, store.ProjectSpec{
		ID:                 store.DefaultProjectID,
		Name:               "Default project",
		WorkspacePath:      badWorkspace,
		QueueAutoWhenReady: true,
		QueueMaxConcurrent: 1,
		QueueBacklogCap:    100,
	}); err != nil {
		t.Fatalf("point project at bad workspace: %v", err)
	}
	artifactDir := filepath.Join(badWorkspace, "artifacts")
	if err := os.RemoveAll(artifactDir); err != nil {
		t.Fatalf("remove artifact dir: %v", err)
	}
	if err := os.WriteFile(artifactDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("create artifact dir blocker: %v", err)
	}

	if _, err := engine.SubmitHumanReview(ctx, runID, stage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictPass), Summary: "approved"}); err == nil {
		t.Fatal("SubmitHumanReview() succeeded despite artifact store failure")
	}
	run, err := st.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != store.RunStatusAwaitingHuman {
		t.Fatalf("run status = %s, want awaiting_human after rollback", run.Status)
	}
	stage = stageByWorkflowID(t, st, runID, "plan_review_human")
	if stage.Status != store.StageStatusRunning {
		t.Fatalf("human stage status = %s, want running after rollback", stage.Status)
	}

	restoredWorkspace := filepath.Join(t.TempDir(), "restored-workspace")
	if _, err := st.EnsureProject(ctx, store.ProjectSpec{
		ID:                 store.DefaultProjectID,
		Name:               "Default project",
		WorkspacePath:      restoredWorkspace,
		QueueAutoWhenReady: true,
		QueueMaxConcurrent: 1,
		QueueBacklogCap:    100,
	}); err != nil {
		t.Fatalf("restore project workspace: %v", err)
	}
	if _, err := engine.SubmitHumanReview(ctx, runID, stage.ID, HumanReviewSubmission{Verdict: string(report.ReviewVerdictPass), Summary: "approved after retry"}); err != nil {
		t.Fatalf("retry SubmitHumanReview() error = %v", err)
	}
	waitForNotRunStatus(t, st, runID, store.RunStatusAwaitingHuman)
}

func stageByWorkflowID(t *testing.T, st *store.Store, runID, workflowStageID string) store.Stage {
	t.Helper()
	stages, err := st.ListStages(ctxBackground(), runID)
	if err != nil {
		t.Fatalf("list stages: %v", err)
	}
	for _, stage := range stages {
		if stage.WorkflowStageID == workflowStageID {
			return stage
		}
	}
	t.Fatalf("workflow stage %s not found", workflowStageID)
	return store.Stage{}
}

func hasArtifactKind(t *testing.T, st *store.Store, runID, kind string) bool {
	t.Helper()
	artifacts, err := st.ListArtifacts(ctxBackground(), runID)
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	for _, artifact := range artifacts {
		if artifact.Kind == kind {
			return true
		}
	}
	return false
}

func eventByWorkflowStage(t *testing.T, st *store.Store, runID, typ, workflowStageID string) event.Event {
	t.Helper()
	events, err := st.ListEvents(ctxBackground(), runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if ev.Type != typ {
			continue
		}
		if got, _ := ev.Data["workflow_stage_id"].(string); got == workflowStageID {
			return ev
		}
	}
	t.Fatalf("event %s for workflow stage %s not found", typ, workflowStageID)
	return event.Event{}
}

type humanTestReviewRunner struct {
	capturingRunner
	verdict report.Verdict
}

func (r *humanTestReviewRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	rep, err := r.capturingRunner.Dispatch(ctx, disp)
	if err != nil || disp.StageType != contract.StageTypeReview || disp.Input["review_role"] != contract.ReviewRoleArbiter {
		return rep, err
	}
	rep.Verdict = &r.verdict
	if r.verdict == report.ReviewVerdictChangesRequested {
		rep.Payload["arbitration_decisions"] = []any{map[string]any{"finding_id": "finding-1", "classification": report.ReviewFindingAccepted, "rationale": "real issue"}}
	}
	return rep, nil
}

type lostWorktreeResumeRunner struct {
	dataRoot      string
	sourceRepo    string
	worktreePaths chan string
}

func (r *lostWorktreeResumeRunner) Dispatch(ctx context.Context, disp contract.Dispatch) (report.Report, error) {
	rep := validAdapterReport(disp, "completed")
	switch disp.StageType {
	case contract.StageTypeImplementation:
		wt, err := rworktree.Create(ctx, rworktree.CreateOptions{
			DataRoot:   r.dataRoot,
			ProjectID:  disp.ProjectID,
			RunID:      disp.RunID,
			TaskID:     disp.TaskID,
			AttemptID:  disp.AttemptID,
			SourceRepo: r.sourceRepo,
		})
		if err != nil {
			rep.Status = report.StatusFailed
			rep.Summary = "create worktree failed"
			rep.Errors = []string{err.Error()}
			return rep, nil
		}
		select {
		case r.worktreePaths <- wt.Path:
		default:
		}
		if err := os.WriteFile(filepath.Join(wt.Path, "main.go"), []byte("package main\n\nfunc main() { println(\"worker\") }\n"), 0o600); err != nil {
			rep.Status = report.StatusFailed
			rep.Summary = "write worktree failed"
			rep.Errors = []string{err.Error()}
			return rep, nil
		}
		if err := os.WriteFile(filepath.Join(wt.Path, "asset.bin"), []byte{0x00, 0x01, 0x02, 0xff}, 0o600); err != nil {
			rep.Status = report.StatusFailed
			rep.Summary = "write binary worktree artifact failed"
			rep.Errors = []string{err.Error()}
			return rep, nil
		}
		if err := os.WriteFile(filepath.Join(wt.Path, "tool.sh"), []byte("#!/bin/sh\necho worker\n"), 0o755); err != nil {
			rep.Status = report.StatusFailed
			rep.Summary = "write executable worktree artifact failed"
			rep.Errors = []string{err.Error()}
			return rep, nil
		}
		rep.Summary = "implemented change"
		rep.Payload = map[string]any{"diff_artifact_id": "implementation_diff"}
	case contract.StageTypeReview:
		switch disp.Input["review_role"] {
		case contract.ReviewRoleCritic:
			rep.Summary = "review critic found no issues"
			rep.Payload = map[string]any{"raw_findings": []any{}}
		case contract.ReviewRoleArbiter:
			verdict := report.ReviewVerdictPass
			rep.Summary = "review passed"
			rep.Verdict = &verdict
			rep.Payload = map[string]any{
				"raw_findings":          disp.Input["raw_findings"],
				"arbitration_decisions": []any{},
				"residual_risk":         "low",
				"confidence":            "high",
			}
		}
	case contract.StageTypeValidation:
		rep.Summary = "validation passed"
	}
	return rep, nil
}

func (r *lostWorktreeResumeRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func ctxBackground() context.Context { return context.Background() }
