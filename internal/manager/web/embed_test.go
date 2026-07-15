package web

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
)

func TestNewTaskRunViewsCorrelatesRunsByTaskAndPreservesTaskOrder(t *testing.T) {
	tasks := []store.Task{
		{ID: "task-without-run", Idea: "no run"},
		{ID: "task-review", Idea: "review task"},
		{ID: "task-running", Idea: "running task"},
	}
	runs := []store.Run{
		{ID: "orphan-run", TaskID: "missing-task", Status: store.RunStatusCompleted},
		{ID: "latest-review-run", TaskID: "task-review", Status: store.RunStatusAwaitingHuman},
		{ID: "older-review-run", TaskID: "task-review", Status: store.RunStatusCompleted},
		{ID: "running-run", TaskID: "task-running", Status: store.RunStatusRunning},
	}

	views := NewTaskRunViews(tasks, runs)

	if len(views) != len(tasks) {
		t.Fatalf("len(views) = %d, want %d", len(views), len(tasks))
	}
	assertTaskRunView(t, views[0], "task-without-run", "", false, false)
	assertTaskRunView(t, views[1], "task-review", "latest-review-run", true, true)
	assertTaskRunView(t, views[2], "task-running", "running-run", true, false)
}

func TestNewQueueViewCopiesQueueState(t *testing.T) {
	state := orchestrator.QueueState{
		Policy: orchestrator.QueuePolicy{
			AutoWhenReady: true,
			MaxConcurrent: 3,
			BacklogCap:    21,
		},
		Pending:                5,
		Running:                2,
		RunnerSlots:            4,
		ReadyRunnerSlots:       1,
		EffectiveMaxConcurrent: 3,
		RunsInflight:           2,
		TurnsInflight:          1,
		GlobalMaxConcurrent:    4,
		InteractiveReserve:     1,
	}

	view := NewQueueView(state)

	if view.Pending != 5 || view.Running != 2 {
		t.Fatalf("counts = pending %d running %d, want 5 and 2", view.Pending, view.Running)
	}
	if !view.AutoWhenReady {
		t.Fatal("AutoWhenReady = false, want true")
	}
	if view.MaxConcurrent != 3 || view.BacklogCap != 21 {
		t.Fatalf("policy = max %d backlog %d, want 3 and 21", view.MaxConcurrent, view.BacklogCap)
	}
	if view.RunnerSlots != 4 || view.ReadyRunnerSlots != 1 || view.EffectiveMaxConcurrent != 3 {
		t.Fatalf("runner limits = slots %d ready %d effective %d, want 4, 1, 3", view.RunnerSlots, view.ReadyRunnerSlots, view.EffectiveMaxConcurrent)
	}
	if view.RunsInflight != 2 || view.TurnsInflight != 1 || view.GlobalMaxConcurrent != 4 || view.InteractiveReserve != 1 {
		t.Fatalf("global capacity = runs %d turns %d cap %d reserve %d, want 2, 1, 4, 1", view.RunsInflight, view.TurnsInflight, view.GlobalMaxConcurrent, view.InteractiveReserve)
	}
}

func TestNewRunDataAndRunViewMapBundleFields(t *testing.T) {
	diffPath := writeArtifactFile(t, "diff.patch", "diff --git a/main.go b/main.go\n@@ -1 +1 @@\n-old\n+new\n context\n")
	planPath := writeArtifactFile(t, "plan.md", "ship the tested change")
	reportPath := writeArtifactFile(t, "report.json", `{"status":"completed","verdict":"pass"}`)
	bundle := store.RunBundle{
		Project: store.Project{ID: "project-1", Name: "Project"},
		Task: store.Task{
			ID:        "task-1",
			ProjectID: "project-1",
			Idea:      "task idea",
			Status:    "ready",
		},
		Run: store.Run{
			ID:        "run-1",
			ProjectID: "project-1",
			TaskID:    "task-1",
			Idea:      "run idea",
			Status:    store.RunStatusCompleted,
		},
		Attempt: store.Attempt{ID: "attempt-1", RunID: "run-1", TaskID: "task-1", Status: store.RunStatusCompleted},
		Stages: []store.Stage{
			{
				ID:                 "stage-implementation",
				RunID:              "run-1",
				TaskID:             "task-1",
				StageType:          contract.StageTypeImplementation,
				Adapter:            "pi-impl",
				Status:             "completed",
				TaskPlanArtifactID: "artifact-plan",
			},
			{
				ID:        "stage-validation",
				RunID:     "run-1",
				TaskID:    "task-1",
				StageType: contract.StageTypeValidation,
				Adapter:   "pi-validate",
				Status:    "completed",
			},
		},
		Events: []event.Event{
			{
				Type:    "stage.completed",
				Summary: "implementation done",
				Data: map[string]any{
					"stage_type": contract.StageTypeImplementation,
					"stage_id":   "stage-implementation",
					"status":     "completed",
				},
			},
			{
				Type:    "run.completed",
				Summary: "PR-ready snapshot created",
				Data: map[string]any{
					"stage_type":       contract.StageTypePRReady,
					"terminal_status":  store.RunStatusCompleted,
					"branch":           "agent/issue-122-web-view-model-tests",
					"commit_sha":       "abc123",
					"diff_artifact_id": "artifact-diff",
				},
			},
		},
		Artifacts: []store.Artifact{
			{ID: "artifact-diff", Kind: "diff_patch", MediaType: "text/x-diff", Path: diffPath},
			{ID: "artifact-plan", Kind: "task_plan", MediaType: "text/markdown", Path: planPath},
			{ID: "artifact-report", Kind: "report", MediaType: "application/json", Path: reportPath},
		},
	}

	data := NewRunData(bundle, "csrf-token", "Run title", "unknown-tab")
	view := data.View

	if data.CSRF != "csrf-token" || data.Title != "Run title" || data.Tab != "story" {
		t.Fatalf("RunData = csrf %q title %q tab %q, want csrf-token, Run title, story", data.CSRF, data.Title, data.Tab)
	}
	if view.CSRF != "csrf-token" {
		t.Fatalf("RunView.CSRF = %q, want csrf-token", view.CSRF)
	}
	if reviewTab := NewRunData(bundle, "csrf-token", "Run title", "review").Tab; reviewTab != "review" {
		t.Fatalf("review tab normalized to %q, want review", reviewTab)
	}
	if view.Run.ID != "run-1" || view.Task.ID != "task-1" || view.Attempt.ID != "attempt-1" {
		t.Fatalf("embedded bundle IDs = run %q task %q attempt %q, want run-1 task-1 attempt-1", view.Run.ID, view.Task.ID, view.Attempt.ID)
	}
	if len(view.ArtifactViews) != 3 {
		t.Fatalf("len(ArtifactViews) = %d, want 3", len(view.ArtifactViews))
	}
	if view.DiffPatch.ID != "artifact-diff" || view.DiffPatch.Preview == "" {
		t.Fatalf("DiffPatch = %#v, want artifact-diff with preview", view.DiffPatch)
	}
	if len(view.DiffLines) != 5 {
		t.Fatalf("len(DiffLines) = %d, want 5", len(view.DiffLines))
	}
	if view.DiffLines[0].Class != "diff-meta" || view.DiffLines[2].Class != "diff-del" || view.DiffLines[3].Class != "diff-add" {
		t.Fatalf("diff classes = %#v, want meta/delete/add mapping", view.DiffLines)
	}
	if !view.TaskPlan.Available || view.TaskPlan.StageID != "stage-implementation" || view.TaskPlan.Artifact.ID != "artifact-plan" {
		t.Fatalf("TaskPlan = %#v, want stage implementation artifact-plan", view.TaskPlan)
	}
	if len(view.StageGroups) != 2 {
		t.Fatalf("len(StageGroups) = %d, want 2", len(view.StageGroups))
	}
	impl := view.StageGroups[0]
	if impl.Stage.ID != "stage-implementation" || impl.Label != "Implementation" || impl.Performer != "Agent profile pi-impl" {
		t.Fatalf("implementation stage group = %#v", impl)
	}
	if impl.Summary != "implementation done" || len(impl.Events) != 1 || impl.Events[0].StageLabel != "Implementation" {
		t.Fatalf("implementation events = %#v", impl.Events)
	}
	if view.Outcome.Summary != "PR-ready snapshot created" || view.Outcome.TerminalEvent == nil || view.Outcome.LastEvent == nil {
		t.Fatalf("Outcome = %#v, want terminal PR-ready event summary", view.Outcome)
	}
	if !view.PRReady.Ready || view.PRReady.Branch != "agent/issue-122-web-view-model-tests" || view.PRReady.CommitSHA != "abc123" || view.PRReady.DiffArtifactID != "artifact-diff" {
		t.Fatalf("PRReady = %#v, want branch, commit, and diff artifact", view.PRReady)
	}
}

func TestNewRunViewMarksStageReRunControlsForTerminalComputeStagesOnly(t *testing.T) {
	stages := []store.Stage{
		{ID: "idea", AttemptID: "attempt-1", StageType: contract.StageTypeIdeaRefinement},
		{ID: "implementation", AttemptID: "attempt-1", StageType: contract.StageTypeImplementation},
		{ID: "validation", AttemptID: "attempt-1", StageType: contract.StageTypeValidation},
		{ID: "review", AttemptID: "attempt-1", StageType: contract.StageTypeReview},
		{ID: "commit", AttemptID: "attempt-1", StageType: contract.StageTypeCommit},
		{ID: "pr", AttemptID: "attempt-1", StageType: contract.StageTypePRCreation},
		{ID: "memory", AttemptID: "attempt-1", StageType: contract.StageTypeMemoryUpdate},
		{ID: "stop", AttemptID: "attempt-1", StageType: contract.StageTypeStopReport},
	}
	bundle := store.RunBundle{Run: store.Run{ID: "run-rerun", Status: store.RunStatusCompleted}, Stages: stages}

	view := NewRunView(bundle)

	got := map[string]bool{}
	for _, group := range view.StageGroups {
		got[group.Stage.ID] = group.CanReRun
	}
	want := map[string]bool{
		"idea":           false,
		"implementation": true,
		"validation":     true,
		"review":         true,
		"commit":         false,
		"pr":             false,
		"memory":         false,
		"stop":           false,
	}
	for stageID, wantCanReRun := range want {
		if got[stageID] != wantCanReRun {
			t.Fatalf("CanReRun[%s] = %v, want %v (all: %#v)", stageID, got[stageID], wantCanReRun, got)
		}
	}

	bundle.Run.Status = store.RunStatusRunning
	running := NewRunView(bundle)
	for _, group := range running.StageGroups {
		if group.CanReRun {
			t.Fatalf("running run stage %s CanReRun = true, want false", group.Stage.ID)
		}
	}

	bundle.Run.Status = store.RunStatusNeedsInput
	needsInput := NewRunView(bundle)
	for _, group := range needsInput.StageGroups {
		if group.CanReRun {
			t.Fatalf("needs_input run stage %s CanReRun = true, want false", group.Stage.ID)
		}
	}
}

func TestNewRunViewExposesPauseResumeControls(t *testing.T) {
	bundle := store.RunBundle{Project: store.Project{ID: "default"}, Run: store.Run{ID: "run-control", Status: store.RunStatusRunning}}
	view := NewRunView(bundle)
	if !view.CanPause || view.PauseRequested || view.CanResume || !view.CanCancel {
		t.Fatalf("running controls = pause:%v requested:%v resume:%v cancel:%v", view.CanPause, view.PauseRequested, view.CanResume, view.CanCancel)
	}
	bundle.Events = []event.Event{{Type: "run.pause_requested"}}
	view = NewRunView(bundle)
	if !view.PauseRequested {
		t.Fatal("PauseRequested = false after run.pause_requested event")
	}
	bundle.Run.Status = store.RunStatusPaused
	view = NewRunView(bundle)
	if view.CanPause || !view.CanResume || !view.CanCancel || view.WorkflowEditPath != "/projects/default/runs/run-control/workflow" {
		t.Fatalf("paused controls = pause:%v resume:%v cancel:%v path:%q", view.CanPause, view.CanResume, view.CanCancel, view.WorkflowEditPath)
	}
	bundle.Run.Status = store.RunStatusCompleted
	view = NewRunView(bundle)
	if view.CanPause || view.CanResume || view.CanCancel {
		t.Fatalf("completed controls = pause:%v resume:%v cancel:%v", view.CanPause, view.CanResume, view.CanCancel)
	}
}

func TestNewRunViewSurfacesPendingHumanReview(t *testing.T) {
	bundle := store.RunBundle{
		Run:     store.Run{ID: "run-review", TaskID: "task-review", Status: store.RunStatusAwaitingHuman},
		Task:    store.Task{ID: "task-review"},
		Attempt: store.Attempt{ID: "attempt-review", RunID: "run-review", TaskID: "task-review"},
		Stages: []store.Stage{
			{
				ID:              "stage-review",
				RunID:           "run-review",
				TaskID:          "task-review",
				WorkflowStageID: "workflow-review",
				StageType:       contract.StageTypeReview,
				Adapter:         "critic",
				Status:          store.StageStatusRunning,
			},
		},
		Events: []event.Event{
			{
				Type:    "stage.awaiting_human",
				Summary: "changes requested by arbiter",
				Data: map[string]any{
					"stage_type":             contract.StageTypeReview,
					"pending_stage_id":       "stage-review",
					"workflow_stage_id":      "workflow-review",
					"human_review_packet_id": "packet-artifact",
					"verdict":                "changes_requested",
				},
			},
		},
	}

	view := NewRunView(bundle)

	if view.PendingHumanReview == nil {
		t.Fatal("PendingHumanReview = nil, want review packet")
	}
	if got := *view.PendingHumanReview; got.StageID != "stage-review" || got.WorkflowStageID != "workflow-review" || got.PacketArtifactID != "packet-artifact" {
		t.Fatalf("PendingHumanReview = %#v, want stage/workflow/packet IDs", got)
	}
	if len(view.StageGroups) != 1 || !view.StageGroups[0].Expanded || view.StageGroups[0].Summary != "changes requested by arbiter" {
		t.Fatalf("StageGroups = %#v, want expanded review group with event summary", view.StageGroups)
	}
	if got := view.StageGroups[0].Events[0].Event.Data["verdict"]; got != "changes_requested" {
		t.Fatalf("review verdict event data = %v, want changes_requested", got)
	}
}

func TestNewProjectTasksDataAssemblesItemsAndSortsByOperatorPriority(t *testing.T) {
	project := store.Project{ID: "project-1", Name: "Project"}
	queue := QueueView{AutoWhenReady: false, Pending: 2, Running: 1}
	bundles := []store.RunBundle{
		taskBundle("completed-run", store.RunStatusCompleted, "2026-06-26T13:00:00Z", nil),
		taskBundle("running-run", store.RunStatusRunning, "2026-06-26T11:00:00Z", nil),
		taskBundle("needs-review-run", store.RunStatusAwaitingHuman, "2026-06-26T10:00:00Z", []event.Event{
			{
				Type:    "stage.awaiting_human",
				Summary: "ready for human review",
				Data: map[string]any{
					"stage_type":             contract.StageTypeReview,
					"pending_stage_id":       "needs-review-run-review",
					"workflow_stage_id":      "workflow-review",
					"human_review_packet_id": "needs-review-packet",
				},
			},
		}),
		taskBundle("pending-run", store.RunStatusPending, "2026-06-26T12:00:00Z", nil),
	}

	data := NewProjectTasksData(project, bundles, queue, "csrf-project")

	if data.Project.ID != "project-1" || data.CSRF != "csrf-project" {
		t.Fatalf("ProjectTasksData project/csrf = %q/%q, want project-1/csrf-project", data.Project.ID, data.CSRF)
	}
	if data.Queue.Pending != 2 || data.Queue.Running != 1 {
		t.Fatalf("Queue = %#v, want passthrough counts", data.Queue)
	}
	if len(data.Items) != len(bundles) {
		t.Fatalf("len(Items) = %d, want %d", len(data.Items), len(bundles))
	}
	wantOrder := []string{"needs-review-run", "pending-run", "running-run", "completed-run"}
	for i, want := range wantOrder {
		if got := data.Items[i].Run.ID; got != want {
			t.Fatalf("Items[%d].Run.ID = %q, want %q; items = %#v", i, got, want, data.Items)
		}
	}
	needsReview := data.Items[0]
	if !needsReview.NeedsYou || needsReview.NeedsReason != "diff ready" || needsReview.Link != "/projects/project-1/runs/needs-review-run?tab=review" {
		t.Fatalf("needs-review item = %#v, want diff-ready review link", needsReview)
	}
	pending := data.Items[1]
	if !pending.StartQueued || pending.Link != "/projects/project-1/runs/pending-run" || pending.DetailID != "task-pending-run-modal" {
		t.Fatalf("pending item = %#v, want manual start affordance and project run link", pending)
	}
	if pending.Idea != "task idea pending-run" || pending.UpdatedAt != "2026-06-26T12:00:00Z" {
		t.Fatalf("pending item idea/updated = %q/%q", pending.Idea, pending.UpdatedAt)
	}
}

func TestNewNotificationCenterDataBuildsItemsAndUnreadCount(t *testing.T) {
	notifications := []store.Notification{
		{
			ID:        "notification-run",
			ProjectID: "project-1",
			RunID:     "run-1",
			Class:     store.NotificationClassNeedsYou,
			Title:     "Review needed",
			CreatedAt: "2026-06-26T10:20",
		},
		{
			ID:             "notification-project",
			ProjectID:      "project-2",
			Class:          store.NotificationClassFinished,
			Title:          "Project finished",
			CreatedAt:      "2026-06-26T11:30",
			AcknowledgedAt: "2026-06-26T12:00:00Z",
		},
	}

	data := NewNotificationCenterData(notifications, 7, "csrf-notifications")

	if data.UnreadCount != 7 || data.CSRF != "csrf-notifications" {
		t.Fatalf("NotificationCenterData unread/csrf = %d/%q, want 7/csrf-notifications", data.UnreadCount, data.CSRF)
	}
	if len(data.Items) != 2 {
		t.Fatalf("len(Items) = %d, want 2", len(data.Items))
	}
	first := data.Items[0]
	if first.ID != "notification-run" || first.ProjectID != "project-1" || first.RunID != "run-1" || first.Link != "/projects/project-1/runs/run-1" {
		t.Fatalf("first notification item = %#v, want run link", first)
	}
	if first.Class != store.NotificationClassNeedsYou || first.Title != "Review needed" || first.CreatedAt != "2026-06-26T10:20" || first.RelativeTime != "10:20" || first.Acknowledged {
		t.Fatalf("first notification item details = %#v", first)
	}
	second := data.Items[1]
	if second.ID != "notification-project" || second.Link != "/projects/project-2" || !second.Acknowledged {
		t.Fatalf("second notification item = %#v, want acknowledged project link", second)
	}
}

func assertTaskRunView(t *testing.T, view TaskRunView, wantTaskID, wantRunID string, wantHasRun, wantReviewReady bool) {
	t.Helper()
	if view.Task.ID != wantTaskID {
		t.Fatalf("Task.ID = %q, want %q", view.Task.ID, wantTaskID)
	}
	if view.Run.ID != wantRunID {
		t.Fatalf("Run.ID = %q, want %q", view.Run.ID, wantRunID)
	}
	if view.HasRun != wantHasRun {
		t.Fatalf("HasRun = %v, want %v", view.HasRun, wantHasRun)
	}
	if view.ReviewReady != wantReviewReady {
		t.Fatalf("ReviewReady = %v, want %v", view.ReviewReady, wantReviewReady)
	}
}

func taskBundle(runID, status, updatedAt string, events []event.Event) store.RunBundle {
	taskID := "task-" + runID
	stages := []store.Stage{
		{
			ID:        runID + "-implementation",
			RunID:     runID,
			TaskID:    taskID,
			StageType: contract.StageTypeImplementation,
			Adapter:   "pi",
			Status:    "completed",
		},
	}
	if status == store.RunStatusAwaitingHuman {
		stages = append(stages, store.Stage{
			ID:        runID + "-review",
			RunID:     runID,
			TaskID:    taskID,
			StageType: contract.StageTypeReview,
			Adapter:   "critic",
			Status:    store.StageStatusRunning,
		})
	}
	return store.RunBundle{
		Task: store.Task{
			ID:        taskID,
			ProjectID: "project-1",
			Idea:      "task idea " + runID,
		},
		Run: store.Run{
			ID:        runID,
			ProjectID: "project-1",
			TaskID:    taskID,
			Idea:      "run idea " + runID,
			Status:    status,
			CreatedAt: "2026-06-26T09:00:00Z",
			UpdatedAt: updatedAt,
		},
		Stages: stages,
		Events: events,
	}
}

func writeArtifactFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write artifact %s: %v", name, err)
	}
	return path
}
