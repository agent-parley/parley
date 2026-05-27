package store_test

import (
	"testing"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/testsupport"
)

func TestPlannerApprovalDoesNotAutoQueueWhenPolicyAutoWhenReady(t *testing.T) {
	st := testsupport.OpenStore(t)
	project, err := st.CreateProject("Auto queue project", "", testsupport.TempGitRepo(t), "main")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	project.QueuePolicy = models.QueuePolicyAutoWhenReady
	if err := st.UpdateProjectSettings(project); err != nil {
		t.Fatalf("update project settings: %v", err)
	}
	project, ok := st.GetProject(project.ID)
	if !ok {
		t.Fatalf("project not found")
	}
	session, err := st.CreatePlannerSession(project.ID, "Plan a gated change")
	if err != nil {
		t.Fatalf("create planner session: %v", err)
	}
	approved, err := st.ApprovePlannerSession(project, session.ID)
	if err != nil {
		t.Fatalf("approve planner session: %v", err)
	}
	if approved.Task.Status != models.TaskStatusDraft {
		t.Fatalf("planner approval should create draft task only, got %+v", approved.Task)
	}
	if queued := st.QueuedAttempts(); len(queued) != 0 {
		t.Fatalf("planner approval under auto_when_ready must not queue attempts: %+v", queued)
	}
	if attempts := st.AttemptsForTask(approved.Task.ID); len(attempts) != 0 {
		t.Fatalf("planner approval should not create attempts: %+v", attempts)
	}
}
