package app

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/models"
	plannerexec "github.com/agent-parley/parley/internal/planner"
	"github.com/agent-parley/parley/internal/profiles"
	"github.com/agent-parley/parley/internal/store"
	"github.com/agent-parley/parley/internal/testsupport"
)

type fakePlannerRunner struct {
	result       plannerexec.Result
	err          error
	input        plannerexec.Input
	beforeReturn func()
}

func (r *fakePlannerRunner) Run(ctx context.Context, input plannerexec.Input) (plannerexec.Result, error) {
	r.input = input
	if r.beforeReturn != nil {
		r.beforeReturn()
	}
	return r.result, r.err
}

type blockingPlannerRunner struct {
	result  plannerexec.Result
	input   plannerexec.Input
	started chan struct{}
	release chan struct{}
}

func (r *blockingPlannerRunner) Run(ctx context.Context, input plannerexec.Input) (plannerexec.Result, error) {
	r.input = input
	close(r.started)
	select {
	case <-r.release:
		return r.result, nil
	case <-ctx.Done():
		return plannerexec.Result{}, ctx.Err()
	}
}

func TestPlannerAgentRunReturnsWhileGenerationRunningWithoutCreatingTask(t *testing.T) {
	st := testsupport.OpenStore(t)
	project := createPlanningProject(t, st)
	session, err := st.CreatePlannerSession(project.ID, "Add async planning")
	if err != nil {
		t.Fatal(err)
	}
	runner := &blockingPlannerRunner{
		started: make(chan struct{}),
		release: make(chan struct{}),
		result: plannerexec.Result{Mode: plannerexec.ModeDryRun, PlannerProfile: profiles.ProfilePlanner, CriticProfile: profiles.ProfileCritic, Draft: plannerexec.Draft{Title: "Async generated task", Objective: "Generated objective", Focus: "focus", Boundaries: "boundaries", DoneWhen: "done", Assumptions: []string{"assumption"}, Risks: []string{"risk"}, GraphPreview: []string{"Prompt", "Approval"}}, Summary: "Planner/critic completed."},
	}
	released := false
	releaseRunner := func() {
		if !released {
			close(runner.release)
			released = true
		}
	}
	s := &Server{cfg: config.Config{ExecutionMode: config.ExecutionModeDryRun}, store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), plannerRunner: runner, csrfToken: "test"}
	req := httptest.NewRequest(http.MethodPost, "/projects/"+project.ID+"/planner/"+session.ID+"/run-agents", strings.NewReader("csrf_token=test"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		s.projectRoutes(rec, req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		releaseRunner()
		t.Fatalf("planner generation request did not return before runner completed")
	}
	if rec.Code != http.StatusSeeOther {
		releaseRunner()
		t.Fatalf("unexpected status=%d body=%q", rec.Code, rec.Body.String())
	}
	select {
	case <-runner.started:
	case <-time.After(2 * time.Second):
		releaseRunner()
		t.Fatalf("planner runner did not start")
	}
	running, ok := st.GetPlannerSession(session.ID)
	if !ok {
		releaseRunner()
		t.Fatalf("missing session")
	}
	if running.AgentStatus != models.PlannerAgentStatusRunning || running.ActiveGenerationID == "" {
		releaseRunner()
		t.Fatalf("expected running generation state, got %+v", running)
	}
	if len(st.RunsForProject(project.ID)) != 0 {
		releaseRunner()
		t.Fatalf("planner/critic generation must not create a task while running")
	}
	releaseRunner()
	updated := waitForPlannerSession(t, st, session.ID, func(session models.PlannerSession) bool {
		return session.AgentStatus == models.PlannerAgentStatusCompleted
	})
	if updated.DraftTitle != "Async generated task" {
		t.Fatalf("session not updated after release: %+v", updated)
	}
}

func TestPlannerAgentRunUpdatesDraftWithoutCreatingTask(t *testing.T) {
	st := testsupport.OpenStore(t)
	project := createPlanningProject(t, st)
	session, err := st.CreatePlannerSession(project.ID, "Add agent planning")
	if err != nil {
		t.Fatal(err)
	}
	runner := &fakePlannerRunner{result: plannerexec.Result{
		Mode:           plannerexec.ModeDryRun,
		PlannerProfile: profiles.ProfilePlanner,
		CriticProfile:  profiles.ProfileCritic,
		Draft: plannerexec.Draft{
			Title:        "Generated planner task",
			Objective:    "Run planner and critic before approval",
			Focus:        "planner session flow",
			Boundaries:   "keep task execution gated",
			DoneWhen:     "draft can be approved into a task",
			Assumptions:  []string{"dry-run is safe"},
			Risks:        []string{"agent output needs review"},
			GraphPreview: []string{"Prompt", "Planner agent", "Critic agent", "Approval"},
		},
		PlannerMessage: "Planner made a draft.",
		CriticMessage:  "Critic reviewed the draft.",
		Summary:        "Planner/critic completed.",
	}}
	s := &Server{cfg: config.Config{ExecutionMode: config.ExecutionModeDryRun}, store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), plannerRunner: runner, csrfToken: "test"}
	req := httptest.NewRequest(http.MethodPost, "/projects/"+project.ID+"/planner/"+session.ID+"/run-agents", strings.NewReader("csrf_token=test"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.projectRoutes(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("unexpected status=%d body=%q", rec.Code, rec.Body.String())
	}
	updated := waitForPlannerSession(t, st, session.ID, func(session models.PlannerSession) bool {
		return session.AgentStatus == models.PlannerAgentStatusCompleted
	})
	if updated.DraftTitle != "Generated planner task" || updated.AgentStatus != models.PlannerAgentStatusCompleted {
		t.Fatalf("session not updated from agent result: %+v", updated)
	}
	if updated.AgentMode != plannerexec.ModeDryRun || updated.PlannerProfile != profiles.ProfilePlanner || updated.CriticProfile != profiles.ProfileCritic {
		t.Fatalf("agent metadata not recorded: %+v", updated)
	}
	if len(st.RunsForProject(project.ID)) != 0 {
		t.Fatalf("planner/critic run must not create a task before approval")
	}
	if !waitForPlannerMessage(t, st, session.ID, "critic", "Critic reviewed the draft.") {
		t.Fatalf("critic message not recorded: %+v", st.PlannerMessages(session.ID))
	}
	if runner.input.Session.ID != session.ID || len(runner.input.Messages) == 0 {
		t.Fatalf("planner runner did not receive session thread: %+v", runner.input)
	}
}

func TestPlannerAgentRunDoesNotOverwriteApprovedSession(t *testing.T) {
	st := testsupport.OpenStore(t)
	project := createPlanningProject(t, st)
	session, err := st.CreatePlannerSession(project.ID, "Add agent planning")
	if err != nil { t.Fatal(err) }
	runner := &fakePlannerRunner{result: plannerexec.Result{Mode: plannerexec.ModeDryRun, PlannerProfile: profiles.ProfilePlanner, CriticProfile: profiles.ProfileCritic, Draft: plannerexec.Draft{Title: "Generated planner task", Objective: "Generated objective", Focus: "focus", Boundaries: "boundaries", DoneWhen: "done", Assumptions: []string{"assumption"}, Risks: []string{"risk"}, GraphPreview: []string{"Prompt", "Approval"}}, Summary: "Planner/critic completed."}}
	runner.beforeReturn = func() {
		_, _ = st.ApprovePlannerSession(project, session.ID)
	}
	s := &Server{cfg: config.Config{ExecutionMode: config.ExecutionModeDryRun}, store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), plannerRunner: runner, csrfToken: "test"}
	req := httptest.NewRequest(http.MethodPost, "/projects/"+project.ID+"/planner/"+session.ID+"/run-agents", strings.NewReader("csrf_token=test"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.projectRoutes(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("unexpected status=%d body=%q", rec.Code, rec.Body.String())
	}
	updated := waitForPlannerSession(t, st, session.ID, func(session models.PlannerSession) bool {
		return session.Status == models.PlannerStatusApproved && session.ActiveGenerationID == ""
	})
	if updated.Status != models.PlannerStatusApproved || updated.DraftTitle == "Generated planner task" {
		t.Fatalf("stale planner result overwrote approved session: %+v", updated)
	}
	if !waitForPlannerMessage(t, st, session.ID, "planner", "result discarded") {
		t.Fatalf("expected stale-result discard message")
	}
}

func TestPlannerAgentRunDoesNotOverwriteDismissedSession(t *testing.T) {
	st := testsupport.OpenStore(t)
	project := createPlanningProject(t, st)
	session, err := st.CreatePlannerSession(project.ID, "Add agent planning")
	if err != nil { t.Fatal(err) }
	runner := &fakePlannerRunner{result: plannerexec.Result{Mode: plannerexec.ModeDryRun, PlannerProfile: profiles.ProfilePlanner, CriticProfile: profiles.ProfileCritic, Draft: plannerexec.Draft{Title: "Generated planner task", Objective: "Generated objective", Focus: "focus", Boundaries: "boundaries", DoneWhen: "done", Assumptions: []string{"assumption"}, Risks: []string{"risk"}, GraphPreview: []string{"Prompt", "Approval"}}, Summary: "Planner/critic completed."}}
	runner.beforeReturn = func() {
		_, _, _ = st.DismissPlannerSession(session.ID)
	}
	s := &Server{cfg: config.Config{ExecutionMode: config.ExecutionModeDryRun}, store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), plannerRunner: runner, csrfToken: "test"}
	req := httptest.NewRequest(http.MethodPost, "/projects/"+project.ID+"/planner/"+session.ID+"/run-agents", strings.NewReader("csrf_token=test"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.projectRoutes(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("unexpected status=%d body=%q", rec.Code, rec.Body.String())
	}
	generation := waitForPlannerGeneration(t, st, session.ID, models.PlannerGenerationStatusDiscarded)
	if !strings.Contains(generation.Summary, "session is now dismissed") {
		t.Fatalf("expected dismissed discard summary, got %+v", generation)
	}
	updated, ok := st.GetPlannerSession(session.ID)
	if !ok { t.Fatal("missing session") }
	if updated.Status != models.PlannerStatusDismissed || updated.DraftTitle == "Generated planner task" || updated.ActiveGenerationID != "" {
		t.Fatalf("stale planner result overwrote dismissed session: %+v", updated)
	}
}

func TestPlannerAgentRunDiscardsRevisionStaleResultWithoutOverwritingMetadata(t *testing.T) {
	st := testsupport.OpenStore(t)
	project := createPlanningProject(t, st)
	session, err := st.CreatePlannerSession(project.ID, "Add agent planning")
	if err != nil { t.Fatal(err) }
	var replyErr error
	runner := &fakePlannerRunner{result: plannerexec.Result{Mode: plannerexec.ModeDryRun, PlannerProfile: profiles.ProfilePlanner, CriticProfile: profiles.ProfileCritic, Draft: plannerexec.Draft{Title: "Stale generated task", Objective: "Generated objective", Focus: "focus", Boundaries: "boundaries", DoneWhen: "done", Assumptions: []string{"assumption"}, Risks: []string{"risk"}, GraphPreview: []string{"Prompt", "Approval"}}, Summary: "Planner/critic completed."}}
	runner.beforeReturn = func() {
		replyErr = st.AddPlannerReply(session.ID, "New constraint added while generation is running")
	}
	s := &Server{cfg: config.Config{ExecutionMode: config.ExecutionModeDryRun}, store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), plannerRunner: runner, csrfToken: "test"}
	req := httptest.NewRequest(http.MethodPost, "/projects/"+project.ID+"/planner/"+session.ID+"/run-agents", strings.NewReader("csrf_token=test"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.projectRoutes(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("unexpected status=%d body=%q", rec.Code, rec.Body.String())
	}
	if replyErr != nil {
		t.Fatalf("reply during generation failed: %v", replyErr)
	}
	generation := waitForPlannerGeneration(t, st, session.ID, models.PlannerGenerationStatusDiscarded)
	if !strings.Contains(generation.Summary, "planning thread changed") {
		t.Fatalf("expected revision discard summary, got %+v", generation)
	}
	updated, ok := st.GetPlannerSession(session.ID)
	if !ok { t.Fatal("missing session") }
	if updated.DraftTitle == "Stale generated task" || strings.Contains(updated.AgentSummary, "Planner/critic completed") {
		t.Fatalf("stale revision result overwrote session: %+v", updated)
	}
	if updated.ActiveGenerationID != "" || updated.AgentStatus != models.PlannerAgentStatusDiscarded {
		t.Fatalf("revision should mark active generation stale without leaving session running: %+v", updated)
	}
}

func TestPlannerAgentRunMarksSessionFailedOnPlannerRunError(t *testing.T) {
	st := testsupport.OpenStore(t)
	project := createPlanningProject(t, st)
	session, err := st.CreatePlannerSession(project.ID, "Add agent planning")
	if err != nil { t.Fatal(err) }
	runner := &fakePlannerRunner{result: plannerexec.Result{Mode: plannerexec.ModeLocalPi, PlannerProfile: profiles.ProfilePlanner, CriticProfile: profiles.ProfileCritic, PlannerMessage: "Planner produced invalid JSON.", Summary: "Planner output failed to parse.", Diagnostics: []plannerexec.Diagnostic{{Name: "planner-stdout.txt", Kind: models.PlannerDiagnosticKindOutput, Body: "raw planner output"}}}, err: context.DeadlineExceeded}
	s := &Server{cfg: config.Config{ExecutionMode: config.ExecutionModeLocalPi}, store: st, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), plannerRunner: runner, csrfToken: "test"}
	req := httptest.NewRequest(http.MethodPost, "/projects/"+project.ID+"/planner/"+session.ID+"/run-agents", strings.NewReader("csrf_token=test"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.projectRoutes(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("unexpected status=%d body=%q", rec.Code, rec.Body.String())
	}
	updated := waitForPlannerSession(t, st, session.ID, func(session models.PlannerSession) bool {
		return session.AgentStatus == models.PlannerAgentStatusFailed
	})
	if updated.AgentStatus != models.PlannerAgentStatusFailed {
		t.Fatalf("expected failed status, got %+v", updated)
	}
	if !strings.Contains(updated.AgentSummary, "Planner output failed to parse.") || !strings.Contains(updated.AgentSummary, "Diagnostic: planner/critic execution timed out.") {
		t.Fatalf("expected sanitized failure summary to persist, got %+v", updated)
	}
	if !waitForPlannerMessage(t, st, session.ID, "planner", "Diagnostic: planner/critic execution timed out.") {
		t.Fatalf("expected sanitized diagnostic message")
	}
	diagnostics := waitForPlannerDiagnostics(t, st, session.ID, 1)
	var outputDiagnostic models.PlannerDiagnostic
	for _, diagnostic := range diagnostics {
		if diagnostic.Kind == models.PlannerDiagnosticKindOutput {
			outputDiagnostic = diagnostic
		}
	}
	if outputDiagnostic.ID == "" {
		t.Fatalf("expected planner output diagnostic: %+v", diagnostics)
	}
	diagReq := httptest.NewRequest(http.MethodGet, "/projects/"+project.ID+"/planner/"+session.ID+"/diagnostics/"+outputDiagnostic.ID, nil)
	diagRec := httptest.NewRecorder()
	s.projectRoutes(diagRec, diagReq)
	if diagRec.Code != http.StatusOK || !strings.Contains(diagRec.Body.String(), "raw planner output") {
		t.Fatalf("expected planner diagnostic preview, code=%d body=%q", diagRec.Code, diagRec.Body.String())
	}
	otherSession, err := st.CreatePlannerSession(project.ID, "Other planner session")
	if err != nil {
		t.Fatal(err)
	}
	crossSessionReq := httptest.NewRequest(http.MethodGet, "/projects/"+project.ID+"/planner/"+otherSession.ID+"/diagnostics/"+outputDiagnostic.ID, nil)
	crossSessionRec := httptest.NewRecorder()
	s.projectRoutes(crossSessionRec, crossSessionReq)
	if crossSessionRec.Code != http.StatusNotFound {
		t.Fatalf("expected cross-session diagnostic request to be rejected, got %d", crossSessionRec.Code)
	}
}

func createPlanningProject(t testing.TB, st *store.Store) models.Project {
	t.Helper()
	project, err := st.CreateProject("Planning project", "Project context", testsupport.TempGitRepo(t), "main")
	if err != nil {
		t.Fatal(err)
	}
	return project
}

func waitForPlannerSession(t testing.TB, st *store.Store, sessionID string, done func(models.PlannerSession) bool) models.PlannerSession {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var session models.PlannerSession
	for time.Now().Before(deadline) {
		var ok bool
		session, ok = st.GetPlannerSession(sessionID)
		if !ok {
			t.Fatalf("missing session")
		}
		if done(session) {
			return session
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("planner session did not reach expected state: %+v", session)
	return models.PlannerSession{}
}

func waitForPlannerGeneration(t testing.TB, st *store.Store, sessionID, status string) models.PlannerGeneration {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var generations []models.PlannerGeneration
	for time.Now().Before(deadline) {
		generations = st.PlannerGenerationsForSession(sessionID)
		for _, generation := range generations {
			if generation.Status == status {
				return generation
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("planner generation did not reach %s: %+v", status, generations)
	return models.PlannerGeneration{}
}

func waitForPlannerDiagnostics(t testing.TB, st *store.Store, sessionID string, min int) []models.PlannerDiagnostic {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var diagnostics []models.PlannerDiagnostic
	for time.Now().Before(deadline) {
		diagnostics = st.PlannerDiagnosticsForSession(sessionID)
		if len(diagnostics) >= min {
			return diagnostics
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("planner diagnostics did not reach expected count: %+v", diagnostics)
	return nil
}

func waitForPlannerMessage(t testing.TB, st *store.Store, sessionID, role, body string) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hasPlannerMessage(st.PlannerMessages(sessionID), role, body) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func hasPlannerMessage(messages []models.PlannerMessage, role, body string) bool {
	for _, message := range messages {
		if message.Role == role && strings.Contains(message.Body, body) {
			return true
		}
	}
	return false
}
