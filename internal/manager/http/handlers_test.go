package managerhttp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestHandleRunsBacklogRejectionRendersIndexNotice(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	for _, idea := range []string{"first queued", "second queued"} {
		if _, err := st.CreateWorkflowRun(ctx, idea); err != nil {
			t.Fatalf("create queued run: %v", err)
		}
	}

	controller := &fakeRunController{
		state: orchestrator.QueueState{
			Policy:                 orchestrator.QueuePolicy{AutoWhenReady: true, MaxConcurrent: 2, BacklogCap: 2},
			Pending:                2,
			Running:                1,
			RunnerSlots:            1,
			ReadyRunnerSlots:       1,
			EffectiveMaxConcurrent: 1,
		},
	}
	controller.startRunFunc = func(_ context.Context, projectID, idea string) (string, error) {
		if projectID != store.DefaultProjectID {
			t.Fatalf("StartProjectRun projectID = %q", projectID)
		}
		if idea != "ship feature" {
			t.Fatalf("StartRun idea = %q", idea)
		}
		return "", orchestrator.QueueBacklogFullError{Pending: 2, Cap: 2}
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/runs", cookie, url.Values{"idea": {"ship feature"}, "_csrf": {csrf}})
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("POST /runs status = %d want %d body=%s", rec.Code, http.StatusTooManyRequests, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "text/html") {
		t.Fatalf("Content-Type = %q, want html", got)
	}
	body := rec.Body.String()
	assertContains(t, body, "Queue is full")
	assertContains(t, body, "2 pending runs are already waiting")
	assertContains(t, body, "backlog_cap 2")
	assertContains(t, body, "auto_when_ready=true")
	assertContains(t, body, "max_concurrent=2")
	assertContains(t, body, "2 pending / cap 2")
	assertContains(t, body, "Queue policy")
}

func TestHandleRunsHappyPathRedirectsToRun(t *testing.T) {
	st := openRouteTestStore(t)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.startRunInputFunc = func(_ context.Context, projectID string, input contract.TaskInput) (string, error) {
		if projectID != store.DefaultProjectID {
			t.Fatalf("StartProjectRun projectID = %q", projectID)
		}
		if input.Idea != "build the thing" {
			t.Fatalf("StartRun idea = %q", input.Idea)
		}
		if input.RefinementLevel != contract.RefinementLevelStandard {
			t.Fatalf("refinement level = %q, want standard", input.RefinementLevel)
		}
		return "run_happy", nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/runs", cookie, url.Values{"idea": {"build the thing"}, "_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST /runs status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/projects/default/runs/run_happy" {
		t.Fatalf("Location = %q want /projects/default/runs/run_happy", got)
	}
}

func TestHandleRunsPassesExplicitRefinementLevel(t *testing.T) {
	st := openRouteTestStore(t)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.startRunInputFunc = func(_ context.Context, projectID string, input contract.TaskInput) (string, error) {
		if projectID != store.DefaultProjectID {
			t.Fatalf("StartProjectRun projectID = %q", projectID)
		}
		if input.Idea != "build the thing" {
			t.Fatalf("StartRun idea = %q", input.Idea)
		}
		if input.RefinementLevel != contract.RefinementLevelDeep {
			t.Fatalf("refinement level = %q, want deep", input.RefinementLevel)
		}
		if input.WorkflowTemplateID != "team_template" {
			t.Fatalf("workflow template id = %q, want team_template", input.WorkflowTemplateID)
		}
		return "run_deep", nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/runs", cookie, url.Values{"idea": {"build the thing"}, "refinement_level": {"deep"}, "workflow_template_id": {"team_template"}, "_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST /runs status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
}

func TestHandleProjectChatMessagePersistsMessageAndStartsDirectRun(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	var conversationID string
	controller.startRunInputFunc = func(_ context.Context, projectID string, input contract.TaskInput) (string, error) {
		if projectID != store.DefaultProjectID {
			t.Fatalf("StartProjectRun projectID = %q", projectID)
		}
		if input.Idea != "Build chat tracer bullet" {
			t.Fatalf("chat idea = %q", input.Idea)
		}
		if input.RefinementLevel != contract.RefinementLevelDirect {
			t.Fatalf("refinement = %q, want direct", input.RefinementLevel)
		}
		if input.ConversationID == "" {
			t.Fatal("conversation id is empty")
		}
		conversationID = input.ConversationID
		if conversation, err := st.GetConversation(ctx, input.ConversationID); err != nil || conversation.ProjectID != store.DefaultProjectID {
			t.Fatalf("conversation lookup = %+v err=%v", conversation, err)
		}
		return "run_chat", nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/chat/messages", cookie, url.Values{"message": {"Build chat tracer bullet"}, "_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST chat status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/projects/default" {
		t.Fatalf("Location = %q want /projects/default", got)
	}
	messages, err := st.ListMessagesForConversation(ctx, conversationID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Role != store.MessageRoleUser || messages[0].Body != "Build chat tracer bullet" {
		t.Fatalf("messages = %#v, want persisted chat message", messages)
	}
	body := getIndexBody(t, srv)
	assertContains(t, body, "Project Chat")
	assertContains(t, body, "Build chat tracer bullet")
	assertContains(t, body, "/projects/default/chat/events")
}

func TestProjectChatRendersReadyForReviewCard(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "Review from chat", WorkflowTemplateID: workflow.CarefulReviewID, ConversationID: conversation.ID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	markAwaitingHumanReview(t, st, wr)

	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	body := getIndexBody(t, srv)
	assertContains(t, body, "ready for review →")
	assertContains(t, body, "/projects/default/runs/"+wr.Run.ID+"?tab=review")
	assertContains(t, body, "Review from chat")
}

func TestHandleProjectChatMessagePassesRefinementLevel(t *testing.T) {
	st := openRouteTestStore(t)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.startRunInputFunc = func(_ context.Context, projectID string, input contract.TaskInput) (string, error) {
		if projectID != store.DefaultProjectID {
			t.Fatalf("StartProjectRun projectID = %q", projectID)
		}
		if input.Idea != "Deeply refine this" {
			t.Fatalf("chat idea = %q", input.Idea)
		}
		if input.RefinementLevel != contract.RefinementLevelDeep {
			t.Fatalf("refinement = %q, want deep", input.RefinementLevel)
		}
		if input.ConversationID == "" {
			t.Fatal("conversation id is empty")
		}
		return "run_chat_deep", nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/chat/messages", cookie, url.Values{"message": {"Deeply refine this"}, "refinement_level": {"deep"}, "_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST chat status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
}

func TestProjectChatRendersDeepIdeaQuestionsAndAcceptsAnswers(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	conversation, err := st.EnsureProjectConversation(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "clarify audit logging", RefinementLevel: contract.RefinementLevelDeep, ConversationID: conversation.ID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := st.AddMessage(ctx, conversation.ID, store.MessageRoleUser, wr.Task.Idea); err != nil {
		t.Fatalf("add message: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusAwaitingHuman); err != nil {
		t.Fatalf("mark awaiting: %v", err)
	}
	if err := st.UpdateStageStatus(ctx, wr.IdeaIntakeStage.ID, store.StageStatusRunning); err != nil {
		t.Fatalf("mark idea running: %v", err)
	}
	round1Questions := `{"stage_id":"` + wr.IdeaIntakeStage.ID + `","round":1,"max_rounds":3,"questions":["Which audit sink should receive events?"]}`
	if _, err := st.SaveArtifact(ctx, wr.Run.ID, "idea_refinement_questions", "application/json", []byte(round1Questions), ".json"); err != nil {
		t.Fatalf("save round 1 questions: %v", err)
	}
	round1Answers := `{"stage_id":"` + wr.IdeaIntakeStage.ID + `","round":1,"answer_text":"Use the Postgres audit table."}`
	if _, err := st.SaveArtifact(ctx, wr.Run.ID, "idea_refinement_answers", "application/json", []byte(round1Answers), ".json"); err != nil {
		t.Fatalf("save round 1 answers: %v", err)
	}
	round2Questions := `{"stage_id":"` + wr.IdeaIntakeStage.ID + `","round":2,"max_rounds":3,"questions":["Any retention limit?","Should admin exports be included?"]}`
	if _, err := st.SaveArtifact(ctx, wr.Run.ID, "idea_refinement_questions", "application/json", []byte(round2Questions), ".json"); err != nil {
		t.Fatalf("save round 2 questions: %v", err)
	}

	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.deepIdeaFunc = func(_ context.Context, runID, stageID string, submission orchestrator.DeepIdeaAnswersSubmission) (orchestrator.DeepIdeaAnswerReceipt, error) {
		if runID != wr.Run.ID || stageID != wr.IdeaIntakeStage.ID {
			t.Fatalf("SubmitDeepIdeaAnswers run=%s stage=%s", runID, stageID)
		}
		if submission.ActorID != "operator" || submission.AnswerText != "Keep 90 days and include admin exports." {
			t.Fatalf("submission = %#v", submission)
		}
		return orchestrator.DeepIdeaAnswerReceipt{RunID: runID, StageID: stageID, ArtifactID: "artifact_answers", Round: 2}, nil
	}
	srv := newRouteTestServer(t, st, controller)

	body := getIndexBody(t, srv)
	assertContains(t, body, "Deep refinement needs answers before the planner continues.")
	assertContains(t, body, "Which audit sink should receive events?")
	assertContains(t, body, "Use the Postgres audit table.")
	assertContains(t, body, "Round 2 of 3")
	assertContains(t, body, "Any retention limit?")
	assertContains(t, body, "/projects/default/runs/"+wr.Run.ID+"/idea-stages/"+wr.IdeaIntakeStage.ID+"/answers")
	assertContains(t, body, `name="return_to" value="project_chat"`)

	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/runs/"+wr.Run.ID+"/idea-stages/"+wr.IdeaIntakeStage.ID+"/answers", cookie, url.Values{
		"actor_id":    {"operator"},
		"answer_text": {"Keep 90 days and include admin exports."},
		"return_to":   {"project_chat"},
		"_csrf":       {csrf},
	})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST chat answers status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/projects/default" {
		t.Fatalf("Location = %q want /projects/default", got)
	}
}

func TestHandleRunDetailRendersStoryReviewDrillIn(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "Render run story <safely>", RefinementLevel: contract.RefinementLevelDirect})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("update run: %v", err)
	}
	if err := st.UpdateStageStatus(ctx, wr.IdeaIntakeStage.ID, "completed"); err != nil {
		t.Fatalf("update idea stage: %v", err)
	}
	if err := st.UpdateStageStatus(ctx, wr.ImplementationStage.ID, "completed"); err != nil {
		t.Fatalf("update implementation stage: %v", err)
	}
	plan, err := st.SaveArtifact(ctx, wr.Run.ID, "task_plan", "text/markdown", []byte("# Plan\n\nKeep <unsafe> text escaped."), ".md")
	if err != nil {
		t.Fatalf("save plan: %v", err)
	}
	if err := st.UpdateStageTaskPlanArtifactID(ctx, wr.IdeaIntakeStage.ID, plan.ID); err != nil {
		t.Fatalf("link plan: %v", err)
	}
	var diff strings.Builder
	diff.WriteString("diff --git a/file.txt b/file.txt\n")
	diff.WriteString("@@ -1 +1 @@\n")
	diff.WriteString("-old line\n")
	diff.WriteString("+<script>alert(1)</script>\n")
	for i := 0; i < 82; i++ {
		diff.WriteString("+generated line\n")
	}
	diffArtifact, err := st.SaveArtifact(ctx, wr.Run.ID, "diff_patch", "text/x-diff", []byte(diff.String()), ".patch")
	if err != nil {
		t.Fatalf("save diff: %v", err)
	}
	if _, err := st.SaveArtifact(ctx, wr.Run.ID, "agent_output", "text/html", []byte("<h1>raw</h1>"), ".html"); err != nil {
		t.Fatalf("save html artifact: %v", err)
	}
	if _, err := st.AppendEvent(ctx, event.Event{
		SchemaVersion: event.SchemaVersion,
		ProjectID:     wr.Run.ProjectID,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		Type:          "stage.completed",
		Actor:         event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"},
		Summary:       "implementation completed with <unsafe> output",
		Data:          map[string]any{"stage_id": wr.ImplementationStage.ID, "stage_type": contract.StageTypeImplementation},
	}); err != nil {
		t.Fatalf("append stage event: %v", err)
	}
	if _, err := st.AppendEvent(ctx, event.Event{
		SchemaVersion: event.SchemaVersion,
		ProjectID:     wr.Run.ProjectID,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		Type:          "run.completed",
		Actor:         event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"},
		Summary:       "Run stopped at PR-ready",
		Data:          map[string]any{"terminal_status": store.RunStatusCompleted, "branch": "agent/run-story", "commit_sha": "abc1234", "diff_artifact_id": diffArtifact.ID},
	}); err != nil {
		t.Fatalf("append run event: %v", err)
	}

	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/projects/default/runs/"+wr.Run.ID+"?tab=review", nil)
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET run status = %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	assertContains(t, body, "run-page tab-review")
	assertContains(t, body, "Stage timeline")
	assertContains(t, body, "Task plan")
	assertContains(t, body, "Run outcome")
	assertContains(t, body, "implementation completed with &lt;unsafe&gt; output")
	assertContains(t, body, "Keep &lt;unsafe&gt; text escaped.")
	assertContains(t, body, "Show full diff")
	assertContains(t, body, "&#43;&lt;script&gt;alert(1)&lt;/script&gt;")
	assertNotContains(t, body, "<script>alert(1)</script>")
	assertContains(t, body, "Artifacts")
	assertContains(t, body, "download only")
	assertContains(t, body, "PR-ready")
	assertContains(t, body, "agent/run-story")
	assertNotContains(t, body, "name=\"verdict\"")
}

func TestHandleRunDetailRendersAwaitingHumanVerdictForm(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "Review code", WorkflowTemplateID: workflow.CarefulReviewID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	stage := markAwaitingHumanReview(t, st, wr)

	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/projects/default/runs/"+wr.Run.ID+"?tab=review", nil)
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET run status = %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	assertContains(t, body, "Human verdict")
	assertContains(t, body, "name=\"verdict\" value=\"pass\"")
	assertContains(t, body, "name=\"verdict\" value=\"changes_requested\"")
	assertContains(t, body, "name=\"verdict\" value=\"blocked\"")
	assertContains(t, body, "name=\"summary\"")
	assertContains(t, body, "name=\"finding\"")
	assertContains(t, body, "/projects/default/runs/"+wr.Run.ID+"/human-stages/"+stage.ID+"/verdict")
	assertContains(t, body, "data-on:submit=\"@post('")
	assertContains(t, body, "data-signals:csrf")
}

func TestDirectRunURLRedirectPreservesTab(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRun(ctx, "direct run URL")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/runs/"+wr.Run.ID+"?tab=review", nil)
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("GET direct run status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
	want := "/projects/default/runs/" + wr.Run.ID + "?tab=review"
	if got := rec.Header().Get("Location"); got != want {
		t.Fatalf("Location = %q want %q", got, want)
	}
}

func TestHandleRunsRejectsInvalidRefinementLevel(t *testing.T) {
	st := openRouteTestStore(t)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/runs", cookie, url.Values{"idea": {"build the thing"}, "refinement_level": {"max"}, "_csrf": {csrf}})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST /runs status = %d want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	assertContains(t, rec.Body.String(), "refinement_level must be one of")
}

func TestHandleDeepIdeaAnswersFormRedirectsToRun(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "deep clarify", RefinementLevel: contract.RefinementLevelDeep})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.deepIdeaFunc = func(_ context.Context, runID, stageID string, submission orchestrator.DeepIdeaAnswersSubmission) (orchestrator.DeepIdeaAnswerReceipt, error) {
		if runID != wr.Run.ID || stageID != "stage_idea" {
			t.Fatalf("SubmitDeepIdeaAnswers run=%s stage=%s", runID, stageID)
		}
		if submission.ActorID != "alice" || submission.AnswerText != "Use the audit sink." {
			t.Fatalf("submission = %#v", submission)
		}
		return orchestrator.DeepIdeaAnswerReceipt{RunID: runID, StageID: stageID, ArtifactID: "artifact_answers", Round: 1}, nil
	}
	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/runs/"+wr.Run.ID+"/idea-stages/stage_idea/answers", cookie, url.Values{"actor_id": {"alice"}, "answer_text": {"Use the audit sink."}, "_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST answers status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/projects/default/runs/"+wr.Run.ID {
		t.Fatalf("Location = %q", got)
	}
}

func TestHandleHumanReviewVerdictReturnsHTMLFragment(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "Approve code", WorkflowTemplateID: workflow.CarefulReviewID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	stage := markAwaitingHumanReview(t, st, wr)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.humanReviewFunc = func(_ context.Context, runID, stageID string, submission orchestrator.HumanReviewSubmission) (report.Report, error) {
		if runID != wr.Run.ID || stageID != stage.ID {
			t.Fatalf("SubmitHumanReview run=%s stage=%s", runID, stageID)
		}
		if submission.Verdict != string(report.ReviewVerdictPass) || submission.Summary != "Looks good" {
			t.Fatalf("submission = %#v", submission)
		}
		if len(submission.Findings) != 1 || submission.Findings[0].Summary != "Keep the test" {
			t.Fatalf("findings = %#v", submission.Findings)
		}
		if err := st.UpdateRunStatus(ctx, runID, store.RunStatusRunning); err != nil {
			t.Fatalf("update run: %v", err)
		}
		if err := st.UpdateStageStatus(ctx, stageID, report.StatusCompleted); err != nil {
			t.Fatalf("update stage: %v", err)
		}
		verdict := report.ReviewVerdictPass
		return report.Report{Status: report.StatusCompleted, Verdict: &verdict}, nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/runs/"+wr.Run.ID+"/human-stages/"+stage.ID+"/verdict", cookie, url.Values{"verdict": {"pass"}, "summary": {"Looks good"}, "finding": {"Keep the test", ""}, "_csrf": {csrf}})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST verdict status = %d want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "text/html") {
		t.Fatalf("Content-Type = %q, want html", got)
	}
	body := rec.Body.String()
	assertContains(t, body, "id=\"run-summary\"")
	assertContains(t, body, "id=\"review-panel\"")
	assertNotContains(t, body, "application/json")
	assertNotContains(t, body, "\"run_id\"")
}

func TestHandleHumanReviewVerdictInvalidVerdictIsBadRequest(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "Reject bad verdict", WorkflowTemplateID: workflow.CarefulReviewID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	stage := markAwaitingHumanReview(t, st, wr)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.humanReviewFunc = func(_ context.Context, _ string, _ string, submission orchestrator.HumanReviewSubmission) (report.Report, error) {
		if submission.Verdict != "bogus" {
			t.Fatalf("verdict = %q", submission.Verdict)
		}
		return report.Report{}, orchestrator.ErrInvalidHumanReview
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/runs/"+wr.Run.ID+"/human-stages/"+stage.ID+"/verdict", cookie, url.Values{"verdict": {"bogus"}, "summary": {"bad"}, "_csrf": {csrf}})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST invalid verdict status = %d want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestHandleHumanReviewVerdictDoubleSubmitIsConflict(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "Double submit", WorkflowTemplateID: workflow.CarefulReviewID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	stage := markAwaitingHumanReview(t, st, wr)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.humanReviewFunc = func(context.Context, string, string, orchestrator.HumanReviewSubmission) (report.Report, error) {
		return report.Report{}, orchestrator.ErrHumanReviewNotAwaiting
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/runs/"+wr.Run.ID+"/human-stages/"+stage.ID+"/verdict", cookie, url.Values{"verdict": {"pass"}, "summary": {"late"}, "_csrf": {csrf}})
	if rec.Code != http.StatusConflict {
		t.Fatalf("POST double-submit status = %d want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
}

func TestHandleStartQueuedRunRedirectsOnSuccess(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	queued, err := st.CreateWorkflowRun(ctx, "manual")
	if err != nil {
		t.Fatalf("create queued run: %v", err)
	}
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.startQueuedRunFunc = func(_ context.Context, runID string) error {
		if runID != queued.Run.ID {
			t.Fatalf("StartQueuedRun runID = %q", runID)
		}
		return nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/runs/"+queued.Run.ID+"/start", cookie, url.Values{"_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST start status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
	want := "/projects/default/runs/" + queued.Run.ID
	if got := rec.Header().Get("Location"); got != want {
		t.Fatalf("Location = %q want %s", got, want)
	}
}

func TestHandleStartQueuedRunConflictMappings(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "not pending", err: orchestrator.ErrRunNotPending},
		{name: "no slots", err: orchestrator.ErrNoRunnerSlots},
		{name: "held", err: orchestrator.ErrRunHeld},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := openRouteTestStore(t)
			queued, err := st.CreateWorkflowRun(ctx, "conflict")
			if err != nil {
				t.Fatalf("create queued run: %v", err)
			}
			controller := &fakeRunController{state: defaultRouteQueueState()}
			controller.startQueuedRunFunc = func(context.Context, string) error { return tc.err }

			srv := newRouteTestServer(t, st, controller)
			cookie, csrf := getCSRFToken(t, srv)
			rec := postForm(t, srv, "/runs/"+queued.Run.ID+"/start", cookie, url.Values{"_csrf": {csrf}})
			if rec.Code != http.StatusConflict {
				t.Fatalf("POST start status = %d want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
			}
			assertContains(t, rec.Body.String(), tc.err.Error())
		})
	}
}

func TestHandleIndexRendersQueueStateAndManualStartAffordance(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	queued, err := st.CreateWorkflowRun(ctx, "queued idea")
	if err != nil {
		t.Fatalf("create queued run: %v", err)
	}
	running, err := st.CreateWorkflowRun(ctx, "running idea")
	if err != nil {
		t.Fatalf("create running run: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, running.Run.ID, store.RunStatusRunning); err != nil {
		t.Fatalf("mark running: %v", err)
	}

	controller := &fakeRunController{
		state: orchestrator.QueueState{
			Policy:                 orchestrator.QueuePolicy{AutoWhenReady: false, MaxConcurrent: 2, BacklogCap: 5},
			Pending:                1,
			Running:                1,
			RunnerSlots:            1,
			ReadyRunnerSlots:       1,
			EffectiveMaxConcurrent: 1,
		},
	}
	srv := newRouteTestServer(t, st, controller)

	body := getIndexBody(t, srv)
	assertContains(t, body, "queued idea")
	assertContains(t, body, "running idea")
	assertContains(t, body, ">queued</span>")
	assertContains(t, body, ">running</span>")
	assertContains(t, body, "1 pending / cap 5")
	assertContains(t, body, "effective 1 · configured 2 · ready slots 1/1")
	assertContains(t, body, "Auto when ready</dt><dd>false")
	assertContains(t, body, "waiting for a free runner slot · manual start required")
	assertContains(t, body, "/projects/default/runs/"+queued.Run.ID+"/start")
	assertContains(t, body, "Start queued run")

	controller.state.Policy.AutoWhenReady = true
	body = getIndexBody(t, srv)
	assertContains(t, body, "Auto when ready</dt><dd>true")
	assertNotContains(t, body, "manual start required")
	assertNotContains(t, body, "Start queued run")
}

func markAwaitingHumanReview(t *testing.T, st *store.Store, wr store.WorkflowRun) store.Stage {
	t.Helper()
	ctx := context.Background()
	stages, err := st.ListStagesForAttempt(ctx, wr.Run.ID, wr.Attempt.ID)
	if err != nil {
		t.Fatalf("list stages: %v", err)
	}
	var stage store.Stage
	for _, candidate := range stages {
		if candidate.WorkflowStageID == "change_review_human" {
			stage = candidate
			break
		}
	}
	if stage.ID == "" {
		t.Fatalf("change_review_human stage not found: %#v", stages)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusAwaitingHuman); err != nil {
		t.Fatalf("mark run awaiting: %v", err)
	}
	if err := st.UpdateStageStatus(ctx, stage.ID, store.StageStatusRunning); err != nil {
		t.Fatalf("mark stage running: %v", err)
	}
	if _, err := st.AppendEvent(ctx, event.Event{
		SchemaVersion: event.SchemaVersion,
		ProjectID:     wr.Run.ProjectID,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		Type:          "stage.awaiting_human",
		Actor:         event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"},
		Summary:       "human review stage awaiting verdict",
		Data: map[string]any{
			"status":                 store.RunStatusAwaitingHuman,
			"pending_stage_id":       stage.ID,
			"stage_id":               stage.ID,
			"stage_type":             contract.StageTypeReview,
			"workflow_stage_id":      stage.WorkflowStageID,
			"human_review_packet_id": "artifact_packet",
		},
	}); err != nil {
		t.Fatalf("append awaiting event: %v", err)
	}
	return stage
}

type fakeRunController struct {
	state              orchestrator.QueueState
	queueStateFunc     func(context.Context) (orchestrator.QueueState, error)
	startRunFunc       func(context.Context, string, string) (string, error)
	startRunInputFunc  func(context.Context, string, contract.TaskInput) (string, error)
	startQueuedRunFunc func(context.Context, string) error
	cancelRunFunc      func(context.Context, string) error
	humanReviewFunc    func(context.Context, string, string, orchestrator.HumanReviewSubmission) (report.Report, error)
	deepIdeaFunc       func(context.Context, string, string, orchestrator.DeepIdeaAnswersSubmission) (orchestrator.DeepIdeaAnswerReceipt, error)
}

func (f *fakeRunController) StartProjectRun(ctx context.Context, projectID, idea string) (string, error) {
	if f.startRunFunc != nil {
		return f.startRunFunc(ctx, projectID, idea)
	}
	return "", errors.New("unexpected StartProjectRun call")
}

func (f *fakeRunController) StartProjectRunInput(ctx context.Context, projectID string, input contract.TaskInput) (string, error) {
	if f.startRunInputFunc != nil {
		return f.startRunInputFunc(ctx, projectID, input)
	}
	if f.startRunFunc != nil {
		return f.startRunFunc(ctx, projectID, input.Idea)
	}
	return "", errors.New("unexpected StartProjectRunInput call")
}

func (f *fakeRunController) StartQueuedRun(ctx context.Context, runID string) error {
	if f.startQueuedRunFunc != nil {
		return f.startQueuedRunFunc(ctx, runID)
	}
	return errors.New("unexpected StartQueuedRun call")
}

func (f *fakeRunController) CancelRun(ctx context.Context, runID string) error {
	if f.cancelRunFunc != nil {
		return f.cancelRunFunc(ctx, runID)
	}
	return errors.New("unexpected CancelRun call")
}

func (f *fakeRunController) SubmitHumanReview(ctx context.Context, runID, stageID string, submission orchestrator.HumanReviewSubmission) (report.Report, error) {
	if f.humanReviewFunc != nil {
		return f.humanReviewFunc(ctx, runID, stageID, submission)
	}
	return report.Report{}, errors.New("unexpected SubmitHumanReview call")
}

func (f *fakeRunController) SubmitDeepIdeaAnswers(ctx context.Context, runID, stageID string, submission orchestrator.DeepIdeaAnswersSubmission) (orchestrator.DeepIdeaAnswerReceipt, error) {
	if f.deepIdeaFunc != nil {
		return f.deepIdeaFunc(ctx, runID, stageID, submission)
	}
	return orchestrator.DeepIdeaAnswerReceipt{}, errors.New("unexpected SubmitDeepIdeaAnswers call")
}

func (f *fakeRunController) QueueState(ctx context.Context) (orchestrator.QueueState, error) {
	if f.queueStateFunc != nil {
		return f.queueStateFunc(ctx)
	}
	return f.state, nil
}

func defaultRouteQueueState() orchestrator.QueueState {
	return orchestrator.QueueState{
		Policy:                 orchestrator.QueuePolicy{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100},
		Pending:                0,
		Running:                0,
		RunnerSlots:            1,
		ReadyRunnerSlots:       1,
		EffectiveMaxConcurrent: 1,
	}
}

func openRouteTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func newRouteTestServer(t *testing.T, st *store.Store, controller *fakeRunController) *Server {
	t.Helper()
	renderer, err := web.NewRenderer()
	if err != nil {
		t.Fatalf("new renderer: %v", err)
	}
	return NewServer("127.0.0.1:8080", st, controller, NewHub(), renderer)
}

var csrfInputRE = regexp.MustCompile(`name="_csrf" value="([^"]+)"`)

func getCSRFToken(t *testing.T, srv *Server) (*http.Cookie, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/", nil)
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / status = %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	matches := csrfInputRE.FindStringSubmatch(rec.Body.String())
	if len(matches) != 2 {
		t.Fatalf("CSRF token not found in body: %s", rec.Body.String())
	}
	for _, cookie := range rec.Result().Cookies() {
		if cookie.Name == "parley_session" {
			return cookie, matches[1]
		}
	}
	t.Fatalf("parley_session cookie not set")
	return nil, ""
}

func getIndexBody(t *testing.T, srv *Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/", nil)
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / status = %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func postForm(t *testing.T, srv *Server, path string, cookie *http.Cookie, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8080"+path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	srv.routes().ServeHTTP(rec, req)
	return rec
}

func assertContains(t *testing.T, body, want string) {
	t.Helper()
	if !strings.Contains(body, want) {
		t.Fatalf("body missing %q:\n%s", want, body)
	}
}

func assertNotContains(t *testing.T, body, unwanted string) {
	t.Helper()
	if strings.Contains(body, unwanted) {
		t.Fatalf("body unexpectedly contains %q:\n%s", unwanted, body)
	}
}
