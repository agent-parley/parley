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
	assertContains(t, body, "The verdict form is intentionally out of scope")
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
