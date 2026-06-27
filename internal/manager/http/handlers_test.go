package managerhttp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/secrets"
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

func TestHandleRunsCoercesLegacyRefinementLevel(t *testing.T) {
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
		if input.WorkflowTemplateID != "team_template" {
			t.Fatalf("workflow template id = %q, want team_template", input.WorkflowTemplateID)
		}
		return "run_legacy", nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/runs", cookie, url.Values{"idea": {"build the thing"}, "refinement_level": {"deep"}, "workflow_template_id": {"team_template"}, "_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST /runs status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
}

func TestHandleProjectChatMessageSubmitsConversationTurn(t *testing.T) {
	st := openRouteTestStore(t)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.submitConversationMessageFunc = func(_ context.Context, projectID, body string) (store.Message, error) {
		if projectID != store.DefaultProjectID {
			t.Fatalf("SubmitConversationMessage projectID = %q", projectID)
		}
		if body != "Build chat tracer bullet" {
			t.Fatalf("chat body = %q", body)
		}
		return store.Message{ID: "msg_chat", ProjectID: projectID, Role: store.MessageRoleUser, Body: body}, nil
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
	body := getIndexBody(t, srv)
	assertContains(t, body, "Project Chat")
	assertContains(t, body, "/projects/default/chat/events")
}

func TestProjectHomeRendersReadyForReviewInTasksOverview(t *testing.T) {
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
	assertContains(t, body, "Tasks")
	assertContains(t, body, "⚠ needs you: diff ready")
	assertContains(t, body, "/projects/default/runs/"+wr.Run.ID+"?tab=review")
	assertContains(t, body, "Review from chat")
	assertNotContains(t, body, "Tasks started from this chat")
}

func TestProjectSettingsRendersRulesPreferencesAndHomeLink(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	if _, err := st.UpdateProjectRules(ctx, store.DefaultProjectID, "Run focused checks.\n"); err != nil {
		t.Fatalf("update rules: %v", err)
	}
	if _, err := st.UpdateProjectPreferences(ctx, store.DefaultProjectID, "Prefer concise updates.\n"); err != nil {
		t.Fatalf("update preferences: %v", err)
	}

	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	home := getIndexBody(t, srv)
	assertContains(t, home, "/projects/default/settings")
	body := getSettingsBody(t, srv, "/projects/default/settings")
	assertContains(t, body, "Project Settings")
	assertContains(t, body, "Rules")
	assertContains(t, body, "Run focused checks.")
	assertContains(t, body, "Preferences")
	assertContains(t, body, "Prefer concise updates.")
	assertContains(t, body, "Notifications")
	assertContains(t, body, "Only when")
	assertContains(t, body, "When anything finishes")
	assertContains(t, body, "No repository configured; repo candidate loading is unavailable.")
	assertNotContains(t, body, "title=")
}

func TestProjectSettingsSaveSwapsSingleSectionAndAllowsEmpty(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	if _, err := st.UpdateProjectPreferences(ctx, store.DefaultProjectID, "Existing preferences\n"); err != nil {
		t.Fatalf("seed preferences: %v", err)
	}

	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/settings/preferences", cookie, url.Values{"content": {""}, "_csrf": {csrf}})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST settings preferences status = %d want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	body := rec.Body.String()
	assertContains(t, body, "id=\"settings-preferences\"")
	assertContains(t, body, "saved · updated")
	assertNotContains(t, body, "settings-rules")
	project, err := st.GetProject(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("get project: %v", err)
	}
	if project.ProjectPreferences != "" {
		t.Fatalf("project preferences = %q, want empty", project.ProjectPreferences)
	}
}

func TestProjectNotificationSettingsSaveSwapsSection(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	cookie, csrf := getCSRFToken(t, srv)

	rec := postForm(t, srv, "/projects/default/settings/notifications", cookie, url.Values{"when_finished": {"1"}, "_csrf": {csrf}})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST settings notifications status = %d want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	body := rec.Body.String()
	assertContains(t, body, "id=\"settings-notifications\"")
	assertContains(t, body, "saved · updated")
	assertNotContains(t, body, "settings-rules")
	prefs, err := st.GetProjectNotificationPreferences(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("get notification prefs: %v", err)
	}
	if prefs.OnlyWhenNeeded || !prefs.WhenFinished {
		t.Fatalf("notification prefs = %+v, want only when_finished", prefs)
	}
}

func TestSystemSettingsExternalSinkCreationSealsSecretAndShowsWebhookSecretOnce(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	svc, err := secrets.New(ctx, st, secrets.Config{})
	if err != nil {
		t.Fatalf("new secrets: %v", err)
	}
	srv := newRouteTestServerWithSecrets(t, st, &fakeRunController{state: defaultRouteQueueState()}, svc)
	body := getSettingsBody(t, srv, "/settings")
	assertContains(t, body, "System Settings")
	assertContains(t, body, "External notification sinks")
	assertNotContains(t, body, externalSinkSecretUnavailable)
	cookie, csrf := getCSRFToken(t, srv)

	rec := postForm(t, srv, "/settings/notification-sinks/webhook", cookie, url.Values{
		"url":            {"https://hooks.example/parley"},
		"http_method":    {"POST"},
		"enabled":        {"1"},
		"send_needs_you": {"1"},
		"_csrf":          {csrf},
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST webhook sink status = %d want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	body = rec.Body.String()
	assertContains(t, body, "shown once")
	secret := regexp.MustCompile(`<code>([^<]+)</code>`).FindStringSubmatch(body)
	if len(secret) != 2 {
		t.Fatalf("one-time secret not found in response: %s", body)
	}
	sinks, err := st.ListNotificationSinks(ctx)
	if err != nil {
		t.Fatalf("list sinks: %v", err)
	}
	if len(sinks) != 1 || sinks[0].Type != store.NotificationSinkTypeWebhook || !sinks[0].SendNeedsYou || sinks[0].SendFinished {
		t.Fatalf("sinks = %+v, want one needs_you webhook", sinks)
	}
	if strings.Contains(string(sinks[0].SecretCiphertext), secret[1]) {
		t.Fatal("stored webhook secret contains plaintext")
	}
	body = getSettingsBody(t, srv, "/settings")
	assertContains(t, body, "Secret: configured")
	assertNotContains(t, body, secret[1])
}

func TestSystemSettingsExternalSinkCreationRefusedWhenSecretsUnavailable(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	svc, err := secrets.New(ctx, st, secrets.Config{KeyProvider: noSecretsProvider{}})
	if err != nil {
		t.Fatalf("new secrets: %v", err)
	}
	if svc.Available() {
		t.Fatal("test secrets service unexpectedly available")
	}
	srv := newRouteTestServerWithSecrets(t, st, &fakeRunController{state: defaultRouteQueueState()}, svc)
	body := getSettingsBody(t, srv, "/settings")
	assertContains(t, body, externalSinkSecretUnavailable)
	cookie, csrf := getCSRFToken(t, srv)

	rec := postForm(t, srv, "/settings/notification-sinks/gotify", cookie, url.Values{
		"base_url":       {"https://gotify.example"},
		"app_token":      {"plaintext-token"},
		"enabled":        {"1"},
		"send_needs_you": {"1"},
		"_csrf":          {csrf},
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST gotify without secrets status = %d want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	assertContains(t, rec.Body.String(), externalSinkSecretUnavailable)
	sinks, err := st.ListNotificationSinks(ctx)
	if err != nil {
		t.Fatalf("list sinks: %v", err)
	}
	if len(sinks) != 0 {
		t.Fatalf("sinks = %+v, want none", sinks)
	}
	if _, err := st.InsertNotification(ctx, store.NotificationInput{ProjectID: store.DefaultProjectID, Class: store.NotificationClassNeedsYou, Title: "in-app still works"}); err != nil {
		t.Fatalf("insert in-app notification with unavailable secrets: %v", err)
	}
}

func TestNotificationAckRedirectsToRunAndMarkAllSwapsCenter(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRun(ctx, "notify from a run")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	first, err := st.InsertNotification(ctx, store.NotificationInput{ProjectID: wr.Project.ID, RunID: wr.Run.ID, Class: store.NotificationClassNeedsYou, Title: "Review needed: notify from a run"})
	if err != nil {
		t.Fatalf("insert notification: %v", err)
	}
	if _, err := st.InsertNotification(ctx, store.NotificationInput{ProjectID: wr.Project.ID, RunID: wr.Run.ID, Class: store.NotificationClassFinished, Title: "Run completed: notify from a run"}); err != nil {
		t.Fatalf("insert second notification: %v", err)
	}

	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/notifications/"+first.ID+"/ack", cookie, url.Values{"redirect": {"/projects/default/runs/" + wr.Run.ID}, "_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST notification ack status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/projects/default/runs/"+wr.Run.ID {
		t.Fatalf("ack redirect Location = %q", got)
	}
	acked, err := st.GetNotification(ctx, first.ID)
	if err != nil {
		t.Fatalf("get acked notification: %v", err)
	}
	if acked.AcknowledgedAt == "" {
		t.Fatalf("notification not acknowledged: %+v", acked)
	}

	rec = postForm(t, srv, "/notifications/ack-all", cookie, url.Values{"_csrf": {csrf}})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST notifications ack-all status = %d want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	body := rec.Body.String()
	assertContains(t, body, "id=\"notification-center\"")
	assertNotContains(t, body, "notification-badge")
	unread, err := st.CountUnreadNotifications(ctx)
	if err != nil {
		t.Fatalf("count unread: %v", err)
	}
	if unread != 0 {
		t.Fatalf("unread after mark all = %d, want 0", unread)
	}
}

func TestInAppNotificationSinkBroadcastsGlobalNotificationFragment(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRun(ctx, "broadcast notification")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	notification, err := st.InsertNotification(ctx, store.NotificationInput{ProjectID: wr.Project.ID, RunID: wr.Run.ID, Class: store.NotificationClassFinished, Title: "Run completed: broadcast notification"})
	if err != nil {
		t.Fatalf("insert notification: %v", err)
	}
	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	ch, unsubscribe := srv.hub.Subscribe(NotificationsTopic)
	defer unsubscribe()
	projectCh, unsubscribeProject := srv.hub.Subscribe(projectChatTopic(wr.Project.ID))
	defer unsubscribeProject()
	sink := NewInAppNotificationSink(st, srv.hub, srv.renderer)
	if err := sink.Notify(ctx, notification); err != nil {
		t.Fatalf("notify sink: %v", err)
	}
	select {
	case msg := <-ch:
		if msg.Event.Type != "notification.created" {
			t.Fatalf("event type = %q", msg.Event.Type)
		}
		assertContains(t, msg.Fragment, "id=\"notification-center\"")
		assertContains(t, msg.Fragment, "Run completed: broadcast notification")
		assertContains(t, msg.Fragment, "notification-badge")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for notification broadcast")
	}
	select {
	case msg := <-projectCh:
		t.Fatalf("notification broadcast leaked onto project chat topic: %+v", msg.Event)
	default:
	}
}

func TestProjectSettingsLoadFromRepoIsDraftUntilSaved(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	repo := t.TempDir()
	writeRepoCandidate(t, repo, store.ProjectRulesCandidatePath, "Candidate repo rules\n")
	spec := store.DefaultProjectSpec(st.DataDir())
	spec.RepositoryPath = repo
	if _, err := st.EnsureProject(ctx, spec); err != nil {
		t.Fatalf("ensure project repo: %v", err)
	}
	if _, err := st.UpdateProjectRules(ctx, store.DefaultProjectID, "Saved app rules\n"); err != nil {
		t.Fatalf("seed rules: %v", err)
	}

	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	body := getSettingsBody(t, srv, "/projects/default/settings")
	assertContains(t, body, "⚠ repo candidate differs")
	assertContains(t, body, "Load from repo")
	assertContains(t, body, "Loads <code>.parley/rules.md</code> into this editor as an unsaved draft.")

	candidate := getSettingsBody(t, srv, "/projects/default/settings/rules/candidate")
	assertContains(t, candidate, "Candidate repo rules")
	assertContains(t, candidate, "repo candidate loaded as an unsaved draft · Save to commit")
	project, err := st.GetProject(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("get project after draft load: %v", err)
	}
	if project.ProjectRules != "Saved app rules\n" {
		t.Fatalf("candidate load changed app rules = %q", project.ProjectRules)
	}

	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/settings/rules", cookie, url.Values{"content": {"Candidate repo rules\n"}, "_csrf": {csrf}})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST settings rules status = %d want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	project, err = st.GetProject(ctx, store.DefaultProjectID)
	if err != nil {
		t.Fatalf("get project after save: %v", err)
	}
	if project.ProjectRules != "Candidate repo rules\n" {
		t.Fatalf("saved project rules = %q", project.ProjectRules)
	}
	body = getSettingsBody(t, srv, "/projects/default/settings")
	assertNotContains(t, body, "⚠ repo candidate differs")
}

func TestProjectSettingsDiffBadgeIgnoresTrailingWhitespace(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	repo := t.TempDir()
	writeRepoCandidate(t, repo, store.ProjectRulesCandidatePath, "Shared project rules\n\t ")
	spec := store.DefaultProjectSpec(st.DataDir())
	spec.RepositoryPath = repo
	if _, err := st.EnsureProject(ctx, spec); err != nil {
		t.Fatalf("ensure project repo: %v", err)
	}
	if _, err := st.UpdateProjectRules(ctx, store.DefaultProjectID, "Shared project rules"); err != nil {
		t.Fatalf("seed matching rules: %v", err)
	}

	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	body := getSettingsBody(t, srv, "/projects/default/settings")
	assertNotContains(t, body, "⚠ repo candidate differs")

	if _, err := st.UpdateProjectRules(ctx, store.DefaultProjectID, "Different project rules"); err != nil {
		t.Fatalf("seed differing rules: %v", err)
	}
	body = getSettingsBody(t, srv, "/projects/default/settings")
	assertContains(t, body, "⚠ repo candidate differs")
}

func TestProjectSettingsLoadFromRepoMissingCandidateRendersNotice(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	repo := t.TempDir()
	spec := store.DefaultProjectSpec(st.DataDir())
	spec.RepositoryPath = repo
	if _, err := st.EnsureProject(ctx, spec); err != nil {
		t.Fatalf("ensure project repo: %v", err)
	}

	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	body := getSettingsBody(t, srv, "/projects/default/settings/preferences/candidate")
	assertContains(t, body, "No `.parley/preferences.md` in the repository.")
	assertContains(t, body, "id=\"settings-preferences\"")
}

func TestProjectTasksOverviewOrderingNeedsYouBadgeAndProjectsCount(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)

	completed, err := st.CreateWorkflowRun(ctx, "zzz recent completed")
	if err != nil {
		t.Fatalf("create completed run: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, completed.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("mark completed: %v", err)
	}
	running, err := st.CreateWorkflowRun(ctx, "mmm active running")
	if err != nil {
		t.Fatalf("create running run: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, running.Run.ID, store.RunStatusRunning); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	firstReview, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "aaa diff ready", WorkflowTemplateID: workflow.CarefulReviewID})
	if err != nil {
		t.Fatalf("create first review run: %v", err)
	}
	markAwaitingHumanReview(t, st, firstReview)
	review, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "bbb diff ready", WorkflowTemplateID: workflow.CarefulReviewID})
	if err != nil {
		t.Fatalf("create review run: %v", err)
	}
	markAwaitingHumanReview(t, st, review)

	if _, err := st.EnsureProject(ctx, store.ProjectSpec{ID: "other", Name: "Other project", WorkspacePath: t.TempDir(), QueueAutoWhenReady: true, QueueMaxConcurrent: 1, QueueBacklogCap: 10}); err != nil {
		t.Fatalf("ensure other project: %v", err)
	}
	other, err := st.CreateWorkflowRunForProjectInput(ctx, "other", contract.TaskInput{Idea: "other project review", WorkflowTemplateID: workflow.CarefulReviewID})
	if err != nil {
		t.Fatalf("create other project run: %v", err)
	}
	markAwaitingHumanReview(t, st, other)

	srv := newRouteTestServer(t, st, &fakeRunController{state: defaultRouteQueueState()})
	body := getIndexBody(t, srv)
	assertContains(t, body, "id=\"project-tasks-overview\"")
	assertContains(t, body, "⚠ needs you: diff ready")
	assertContains(t, body, "/projects/default/runs/"+firstReview.Run.ID+"?tab=review")
	assertContains(t, body, "/projects/default/runs/"+review.Run.ID+"?tab=review")
	assertBefore(t, body, "aaa diff ready", "mmm active running")
	assertBefore(t, body, "bbb diff ready", "mmm active running")
	assertBefore(t, body, "mmm active running", "zzz recent completed")

	projectsBody := getProjectsBody(t, srv)
	assertContains(t, projectsBody, "Cross-project needs-you count: <strong>3</strong>")
	assertContains(t, projectsBody, "Default project")
	assertContains(t, projectsBody, "⚠ 2 needs you")
	assertContains(t, projectsBody, "Other project")
	assertContains(t, projectsBody, "⚠ 1 needs you")
}

func TestHandleProjectChatMessageIgnoresRefinementLevelInTracerSlice(t *testing.T) {
	st := openRouteTestStore(t)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.submitConversationMessageFunc = func(_ context.Context, projectID, body string) (store.Message, error) {
		if projectID != store.DefaultProjectID {
			t.Fatalf("SubmitConversationMessage projectID = %q", projectID)
		}
		if body != "Refine this conversationally" {
			t.Fatalf("chat body = %q", body)
		}
		return store.Message{ID: "msg_chat", ProjectID: projectID, Role: store.MessageRoleUser, Body: body}, nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/chat/messages", cookie, url.Values{"message": {"Refine this conversationally"}, "refinement_level": {"standard"}, "_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST chat status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
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
	assertNotContains(t, rec.Body.String(), "deep")
}

func TestHandleReRunStageReturnsHTMLFragment(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRun(ctx, "rerun via endpoint")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.reRunStageFunc = func(_ context.Context, runID, stageID string) (store.Attempt, error) {
		if runID != wr.Run.ID || stageID != wr.ImplementationStage.ID {
			t.Fatalf("ReRunStage run=%s stage=%s", runID, stageID)
		}
		if err := st.UpdateRunStatus(ctx, runID, store.RunStatusRunning); err != nil {
			t.Fatalf("update run: %v", err)
		}
		return store.Attempt{ID: "attempt_rerun", RunID: runID}, nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/runs/"+wr.Run.ID+"/stages/"+wr.ImplementationStage.ID+"/rerun", cookie, url.Values{"_csrf": {csrf}})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST rerun status = %d want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "text/html") {
		t.Fatalf("Content-Type = %q, want html", got)
	}
	body := rec.Body.String()
	assertContains(t, body, "id=\"run-summary\"")
	assertContains(t, body, "id=\"story-panel\"")
	assertNotContains(t, body, "application/json")
}

func TestHandleReRunStageInvalidRequestDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	st := openRouteTestStore(t)
	wr, err := st.CreateWorkflowRun(ctx, "bad rerun endpoint")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, store.RunStatusCompleted); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	beforeAttempts, err := st.CountAttemptsForRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.reRunStageFunc = func(context.Context, string, string) (store.Attempt, error) {
		return store.Attempt{}, orchestrator.ErrStageReRunInvalidTarget
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/projects/default/runs/"+wr.Run.ID+"/stages/commit_feature_branch/rerun", cookie, url.Values{"_csrf": {csrf}})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST invalid rerun status = %d want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	afterAttempts, err := st.CountAttemptsForRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("count attempts after: %v", err)
	}
	run, err := st.GetRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if afterAttempts != beforeAttempts || run.Status != store.RunStatusCompleted {
		t.Fatalf("endpoint mutated attempts/status = %d/%s, want %d/%s", afterAttempts, run.Status, beforeAttempts, store.RunStatusCompleted)
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
	state                         orchestrator.QueueState
	queueStateFunc                func(context.Context) (orchestrator.QueueState, error)
	startRunFunc                  func(context.Context, string, string) (string, error)
	startRunInputFunc             func(context.Context, string, contract.TaskInput) (string, error)
	submitConversationMessageFunc func(context.Context, string, string) (store.Message, error)
	startQueuedRunFunc            func(context.Context, string) error
	cancelRunFunc                 func(context.Context, string) error
	reRunStageFunc                func(context.Context, string, string) (store.Attempt, error)
	humanReviewFunc               func(context.Context, string, string, orchestrator.HumanReviewSubmission) (report.Report, error)
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

func (f *fakeRunController) SubmitConversationMessage(ctx context.Context, projectID, body string) (store.Message, error) {
	if f.submitConversationMessageFunc != nil {
		return f.submitConversationMessageFunc(ctx, projectID, body)
	}
	return store.Message{}, errors.New("unexpected SubmitConversationMessage call")
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

func (f *fakeRunController) ReRunStage(ctx context.Context, runID, stageID string) (store.Attempt, error) {
	if f.reRunStageFunc != nil {
		return f.reRunStageFunc(ctx, runID, stageID)
	}
	return store.Attempt{}, errors.New("unexpected ReRunStage call")
}

func (f *fakeRunController) SubmitHumanReview(ctx context.Context, runID, stageID string, submission orchestrator.HumanReviewSubmission) (report.Report, error) {
	if f.humanReviewFunc != nil {
		return f.humanReviewFunc(ctx, runID, stageID, submission)
	}
	return report.Report{}, errors.New("unexpected SubmitHumanReview call")
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
	return newRouteTestServerWithSecrets(t, st, controller, nil)
}

func newRouteTestServerWithSecrets(t *testing.T, st *store.Store, controller *fakeRunController, secretService *secrets.Service) *Server {
	t.Helper()
	renderer, err := web.NewRenderer()
	if err != nil {
		t.Fatalf("new renderer: %v", err)
	}
	return NewServer("127.0.0.1:8080", st, controller, NewHub(), renderer, secretService)
}

type noSecretsProvider struct{}

func (noSecretsProvider) ResolveKey(context.Context, secrets.KeyRequest) (secrets.KeyMaterial, error) {
	return secrets.KeyMaterial{}, secrets.ErrNoKEK
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

func getProjectsBody(t *testing.T, srv *Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/projects", nil)
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /projects status = %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func getSettingsBody(t *testing.T, srv *Server, path string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080"+path, nil)
	srv.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d want %d body=%s", path, rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func writeRepoCandidate(t *testing.T, repo, rel, content string) {
	t.Helper()
	path := filepath.Join(repo, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir candidate dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write candidate: %v", err)
	}
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

func assertBefore(t *testing.T, body, first, second string) {
	t.Helper()
	firstIndex := strings.Index(body, first)
	secondIndex := strings.Index(body, second)
	if firstIndex < 0 || secondIndex < 0 || firstIndex >= secondIndex {
		t.Fatalf("body order want %q before %q (indexes %d, %d):\n%s", first, second, firstIndex, secondIndex, body)
	}
}
