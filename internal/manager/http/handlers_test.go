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
	controller.startRunFunc = func(_ context.Context, idea string) (string, error) {
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
	controller.startRunFunc = func(_ context.Context, idea string) (string, error) {
		if idea != "build the thing" {
			t.Fatalf("StartRun idea = %q", idea)
		}
		return "run_happy", nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/runs", cookie, url.Values{"idea": {"build the thing"}, "_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST /runs status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/runs/run_happy" {
		t.Fatalf("Location = %q want /runs/run_happy", got)
	}
}

func TestHandleStartQueuedRunRedirectsOnSuccess(t *testing.T) {
	st := openRouteTestStore(t)
	controller := &fakeRunController{state: defaultRouteQueueState()}
	controller.startQueuedRunFunc = func(_ context.Context, runID string) error {
		if runID != "run_manual" {
			t.Fatalf("StartQueuedRun runID = %q", runID)
		}
		return nil
	}

	srv := newRouteTestServer(t, st, controller)
	cookie, csrf := getCSRFToken(t, srv)
	rec := postForm(t, srv, "/runs/run_manual/start", cookie, url.Values{"_csrf": {csrf}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST /runs/run_manual/start status = %d want %d body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/runs/run_manual" {
		t.Fatalf("Location = %q want /runs/run_manual", got)
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
			st := openRouteTestStore(t)
			controller := &fakeRunController{state: defaultRouteQueueState()}
			controller.startQueuedRunFunc = func(context.Context, string) error { return tc.err }

			srv := newRouteTestServer(t, st, controller)
			cookie, csrf := getCSRFToken(t, srv)
			rec := postForm(t, srv, "/runs/run_conflict/start", cookie, url.Values{"_csrf": {csrf}})
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
	assertContains(t, body, "/runs/"+queued.Run.ID+"/start")
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
	startRunFunc       func(context.Context, string) (string, error)
	startQueuedRunFunc func(context.Context, string) error
	cancelRunFunc      func(context.Context, string) error
}

func (f *fakeRunController) StartRun(ctx context.Context, idea string) (string, error) {
	if f.startRunFunc != nil {
		return f.startRunFunc(ctx, idea)
	}
	return "", errors.New("unexpected StartRun call")
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
