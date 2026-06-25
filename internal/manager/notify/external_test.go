package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/secrets"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/ids"
)

func TestWebhookPayloadIsVersionedAndSigned(t *testing.T) {
	ctx := context.Background()
	st, svc := newNotifyTestStore(t)
	var gotBody []byte
	var gotTimestamp string
	var gotSignature string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		gotTimestamp = r.Header.Get("X-Parley-Timestamp")
		gotSignature = r.Header.Get("X-Parley-Signature")
		gotBody = readRequestBody(t, r)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	secret := "webhook-secret-value"
	insertNotifyTestSink(t, ctx, st, svc, store.NotificationSinkInput{Type: store.NotificationSinkTypeWebhook, Enabled: true, URL: server.URL, HTTPMethod: http.MethodPost, AllowInsecureHTTP: true, SendNeedsYou: true}, secret)
	deliverer := NewExternalSink(st, svc, ExternalSinkOptions{Now: func() time.Time { return time.Unix(1710000000, 0).UTC() }})
	notification := testNotification(store.NotificationClassNeedsYou)
	if err := deliverer.Notify(ctx, notification); err != nil {
		t.Fatalf("notify: %v", err)
	}
	if gotTimestamp != "1710000000" {
		t.Fatalf("timestamp = %q, want unix seconds", gotTimestamp)
	}
	wantSignature := WebhookSignature([]byte(secret), gotTimestamp, gotBody)
	if gotSignature != wantSignature {
		t.Fatalf("signature = %q, want %q", gotSignature, wantSignature)
	}
	var payload WebhookPayload
	if err := json.Unmarshal(gotBody, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.SchemaVersion != 1 || payload.ID != notification.ID || payload.Class != notification.Class || payload.ProjectID != notification.ProjectID || payload.RunID != notification.RunID || payload.CreatedAt != notification.CreatedAt {
		t.Fatalf("payload = %+v, notification = %+v", payload, notification)
	}
	if payload.URL != "/projects/"+notification.ProjectID+"/runs/"+notification.RunID {
		t.Fatalf("payload URL = %q", payload.URL)
	}
	if bytes.Contains(gotBody, []byte(secret)) {
		t.Fatal("webhook payload contains signing secret")
	}
}

func TestExternalSinkClassFilterIntersection(t *testing.T) {
	ctx := context.Background()
	st, svc := newNotifyTestStore(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	insertNotifyTestSink(t, ctx, st, svc, store.NotificationSinkInput{Type: store.NotificationSinkTypeWebhook, Enabled: true, URL: server.URL, HTTPMethod: http.MethodPost, AllowInsecureHTTP: true, SendNeedsYou: false, SendFinished: true}, "secret")
	deliverer := NewExternalSink(st, svc, ExternalSinkOptions{})
	if err := deliverer.Notify(ctx, testNotification(store.NotificationClassNeedsYou)); err != nil {
		t.Fatalf("needs_you notify: %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("needs_you calls = %d, want 0", got)
	}
	if err := deliverer.Notify(ctx, testNotification(store.NotificationClassFinished)); err != nil {
		t.Fatalf("finished notify: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("finished calls = %d, want 1", got)
	}
}

func TestExternalSinkRetryClassification(t *testing.T) {
	t.Run("5xx retried until success", func(t *testing.T) {
		ctx := context.Background()
		st, svc := newNotifyTestStore(t)
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) < 3 {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()
		insertNotifyTestSink(t, ctx, st, svc, store.NotificationSinkInput{Type: store.NotificationSinkTypeWebhook, Enabled: true, URL: server.URL, HTTPMethod: http.MethodPost, AllowInsecureHTTP: true, SendFinished: true}, "secret")
		deliverer := NewExternalSink(st, svc, ExternalSinkOptions{RetryBackoff: time.Millisecond})
		if err := deliverer.Notify(ctx, testNotification(store.NotificationClassFinished)); err != nil {
			t.Fatalf("notify: %v", err)
		}
		if got := calls.Load(); got != 3 {
			t.Fatalf("calls = %d, want 3", got)
		}
	})

	t.Run("4xx terminal", func(t *testing.T) {
		ctx := context.Background()
		st, svc := newNotifyTestStore(t)
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
		}))
		defer server.Close()
		insertNotifyTestSink(t, ctx, st, svc, store.NotificationSinkInput{Type: store.NotificationSinkTypeWebhook, Enabled: true, URL: server.URL, HTTPMethod: http.MethodPost, AllowInsecureHTTP: true, SendFinished: true}, "secret")
		deliverer := NewExternalSink(st, svc, ExternalSinkOptions{RetryBackoff: time.Millisecond})
		if err := deliverer.Notify(ctx, testNotification(store.NotificationClassFinished)); err == nil {
			t.Fatal("notify error = nil, want terminal status error")
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("calls = %d, want 1", got)
		}
	})

	t.Run("timeout retried", func(t *testing.T) {
		ctx := context.Background()
		st, svc := newNotifyTestStore(t)
		var calls atomic.Int32
		client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			<-req.Context().Done()
			return nil, req.Context().Err()
		})}
		insertNotifyTestSink(t, ctx, st, svc, store.NotificationSinkInput{Type: store.NotificationSinkTypeWebhook, Enabled: true, URL: "https://example.invalid/hook", HTTPMethod: http.MethodPost, SendFinished: true}, "secret")
		deliverer := NewExternalSink(st, svc, ExternalSinkOptions{Client: client, DeliveryTimeout: 10 * time.Millisecond, MaxAttempts: 2, RetryBackoff: time.Millisecond})
		if err := deliverer.Notify(ctx, testNotification(store.NotificationClassFinished)); err == nil {
			t.Fatal("notify error = nil, want timeout error")
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("calls = %d, want 2", got)
		}
	})
}

func TestExternalSinkPlainHTTPRequiresOptIn(t *testing.T) {
	ctx := context.Background()
	st, svc := newNotifyTestStore(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	insertNotifyTestSink(t, ctx, st, svc, store.NotificationSinkInput{Type: store.NotificationSinkTypeWebhook, Enabled: true, URL: server.URL, HTTPMethod: http.MethodPost, AllowInsecureHTTP: false, SendFinished: true}, "secret")
	deliverer := NewExternalSink(st, svc, ExternalSinkOptions{})
	if err := deliverer.Notify(ctx, testNotification(store.NotificationClassFinished)); err == nil || !strings.Contains(err.Error(), "allow_insecure_http") {
		t.Fatalf("notify error = %v, want allow_insecure_http refusal", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("calls = %d, want 0", got)
	}
}

func TestExternalSinkUsesVerifiedHTTPS(t *testing.T) {
	ctx := context.Background()
	st, svc := newNotifyTestStore(t)
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	insertNotifyTestSink(t, ctx, st, svc, store.NotificationSinkInput{Type: store.NotificationSinkTypeWebhook, Enabled: true, URL: server.URL, HTTPMethod: http.MethodPost, SendFinished: true}, "secret")
	deliverer := NewExternalSink(st, svc, ExternalSinkOptions{Client: server.Client()})
	if err := deliverer.Notify(ctx, testNotification(store.NotificationClassFinished)); err != nil {
		t.Fatalf("notify: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestGotifyDeliverySealsTokenAndSendsMessage(t *testing.T) {
	ctx := context.Background()
	st, svc := newNotifyTestStore(t)
	var gotToken string
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.URL.Query().Get("token")
		gotBody = readRequestBody(t, r)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	secret := "gotify-token-value"
	insertNotifyTestSink(t, ctx, st, svc, store.NotificationSinkInput{Type: store.NotificationSinkTypeGotify, Enabled: true, BaseURL: server.URL, Priority: 7, AllowInsecureHTTP: true, SendNeedsYou: true}, secret)
	var stored []byte
	if err := st.DB().QueryRowContext(ctx, `SELECT secret_ciphertext FROM notification_sinks LIMIT 1`).Scan(&stored); err != nil {
		t.Fatalf("read stored secret: %v", err)
	}
	if bytes.Contains(stored, []byte(secret)) {
		t.Fatal("stored gotify token contains plaintext")
	}
	deliverer := NewExternalSink(st, svc, ExternalSinkOptions{})
	if err := deliverer.Notify(ctx, testNotification(store.NotificationClassNeedsYou)); err != nil {
		t.Fatalf("notify: %v", err)
	}
	if gotToken != secret {
		t.Fatalf("gotify token = %q, want secret", gotToken)
	}
	if !bytes.Contains(gotBody, []byte(`"priority":7`)) || !bytes.Contains(gotBody, []byte(`"title":"Needs review"`)) {
		t.Fatalf("gotify body = %s", gotBody)
	}
}

func TestGotifyDeliveryErrorDoesNotExposeToken(t *testing.T) {
	ctx := context.Background()
	st, svc := newNotifyTestStore(t)
	secret := "gotify-token-value"
	insertNotifyTestSink(t, ctx, st, svc, store.NotificationSinkInput{Type: store.NotificationSinkTypeGotify, Enabled: true, BaseURL: "https://gotify.example", Priority: 5, SendNeedsYou: true}, secret)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed for " + req.URL.String())
	})}
	deliverer := NewExternalSink(st, svc, ExternalSinkOptions{Client: client, MaxAttempts: 1})
	err := deliverer.Notify(ctx, testNotification(store.NotificationClassNeedsYou))
	if err == nil {
		t.Fatal("notify error = nil, want network failure")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("delivery error leaked token: %v", err)
	}
}

func newNotifyTestStore(t *testing.T) (*store.Store, *secrets.Service) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc, err := secrets.New(ctx, st, secrets.Config{})
	if err != nil {
		t.Fatalf("new secrets: %v", err)
	}
	if !svc.Available() {
		t.Fatalf("secrets unavailable: state=%s err=%v", svc.State(), svc.Err())
	}
	return st, svc
}

func insertNotifyTestSink(t *testing.T, ctx context.Context, st *store.Store, svc *secrets.Service, input store.NotificationSinkInput, secret string) store.NotificationSink {
	t.Helper()
	input.ID = ids.New("nsk")
	table, column, rowID := store.NotificationSinkSecretAD(input.ID)
	ciphertext, err := svc.Seal(ctx, []byte(secret), secrets.AssociatedData{Table: table, Column: column, RowID: rowID})
	if err != nil {
		t.Fatalf("seal sink secret: %v", err)
	}
	input.SecretCiphertext = ciphertext
	sink, err := st.InsertNotificationSink(ctx, input)
	if err != nil {
		t.Fatalf("insert sink: %v", err)
	}
	return sink
}

func testNotification(class string) store.Notification {
	title := "Finished"
	if class == store.NotificationClassNeedsYou {
		title = "Needs review"
	}
	return store.Notification{ID: "ntf_test", ProjectID: "default", RunID: "run_test", Class: class, Title: title, CreatedAt: "2026-06-25T00:00:00Z"}
}

func readRequestBody(t *testing.T, r *http.Request) []byte {
	t.Helper()
	body, err := ioReadAll(r)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return body
}

func ioReadAll(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }
