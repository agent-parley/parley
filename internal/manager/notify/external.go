package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/manager/secrets"
	"github.com/agent-parley/parley/internal/manager/store"
)

const (
	DefaultDeliveryTimeout = 10 * time.Second
	DefaultMaxAttempts     = 3
	DefaultRetryBackoff    = 250 * time.Millisecond
)

type ExternalSinkOptions struct {
	Client          *http.Client
	DeliveryTimeout time.Duration
	MaxAttempts     int
	RetryBackoff    time.Duration
	BaseURL         string
	Now             func() time.Time
}

type ExternalSink struct {
	store    *store.Store
	secrets  *secrets.Service
	client   *http.Client
	timeout  time.Duration
	attempts int
	backoff  time.Duration
	baseURL  string
	now      func() time.Time
}

func NewExternalSink(st *store.Store, secretService *secrets.Service, opts ExternalSinkOptions) *ExternalSink {
	client := notificationHTTPClient(opts.Client)
	timeout := opts.DeliveryTimeout
	if timeout <= 0 {
		timeout = DefaultDeliveryTimeout
	}
	attempts := opts.MaxAttempts
	if attempts <= 0 {
		attempts = DefaultMaxAttempts
	}
	backoff := opts.RetryBackoff
	if backoff <= 0 {
		backoff = DefaultRetryBackoff
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &ExternalSink{store: st, secrets: secretService, client: client, timeout: timeout, attempts: attempts, backoff: backoff, baseURL: strings.TrimRight(strings.TrimSpace(opts.BaseURL), "/"), now: now}
}

func notificationHTTPClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	cloned := *client
	cloned.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &cloned
}

func (s *ExternalSink) Notify(ctx context.Context, notification store.Notification) error {
	if s == nil || s.store == nil {
		return nil
	}
	sinks, err := s.store.ListEnabledNotificationSinksForClass(ctx, notification.Class)
	if err != nil {
		return err
	}
	if len(sinks) == 0 {
		return nil
	}
	if s.secrets == nil || !s.secrets.Available() {
		return secrets.ErrUnavailable
	}
	var errs []error
	for _, sink := range sinks {
		if err := s.deliverWithRetry(ctx, sink, notification); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *ExternalSink) deliverWithRetry(ctx context.Context, sink store.NotificationSink, notification store.Notification) error {
	var last error
	for attempt := 1; attempt <= s.attempts; attempt++ {
		err, retryable := s.deliverOnce(ctx, sink, notification)
		if err == nil {
			return nil
		}
		last = err
		if !retryable || attempt == s.attempts {
			return last
		}
		timer := time.NewTimer(s.backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
	return last
}

func (s *ExternalSink) deliverOnce(ctx context.Context, sink store.NotificationSink, notification store.Notification) (error, bool) {
	deliveryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var req *http.Request
	var err error
	switch sink.Type {
	case store.NotificationSinkTypeGotify:
		req, err = s.gotifyRequest(deliveryCtx, sink, notification)
	case store.NotificationSinkTypeWebhook:
		req, err = s.webhookRequest(deliveryCtx, sink, notification)
	default:
		return fmt.Errorf("notification sink %s has unsupported type %q", sink.ID, sink.Type), false
	}
	if err != nil {
		return err, false
	}
	resp, err := s.client.Do(req)
	if err != nil {
		if deliveryCtx.Err() != nil {
			return fmt.Errorf("%s notification delivery timed out: %w", sink.Type, deliveryCtx.Err()), true
		}
		return fmt.Errorf("%s notification delivery failed", sink.Type), true
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil, false
	}
	if resp.StatusCode >= 500 {
		return fmt.Errorf("%s notification delivery retryable status %d", sink.Type, resp.StatusCode), true
	}
	return fmt.Errorf("%s notification delivery terminal status %d", sink.Type, resp.StatusCode), false
}

func (s *ExternalSink) gotifyRequest(ctx context.Context, sink store.NotificationSink, notification store.Notification) (*http.Request, error) {
	if err := ValidateSinkURL(sink.BaseURL, sink.AllowInsecureHTTP); err != nil {
		return nil, err
	}
	token, err := s.openSinkSecret(ctx, sink)
	if err != nil {
		return nil, err
	}
	endpoint, err := gotifyMessageEndpoint(sink.BaseURL, string(token))
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(gotifyMessage{Title: notification.Title, Message: notificationBody(notification, s.deepLink(notification)), Priority: sink.Priority})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func (s *ExternalSink) webhookRequest(ctx context.Context, sink store.NotificationSink, notification store.Notification) (*http.Request, error) {
	if err := ValidateSinkURL(sink.URL, sink.AllowInsecureHTTP); err != nil {
		return nil, err
	}
	secret, err := s.openSinkSecret(ctx, sink)
	if err != nil {
		return nil, err
	}
	payload := WebhookPayload{
		SchemaVersion: 1,
		ID:            notification.ID,
		Class:         notification.Class,
		Title:         notification.Title,
		ProjectID:     notification.ProjectID,
		RunID:         notification.RunID,
		CreatedAt:     notification.CreatedAt,
		URL:           s.deepLink(notification),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	timestamp := strconv.FormatInt(s.now().UTC().Unix(), 10)
	signature := WebhookSignature(secret, timestamp, body)
	method := strings.ToUpper(strings.TrimSpace(sink.HTTPMethod))
	if method == "" {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, sink.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Parley-Timestamp", timestamp)
	req.Header.Set("X-Parley-Signature", signature)
	return req, nil
}

func (s *ExternalSink) openSinkSecret(ctx context.Context, sink store.NotificationSink) ([]byte, error) {
	table, column, rowID := store.NotificationSinkSecretAD(sink.ID)
	plaintext, err := s.secrets.Open(ctx, sink.SecretCiphertext, secrets.AssociatedData{Table: table, Column: column, RowID: rowID})
	if err != nil {
		return nil, fmt.Errorf("open %s notification sink secret: %w", sink.Type, err)
	}
	return plaintext, nil
}

func (s *ExternalSink) deepLink(notification store.Notification) string {
	linkPath := "/projects/" + url.PathEscape(notification.ProjectID)
	if notification.RunID != "" {
		linkPath += "/runs/" + url.PathEscape(notification.RunID)
	}
	if s.baseURL == "" {
		return linkPath
	}
	base, err := url.Parse(s.baseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return linkPath
	}
	base.Path = strings.TrimRight(base.Path, "/") + linkPath
	base.RawQuery = ""
	base.Fragment = ""
	return base.String()
}

func ValidateSinkURL(raw string, allowInsecureHTTP bool) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("notification sink URL must be an absolute URL")
	}
	switch parsed.Scheme {
	case "https":
		return nil
	case "http":
		if allowInsecureHTTP {
			return nil
		}
		return fmt.Errorf("plain http notification sinks require allow_insecure_http")
	default:
		return fmt.Errorf("notification sink URL scheme must be https")
	}
}

func GenerateWebhookSigningSecret() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate webhook signing secret: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func WebhookSignature(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func gotifyMessageEndpoint(baseURL, token string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", err
	}
	parsed.Path = path.Join(parsed.Path, "message")
	query := parsed.Query()
	query.Set("token", token)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func notificationBody(notification store.Notification, link string) string {
	if notification.RunID == "" {
		return fmt.Sprintf("%s\nProject: %s", notification.Title, notification.ProjectID)
	}
	return fmt.Sprintf("%s\nProject: %s\nRun: %s\n%s", notification.Title, notification.ProjectID, notification.RunID, link)
}

type gotifyMessage struct {
	Title    string `json:"title"`
	Message  string `json:"message"`
	Priority int    `json:"priority"`
}

type WebhookPayload struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Class         string `json:"class"`
	Title         string `json:"title"`
	ProjectID     string `json:"project_id"`
	RunID         string `json:"run_id"`
	CreatedAt     string `json:"created_at"`
	URL           string `json:"url"`
}
