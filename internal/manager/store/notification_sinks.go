package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/shared/ids"
)

const (
	NotificationSinkTypeGotify  = "gotify"
	NotificationSinkTypeWebhook = "webhook"
)

const notificationSinksTable = "notification_sinks"

// NotificationSink is a system-wide external notification destination. Project
// notification preferences decide WHAT gets persisted; these rows decide WHERE
// qualifying notifications are delivered.
type NotificationSink struct {
	ID                string
	Type              string
	Enabled           bool
	BaseURL           string
	URL               string
	HTTPMethod        string
	Priority          int
	SecretCiphertext  []byte
	AllowInsecureHTTP bool
	SendNeedsYou      bool
	SendFinished      bool
	CreatedAt         string
	UpdatedAt         string
}

type NotificationSinkInput struct {
	ID                string
	Type              string
	Enabled           bool
	BaseURL           string
	URL               string
	HTTPMethod        string
	Priority          int
	SecretCiphertext  []byte
	AllowInsecureHTTP bool
	SendNeedsYou      bool
	SendFinished      bool
}

type NotificationSinkUpdate struct {
	Enabled           bool
	BaseURL           string
	URL               string
	HTTPMethod        string
	Priority          int
	AllowInsecureHTTP bool
	SendNeedsYou      bool
	SendFinished      bool
	SecretCiphertext  []byte
	ReplaceSecret     bool
}

func NotificationSinkSecretAD(rowID string) (table, column, id string) {
	return notificationSinksTable, "secret_ciphertext", strings.TrimSpace(rowID)
}

func (s *Store) InsertNotificationSink(ctx context.Context, input NotificationSinkInput) (NotificationSink, error) {
	input = normalizeNotificationSinkInput(input)
	if err := validateNotificationSinkInput(input, true); err != nil {
		return NotificationSink{}, err
	}
	sinkID := strings.TrimSpace(input.ID)
	if sinkID == "" {
		sinkID = ids.New("nsk")
	}
	sink := NotificationSink{
		ID:                sinkID,
		Type:              input.Type,
		Enabled:           input.Enabled,
		BaseURL:           input.BaseURL,
		URL:               input.URL,
		HTTPMethod:        input.HTTPMethod,
		Priority:          input.Priority,
		SecretCiphertext:  append([]byte(nil), input.SecretCiphertext...),
		AllowInsecureHTTP: input.AllowInsecureHTTP,
		SendNeedsYou:      input.SendNeedsYou,
		SendFinished:      input.SendFinished,
		CreatedAt:         nowRFC3339(),
	}
	sink.UpdatedAt = sink.CreatedAt
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `INSERT INTO notification_sinks(id, type, enabled, base_url, url, http_method, priority, secret_ciphertext, allow_insecure_http, send_needs_you, send_finished, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, sink.ID, sink.Type, boolInt(sink.Enabled), sink.BaseURL, sink.URL, sink.HTTPMethod, sink.Priority, sink.SecretCiphertext, boolInt(sink.AllowInsecureHTTP), boolInt(sink.SendNeedsYou), boolInt(sink.SendFinished), sink.CreatedAt, sink.UpdatedAt)
	if err != nil {
		return NotificationSink{}, fmt.Errorf("insert notification sink: %w", err)
	}
	return sink, nil
}

func (s *Store) UpdateNotificationSink(ctx context.Context, sinkID string, update NotificationSinkUpdate) (NotificationSink, error) {
	sinkID = strings.TrimSpace(sinkID)
	current, err := s.GetNotificationSink(ctx, sinkID)
	if err != nil {
		return NotificationSink{}, err
	}
	input := normalizeNotificationSinkInput(NotificationSinkInput{
		Type:              current.Type,
		Enabled:           update.Enabled,
		BaseURL:           update.BaseURL,
		URL:               update.URL,
		HTTPMethod:        update.HTTPMethod,
		Priority:          update.Priority,
		SecretCiphertext:  current.SecretCiphertext,
		AllowInsecureHTTP: update.AllowInsecureHTTP,
		SendNeedsYou:      update.SendNeedsYou,
		SendFinished:      update.SendFinished,
	})
	if update.ReplaceSecret {
		input.SecretCiphertext = append([]byte(nil), update.SecretCiphertext...)
	}
	if err := validateNotificationSinkInput(input, true); err != nil {
		return NotificationSink{}, err
	}
	now := nowRFC3339()
	s.mu.Lock()
	defer s.mu.Unlock()
	var result sql.Result
	if update.ReplaceSecret {
		result, err = s.db.ExecContext(ctx, `UPDATE notification_sinks SET enabled = ?, base_url = ?, url = ?, http_method = ?, priority = ?, secret_ciphertext = ?, allow_insecure_http = ?, send_needs_you = ?, send_finished = ?, updated_at = ? WHERE id = ?`, boolInt(input.Enabled), input.BaseURL, input.URL, input.HTTPMethod, input.Priority, input.SecretCiphertext, boolInt(input.AllowInsecureHTTP), boolInt(input.SendNeedsYou), boolInt(input.SendFinished), now, sinkID)
	} else {
		result, err = s.db.ExecContext(ctx, `UPDATE notification_sinks SET enabled = ?, base_url = ?, url = ?, http_method = ?, priority = ?, allow_insecure_http = ?, send_needs_you = ?, send_finished = ?, updated_at = ? WHERE id = ?`, boolInt(input.Enabled), input.BaseURL, input.URL, input.HTTPMethod, input.Priority, boolInt(input.AllowInsecureHTTP), boolInt(input.SendNeedsYou), boolInt(input.SendFinished), now, sinkID)
	}
	if err != nil {
		return NotificationSink{}, fmt.Errorf("update notification sink: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return NotificationSink{}, fmt.Errorf("update notification sink rows affected: %w", err)
	}
	if changed == 0 {
		return NotificationSink{}, fmt.Errorf("get notification sink %s: %w", sinkID, sql.ErrNoRows)
	}
	return s.GetNotificationSink(ctx, sinkID)
}

func (s *Store) DeleteNotificationSink(ctx context.Context, sinkID string) error {
	sinkID = strings.TrimSpace(sinkID)
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `DELETE FROM notification_sinks WHERE id = ?`, sinkID)
	if err != nil {
		return fmt.Errorf("delete notification sink: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete notification sink rows affected: %w", err)
	}
	if changed == 0 {
		return fmt.Errorf("get notification sink %s: %w", sinkID, sql.ErrNoRows)
	}
	return nil
}

func (s *Store) GetNotificationSink(ctx context.Context, sinkID string) (NotificationSink, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, type, enabled, base_url, url, http_method, priority, secret_ciphertext, allow_insecure_http, send_needs_you, send_finished, created_at, updated_at FROM notification_sinks WHERE id = ?`, strings.TrimSpace(sinkID))
	sink, err := scanNotificationSink(row)
	if err != nil {
		return NotificationSink{}, fmt.Errorf("get notification sink %s: %w", sinkID, err)
	}
	return sink, nil
}

func (s *Store) ListNotificationSinks(ctx context.Context) ([]NotificationSink, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, type, enabled, base_url, url, http_method, priority, secret_ciphertext, allow_insecure_http, send_needs_you, send_finished, created_at, updated_at FROM notification_sinks ORDER BY created_at ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list notification sinks: %w", err)
	}
	defer rows.Close()
	return collectNotificationSinks(rows)
}

func (s *Store) ListEnabledNotificationSinksForClass(ctx context.Context, class string) ([]NotificationSink, error) {
	class = strings.TrimSpace(class)
	column := ""
	switch class {
	case NotificationClassNeedsYou:
		column = "send_needs_you"
	case NotificationClassFinished:
		column = "send_finished"
	default:
		return []NotificationSink{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, type, enabled, base_url, url, http_method, priority, secret_ciphertext, allow_insecure_http, send_needs_you, send_finished, created_at, updated_at FROM notification_sinks WHERE enabled = 1 AND `+column+` = 1 ORDER BY created_at ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list enabled notification sinks: %w", err)
	}
	defer rows.Close()
	return collectNotificationSinks(rows)
}

type notificationSinkRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func collectNotificationSinks(rows notificationSinkRows) ([]NotificationSink, error) {
	sinks := make([]NotificationSink, 0)
	for rows.Next() {
		sink, err := scanNotificationSink(rows)
		if err != nil {
			return nil, fmt.Errorf("scan notification sink: %w", err)
		}
		sinks = append(sinks, sink)
	}
	return sinks, rows.Err()
}

type notificationSinkScanner interface {
	Scan(dest ...any) error
}

func scanNotificationSink(scanner notificationSinkScanner) (NotificationSink, error) {
	var sink NotificationSink
	var enabled, allowHTTP, needsYou, finished int
	if err := scanner.Scan(&sink.ID, &sink.Type, &enabled, &sink.BaseURL, &sink.URL, &sink.HTTPMethod, &sink.Priority, &sink.SecretCiphertext, &allowHTTP, &needsYou, &finished, &sink.CreatedAt, &sink.UpdatedAt); err != nil {
		return NotificationSink{}, err
	}
	sink.Enabled = enabled != 0
	sink.AllowInsecureHTTP = allowHTTP != 0
	sink.SendNeedsYou = needsYou != 0
	sink.SendFinished = finished != 0
	return sink, nil
}

func normalizeNotificationSinkInput(input NotificationSinkInput) NotificationSinkInput {
	input.ID = strings.TrimSpace(input.ID)
	input.Type = strings.ToLower(strings.TrimSpace(input.Type))
	input.BaseURL = strings.TrimSpace(input.BaseURL)
	input.URL = strings.TrimSpace(input.URL)
	input.HTTPMethod = strings.ToUpper(strings.TrimSpace(input.HTTPMethod))
	if input.HTTPMethod == "" {
		input.HTTPMethod = "POST"
	}
	if input.Priority == 0 {
		input.Priority = 5
	}
	return input
}

func validateNotificationSinkInput(input NotificationSinkInput, requireSecret bool) error {
	switch input.Type {
	case NotificationSinkTypeGotify:
		if input.BaseURL == "" {
			return fmt.Errorf("gotify base URL is required")
		}
	case NotificationSinkTypeWebhook:
		if input.URL == "" {
			return fmt.Errorf("webhook URL is required")
		}
		if !validWebhookMethod(input.HTTPMethod) {
			return fmt.Errorf("webhook method must be POST, PUT, or PATCH")
		}
	default:
		return fmt.Errorf("notification sink type must be gotify or webhook")
	}
	if requireSecret && len(input.SecretCiphertext) == 0 {
		return fmt.Errorf("notification sink secret is required")
	}
	return nil
}

func validWebhookMethod(method string) bool {
	switch method {
	case "POST", "PUT", "PATCH":
		return true
	default:
		return false
	}
}
