package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/shared/ids"
)

const (
	NotificationClassNeedsYou = "needs_you"
	NotificationClassFinished = "finished"
)

type ProjectNotificationPreferences struct {
	OnlyWhenNeeded bool
	WhenFinished   bool
}

type Notification struct {
	ID             string
	ProjectID      string
	RunID          string
	Class          string
	Title          string
	CreatedAt      string
	AcknowledgedAt string
}

type NotificationInput struct {
	ProjectID string
	RunID     string
	Class     string
	Title     string
}

func (s *Store) GetProjectNotificationPreferences(ctx context.Context, projectID string) (ProjectNotificationPreferences, error) {
	project, err := s.GetProject(ctx, normalizeProjectID(projectID))
	if err != nil {
		return ProjectNotificationPreferences{}, err
	}
	return ProjectNotificationPreferences{OnlyWhenNeeded: project.NotificationOnlyWhenNeeded, WhenFinished: project.NotificationWhenFinished}, nil
}

func (s *Store) UpdateProjectNotificationPreferences(ctx context.Context, projectID string, prefs ProjectNotificationPreferences) (Project, error) {
	projectID = normalizeProjectID(projectID)
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `UPDATE projects SET notification_only_when_needed = ?, notification_when_finished = ?, updated_at = ? WHERE id = ?`, boolInt(prefs.OnlyWhenNeeded), boolInt(prefs.WhenFinished), nowRFC3339(), projectID)
	if err != nil {
		return Project{}, fmt.Errorf("update notification preferences: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Project{}, fmt.Errorf("update notification preferences rows affected: %w", err)
	}
	if changed == 0 {
		return Project{}, fmt.Errorf("get project %s: %w", projectID, sql.ErrNoRows)
	}
	return s.GetProject(ctx, projectID)
}

func (s *Store) InsertNotification(ctx context.Context, input NotificationInput) (Notification, error) {
	input.ProjectID = normalizeProjectID(input.ProjectID)
	input.Class = strings.TrimSpace(input.Class)
	input.Title = strings.TrimSpace(input.Title)
	if input.Class == "" {
		return Notification{}, fmt.Errorf("notification class is required")
	}
	if input.Title == "" {
		return Notification{}, fmt.Errorf("notification title is required")
	}
	notification := Notification{
		ID:        ids.New("ntf"),
		ProjectID: input.ProjectID,
		RunID:     strings.TrimSpace(input.RunID),
		Class:     input.Class,
		Title:     input.Title,
		CreatedAt: nowRFC3339(),
	}
	runID := nullableStringParam(notification.RunID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO notifications(id, project_id, run_id, class, title, created_at, acknowledged_at) VALUES (?, ?, ?, ?, ?, ?, NULL)`, notification.ID, notification.ProjectID, runID, notification.Class, notification.Title, notification.CreatedAt); err != nil {
		return Notification{}, fmt.Errorf("insert notification: %w", err)
	}
	return notification, nil
}

func (s *Store) GetNotification(ctx context.Context, notificationID string) (Notification, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, project_id, run_id, class, title, created_at, acknowledged_at FROM notifications WHERE id = ?`, strings.TrimSpace(notificationID))
	notification, err := scanNotification(row)
	if err != nil {
		return Notification{}, fmt.Errorf("get notification %s: %w", notificationID, err)
	}
	return notification, nil
}

func (s *Store) ListNotifications(ctx context.Context, limit int) ([]Notification, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, run_id, class, title, created_at, acknowledged_at FROM notifications ORDER BY created_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	defer rows.Close()
	notifications := make([]Notification, 0)
	for rows.Next() {
		notification, err := scanNotification(rows)
		if err != nil {
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		notifications = append(notifications, notification)
	}
	return notifications, rows.Err()
}

func (s *Store) CountUnreadNotifications(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM notifications WHERE acknowledged_at IS NULL`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count unread notifications: %w", err)
	}
	return count, nil
}

func (s *Store) AcknowledgeNotification(ctx context.Context, notificationID string) (Notification, error) {
	notificationID = strings.TrimSpace(notificationID)
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `UPDATE notifications SET acknowledged_at = COALESCE(acknowledged_at, ?) WHERE id = ?`, nowRFC3339(), notificationID)
	if err != nil {
		return Notification{}, fmt.Errorf("acknowledge notification: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Notification{}, fmt.Errorf("acknowledge notification rows affected: %w", err)
	}
	if changed == 0 {
		return Notification{}, fmt.Errorf("get notification %s: %w", notificationID, sql.ErrNoRows)
	}
	return s.GetNotification(ctx, notificationID)
}

func (s *Store) AcknowledgeAllNotifications(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.ExecContext(ctx, `UPDATE notifications SET acknowledged_at = ? WHERE acknowledged_at IS NULL`, nowRFC3339()); err != nil {
		return fmt.Errorf("acknowledge all notifications: %w", err)
	}
	return nil
}

type notificationScanner interface {
	Scan(dest ...any) error
}

func scanNotification(scanner notificationScanner) (Notification, error) {
	var notification Notification
	var runID, acknowledgedAt sql.NullString
	if err := scanner.Scan(&notification.ID, &notification.ProjectID, &runID, &notification.Class, &notification.Title, &notification.CreatedAt, &acknowledgedAt); err != nil {
		return Notification{}, err
	}
	if runID.Valid {
		notification.RunID = runID.String
	}
	if acknowledgedAt.Valid {
		notification.AcknowledgedAt = acknowledgedAt.String
	}
	return notification, nil
}

func nullableStringParam(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}
