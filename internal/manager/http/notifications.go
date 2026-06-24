package managerhttp

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/shared/event"
)

const NotificationsTopic = "notifications"

type InAppNotificationSink struct {
	store    *store.Store
	hub      *Hub
	renderer web.Renderer
}

func NewInAppNotificationSink(st *store.Store, hub *Hub, renderer web.Renderer) *InAppNotificationSink {
	return &InAppNotificationSink{store: st, hub: hub, renderer: renderer}
}

func (s *InAppNotificationSink) Notify(ctx context.Context, notification store.Notification) error {
	data, err := notificationCenterData(ctx, s.store, "")
	if err != nil {
		return err
	}
	fragment, err := s.renderer.RenderNotificationCenter(data)
	if err != nil {
		return err
	}
	s.hub.Broadcast(NotificationsTopic, event.Event{
		SchemaVersion: event.SchemaVersion,
		ProjectID:     notification.ProjectID,
		RunID:         notification.RunID,
		Type:          "notification.created",
		Summary:       notification.Title,
		Sequence:      notificationSequence(notification.CreatedAt),
	}, fragment)
	return nil
}

func notificationCenterData(ctx context.Context, st *store.Store, csrf string) (web.NotificationCenterData, error) {
	notifications, err := st.ListNotifications(ctx, 20)
	if err != nil {
		return web.NotificationCenterData{}, err
	}
	unread, err := st.CountUnreadNotifications(ctx)
	if err != nil {
		return web.NotificationCenterData{}, err
	}
	return web.NewNotificationCenterData(notifications, unread, csrf), nil
}

func (s *Server) notificationCenterData(ctx context.Context, csrf string) (web.NotificationCenterData, error) {
	return notificationCenterData(ctx, s.store, csrf)
}

func (s *Server) renderNotificationCenter(ctx context.Context, csrf string) (string, error) {
	data, err := s.notificationCenterData(ctx, csrf)
	if err != nil {
		return "", err
	}
	return s.renderer.RenderNotificationCenter(data)
}

func (s *Server) handleNotificationEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.security.requireSession(r) {
		http.Error(w, "session required", http.StatusUnauthorized)
		return
	}
	fragment, err := s.renderNotificationCenter(r.Context(), csrfFromContext(r.Context()))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writer, ok := NewSSEWriter(w)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	writer.Patch(event.Event{Type: "notifications.snapshot", Sequence: parseLastEventID(r)}, fragment)

	ch, unsubscribe := s.hub.Subscribe(NotificationsTopic)
	defer unsubscribe()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			writer.Patch(msg.Event, msg.Fragment)
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) handleAcknowledgeAllNotifications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if err := s.store.AcknowledgeAllNotifications(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.broadcastNotifications(r.Context(), "notifications.ack_all"); err != nil {
		log.Printf("broadcast notifications after mark-all failed: %v", err)
	}
	fragment, err := s.renderNotificationCenter(r.Context(), csrfFromContext(r.Context()))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(fragment))
}

func (s *Server) handleNotificationPath(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/notifications/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) == 2 && parts[0] != "" && parts[1] == "ack" {
		s.handleAcknowledgeNotification(w, r, parts[0])
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleAcknowledgeNotification(w http.ResponseWriter, r *http.Request, notificationID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	notification, err := s.store.AcknowledgeNotification(r.Context(), notificationID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if err := s.broadcastNotifications(r.Context(), "notifications.ack"); err != nil {
		log.Printf("broadcast notifications after ack failed: %v", err)
	}
	http.Redirect(w, r, notificationRedirect(notification, r.Form.Get("redirect")), http.StatusSeeOther)
}

func (s *Server) broadcastNotifications(ctx context.Context, typ string) error {
	fragment, err := s.renderNotificationCenter(ctx, "")
	if err != nil {
		return err
	}
	s.hub.Broadcast(NotificationsTopic, event.Event{SchemaVersion: event.SchemaVersion, Type: typ, Summary: "notifications updated", Sequence: time.Now().UnixNano()}, fragment)
	return nil
}

func notificationRedirect(notification store.Notification, requested string) string {
	requested = strings.TrimSpace(requested)
	if strings.HasPrefix(requested, "/projects/") && !strings.HasPrefix(requested, "//") {
		return requested
	}
	if notification.RunID != "" {
		return fmt.Sprintf("/projects/%s/runs/%s", notification.ProjectID, notification.RunID)
	}
	return "/projects/" + notification.ProjectID
}

func notificationSequence(createdAt string) int64 {
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		return t.UnixNano()
	}
	return time.Now().UnixNano()
}
