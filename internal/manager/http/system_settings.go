package managerhttp

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/agent-parley/parley/internal/manager/notify"
	"github.com/agent-parley/parley/internal/manager/secrets"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/shared/ids"
)

const (
	externalSinkSecretUnavailable    = "Set PARLEY_SECRETS_KEK or restore the configured secrets key to enable external notification sinks. In-app notifications still work."
	forgeCredentialSecretUnavailable = "Set PARLEY_SECRETS_KEK or restore the configured secrets key to enable forge credentials. Auto-merge will stay disabled until credentials can be opened."
)

func (s *Server) handleSystemSettings(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/settings" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	data, err := s.systemSettingsData(r, "", "", "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writePage(w, "system_settings.html", data)
}

func (s *Server) handleSystemSettingsPath(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/settings/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	if parts[0] == "forge-credentials" {
		s.handleForgeCredentialPath(w, r, parts)
		return
	}
	if parts[0] == "agent-profiles" {
		if len(parts) == 1 {
			s.handleGlobalAgentProfileSave(w, r)
			return
		}
		if len(parts) == 2 {
			switch parts[1] {
			case "delete":
				s.handleGlobalAgentProfileDelete(w, r)
			case "clear-default":
				s.handleGlobalAgentProfileClearDefault(w, r)
			default:
				http.NotFound(w, r)
			}
			return
		}
		http.NotFound(w, r)
		return
	}
	if parts[0] != "notification-sinks" {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == store.NotificationSinkTypeGotify {
		s.handleCreateGotifyNotificationSink(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == store.NotificationSinkTypeWebhook {
		s.handleCreateWebhookNotificationSink(w, r)
		return
	}
	if len(parts) == 3 && parts[1] != "" {
		sinkID := parts[1]
		switch parts[2] {
		case "update":
			s.handleUpdateNotificationSink(w, r, sinkID)
		case "delete":
			s.handleDeleteNotificationSink(w, r, sinkID)
		case "regenerate-secret":
			s.handleRegenerateWebhookSecret(w, r, sinkID)
		default:
			http.NotFound(w, r)
		}
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleForgeCredentialPath(w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) == 1 {
		s.handleCreateForgeCredential(w, r)
		return
	}
	if len(parts) == 3 && parts[1] != "" && parts[2] == "delete" {
		s.handleDeleteForgeCredential(w, r, parts[1])
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleCreateForgeCredential(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.secretsAvailable() {
		s.writeForgeCredentialsFragment(w, r, http.StatusBadRequest, forgeCredentialSecretUnavailable, "")
		return
	}
	token := strings.TrimSpace(r.Form.Get("token"))
	if token == "" {
		s.writeForgeCredentialsFragment(w, r, http.StatusBadRequest, "Forge token is required.", "")
		return
	}
	credentialID := ids.New("fcr")
	ciphertext, err := s.sealForgeCredentialSecret(r, credentialID, []byte(token))
	if err != nil {
		s.writeForgeCredentialsFragment(w, r, http.StatusInternalServerError, "Could not seal forge token.", "")
		return
	}
	credential, err := s.store.InsertForgeCredential(r.Context(), store.ForgeCredentialInput{ID: credentialID, Host: r.Form.Get("host"), SecretCiphertext: ciphertext})
	if err != nil {
		s.writeForgeCredentialsFragment(w, r, http.StatusBadRequest, err.Error(), "")
		return
	}
	s.writeForgeCredentialsFragment(w, r, http.StatusAccepted, "Forge credential stored. Use `"+credential.ID+"` in workflow template merge settings.", "")
}

func (s *Server) handleDeleteForgeCredential(w http.ResponseWriter, r *http.Request, credentialID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if err := s.store.DeleteForgeCredential(r.Context(), credentialID); err != nil {
		s.writeForgeCredentialsFragment(w, r, http.StatusBadRequest, err.Error(), "")
		return
	}
	s.writeForgeCredentialsFragment(w, r, http.StatusAccepted, "Forge credential deleted.", "")
}

func (s *Server) handleCreateGotifyNotificationSink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.secretsAvailable() {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, externalSinkSecretUnavailable, "", "")
		return
	}
	baseURL := strings.TrimSpace(r.Form.Get("base_url"))
	allowHTTP := r.Form.Get("allow_insecure_http") != ""
	if err := notify.ValidateSinkURL(baseURL, allowHTTP); err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, err.Error(), "", "")
		return
	}
	appToken := strings.TrimSpace(r.Form.Get("app_token"))
	if appToken == "" {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, "Gotify app token is required.", "", "")
		return
	}
	priority, err := parseSinkPriority(r.Form.Get("priority"))
	if err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, err.Error(), "", "")
		return
	}
	sinkID := ids.New("nsk")
	ciphertext, err := s.sealNotificationSinkSecret(r, sinkID, []byte(appToken))
	if err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusInternalServerError, "Could not seal Gotify app token.", "", "")
		return
	}
	needsYou, finished := parseSinkClasses(r)
	_, err = s.store.InsertNotificationSink(r.Context(), store.NotificationSinkInput{
		ID:                sinkID,
		Type:              store.NotificationSinkTypeGotify,
		Enabled:           r.Form.Get("enabled") != "",
		BaseURL:           baseURL,
		Priority:          priority,
		SecretCiphertext:  ciphertext,
		AllowInsecureHTTP: allowHTTP,
		SendNeedsYou:      needsYou,
		SendFinished:      finished,
	})
	if err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, err.Error(), "", "")
		return
	}
	s.writeExternalSinksFragment(w, r, http.StatusAccepted, "", "Gotify sink created.", "")
}

func (s *Server) handleCreateWebhookNotificationSink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.secretsAvailable() {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, externalSinkSecretUnavailable, "", "")
		return
	}
	webhookURL := strings.TrimSpace(r.Form.Get("url"))
	allowHTTP := r.Form.Get("allow_insecure_http") != ""
	if err := notify.ValidateSinkURL(webhookURL, allowHTTP); err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, err.Error(), "", "")
		return
	}
	method := strings.ToUpper(strings.TrimSpace(r.Form.Get("http_method")))
	if method == "" {
		method = http.MethodPost
	}
	if method != http.MethodPost && method != http.MethodPut && method != http.MethodPatch {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, "Webhook method must be POST, PUT, or PATCH.", "", "")
		return
	}
	secret, err := notify.GenerateWebhookSigningSecret()
	if err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusInternalServerError, "Could not generate webhook signing secret.", "", "")
		return
	}
	sinkID := ids.New("nsk")
	ciphertext, err := s.sealNotificationSinkSecret(r, sinkID, []byte(secret))
	if err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusInternalServerError, "Could not seal webhook signing secret.", "", "")
		return
	}
	needsYou, finished := parseSinkClasses(r)
	_, err = s.store.InsertNotificationSink(r.Context(), store.NotificationSinkInput{
		ID:                sinkID,
		Type:              store.NotificationSinkTypeWebhook,
		Enabled:           r.Form.Get("enabled") != "",
		URL:               webhookURL,
		HTTPMethod:        method,
		SecretCiphertext:  ciphertext,
		AllowInsecureHTTP: allowHTTP,
		SendNeedsYou:      needsYou,
		SendFinished:      finished,
	})
	if err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, err.Error(), "", "")
		return
	}
	s.writeExternalSinksFragment(w, r, http.StatusAccepted, "", "Webhook sink created. Copy the signing secret now; it will not be shown again.", secret)
}

func (s *Server) handleUpdateNotificationSink(w http.ResponseWriter, r *http.Request, sinkID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	current, err := s.store.GetNotificationSink(r.Context(), sinkID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	allowHTTP := r.Form.Get("allow_insecure_http") != ""
	needsYou, finished := parseSinkClasses(r)
	update := store.NotificationSinkUpdate{
		Enabled:           r.Form.Get("enabled") != "",
		Priority:          current.Priority,
		AllowInsecureHTTP: allowHTTP,
		SendNeedsYou:      needsYou,
		SendFinished:      finished,
	}
	if current.Type == store.NotificationSinkTypeGotify {
		baseURL := strings.TrimSpace(r.Form.Get("base_url"))
		if err := notify.ValidateSinkURL(baseURL, allowHTTP); err != nil {
			s.writeExternalSinksFragment(w, r, http.StatusBadRequest, err.Error(), "", "")
			return
		}
		priority, err := parseSinkPriority(r.Form.Get("priority"))
		if err != nil {
			s.writeExternalSinksFragment(w, r, http.StatusBadRequest, err.Error(), "", "")
			return
		}
		update.BaseURL = baseURL
		update.Priority = priority
		newToken := strings.TrimSpace(r.Form.Get("app_token"))
		if newToken != "" {
			if !s.secretsAvailable() {
				s.writeExternalSinksFragment(w, r, http.StatusBadRequest, externalSinkSecretUnavailable, "", "")
				return
			}
			ciphertext, err := s.sealNotificationSinkSecret(r, current.ID, []byte(newToken))
			if err != nil {
				s.writeExternalSinksFragment(w, r, http.StatusInternalServerError, "Could not seal Gotify app token.", "", "")
				return
			}
			update.SecretCiphertext = ciphertext
			update.ReplaceSecret = true
		}
	} else {
		webhookURL := strings.TrimSpace(r.Form.Get("url"))
		if err := notify.ValidateSinkURL(webhookURL, allowHTTP); err != nil {
			s.writeExternalSinksFragment(w, r, http.StatusBadRequest, err.Error(), "", "")
			return
		}
		method := strings.ToUpper(strings.TrimSpace(r.Form.Get("http_method")))
		if method == "" {
			method = http.MethodPost
		}
		update.URL = webhookURL
		update.HTTPMethod = method
	}
	if _, err := s.store.UpdateNotificationSink(r.Context(), current.ID, update); err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, err.Error(), "", "")
		return
	}
	s.writeExternalSinksFragment(w, r, http.StatusAccepted, "", "Notification sink updated.", "")
}

func (s *Server) handleDeleteNotificationSink(w http.ResponseWriter, r *http.Request, sinkID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if err := s.store.DeleteNotificationSink(r.Context(), sinkID); err != nil {
		http.NotFound(w, r)
		return
	}
	s.writeExternalSinksFragment(w, r, http.StatusAccepted, "", "Notification sink deleted.", "")
}

func (s *Server) handleRegenerateWebhookSecret(w http.ResponseWriter, r *http.Request, sinkID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.secretsAvailable() {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, externalSinkSecretUnavailable, "", "")
		return
	}
	current, err := s.store.GetNotificationSink(r.Context(), sinkID)
	if err != nil || current.Type != store.NotificationSinkTypeWebhook {
		http.NotFound(w, r)
		return
	}
	secret, err := notify.GenerateWebhookSigningSecret()
	if err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusInternalServerError, "Could not generate webhook signing secret.", "", "")
		return
	}
	ciphertext, err := s.sealNotificationSinkSecret(r, current.ID, []byte(secret))
	if err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusInternalServerError, "Could not seal webhook signing secret.", "", "")
		return
	}
	if _, err := s.store.UpdateNotificationSink(r.Context(), current.ID, store.NotificationSinkUpdate{
		Enabled:           current.Enabled,
		URL:               current.URL,
		HTTPMethod:        current.HTTPMethod,
		Priority:          current.Priority,
		AllowInsecureHTTP: current.AllowInsecureHTTP,
		SendNeedsYou:      current.SendNeedsYou,
		SendFinished:      current.SendFinished,
		SecretCiphertext:  ciphertext,
		ReplaceSecret:     true,
	}); err != nil {
		s.writeExternalSinksFragment(w, r, http.StatusBadRequest, err.Error(), "", "")
		return
	}
	s.writeExternalSinksFragment(w, r, http.StatusAccepted, "", "Webhook signing secret regenerated. Copy it now; it will not be shown again.", secret)
}

func (s *Server) systemSettingsData(r *http.Request, notice, status, oneTimeSecret string) (web.SystemSettingsData, error) {
	csrf := csrfFromContext(r.Context())
	sinks, err := s.externalNotificationSinksData(r, notice, status, oneTimeSecret)
	if err != nil {
		return web.SystemSettingsData{}, err
	}
	forgeCredentials, err := s.forgeCredentialsData(r, "", "")
	if err != nil {
		return web.SystemSettingsData{}, err
	}
	agentProfiles, err := s.globalAgentProfileEditorData(r, "", "")
	if err != nil {
		return web.SystemSettingsData{}, err
	}
	center, err := s.notificationCenterData(r.Context(), csrf)
	if err != nil {
		return web.SystemSettingsData{}, err
	}
	return web.SystemSettingsData{Sinks: sinks, ForgeCredentials: forgeCredentials, AgentProfiles: agentProfiles, Center: center, CSRF: csrf, Title: "Parley · System Settings"}, nil
}

func (s *Server) externalNotificationSinksData(r *http.Request, notice, status, oneTimeSecret string) (web.ExternalNotificationSinksData, error) {
	sinks, err := s.store.ListNotificationSinks(r.Context())
	if err != nil {
		return web.ExternalNotificationSinksData{}, err
	}
	views := make([]web.NotificationSinkData, 0, len(sinks))
	for _, sink := range sinks {
		views = append(views, newNotificationSinkData(sink))
	}
	secretsAvailable := s.secretsAvailable()
	message := ""
	if !secretsAvailable {
		message = externalSinkSecretUnavailable
	}
	return web.ExternalNotificationSinksData{
		Sinks:                views,
		SecretsAvailable:     secretsAvailable,
		SecretsMessage:       message,
		Notice:               notice,
		Status:               status,
		OneTimeWebhookSecret: oneTimeSecret,
		CreateGotifyPath:     "/settings/notification-sinks/gotify",
		CreateWebhookPath:    "/settings/notification-sinks/webhook",
		CSRF:                 csrfFromContext(r.Context()),
	}, nil
}

func (s *Server) forgeCredentialsData(r *http.Request, notice, status string) (web.ForgeCredentialsData, error) {
	credentials, err := s.store.ListForgeCredentials(r.Context())
	if err != nil {
		return web.ForgeCredentialsData{}, err
	}
	views := make([]web.ForgeCredentialData, 0, len(credentials))
	for _, credential := range credentials {
		views = append(views, web.ForgeCredentialData{
			ID:               credential.ID,
			Host:             credential.Host,
			SecretConfigured: len(credential.SecretCiphertext) > 0,
			DeletePath:       "/settings/forge-credentials/" + credential.ID + "/delete",
			UpdatedAt:        credential.UpdatedAt,
		})
	}
	secretsAvailable := s.secretsAvailable()
	message := ""
	if !secretsAvailable {
		message = forgeCredentialSecretUnavailable
	}
	return web.ForgeCredentialsData{
		Credentials:      views,
		SecretsAvailable: secretsAvailable,
		SecretsMessage:   message,
		Notice:           notice,
		Status:           status,
		CreatePath:       "/settings/forge-credentials",
		CSRF:             csrfFromContext(r.Context()),
	}, nil
}

func newNotificationSinkData(sink store.NotificationSink) web.NotificationSinkData {
	typeLabel := "Gotify"
	if sink.Type == store.NotificationSinkTypeWebhook {
		typeLabel = "Webhook"
	}
	return web.NotificationSinkData{
		ID:                sink.ID,
		Type:              sink.Type,
		TypeLabel:         typeLabel,
		Enabled:           sink.Enabled,
		BaseURL:           sink.BaseURL,
		URL:               sink.URL,
		HTTPMethod:        sink.HTTPMethod,
		Priority:          sink.Priority,
		AllowInsecureHTTP: sink.AllowInsecureHTTP,
		SendNeedsYou:      sink.SendNeedsYou,
		SendFinished:      sink.SendFinished,
		SecretConfigured:  len(sink.SecretCiphertext) > 0,
		UpdatePath:        "/settings/notification-sinks/" + sink.ID + "/update",
		DeletePath:        "/settings/notification-sinks/" + sink.ID + "/delete",
		RegeneratePath:    "/settings/notification-sinks/" + sink.ID + "/regenerate-secret",
		UpdatedAt:         sink.UpdatedAt,
	}
}

func (s *Server) sealNotificationSinkSecret(r *http.Request, sinkID string, plaintext []byte) ([]byte, error) {
	table, column, rowID := store.NotificationSinkSecretAD(sinkID)
	return s.secrets.Seal(r.Context(), plaintext, secrets.AssociatedData{Table: table, Column: column, RowID: rowID})
}

func (s *Server) sealForgeCredentialSecret(r *http.Request, credentialID string, plaintext []byte) ([]byte, error) {
	table, column, rowID := store.ForgeCredentialSecretAD(credentialID)
	return s.secrets.Seal(r.Context(), plaintext, secrets.AssociatedData{Table: table, Column: column, RowID: rowID})
}

func (s *Server) secretsAvailable() bool {
	return s.secrets != nil && s.secrets.Available()
}

func (s *Server) writeExternalSinksFragment(w http.ResponseWriter, r *http.Request, statusCode int, notice, status, oneTimeSecret string) {
	data, err := s.externalNotificationSinksData(r, notice, status, oneTimeSecret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fragment, err := s.renderer.ExecutePage("external_notification_sinks.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(fragment)))
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(fragment))
}

func (s *Server) writeForgeCredentialsFragment(w http.ResponseWriter, r *http.Request, statusCode int, notice, status string) {
	data, err := s.forgeCredentialsData(r, notice, status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fragment, err := s.renderer.ExecutePage("forge_credentials.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(fragment)))
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(fragment))
}

func parseSinkPriority(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 5, nil
	}
	priority, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("Gotify priority must be a number.")
	}
	return priority, nil
}

func parseSinkClasses(r *http.Request) (bool, bool) {
	return r.Form.Get("send_needs_you") != "", r.Form.Get("send_finished") != ""
}
