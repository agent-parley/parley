package managerhttp

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net"
	"net/http"
	"sync"
)

type contextKey string

const csrfContextKey contextKey = "csrf"

type sessionState struct {
	Token string
	CSRF  string
}

type security struct {
	addr     string
	mu       sync.RWMutex
	sessions map[string]sessionState
}

func newSecurity(addr string) *security {
	return &security{addr: addr, sessions: map[string]sessionState{}}
}

func (s *security) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.allowedHost(r.Host) {
			http.Error(w, "bad host", http.StatusForbidden)
			return
		}
		if !s.allowedOrigin(r) {
			http.Error(w, "bad origin", http.StatusForbidden)
			return
		}
		state := s.ensureSession(w, r)
		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil {
				http.Error(w, "bad form", http.StatusBadRequest)
				return
			}
			if r.Form.Get("_csrf") == "" || r.Form.Get("_csrf") != state.CSRF {
				http.Error(w, "csrf", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), csrfContextKey, state.CSRF)))
	})
}

func (s *security) requireSession(r *http.Request) bool {
	cookie, err := r.Cookie("parley_session")
	if err != nil || cookie.Value == "" {
		return false
	}
	s.mu.RLock()
	_, ok := s.sessions[cookie.Value]
	s.mu.RUnlock()
	return ok
}

func (s *security) ensureSession(w http.ResponseWriter, r *http.Request) sessionState {
	if cookie, err := r.Cookie("parley_session"); err == nil && cookie.Value != "" {
		s.mu.RLock()
		state, ok := s.sessions[cookie.Value]
		s.mu.RUnlock()
		if ok {
			return state
		}
	}
	state := sessionState{Token: randomToken(), CSRF: randomToken()}
	s.mu.Lock()
	s.sessions[state.Token] = state
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "parley_session", Value: state.Token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
	return state
}

func csrfFromContext(ctx context.Context) string {
	csrf, _ := ctx.Value(csrfContextKey).(string)
	return csrf
}

func (s *security) allowedHost(host string) bool {
	for _, allowed := range s.allowedHosts() {
		if host == allowed {
			return true
		}
	}
	return false
}

func (s *security) allowedOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	for _, host := range s.allowedHosts() {
		if origin == "http://"+host {
			return true
		}
	}
	return false
}

func (s *security) allowedHosts() []string {
	host, port, err := net.SplitHostPort(s.addr)
	if err != nil {
		return []string{s.addr}
	}
	if host != "127.0.0.1" && host != "localhost" {
		return []string{s.addr}
	}
	return []string{net.JoinHostPort("127.0.0.1", port), net.JoinHostPort("localhost", port)}
}

func randomToken() string {
	buf := make([]byte, 32)
	_, _ = rand.Read(buf)
	return base64.RawURLEncoding.EncodeToString(buf)
}
