package app

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"net/url"
	"strings"
)

func newCSRFToken() string {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

func withHostAllowlist(next http.Handler, bindAddr string) http.Handler {
	allowed := allowedLocalHosts(bindAddr)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := allowed[normalizeHost(r.Host)]; !ok {
			http.Error(w, "host not allowed", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func allowedLocalHosts(bindAddr string) map[string]struct{} {
	_, port, err := net.SplitHostPort(bindAddr)
	if err != nil || port == "" {
		port = "7345"
	}
	allowed := map[string]struct{}{}
	for _, host := range []string{"127.0.0.1", "localhost", "::1"} {
		allowed[normalizeHost(net.JoinHostPort(host, port))] = struct{}{}
	}
	return allowed
}

func withRequestSafety(next http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isUnsafeMethod(r.Method) {
			next.ServeHTTP(w, r)
			return
		}
		if !sameOriginRequest(r) {
			http.Error(w, "cross-origin state-changing request rejected", http.StatusForbidden)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "invalid form", http.StatusBadRequest)
			return
		}
		if !validCSRFToken(r.FormValue("csrf_token"), token) {
			http.Error(w, "invalid csrf token", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isUnsafeMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func sameOriginRequest(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin != "" {
		return originHostMatches(origin, r.Host)
	}
	referer := strings.TrimSpace(r.Header.Get("Referer"))
	if referer != "" {
		return originHostMatches(referer, r.Host)
	}
	return true
}

func originHostMatches(rawURL, requestHost string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return false
	}
	return normalizeHost(parsed.Host) == normalizeHost(requestHost)
}

func normalizeHost(host string) string {
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "" {
		return ""
	}
	if strings.HasPrefix(host, "[") {
		if h, p, err := net.SplitHostPort(host); err == nil {
			return net.JoinHostPort(strings.ToLower(h), p)
		}
		return host
	}
	h, p, err := net.SplitHostPort(host)
	if err == nil {
		return net.JoinHostPort(strings.ToLower(h), p)
	}
	return host
}

func validCSRFToken(value, expected string) bool {
	if value == "" || expected == "" || len(value) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(value), []byte(expected)) == 1
}
