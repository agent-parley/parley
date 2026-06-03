package managerhttp

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/agent-parley/parley/internal/shared/event"
)

type PatchMessage struct {
	Event    event.Event
	Fragment string
}

type Hub struct {
	mu   sync.RWMutex
	subs map[string]map[chan PatchMessage]struct{}
}

func NewHub() *Hub { return &Hub{subs: map[string]map[chan PatchMessage]struct{}{}} }

func (h *Hub) Subscribe(runID string) (chan PatchMessage, func()) {
	ch := make(chan PatchMessage, 16)
	h.mu.Lock()
	if h.subs[runID] == nil {
		h.subs[runID] = map[chan PatchMessage]struct{}{}
	}
	h.subs[runID][ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs[runID], ch)
		if len(h.subs[runID]) == 0 {
			delete(h.subs, runID)
		}
		h.mu.Unlock()
		close(ch)
	}
}

func (h *Hub) Broadcast(runID string, ev event.Event, fragment string) {
	h.mu.RLock()
	subs := make([]chan PatchMessage, 0, len(h.subs[runID]))
	for ch := range h.subs[runID] {
		subs = append(subs, ch)
	}
	h.mu.RUnlock()
	msg := PatchMessage{Event: ev, Fragment: fragment}
	for _, ch := range subs {
		select {
		case ch <- msg:
		default:
		}
	}
}

type SSEWriter struct {
	w http.ResponseWriter
	f http.Flusher
}

func NewSSEWriter(w http.ResponseWriter) (SSEWriter, bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return SSEWriter{}, false
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	return SSEWriter{w: w, f: flusher}, true
}

func (s SSEWriter) Patch(ev event.Event, fragment string) {
	fragment = strings.ReplaceAll(fragment, "\r", "")
	fragment = strings.ReplaceAll(fragment, "\n", "")
	_, _ = fmt.Fprintf(s.w, "id: %d\n", ev.Sequence)
	_, _ = fmt.Fprint(s.w, "event: datastar-patch-elements\n")
	_, _ = fmt.Fprintf(s.w, "data: elements %s\n\n", fragment)
	s.f.Flush()
}

func parseLastEventID(r *http.Request) int64 {
	value := r.Header.Get("Last-Event-ID")
	if value == "" {
		value = r.URL.Query().Get("last_event_id")
	}
	if value == "" {
		return 0
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
