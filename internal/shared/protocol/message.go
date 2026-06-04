package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

// protocolDebug surfaces the cause of every session close to stderr when
// PARLEY_PROTOCOL_DEBUG is set. The close sites (reader read/handler error,
// writer write error) otherwise discard the underlying error, which makes a
// dropped session ("protocol session closed") impossible to diagnose. The
// message type in each log line identifies the side: only a runner handles
// "dispatch"/"hello"/"ping"/"cancel"; only a manager handles
// "event"/"artifact"/"report"/"result"/"ready"/"pong". Diagnostic only —
// remove once the live loop is stable.
var protocolDebug = os.Getenv("PARLEY_PROTOCOL_DEBUG") != ""

func debugf(format string, args ...any) {
	if protocolDebug {
		log.Printf("[protocol] "+format, args...)
	}
}

const (
	TypeHello    = "hello"
	TypeDispatch = "dispatch"
	TypeCancel   = "cancel"
	TypePing     = "ping"

	TypeReady    = "ready"
	TypeEvent    = "event"
	TypeArtifact = "artifact"
	TypeReport   = "report"
	TypeResult   = "result"
	TypeLog      = "log"
	TypePong     = "pong"
)

var ErrSessionClosed = errors.New("protocol session closed")

// MaxMessageBytes bounds a single protocol message. coder/websocket defaults
// the read limit to 32 KiB, which is far too small for this channel: artifacts
// (diffs, validation logs, reports) are carried inline as base64 JSON (see
// ArtifactPayload), so any non-trivial diff trips the limit and drops the
// session with StatusMessageTooBig. 64 MiB is a generous bound for the
// skeleton; chunked artifact transfer is the documented later refinement for
// anything larger.
const MaxMessageBytes = 64 << 20

// Message is the symmetric Manager<->Runner JSON envelope.
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type HelloPayload struct {
	RunnerID string `json:"runner_id"`
}

type ReadyPayload struct {
	RunnerID     string       `json:"runner_id"`
	Capabilities Capabilities `json:"capabilities"`
}

type Capabilities struct {
	Adapters []string `json:"adapters"`
}

type DispatchPayload = contract.Dispatch

type CancelPayload struct {
	RunID     string `json:"run_id"`
	TaskID    string `json:"task_id"`
	AttemptID string `json:"attempt_id,omitempty"`
}

type EventPayload = event.Event

// ArtifactPayload transfers a durable artifact from Runner to Manager over the
// session. Content is carried inline (base64 in JSON) for the skeleton; chunked
// transfer is a later refinement behind this same message type.
type ArtifactPayload struct {
	RunID      string `json:"run_id"`
	TaskID     string `json:"task_id"`
	AttemptID  string `json:"attempt_id"`
	ArtifactID string `json:"artifact_id"`
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	MediaType  string `json:"media_type"`
	Content    []byte `json:"content"`
}

type ReportPayload = report.Report

type ResultPayload struct {
	RunID          string `json:"run_id"`
	TaskID         string `json:"task_id"`
	AttemptID      string `json:"attempt_id"`
	TerminalStatus string `json:"terminal_status"`
}

type LogPayload struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

// NewMessage marshals payload into the typed protocol envelope.
func NewMessage(typ string, payload any) (Message, error) {
	if payload == nil {
		payload = map[string]any{}
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return Message{}, fmt.Errorf("marshal %s payload: %w", typ, err)
	}
	return Message{Type: typ, Payload: b}, nil
}

// MustMessage is for tests and static empty envelopes.
func MustMessage(typ string, payload any) Message {
	msg, err := NewMessage(typ, payload)
	if err != nil {
		panic(err)
	}
	return msg
}

func DecodePayload[T any](msg Message) (T, error) {
	var out T
	if len(msg.Payload) == 0 {
		return out, fmt.Errorf("message %s has empty payload", msg.Type)
	}
	if err := json.Unmarshal(msg.Payload, &out); err != nil {
		return out, fmt.Errorf("decode %s payload: %w", msg.Type, err)
	}
	return out, nil
}

type Handler func(context.Context, Message) error

type writeRequest struct {
	ctx  context.Context
	msg  Message
	done chan error
}

// Session wraps a websocket connection without encoding which side dialed.
type Session struct {
	conn *websocket.Conn

	mu       sync.RWMutex
	handlers map[string]Handler

	writes chan writeRequest
	done   chan struct{}
	once   sync.Once
}

func NewSession(conn *websocket.Conn) *Session {
	conn.SetReadLimit(MaxMessageBytes)
	return &Session{
		conn:     conn,
		handlers: make(map[string]Handler),
		writes:   make(chan writeRequest, 64),
		done:     make(chan struct{}),
	}
}

func (s *Session) Handle(typ string, handler Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[typ] = handler
}

func (s *Session) Start(ctx context.Context) {
	go s.writer(ctx)
	go s.reader(ctx)
}

func (s *Session) Send(ctx context.Context, msg Message) error {
	if msg.Type == "" {
		return fmt.Errorf("message type is required")
	}
	if len(msg.Payload) == 0 {
		msg.Payload = json.RawMessage(`{}`)
	}
	req := writeRequest{ctx: ctx, msg: msg, done: make(chan error, 1)}
	select {
	case s.writes <- req:
	case <-s.done:
		return ErrSessionClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.done:
		return err
	case <-s.done:
		return ErrSessionClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Session) Close(status websocket.StatusCode, reason string) error {
	s.close()
	return s.conn.Close(status, reason)
}

func (s *Session) Done() <-chan struct{} {
	return s.done
}

func (s *Session) writer(ctx context.Context) {
	for {
		select {
		case req := <-s.writes:
			err := wsjson.Write(req.ctx, s.conn, req.msg)
			req.done <- err
			if err != nil {
				debugf("writer write error (type=%s, payload_bytes=%d): %v", req.msg.Type, len(req.msg.Payload), err)
				s.close()
				return
			}
		case <-s.done:
			return
		case <-ctx.Done():
			s.close()
			return
		}
	}
}

func (s *Session) reader(ctx context.Context) {
	for {
		var msg Message
		if err := wsjson.Read(ctx, s.conn, &msg); err != nil {
			if status := websocket.CloseStatus(err); status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
				debugf("reader: peer closed session normally (%s)", status)
			} else {
				debugf("reader read error: %v", err)
			}
			s.close()
			return
		}
		if msg.Type == "" {
			debugf("reader received empty-type message")
			s.close()
			return
		}
		s.mu.RLock()
		handler := s.handlers[msg.Type]
		s.mu.RUnlock()
		if handler == nil {
			continue
		}
		if err := handler(ctx, msg); err != nil {
			debugf("handler %q returned error: %v", msg.Type, err)
			s.close()
			return
		}
	}
}

func (s *Session) close() {
	s.once.Do(func() {
		close(s.done)
		_ = s.conn.Close(websocket.StatusNormalClosure, "session closed")
	})
}
