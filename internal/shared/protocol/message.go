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
	TypeHello            = "hello"
	TypeDispatch         = "dispatch"
	TypeCancel           = "cancel"
	TypeEvictWarmSession = "evict_warm_session"
	TypePing             = "ping"

	TypeReady    = "ready"
	TypeEvent    = "event"
	TypeArtifact = "artifact"
	TypeReport   = "report"
	TypeResult   = "result"
	TypeLog      = "log"
	TypePong     = "pong"
)

var ErrSessionClosed = errors.New("protocol session closed")

// ArtifactChunkBytes is the raw byte budget for one artifact chunk. Artifact
// content travels as an adjacent binary websocket frame; the JSON artifact
// envelope carries metadata only.
const ArtifactChunkBytes = 1 << 20

// MaxMessageBytes bounds a single websocket message. coder/websocket defaults
// the read limit to 32 KiB, which is far too small for this channel: artifact
// binary frames are ArtifactChunkBytes, and other JSON payloads need headroom.
const MaxMessageBytes = 4 * ArtifactChunkBytes

// Message is the symmetric Manager<->Runner JSON envelope.
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`

	artifact *ArtifactPayload
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

type EvictWarmSessionPayload struct {
	WarmSessionKey string `json:"warm_session_key"`
}

type EventPayload = event.Event

// ArtifactPayload transfers a durable artifact from Runner to Manager over the
// session. Large artifacts are split across ordered TypeArtifact headers with
// Seq starting at 0 and Last marking the terminal chunk. Content is the raw
// bytes for this chunk, carried in the binary websocket frame immediately after
// the JSON metadata header.
type ArtifactPayload struct {
	RunID      string `json:"run_id"`
	TaskID     string `json:"task_id"`
	AttemptID  string `json:"attempt_id"`
	ArtifactID string `json:"artifact_id"`
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	MediaType  string `json:"media_type"`
	Seq        int    `json:"seq"`
	Last       bool   `json:"last"`
	Content    []byte `json:"content,omitempty"`
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
	if msg.artifact != nil {
		if target, ok := any(&out).(*ArtifactPayload); ok {
			*target = *msg.artifact
			return out, nil
		}
	}
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
	ctx       context.Context
	msg       Message
	binary    []byte
	hasBinary bool
	done      chan error
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
	return s.send(ctx, msg, nil, false)
}

func (s *Session) sendBinary(ctx context.Context, msg Message, binary []byte) error {
	return s.send(ctx, msg, binary, true)
}

func (s *Session) send(ctx context.Context, msg Message, binary []byte, hasBinary bool) error {
	if msg.Type == "" {
		return fmt.Errorf("message type is required")
	}
	if len(msg.Payload) == 0 {
		msg.Payload = json.RawMessage(`{}`)
	}
	req := writeRequest{ctx: ctx, msg: msg, binary: binary, hasBinary: hasBinary, done: make(chan error, 1)}
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
	return s.closeWith(func() error {
		return s.conn.Close(status, reason)
	})
}

func (s *Session) Done() <-chan struct{} {
	return s.done
}

func (s *Session) writer(ctx context.Context) {
	for {
		select {
		case req := <-s.writes:
			err := s.write(req)
			req.done <- err
			if err != nil {
				debugf("writer write error (type=%s, payload_bytes=%d, binary_bytes=%d): %v", req.msg.Type, len(req.msg.Payload), len(req.binary), err)
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

func (s *Session) write(req writeRequest) error {
	b, err := json.Marshal(req.msg)
	if err != nil {
		return fmt.Errorf("marshal %s message: %w", req.msg.Type, err)
	}
	if err := s.conn.Write(req.ctx, websocket.MessageText, b); err != nil {
		return err
	}
	if req.hasBinary {
		if err := s.conn.Write(req.ctx, websocket.MessageBinary, req.binary); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) reader(ctx context.Context) {
	var pendingArtifact *ArtifactPayload
	var pendingPayload json.RawMessage
	for {
		typ, data, err := s.conn.Read(ctx)
		if err != nil {
			if status := websocket.CloseStatus(err); status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
				debugf("reader: peer closed session normally (%s)", status)
			} else {
				debugf("reader read error: %v", err)
			}
			s.close()
			return
		}
		switch typ {
		case websocket.MessageText:
			if pendingArtifact != nil {
				debugf("reader received text frame before binary artifact content (artifact_id=%s)", pendingArtifact.ArtifactID)
				s.close()
				return
			}
			var msg Message
			if err := json.Unmarshal(data, &msg); err != nil {
				debugf("reader decode text frame: %v", err)
				s.close()
				return
			}
			if msg.Type == "" {
				debugf("reader received empty-type message")
				s.close()
				return
			}
			if msg.Type == TypeArtifact {
				artifact, err := DecodePayload[ArtifactPayload](msg)
				if err != nil {
					debugf("reader decode artifact header: %v", err)
					s.close()
					return
				}
				if len(artifact.Content) != 0 {
					debugf("reader artifact header carried %d JSON content bytes", len(artifact.Content))
					s.close()
					return
				}
				artifact.Content = nil
				pendingArtifact = &artifact
				pendingPayload = msg.Payload
				continue
			}
			if err := s.dispatch(ctx, msg); err != nil {
				debugf("handler %q returned error: %v", msg.Type, err)
				s.close()
				return
			}
		case websocket.MessageBinary:
			if pendingArtifact == nil {
				debugf("reader received binary frame without pending artifact header")
				s.close()
				return
			}
			artifact := *pendingArtifact
			artifact.Content = data
			msg := Message{Type: TypeArtifact, Payload: pendingPayload, artifact: &artifact}
			pendingArtifact = nil
			pendingPayload = nil
			if err := s.dispatch(ctx, msg); err != nil {
				debugf("handler %q returned error: %v", msg.Type, err)
				s.close()
				return
			}
		default:
			debugf("reader received unsupported websocket frame type %v", typ)
			s.close()
			return
		}
	}
}

func (s *Session) dispatch(ctx context.Context, msg Message) error {
	s.mu.RLock()
	handler := s.handlers[msg.Type]
	s.mu.RUnlock()
	if handler == nil {
		return nil
	}
	return handler(ctx, msg)
}

func (s *Session) close() {
	_ = s.closeWith(s.conn.CloseNow)
}

func (s *Session) closeWith(closeConn func() error) (err error) {
	s.once.Do(func() {
		close(s.done)
		err = closeConn()
	})
	return err
}
