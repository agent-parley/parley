package protocol_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/agent-parley/parley/internal/shared/protocol"
)

func TestArtifactPayloadMetadataRoundTrip(t *testing.T) {
	in := protocol.ArtifactPayload{
		RunID:      "run_1",
		TaskID:     "task_1",
		AttemptID:  "attempt_1",
		ArtifactID: "artifact_1",
		Name:       "diff.patch",
		Kind:       "diff",
		MediaType:  "text/x-diff",
		Seq:        0,
		Last:       true,
	}
	msg, err := protocol.NewMessage(protocol.TypeArtifact, in)
	if err != nil {
		t.Fatalf("encode artifact: %v", err)
	}
	if msg.Type != protocol.TypeArtifact {
		t.Fatalf("unexpected type %q", msg.Type)
	}
	out, err := protocol.DecodePayload[protocol.ArtifactPayload](msg)
	if err != nil {
		t.Fatalf("decode artifact: %v", err)
	}
	if out.ArtifactID != in.ArtifactID || out.Name != in.Name || out.Kind != in.Kind {
		t.Fatalf("metadata mismatch: %+v", out)
	}
	if out.Seq != in.Seq || out.Last != in.Last {
		t.Fatalf("chunk metadata mismatch: %+v", out)
	}
	if len(out.Content) != 0 {
		t.Fatalf("metadata header content length = %d, want 0", len(out.Content))
	}
}

func TestSessionRejectsUnexpectedBinaryFrame(t *testing.T) {
	serverDone := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		sess := protocol.NewSession(conn)
		sess.Start(context.Background())
		<-sess.Done()
		close(serverDone)
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageBinary, []byte("orphan")); err != nil {
		t.Fatalf("write orphan binary: %v", err)
	}
	select {
	case <-serverDone:
	case <-ctx.Done():
		t.Fatal("session did not close after orphan binary frame")
	}
}

func TestSessionRejectsArtifactHeaderNotFollowedByBinary(t *testing.T) {
	serverDone := make(chan struct{})
	artifactHandled := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		sess := protocol.NewSession(conn)
		sess.Handle(protocol.TypeArtifact, func(context.Context, protocol.Message) error {
			artifactHandled <- struct{}{}
			return nil
		})
		sess.Start(context.Background())
		<-sess.Done()
		close(serverDone)
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	writeTextMessage(t, ctx, conn, protocol.MustMessage(protocol.TypeArtifact, protocol.ArtifactPayload{
		RunID:      "run_1",
		TaskID:     "task_1",
		AttemptID:  "attempt_1",
		ArtifactID: "artifact_1",
		Name:       "artifact.log",
		Kind:       "validation_log",
		MediaType:  "text/plain",
		Seq:        0,
		Last:       true,
	}))
	writeTextMessage(t, ctx, conn, protocol.MustMessage(protocol.TypePing, map[string]any{}))
	select {
	case <-serverDone:
	case <-ctx.Done():
		t.Fatal("session did not close after artifact header without binary payload")
	}
	select {
	case <-artifactHandled:
		t.Fatal("artifact handler was called before binary payload arrived")
	default:
	}
}

func TestSessionRoundTripHandshakeAndPing(t *testing.T) {
	serverReady := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		sess := protocol.NewSession(conn)
		sess.Handle(protocol.TypeHello, func(ctx context.Context, msg protocol.Message) error {
			hello, err := protocol.DecodePayload[protocol.HelloPayload](msg)
			if err != nil {
				return err
			}
			ready, err := protocol.NewMessage(protocol.TypeReady, protocol.ReadyPayload{RunnerID: hello.RunnerID, Capabilities: protocol.Capabilities{Adapters: []string{"noop"}}})
			if err != nil {
				return err
			}
			return sess.Send(ctx, ready)
		})
		sess.Handle(protocol.TypePing, func(ctx context.Context, _ protocol.Message) error {
			return sess.Send(ctx, protocol.MustMessage(protocol.TypePong, map[string]any{}))
		})
		close(serverReady)
		sess.Start(context.Background())
		<-sess.Done()
	}))
	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := protocol.NewSession(conn)
	readyCh := make(chan protocol.ReadyPayload, 1)
	pongCh := make(chan struct{}, 1)
	client.Handle(protocol.TypeReady, func(_ context.Context, msg protocol.Message) error {
		ready, err := protocol.DecodePayload[protocol.ReadyPayload](msg)
		if err != nil {
			return err
		}
		readyCh <- ready
		return nil
	})
	client.Handle(protocol.TypePong, func(context.Context, protocol.Message) error {
		pongCh <- struct{}{}
		return nil
	})
	client.Start(context.Background())
	<-serverReady
	if err := client.Send(ctx, protocol.MustMessage(protocol.TypeHello, protocol.HelloPayload{RunnerID: "runner_test"})); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	select {
	case ready := <-readyCh:
		if ready.RunnerID != "runner_test" || len(ready.Capabilities.Adapters) != 1 || ready.Capabilities.Adapters[0] != "noop" {
			t.Fatalf("unexpected ready: %+v", ready)
		}
	case <-ctx.Done():
		t.Fatal("ready timeout")
	}
	if err := client.Send(ctx, protocol.MustMessage(protocol.TypePing, map[string]any{})); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	select {
	case <-pongCh:
	case <-ctx.Done():
		t.Fatal("pong timeout")
	}
	_ = client.Close(websocket.StatusNormalClosure, "test done")
}

func writeTextMessage(t *testing.T, ctx context.Context, conn *websocket.Conn, msg protocol.Message) {
	t.Helper()
	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal %s message: %v", msg.Type, err)
	}
	if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatalf("write %s message: %v", msg.Type, err)
	}
}
