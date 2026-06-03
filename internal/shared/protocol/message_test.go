package protocol_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/agent-parley/parley/internal/shared/protocol"
)

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
