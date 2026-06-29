package protocol_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/agent-parley/parley/internal/shared/protocol"
)

func TestSendArtifactSmallArtifactSingleFrame(t *testing.T) {
	content := []byte("small artifact")
	chunks := sendArtifactAndCollect(t, protocol.ArtifactPayload{
		RunID:      "run_small",
		TaskID:     "task_small",
		AttemptID:  "attempt_small",
		ArtifactID: "artifact_small",
		Name:       "small.txt",
		Kind:       "log",
		MediaType:  "text/plain",
		Content:    content,
	}, 1)
	if len(chunks) != 1 {
		t.Fatalf("chunk count = %d, want 1", len(chunks))
	}
	if chunks[0].Seq != 0 || !chunks[0].Last {
		t.Fatalf("unexpected terminal metadata: seq=%d last=%v", chunks[0].Seq, chunks[0].Last)
	}
	if !bytes.Equal(chunks[0].Content, content) {
		t.Fatalf("content mismatch")
	}
}

func TestSendArtifactWritesBinaryFrameContent(t *testing.T) {
	content := []byte{0x00, 0x01, 'l', 'o', 'g', 0xff}
	serverDone := make(chan error, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		typ, data, err := conn.Read(ctx)
		if err != nil {
			serverDone <- err
			return
		}
		if typ != websocket.MessageText {
			serverDone <- fmt.Errorf("first frame type = %v, want %v", typ, websocket.MessageText)
			return
		}
		if bytes.Contains(data, []byte(base64.StdEncoding.EncodeToString(content))) {
			serverDone <- fmt.Errorf("artifact content was base64-encoded in JSON header")
			return
		}
		var msg protocol.Message
		if err := json.Unmarshal(data, &msg); err != nil {
			serverDone <- err
			return
		}
		if msg.Type != protocol.TypeArtifact {
			serverDone <- fmt.Errorf("message type = %q, want %q", msg.Type, protocol.TypeArtifact)
			return
		}
		header, err := protocol.DecodePayload[protocol.ArtifactPayload](msg)
		if err != nil {
			serverDone <- err
			return
		}
		if header.Seq != 0 || !header.Last || len(header.Content) != 0 {
			serverDone <- fmt.Errorf("unexpected header: seq=%d last=%v content=%d", header.Seq, header.Last, len(header.Content))
			return
		}
		typ, data, err = conn.Read(ctx)
		if err != nil {
			serverDone <- err
			return
		}
		if typ != websocket.MessageBinary {
			serverDone <- fmt.Errorf("second frame type = %v, want %v", typ, websocket.MessageBinary)
			return
		}
		if !bytes.Equal(data, content) {
			serverDone <- fmt.Errorf("binary content mismatch")
			return
		}
		serverDone <- nil
		// Keep the connection open until the client initiates close, so the
		// teardown does not race the client's in-flight SendArtifact return.
		_, _, _ = conn.Read(ctx)
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := protocol.NewSession(conn)
	client.Start(context.Background())
	if err := protocol.SendArtifact(ctx, client, protocol.ArtifactPayload{
		RunID:      "run_binary",
		TaskID:     "task_binary",
		AttemptID:  "attempt_binary",
		ArtifactID: "artifact_binary",
		Name:       "binary.log",
		Kind:       "validation_log",
		MediaType:  "application/octet-stream",
		Content:    content,
	}); err != nil {
		t.Fatalf("send artifact: %v", err)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("server: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for raw frames")
	}
	_ = client.Close(websocket.StatusNormalClosure, "test done")
}

func TestSendArtifactEmptyArtifactSingleFrame(t *testing.T) {
	chunks := sendArtifactAndCollect(t, protocol.ArtifactPayload{
		RunID:      "run_empty",
		TaskID:     "task_empty",
		AttemptID:  "attempt_empty",
		ArtifactID: "artifact_empty",
		Name:       "empty.txt",
		Kind:       "log",
		MediaType:  "text/plain",
	}, 1)
	if len(chunks) != 1 {
		t.Fatalf("chunk count = %d, want 1", len(chunks))
	}
	if chunks[0].Seq != 0 || !chunks[0].Last {
		t.Fatalf("unexpected terminal metadata: seq=%d last=%v", chunks[0].Seq, chunks[0].Last)
	}
	if len(chunks[0].Content) != 0 {
		t.Fatalf("content length = %d, want 0", len(chunks[0].Content))
	}
}

func TestSendArtifactMultiChunkRoundTrip(t *testing.T) {
	content := patternedBytes(3*protocol.ArtifactChunkBytes + protocol.ArtifactChunkBytes/2)
	chunks := sendArtifactAndCollect(t, protocol.ArtifactPayload{
		RunID:      "run_large",
		TaskID:     "task_large",
		AttemptID:  "attempt_large",
		ArtifactID: "artifact_large",
		Name:       "large.log",
		Kind:       "validation_log",
		MediaType:  "text/plain",
		Content:    content,
	}, 4)
	if len(chunks) != 4 {
		t.Fatalf("chunk count = %d, want 4", len(chunks))
	}
	var assembled []byte
	for i, chunk := range chunks {
		if chunk.Seq != i {
			t.Fatalf("chunk %d seq = %d", i, chunk.Seq)
		}
		if chunk.Last != (i == len(chunks)-1) {
			t.Fatalf("chunk %d last = %v", i, chunk.Last)
		}
		assembled = append(assembled, chunk.Content...)
	}
	if !bytes.Equal(assembled, content) {
		t.Fatalf("assembled content mismatch")
	}
}

func TestSendArtifactLargerThanLegacyMessageLimit(t *testing.T) {
	const legacyMaxMessageBytes = 64 << 20
	content := patternedBytes(legacyMaxMessageBytes + protocol.ArtifactChunkBytes)
	wantChunks := len(content) / protocol.ArtifactChunkBytes
	chunks := sendArtifactAndCollect(t, protocol.ArtifactPayload{
		RunID:      "run_legacy_limit",
		TaskID:     "task_legacy_limit",
		AttemptID:  "attempt_legacy_limit",
		ArtifactID: "artifact_legacy_limit",
		Name:       "huge.log",
		Kind:       "validation_log",
		MediaType:  "text/plain",
		Content:    content,
	}, wantChunks)
	if len(chunks) != wantChunks {
		t.Fatalf("chunk count = %d, want %d", len(chunks), wantChunks)
	}
	var total int
	for i, chunk := range chunks {
		if chunk.Seq != i {
			t.Fatalf("chunk %d seq = %d", i, chunk.Seq)
		}
		if chunk.Last != (i == len(chunks)-1) {
			t.Fatalf("chunk %d last = %v", i, chunk.Last)
		}
		total += len(chunk.Content)
	}
	if total != len(content) {
		t.Fatalf("transferred bytes = %d, want %d", total, len(content))
	}
}

func sendArtifactAndCollect(t *testing.T, payload protocol.ArtifactPayload, wantChunks int) []protocol.ArtifactPayload {
	t.Helper()
	chunks := make(chan protocol.ArtifactPayload, wantChunks)
	serverReady := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		sess := protocol.NewSession(conn)
		sess.Handle(protocol.TypeArtifact, func(_ context.Context, msg protocol.Message) error {
			chunk, err := protocol.DecodePayload[protocol.ArtifactPayload](msg)
			if err != nil {
				return err
			}
			chunks <- chunk
			return nil
		})
		close(serverReady)
		sess.Start(context.Background())
		<-sess.Done()
	}))
	defer ts.Close()

	// Large artifacts must tolerate -race overhead on 2-vCPU CI runners.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := protocol.NewSession(conn)
	client.Start(context.Background())
	<-serverReady
	if err := protocol.SendArtifact(ctx, client, payload); err != nil {
		t.Fatalf("send artifact: %v", err)
	}
	got := make([]protocol.ArtifactPayload, 0, wantChunks)
	for len(got) < wantChunks {
		select {
		case chunk := <-chunks:
			got = append(got, chunk)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for chunks: got %d want %d", len(got), wantChunks)
		}
	}
	_ = client.Close(websocket.StatusNormalClosure, "test done")
	return got
}

func patternedBytes(size int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(i % 251)
	}
	return out
}
