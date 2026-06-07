package protocol_test

import (
	"bytes"
	"context"
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
