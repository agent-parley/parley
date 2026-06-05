package runnerclient

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/shared/protocol"
)

func TestArtifactReassemblerMultiChunkRoundTrip(t *testing.T) {
	content := reassemblerPatternedBytes(3*protocol.ArtifactChunkBytes + protocol.ArtifactChunkBytes/2)
	reassembler := newArtifactReassembler()
	defer func() { _ = reassembler.Close() }()

	var complete protocol.ArtifactPayload
	var ready bool
	for _, chunk := range artifactChunksForTest(content, false) {
		var err error
		complete, ready, err = reassembler.Accept(chunk)
		if err != nil {
			t.Fatalf("accept chunk %d: %v", chunk.Seq, err)
		}
		if chunk.Last != ready {
			t.Fatalf("ready after seq %d = %v, want %v", chunk.Seq, ready, chunk.Last)
		}
	}
	if !ready {
		t.Fatal("artifact did not complete")
	}
	if complete.Seq != 0 || !complete.Last {
		t.Fatalf("unexpected complete metadata: seq=%d last=%v", complete.Seq, complete.Last)
	}
	if !bytes.Equal(complete.Content, content) {
		t.Fatalf("content mismatch")
	}
}

func TestArtifactReassemblerSmallArtifact(t *testing.T) {
	content := []byte("small artifact")
	reassembler := newArtifactReassembler()
	defer func() { _ = reassembler.Close() }()
	complete, ready, err := reassembler.Accept(baseArtifactForTest(content, 0, true))
	if err != nil {
		t.Fatalf("accept small artifact: %v", err)
	}
	if !ready {
		t.Fatal("small artifact did not complete")
	}
	if !bytes.Equal(complete.Content, content) {
		t.Fatalf("content mismatch")
	}
}

func TestArtifactReassemblerRejectsGappedSequence(t *testing.T) {
	reassembler := newArtifactReassembler()
	defer func() { _ = reassembler.Close() }()
	if _, ready, err := reassembler.Accept(baseArtifactForTest([]byte("first"), 0, false)); err != nil || ready {
		t.Fatalf("first chunk ready=%v err=%v", ready, err)
	}
	_, _, err := reassembler.Accept(baseArtifactForTest([]byte("third"), 2, true))
	if err == nil {
		t.Fatal("expected gap error")
	}
	if !strings.Contains(err.Error(), "expected seq 1, got 2") {
		t.Fatalf("unexpected gap error: %v", err)
	}
}

func TestHandleArtifactReassemblesBeforeCallingHandler(t *testing.T) {
	content := reassemblerPatternedBytes(2*protocol.ArtifactChunkBytes + 17)
	client := &Client{artifacts: newArtifactReassembler()}
	defer client.cleanupArtifacts()
	var saved protocol.ArtifactPayload
	saves := 0
	client.SetHandlers(nil, func(_ context.Context, art protocol.ArtifactPayload) error {
		saves++
		saved = art
		return nil
	}, nil, nil, nil)

	chunks := artifactChunksForTest(content, false)
	for i, chunk := range chunks {
		msg := protocol.MustMessage(protocol.TypeArtifact, chunk)
		if err := client.handleArtifact(context.Background(), msg); err != nil {
			t.Fatalf("handle chunk %d: %v", i, err)
		}
		wantSaves := 0
		if i == len(chunks)-1 {
			wantSaves = 1
		}
		if saves != wantSaves {
			t.Fatalf("saves after chunk %d = %d, want %d", i, saves, wantSaves)
		}
	}
	if saved.ArtifactID != "artifact_test" {
		t.Fatalf("unexpected artifact: %+v", saved)
	}
	if !bytes.Equal(saved.Content, content) {
		t.Fatalf("saved content mismatch")
	}
}

func artifactChunksForTest(content []byte, small bool) []protocol.ArtifactPayload {
	if small || len(content) <= protocol.ArtifactChunkBytes {
		return []protocol.ArtifactPayload{baseArtifactForTest(content, 0, true)}
	}
	chunks := []protocol.ArtifactPayload{}
	for seq, offset := 0, 0; offset < len(content); seq++ {
		end := offset + protocol.ArtifactChunkBytes
		if end > len(content) {
			end = len(content)
		}
		chunks = append(chunks, baseArtifactForTest(content[offset:end], seq, end == len(content)))
		offset = end
	}
	return chunks
}

func baseArtifactForTest(content []byte, seq int, last bool) protocol.ArtifactPayload {
	return protocol.ArtifactPayload{
		RunID:      "run_test",
		TaskID:     "task_test",
		AttemptID:  "attempt_test",
		ArtifactID: "artifact_test",
		Name:       "artifact.log",
		Kind:       "validation_log",
		MediaType:  "text/plain",
		Seq:        seq,
		Last:       last,
		Content:    content,
	}
}

func reassemblerPatternedBytes(size int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = byte((i * 17) % 251)
	}
	return out
}
