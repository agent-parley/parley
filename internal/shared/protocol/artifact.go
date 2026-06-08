package protocol

import (
	"context"
	"fmt"
)

// SendArtifact sends an artifact over the session as ordered TypeArtifact
// metadata headers followed by adjacent binary content frames. Small artifacts
// are a single header+binary pair with Seq 0 and Last true.
func SendArtifact(ctx context.Context, session *Session, payload ArtifactPayload) error {
	if session == nil {
		return fmt.Errorf("protocol session is required")
	}
	content := payload.Content
	if len(content) == 0 {
		payload.Seq = 0
		payload.Last = true
		payload.Content = nil
		msg, err := NewMessage(TypeArtifact, payload)
		if err != nil {
			return err
		}
		return session.sendBinary(ctx, msg, content)
	}
	for seq, offset := 0, 0; offset < len(content); seq++ {
		end := offset + ArtifactChunkBytes
		if end > len(content) {
			end = len(content)
		}
		chunk := payload
		chunk.Seq = seq
		chunk.Last = end == len(content)
		chunk.Content = nil
		msg, err := NewMessage(TypeArtifact, chunk)
		if err != nil {
			return err
		}
		if err := session.sendBinary(ctx, msg, content[offset:end]); err != nil {
			return err
		}
		offset = end
	}
	return nil
}
