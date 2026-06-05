package protocol

import (
	"context"
	"fmt"
)

// SendArtifact sends an artifact over the session as ordered TypeArtifact
// chunks. Small artifacts are a single frame with Seq 0 and Last true.
func SendArtifact(ctx context.Context, session *Session, payload ArtifactPayload) error {
	if session == nil {
		return fmt.Errorf("protocol session is required")
	}
	content := payload.Content
	if len(content) == 0 {
		payload.Seq = 0
		payload.Last = true
		msg, err := NewMessage(TypeArtifact, payload)
		if err != nil {
			return err
		}
		return session.Send(ctx, msg)
	}
	for seq, offset := 0, 0; offset < len(content); seq++ {
		end := offset + ArtifactChunkBytes
		if end > len(content) {
			end = len(content)
		}
		chunk := payload
		chunk.Seq = seq
		chunk.Last = end == len(content)
		chunk.Content = content[offset:end]
		msg, err := NewMessage(TypeArtifact, chunk)
		if err != nil {
			return err
		}
		if err := session.Send(ctx, msg); err != nil {
			return err
		}
		offset = end
	}
	return nil
}
