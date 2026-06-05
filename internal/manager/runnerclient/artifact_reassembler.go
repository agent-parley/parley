package runnerclient

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/agent-parley/parley/internal/shared/protocol"
)

type artifactReassembler struct {
	mu      sync.Mutex
	entries map[string]*artifactAssembly
}

type artifactAssembly struct {
	header  protocol.ArtifactPayload
	nextSeq int
	file    *os.File
	path    string
}

func newArtifactReassembler() *artifactReassembler {
	return &artifactReassembler{entries: map[string]*artifactAssembly{}}
}

func (r *artifactReassembler) Accept(art protocol.ArtifactPayload) (protocol.ArtifactPayload, bool, error) {
	if art.ArtifactID == "" {
		return protocol.ArtifactPayload{}, false, fmt.Errorf("artifact_id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	entry := r.entries[art.ArtifactID]
	if entry == nil {
		if art.Seq != 0 {
			return protocol.ArtifactPayload{}, false, fmt.Errorf("artifact %s: expected seq 0, got %d", art.ArtifactID, art.Seq)
		}
		file, err := os.CreateTemp("", "parley-artifact-*")
		if err != nil {
			return protocol.ArtifactPayload{}, false, fmt.Errorf("create artifact spool file: %w", err)
		}
		header := art
		header.Content = nil
		entry = &artifactAssembly{header: header, file: file, path: file.Name()}
		r.entries[art.ArtifactID] = entry
	}
	if err := entry.validateHeader(art); err != nil {
		return protocol.ArtifactPayload{}, false, err
	}
	if art.Seq != entry.nextSeq {
		return protocol.ArtifactPayload{}, false, fmt.Errorf("artifact %s: expected seq %d, got %d", art.ArtifactID, entry.nextSeq, art.Seq)
	}
	if _, err := entry.file.Write(art.Content); err != nil {
		return protocol.ArtifactPayload{}, false, fmt.Errorf("write artifact %s chunk %d: %w", art.ArtifactID, art.Seq, err)
	}
	entry.nextSeq++
	if !art.Last {
		return protocol.ArtifactPayload{}, false, nil
	}

	complete, err := entry.complete()
	delete(r.entries, art.ArtifactID)
	if err != nil {
		return protocol.ArtifactPayload{}, false, err
	}
	return complete, true, nil
}

func (r *artifactReassembler) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for id, entry := range r.entries {
		if err := entry.cleanup(); err != nil {
			errs = append(errs, fmt.Errorf("artifact %s: %w", id, err))
		}
		delete(r.entries, id)
	}
	return errors.Join(errs...)
}

func (a *artifactAssembly) validateHeader(art protocol.ArtifactPayload) error {
	header := a.header
	if art.RunID != header.RunID {
		return fmt.Errorf("artifact %s: run_id changed from %q to %q", art.ArtifactID, header.RunID, art.RunID)
	}
	if art.TaskID != header.TaskID {
		return fmt.Errorf("artifact %s: task_id changed from %q to %q", art.ArtifactID, header.TaskID, art.TaskID)
	}
	if art.AttemptID != header.AttemptID {
		return fmt.Errorf("artifact %s: attempt_id changed from %q to %q", art.ArtifactID, header.AttemptID, art.AttemptID)
	}
	if art.Name != header.Name {
		return fmt.Errorf("artifact %s: name changed from %q to %q", art.ArtifactID, header.Name, art.Name)
	}
	if art.Kind != header.Kind {
		return fmt.Errorf("artifact %s: kind changed from %q to %q", art.ArtifactID, header.Kind, art.Kind)
	}
	if art.MediaType != header.MediaType {
		return fmt.Errorf("artifact %s: media_type changed from %q to %q", art.ArtifactID, header.MediaType, art.MediaType)
	}
	return nil
}

func (a *artifactAssembly) complete() (protocol.ArtifactPayload, error) {
	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		_ = a.cleanup()
		return protocol.ArtifactPayload{}, fmt.Errorf("rewind artifact spool file: %w", err)
	}
	content, err := io.ReadAll(a.file)
	if err != nil {
		_ = a.cleanup()
		return protocol.ArtifactPayload{}, fmt.Errorf("read artifact spool file: %w", err)
	}
	out := a.header
	out.Seq = 0
	out.Last = true
	out.Content = content
	if err := a.cleanup(); err != nil {
		return protocol.ArtifactPayload{}, err
	}
	return out, nil
}

func (a *artifactAssembly) cleanup() error {
	var errs []error
	if a.file != nil {
		if err := a.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close artifact spool file: %w", err))
		}
		a.file = nil
	}
	if a.path != "" {
		if err := os.Remove(a.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove artifact spool file: %w", err))
		}
		a.path = ""
	}
	return errors.Join(errs...)
}
