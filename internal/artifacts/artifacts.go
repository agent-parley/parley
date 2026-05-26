package artifacts

import (
	"github.com/agent-parley/parley/internal/models"
	storepkg "github.com/agent-parley/parley/internal/store"
)

type Writer struct {
	store *storepkg.Store
}

func NewWriter(store *storepkg.Store) *Writer {
	return &Writer{store: store}
}

func (w *Writer) Write(runID, taskID, dir, name, kind, body string) (models.Artifact, error) {
	return w.WriteForAttempt(runID, taskID, 0, dir, name, kind, body)
}

func (w *Writer) WriteForAttempt(runID, taskID string, attemptNumber int, dir, name, kind, body string) (models.Artifact, error) {
	return w.WriteForAttemptWithSensitivity(runID, taskID, attemptNumber, dir, name, kind, models.SensitivityNormal, body)
}
