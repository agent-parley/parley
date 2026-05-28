//go:build windows

package artifacts

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/pathsafe"
	"github.com/agent-parley/parley/internal/secretpolicy"
)

func (w *Writer) WriteForAttemptWithSensitivity(runID, taskID string, attemptNumber int, dir, name, kind, sensitivity, body string) (models.Artifact, error) {
	sensitivity = secretpolicy.ClassifyArtifact(name, kind, sensitivity, body)
	root, err := filepath.EvalSymlinks(w.store.DataRoot())
	if err != nil {
		return models.Artifact{}, err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return models.Artifact{}, err
	}
	if filepath.IsAbs(name) {
		return models.Artifact{}, fmt.Errorf("artifact name must be relative")
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return models.Artifact{}, err
	}
	if rel, err := filepath.Rel(root, absDir); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return models.Artifact{}, fmt.Errorf("artifact directory is outside the data root")
	}
	if err := pathsafe.MkdirAllNoSymlink(absDir, 0o700); err != nil {
		return models.Artifact{}, err
	}
	path := filepath.Join(absDir, filepath.Clean(name))
	absPath, err := filepath.Abs(path)
	if err != nil {
		return models.Artifact{}, err
	}
	if rel, err := filepath.Rel(root, absPath); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return models.Artifact{}, fmt.Errorf("artifact path is outside the data root")
	}
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(absPath))
	if err != nil {
		return models.Artifact{}, err
	}
	resolvedParent, err = filepath.Abs(resolvedParent)
	if err != nil {
		return models.Artifact{}, err
	}
	if rel, err := filepath.Rel(root, resolvedParent); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return models.Artifact{}, fmt.Errorf("artifact parent path is outside the data root")
	}
	if err := pathsafe.WriteFileNoFollow(absPath, []byte(body), 0o600); err != nil {
		return models.Artifact{}, err
	}
	sum := sha256.Sum256([]byte(body))
	artifact := models.Artifact{
		RunID: runID, TaskID: taskID, AttemptNumber: attemptNumber, Kind: kind, Path: absPath,
		MediaType: "text/plain; charset=utf-8", Sensitivity: sensitivity,
		SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:]), CreatedAt: time.Now().UTC(),
	}
	return w.store.SaveArtifact(artifact)
}
