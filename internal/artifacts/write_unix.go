//go:build !windows

package artifacts

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/agent-parley/parley/internal/models"
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
	path := filepath.Join(dir, filepath.Clean(name))
	absPath, err := filepath.Abs(path)
	if err != nil {
		return models.Artifact{}, err
	}
	if rel, err := filepath.Rel(root, absPath); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return models.Artifact{}, fmt.Errorf("artifact path is outside the data root")
	}
	relDir, err := filepath.Rel(root, filepath.Dir(absPath))
	if err != nil {
		return models.Artifact{}, err
	}
	if relDir == ".." || strings.HasPrefix(relDir, ".."+string(filepath.Separator)) {
		return models.Artifact{}, fmt.Errorf("artifact directory is outside the data root")
	}
	if err := writeFileNoFollow(root, relDir, filepath.Base(absPath), []byte(body), 0o600); err != nil {
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

func writeFileNoFollow(root, relDir, name string, data []byte, perm os.FileMode) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, os.PathSeparator) {
		return fmt.Errorf("invalid artifact filename")
	}
	rootFD, err := syscall.Open(root, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(rootFD)

	dirFD := rootFD
	var opened []int
	defer func() {
		for i := len(opened) - 1; i >= 0; i-- {
			syscall.Close(opened[i])
		}
	}()

	if relDir != "." && relDir != "" {
		for _, part := range strings.Split(relDir, string(filepath.Separator)) {
			if part == "" || part == "." || part == ".." {
				return fmt.Errorf("invalid artifact directory")
			}
			nextFD, err := syscall.Openat(dirFD, part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
			if err != nil {
				if err != syscall.ENOENT {
					return err
				}
				if err := syscall.Mkdirat(dirFD, part, 0o700); err != nil && err != syscall.EEXIST {
					return err
				}
				nextFD, err = syscall.Openat(dirFD, part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
				if err != nil {
					return err
				}
			}
			opened = append(opened, nextFD)
			dirFD = nextFD
		}
	}

	fileFD, err := syscall.Openat(dirFD, name, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_TRUNC|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, uint32(perm))
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fileFD), name)
	if file == nil {
		syscall.Close(fileFD)
		return fmt.Errorf("failed to open artifact file")
	}
	defer file.Close()
	_, err = file.Write(data)
	return err
}
