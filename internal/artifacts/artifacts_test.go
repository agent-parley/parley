package artifacts_test

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/agent-parley/parley/internal/artifacts"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/testsupport"
)

func TestWriterDefaultsToNormalSensitivity(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	writer := artifacts.NewWriter(st)
	body := "hello artifact"
	artifact, err := writer.WriteForAttempt(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "worker-output.md", models.ArtifactKindWorkerOutput, body)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Sensitivity != models.SensitivityNormal || artifact.SizeBytes != int64(len(body)) {
		t.Fatalf("unexpected artifact metadata: %+v", artifact)
	}
	sum := sha256.Sum256([]byte(body))
	if artifact.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("unexpected sha: %s", artifact.SHA256)
	}
	if !filepath.IsAbs(artifact.Path) {
		t.Fatalf("artifact path should be absolute: %s", artifact.Path)
	}
}

func TestWriterPreservesInternalSensitivity(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	writer := artifacts.NewWriter(st)
	artifact, err := writer.WriteForAttemptWithSensitivity(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "runtime/stderr.txt", models.ArtifactKindWorkerOutput, models.SensitivityInternal, "raw")
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Sensitivity != models.SensitivityInternal {
		t.Fatalf("expected internal sensitivity, got %+v", artifact)
	}
	stored, ok := st.GetArtifact(artifact.ID)
	if !ok || stored.Sensitivity != models.SensitivityInternal {
		t.Fatalf("internal artifact not persisted: %+v ok=%v", stored, ok)
	}
}

func TestWriterClassifiesSecretLikeNormalArtifactAsSecret(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	writer := artifacts.NewWriter(st)
	artifact, err := writer.WriteForAttempt(run.ID, task.ID, 1, st.AttemptDir(task.ProjectID, run.ID, task.ID, 1), "summary.md", models.ArtifactKindSummary, "Authorization: Bearer abcdefghijklmnopqrstuvwxyz")
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Sensitivity != models.SensitivitySecret {
		t.Fatalf("expected secret sensitivity, got %+v", artifact)
	}
	stored, ok := st.GetArtifact(artifact.ID)
	if !ok || stored.Sensitivity != models.SensitivitySecret {
		t.Fatalf("secret artifact not persisted: %+v ok=%v", stored, ok)
	}
}

func TestWriterRejectsAbsoluteArtifactNameAndOutsideDir(t *testing.T) {
	st := testsupport.OpenStore(t)
	_, run, task := testsupport.ProjectAndTask(t, st)
	writer := artifacts.NewWriter(st)
	attemptDir := st.AttemptDir(task.ProjectID, run.ID, task.ID, 1)
	if _, err := writer.WriteForAttempt(run.ID, task.ID, 1, attemptDir, "/absolute.md", models.ArtifactKindSummary, "bad"); err == nil {
		t.Fatalf("expected absolute artifact name rejection")
	}
	if _, err := writer.WriteForAttempt(run.ID, task.ID, 1, t.TempDir(), "outside.md", models.ArtifactKindSummary, "bad"); err == nil {
		t.Fatalf("expected outside data root rejection")
	}
}
