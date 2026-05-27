package executor

import (
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/models"
)

func TestCheckpointBodyUsesExplicitReviewerProfile(t *testing.T) {
	body := CheckpointBody(AttemptInput{Task: models.Task{Adapter: "pi-standard"}}, "reviewer", "reviewer", "pi-reviewer", "completed", "review done", "next", nil, nil)
	if !strings.Contains(body, `"role": "reviewer"`) || !strings.Contains(body, `"profile": "pi-reviewer"`) {
		t.Fatalf("checkpoint body did not use explicit reviewer metadata: %s", body)
	}
}

func TestResumeCheckpointSectionIsBoundedMetadata(t *testing.T) {
	section := ResumeCheckpointSection([]Checkpoint{{Step: "worker", AttemptNumber: 2, Summary: "prior step", Path: "checkpoints/worker.json"}})
	if !strings.Contains(section, "Attempt 2 worker checkpoint") || !strings.Contains(section, "prior step") {
		t.Fatalf("unexpected resume section: %s", section)
	}
	if strings.Contains(section, "stdout") || strings.Contains(section, "stderr") {
		t.Fatalf("resume section should not imply raw logs: %s", section)
	}
}

func TestCheckpointBodyRedactsSecretLikeMetadata(t *testing.T) {
	body := CheckpointBody(AttemptInput{Task: models.Task{Adapter: "pi-standard"}}, "worker", "worker", "pi-standard", "completed", "Authorization: Bearer abcdefghijklmnopqrstuvwxyz", "OPENAI_API_KEY=sk-secretsecretsecretsecret", nil, []string{"token-output.md"})
	for _, leaked := range []string{"abcdefghijklmnopqrstuvwxyz", "sk-secretsecret", "token-output.md"} {
		if strings.Contains(body, leaked) {
			t.Fatalf("checkpoint leaked %q in %s", leaked, body)
		}
	}
	if !strings.Contains(body, "[REDACTED]") {
		t.Fatalf("checkpoint missing redaction marker: %s", body)
	}
}

func TestResumeCheckpointSectionRedactsSecretLikeSummary(t *testing.T) {
	section := ResumeCheckpointSection([]Checkpoint{{Step: "worker", AttemptNumber: 2, Summary: "Authorization: Bearer abcdefghijklmnopqrstuvwxyz", Path: "checkpoints/worker.json"}})
	if strings.Contains(section, "abcdefghijklmnopqrstuvwxyz") || !strings.Contains(section, "[REDACTED]") {
		t.Fatalf("resume section did not redact secret-like summary: %s", section)
	}
}
