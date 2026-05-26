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
