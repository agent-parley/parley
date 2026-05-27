package executor_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/executor"
	"github.com/agent-parley/parley/internal/models"
)

func TestDryRunEmitsAttemptProgress(t *testing.T) {
	var events []string
	_, err := executor.NewDryRunRunner().RunAttempt(context.Background(), executor.AttemptInput{Attempt: models.Attempt{Number: 1}, Progress: func(eventType, summary string, data map[string]any) {
		events = append(events, eventType)
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{models.EventAttemptWorkerStarted, models.EventAttemptWorkerFinished, models.EventAttemptReviewerStarted, models.EventAttemptReviewerFinished}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("unexpected progress events: got %v want %v", events, want)
	}
}

func TestDryRunWritesInternalCheckpointsAndUsesResumeMetadata(t *testing.T) {
	result, err := executor.NewDryRunRunner().RunAttempt(context.Background(), executor.AttemptInput{
		Task: models.Task{Objective: "do work"}, Runner: models.Executor{ID: models.LocalExecutorID},
		ResumeCheckpoints: []executor.Checkpoint{{Step: "worker", AttemptNumber: 1, Summary: "prior worker", Path: "checkpoints/worker.json"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoints := 0
	workerOutputMentionsResume := false
	for _, file := range result.Files {
		if file.Kind == models.ArtifactKindCheckpoint {
			checkpoints++
			if file.Sensitivity != models.SensitivityInternal {
				t.Fatalf("checkpoint is not internal: %+v", file)
			}
			if !strings.Contains(file.Body, "schema_version") {
				t.Fatalf("checkpoint body is not structured JSON-ish: %s", file.Body)
			}
		}
		if file.Name == "worker-output.md" && strings.Contains(file.Body, "Resume checkpoints available: 1") {
			workerOutputMentionsResume = true
		}
	}
	if checkpoints != 2 {
		t.Fatalf("expected worker/reviewer checkpoints, got %d files=%+v", checkpoints, result.Files)
	}
	if !workerOutputMentionsResume {
		t.Fatalf("dry-run output did not mention resume checkpoint count: %+v", result.Files)
	}
}
