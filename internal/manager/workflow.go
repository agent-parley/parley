package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/agent-parley/parley/internal/artifacts"
	"github.com/agent-parley/parley/internal/executor"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/pathsafe"
	"github.com/agent-parley/parley/internal/store"
)

type AttemptRunner = executor.Runner

type WorkflowService struct {
	store  *store.Store
	runner AttemptRunner
	writer *artifacts.Writer
	logger *slog.Logger
}

func NewWorkflowService(store *store.Store, runner AttemptRunner, writer *artifacts.Writer, logger *slog.Logger) *WorkflowService {
	return &WorkflowService{store: store, runner: runner, writer: writer, logger: logger}
}

func (s *WorkflowService) StartAttempt(ctx context.Context, taskID string) error {
	started, err := s.store.BeginAttempt(taskID)
	if err != nil {
		return err
	}
	s.emit(started.Run.ID, started.Task.ID, started.Runner.ID, started.Lease.ID, models.EventTaskStateChanged, models.ActorKindManager, "Attempt dispatch started", nil)
	s.emit(started.Run.ID, started.Task.ID, started.Runner.ID, started.Lease.ID, models.EventLeaseGranted, models.ActorKindManager, "Runner slot reserved", map[string]any{"lease_id": started.Lease.ID})
	s.emit(started.Run.ID, started.Task.ID, started.Runner.ID, started.Lease.ID, models.EventTaskStarted, models.ActorKindRunner, "Worker attempt started using selected runner record", nil)

	attemptDir := s.store.AttemptDir(started.Project.ID, started.Run.ID, started.Task.ID, started.Attempt.Number)
	resumeCheckpoints := s.resumeCheckpoints(started.Task.ID, started.Attempt.Number)
	result, err := s.runAttemptSafely(ctx, started, attemptDir, resumeCheckpoints)
	if err != nil {
		if len(result.Files) > 0 && s.store.AttemptStillRunning(started.Task.ID, started.Attempt.ID, started.Lease.ID) {
			if writeErr := s.writeAttemptFiles(started, result); writeErr != nil {
				s.failAttemptAndEmit(started, writeErr.Error())
				return writeErr
			}
			s.emit(started.Run.ID, started.Task.ID, started.Runner.ID, started.Lease.ID, models.EventArtifactCreated, models.ActorKindRunner, "Attempt failure diagnostics saved", map[string]any{"attempt": started.Attempt.Number})
		}
		failureSummary := err.Error()
		if result.Summary != "" {
			failureSummary = result.Summary
		}
		s.failAttemptAndEmit(started, failureSummary)
		return err
	}
	if !s.store.AttemptStillRunning(started.Task.ID, started.Attempt.ID, started.Lease.ID) {
		return nil
	}
	if err := s.writeAttemptFiles(started, result); err != nil {
		s.failAttemptAndEmit(started, err.Error())
		return err
	}
	s.emit(started.Run.ID, started.Task.ID, started.Runner.ID, started.Lease.ID, models.EventArtifactCreated, models.ActorKindRunner, "Attempt outputs saved", map[string]any{"attempt": started.Attempt.Number})
	if _, _, _, err := s.store.CompleteAttempt(started.Task.ID, started.Attempt.ID, started.Lease.ID, result.Summary); err != nil {
		return err
	}
	s.emit(started.Run.ID, started.Task.ID, started.Runner.ID, started.Lease.ID, models.EventReviewCompleted, models.ActorKindRunner, "Reviewer step completed", nil)
	s.emit(started.Run.ID, started.Task.ID, started.Runner.ID, started.Lease.ID, models.EventLeaseReleased, models.ActorKindManager, "Runner slot released", map[string]any{"lease_id": started.Lease.ID})
	return nil
}

func (s *WorkflowService) runAttemptSafely(ctx context.Context, started store.StartAttemptResult, attemptDir string, resumeCheckpoints []executor.Checkpoint) (result executor.AttemptResult, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if s.logger != nil {
				s.logger.Error("attempt runner panic", "task_id", started.Task.ID, "panic", recovered)
			}
			err = fmt.Errorf("attempt runner panic")
			result = executor.AttemptResult{Summary: "Attempt failed after a runner panic."}
		}
	}()
	return s.runner.RunAttempt(ctx, executor.AttemptInput{Project: started.Project, Run: started.Run, Task: started.Task, Attempt: started.Attempt, Runner: started.Runner, Lease: started.Lease, ArtifactDir: attemptDir, ResumeCheckpoints: resumeCheckpoints})
}

func (s *WorkflowService) resumeCheckpoints(taskID string, beforeAttemptNumber int) []executor.Checkpoint {
	artifacts := s.store.CheckpointArtifactsForTaskBeforeAttempt(taskID, beforeAttemptNumber)
	checkpoints := make([]executor.Checkpoint, 0, len(artifacts))
	for _, artifact := range artifacts {
		checkpoints = append(checkpoints, executor.Checkpoint{Step: checkpointStepFromPath(artifact.Path), Role: checkpointRoleFromPath(artifact.Path), Profile: s.checkpointProfileFromArtifact(artifact), AttemptNumber: artifact.AttemptNumber, ArtifactID: artifact.ID, Path: checkpointReference(artifact), Summary: s.checkpointSummary(artifact), CreatedAt: artifact.CreatedAt})
	}
	return checkpoints
}

func checkpointStepFromPath(path string) string {
	name := filepath.Base(path)
	if name == "reviewer.json" {
		return "reviewer"
	}
	return "worker"
}

func checkpointRoleFromPath(path string) string {
	if checkpointStepFromPath(path) == "reviewer" {
		return "reviewer"
	}
	return "worker"
}

func checkpointReference(artifact models.Artifact) string {
	return fmt.Sprintf("artifact:%s/%s", artifact.ID, filepath.Base(artifact.Path))
}

func (s *WorkflowService) checkpointSummary(artifact models.Artifact) string {
	if artifact.SizeBytes > 16*1024 || !s.artifactPathWithinDataRoot(artifact.Path) {
		return "checkpoint " + filepath.Base(artifact.Path)
	}
	data, err := pathsafe.ReadFileNoFollow(artifact.Path)
	if err != nil {
		return "checkpoint " + filepath.Base(artifact.Path)
	}
	var body struct {
		Summary string `json:"summary"`
		Profile string `json:"profile"`
	}
	if err := json.Unmarshal(data, &body); err != nil || body.Summary == "" {
		return "checkpoint " + filepath.Base(artifact.Path)
	}
	return body.Summary
}

func (s *WorkflowService) checkpointProfileFromArtifact(artifact models.Artifact) string {
	if artifact.SizeBytes > 16*1024 || !s.artifactPathWithinDataRoot(artifact.Path) {
		return ""
	}
	data, err := pathsafe.ReadFileNoFollow(artifact.Path)
	if err != nil {
		return ""
	}
	var body struct{ Profile string `json:"profile"` }
	if err := json.Unmarshal(data, &body); err != nil {
		return ""
	}
	return body.Profile
}

func (s *WorkflowService) artifactPathWithinDataRoot(path string) bool {
	root, err := filepath.EvalSymlinks(s.store.DataRoot())
	if err != nil {
		return false
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return false
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(root, resolved)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func (s *WorkflowService) failAttemptAndEmit(started store.StartAttemptResult, summary string) {
	_, _, _, err := s.store.FailAttempt(started.Task.ID, started.Attempt.ID, started.Lease.ID, summary)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("failed to mark attempt failed", "task_id", started.Task.ID, "error", err)
		}
		return
	}
	s.emit(started.Run.ID, started.Task.ID, started.Runner.ID, started.Lease.ID, models.EventTaskStateChanged, models.ActorKindRunner, "Attempt failed", map[string]any{"attempt": started.Attempt.Number})
	s.emit(started.Run.ID, started.Task.ID, started.Runner.ID, started.Lease.ID, models.EventLeaseReleased, models.ActorKindManager, "Runner slot released after failed attempt", map[string]any{"lease_id": started.Lease.ID})
}

func (s *WorkflowService) writeAttemptFiles(started store.StartAttemptResult, result executor.AttemptResult) error {
	attemptDir := s.store.AttemptDir(started.Project.ID, started.Run.ID, started.Task.ID, started.Attempt.Number)
	for _, file := range result.Files {
		sensitivity := file.Sensitivity
		if sensitivity == "" {
			sensitivity = models.SensitivityNormal
		}
		if _, err := s.writer.WriteForAttemptWithSensitivity(started.Run.ID, started.Task.ID, started.Attempt.Number, attemptDir, file.Name, file.Kind, sensitivity, file.Body); err != nil {
			return err
		}
	}
	return nil
}

func (s *WorkflowService) emit(runID, taskID, runnerID, leaseID, typ, actorKind, summary string, data map[string]any) {
	actorID := runnerID
	if actorKind == models.ActorKindManager {
		actorID = models.ActorKindManager
	}
	_, err := s.store.AppendEvent(models.Event{RunID: runID, TaskID: taskID, ExecutorID: runnerID, LeaseID: leaseID, Type: typ, ActorKind: actorKind, ActorID: actorID, Summary: summary, Data: data})
	if err != nil && s.logger != nil {
		s.logger.Error("failed to append workflow event", "error", err)
	}
}
