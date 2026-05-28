package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/secretpolicy"
	parleysync "github.com/agent-parley/parley/internal/sync"
)

func (s *Server) taskHandoff(w http.ResponseWriter, r *http.Request, project models.Project, run models.Run, task models.Task, parts []string) {
	if len(parts) == 0 && r.Method == http.MethodGet {
		s.handoffStart(w, r, project, run, task)
		return
	}
	if len(parts) == 1 && parts[0] == models.HandoffStatusPreview && r.Method == http.MethodPost {
		s.handoffPreview(w, r, project, run, task)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.handoffApproval(w, r, project, run, task, parts[0])
		return
	}
	if len(parts) == 2 && parts[1] == "approve" && r.Method == http.MethodPost {
		s.handoffApprove(w, r, project, run, task, parts[0])
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handoffStart(w http.ResponseWriter, r *http.Request, project models.Project, run models.Run, task models.Task) {
	source := task.AssignedExecutorID
	if source == "" {
		source = models.LocalExecutorID
	}
	sourceName := source
	if runner, ok := s.store.GetExecutor(source); ok && runner.Name != "" {
		sourceName = runner.Name
	}
	s.render(w, "Preview runner handoff", handoffStartTemplate, map[string]any{
		"Project": project,
		"Run": run,
		"Task": task,
		"Source": source,
		"SourceName": sourceName,
		"Runners": s.store.ListExecutors(),
		"Artifacts": s.normalArtifactsForTask(task.ID),
	})
}

func (s *Server) handoffPreview(w http.ResponseWriter, r *http.Request, project models.Project, run models.Run, task models.Task) {
	if err := r.ParseForm(); err != nil {
		s.badRequest(w, err)
		return
	}
	source := strings.TrimSpace(r.FormValue("source_runner"))
	if source == "" {
		source = task.AssignedExecutorID
	}
	if source == "" {
		source = models.LocalExecutorID
	}
	destination := strings.TrimSpace(r.FormValue("destination_runner"))
	sourceName := source
	if runner, ok := s.store.GetExecutor(source); ok && runner.Name != "" {
		sourceName = runner.Name
	}
	if destination == "" || destination == source {
		s.render(w, "Preview runner handoff", handoffStartTemplate, map[string]any{
			"Project": project,
			"Run": run,
			"Task": task,
			"Source": source,
			"SourceName": sourceName,
			"Runners": s.store.ListExecutors(),
			"Artifacts": s.normalArtifactsForTask(task.ID),
			"Error": "choose a different destination runner",
		})
		return
	}
	if _, ok := s.store.GetExecutor(source); !ok {
		s.badRequest(w, fmt.Errorf("source runner not found"))
		return
	}
	destinationRunner, ok := s.store.GetExecutor(destination)
	if !ok {
		s.badRequest(w, fmt.Errorf("destination runner not found"))
		return
	}
	if destinationRunner.Status != models.ExecutorStatusOnline {
		s.render(w, "Preview runner handoff", handoffStartTemplate, map[string]any{
			"Project": project,
			"Run": run,
			"Task": task,
			"Source": source,
			"SourceName": sourceName,
			"Runners": s.store.ListExecutors(),
			"Artifacts": s.normalArtifactsForTask(task.ID),
			"Error": "choose an online destination runner",
		})
		return
	}
	handoff := s.buildHandoff(project, run, task, source, destination)
	saved, err := s.store.SaveHandoff(handoff)
	if err != nil {
		s.serverError(w, err)
		return
	}
	saved.ManifestPreview = s.handoffManifestPreview(saved)
	if err := s.store.UpdateHandoff(saved); err != nil {
		s.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/tasks/"+task.ID+"/handoff/"+saved.ID, http.StatusSeeOther)
}

func (s *Server) handoffApproval(w http.ResponseWriter, r *http.Request, project models.Project, run models.Run, task models.Task, id string) {
	handoff, ok := s.store.GetHandoff(id)
	if !ok || handoff.TaskID != task.ID {
		http.NotFound(w, r)
		return
	}
	source, ok := s.store.GetExecutor(handoff.SourceExecutorID)
	if !ok {
		http.NotFound(w, r)
		return
	}
	destination, ok := s.store.GetExecutor(handoff.DestinationExecutorID)
	if !ok {
		http.NotFound(w, r)
		return
	}
	s.render(w, "Review handoff", handoffApprovalTemplate, map[string]any{
		"Project": project,
		"Run": run,
		"Task": task,
		"Handoff": handoff,
		"Source": source,
		"Destination": destination,
	})
}

func (s *Server) handoffApprove(w http.ResponseWriter, r *http.Request, project models.Project, run models.Run, task models.Task, id string) {
	handoff, ok := s.store.GetHandoff(id)
	if !ok || handoff.TaskID != task.ID {
		http.NotFound(w, r)
		return
	}
	recorded, changed, err := s.store.RecordHandoffApproval(handoff.ID)
	if err != nil {
		s.serverError(w, err)
		return
	}
	if changed {
		s.emit(run.ID, task.ID, models.EventHandoffApproved, models.ActorKindUser, models.ActorKindUser, "Handoff preview approved", map[string]any{"handoff_id": recorded.ID, "destination_runner": recorded.DestinationExecutorID})
		s.emit(run.ID, task.ID, models.EventHandoffCompleted, models.ActorKindManager, models.ActorKindManager, "Prototype handoff approval recorded without moving runner assignment", map[string]any{"handoff_id": recorded.ID, "destination_runner": recorded.DestinationExecutorID})
	}
	http.Redirect(w, r, "/tasks/"+task.ID+"/handoff/"+recorded.ID, http.StatusSeeOther)
}

func (s *Server) normalArtifactsForTask(taskID string) []models.Artifact {
	artifacts := s.store.ArtifactsForTask(taskID)
	visible := make([]models.Artifact, 0, len(artifacts))
	for _, artifact := range artifacts {
		if secretpolicy.IsPublicPreviewSensitivity(artifact.Sensitivity) {
			visible = append(visible, artifact)
		}
	}
	return visible
}

func (s *Server) buildHandoff(project models.Project, run models.Run, task models.Task, source, destination string) models.Handoff {
	branch := task.BranchName
	if branch == "" {
		branch = "prototype-handoff/" + task.ID
	}
	artifacts := s.store.ArtifactsForTask(task.ID)
	items := make([]models.HandoffItem, 0, len(artifacts))
	for _, artifact := range artifacts {
		rel, err := filepath.Rel(s.store.TaskDir(project.ID, run.ID, task.ID), artifact.Path)
		if err != nil || strings.HasPrefix(rel, "..") {
			rel = filepath.Base(artifact.Path)
		}
		if !secretpolicy.IsHandoffSafeSensitivity(artifact.Sensitivity) {
			continue
		}
		items = append(items, models.HandoffItem{Kind: artifact.Kind, Name: artifactName(artifact.Path), RelativePath: rel, Sensitivity: artifact.Sensitivity, SHA256: artifact.SHA256})
	}
	if len(items) == 0 {
		items = []models.HandoffItem{
			{Kind: models.ArtifactKindPlan, Name: "plan.v1.md", RelativePath: "plan.v1.md", Sensitivity: models.SensitivityNormal},
			{Kind: models.ArtifactKindSummary, Name: "summary.md", RelativePath: "attempts/1/summary.md", Sensitivity: models.SensitivityNormal},
			{Kind: models.ArtifactKindDiff, Name: "diff.patch", RelativePath: "attempts/1/diff.patch", Sensitivity: models.SensitivityNormal},
			{Kind: models.ArtifactKindReview, Name: "review.md", RelativePath: "attempts/1/review.md", Sensitivity: models.SensitivityNormal},
		}
	}
	exclusions := []models.HandoffExclusion{
		{RelativePath: ".env", Reason: "secrets stay local"},
		{RelativePath: ".git/", Reason: "code state moves through Git refs only"},
		{RelativePath: "parley.db", Reason: "manager database is never copied"},
		{RelativePath: "parley.db-wal", Reason: "manager database WAL is never copied"},
		{RelativePath: "parley.db-shm", Reason: "manager database shared-memory sidecar is never copied"},
		{RelativePath: "state.json", Reason: "legacy manager state backup is never copied"},
		{RelativePath: "raw-agent-sessions/", Reason: "raw agent transcripts are not blindly moved"},
		{RelativePath: "container-sockets/", Reason: "container sockets are never exposed"},
	}
	now := time.Now().UTC()
	return models.Handoff{
		ProjectID: project.ID, RunID: run.ID, TaskID: task.ID, SourceExecutorID: source, DestinationExecutorID: destination,
		Status: models.HandoffStatusPreview, BranchName: branch, CommitCheck: fmt.Sprintf("%s has a reviewable Git ref before handoff", branch), RemoteCheck: "destination runner must fetch code from Git, not a shared mount",
		Included: items, Excluded: exclusions, ParleyIgnorePreview: strings.Join(parleysync.HardExclusions, "\n"), CreatedAt: now, UpdatedAt: now,
	}
}

func (s *Server) handoffManifestPreview(handoff models.Handoff) string {
	included := make([]parleysync.Item, 0, len(handoff.Included))
	for _, item := range handoff.Included {
		included = append(included, parleysync.Item{Kind: item.Kind, RelativePath: item.RelativePath, Sensitivity: item.Sensitivity, SHA256: item.SHA256})
	}
	excluded := make([]parleysync.Exclusion, 0, len(handoff.Excluded))
	for _, exclusion := range handoff.Excluded {
		excluded = append(excluded, parleysync.Exclusion{RelativePath: exclusion.RelativePath, Reason: exclusion.Reason})
	}
	manifest := parleysync.Manifest{
		ID: handoff.ID,
		SourceExecutorID: handoff.SourceExecutorID,
		DestinationExecutorID: handoff.DestinationExecutorID,
		ProjectID: handoff.ProjectID,
		RunID: handoff.RunID,
		ExpectedGitRemote: "project configured Git remote",
		ExpectedGitRef: handoff.BranchName,
		Included: included,
		Excluded: excluded,
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "{}"
	}
	return string(data)
}
