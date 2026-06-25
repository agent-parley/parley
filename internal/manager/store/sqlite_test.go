package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestSecretsKeyMetaSchema(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	var tableName string
	if err := st.DB().QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'secrets_keymeta'`).Scan(&tableName); err != nil {
		t.Fatalf("secrets_keymeta table: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `INSERT INTO secrets_keymeta(id, kek_version, wrapped_dek, created_at, updated_at) VALUES (2, 1, x'00', 'now', 'now')`); err == nil {
		t.Fatal("secrets_keymeta accepted id other than 1")
	}
}

func TestStorePersistence(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "build a thing")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	persisted, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, Type: "run.created", Actor: event.Actor{Kind: event.ActorKindUser, ID: "test"}, Summary: "created", Data: map[string]any{"ok": true}})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	if persisted.Sequence != 1 || !strings.HasPrefix(persisted.ID, "evt_") {
		t.Fatalf("bad event sequence/id: %+v", persisted)
	}
	rep := report.Report{SchemaVersion: report.SchemaVersion, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: wr.Attempt.ID, StageID: wr.ImplementationStage.ID, StageType: wr.ImplementationStage.StageType, Actor: report.Actor{Kind: report.ActorKindAgent, ID: "noop"}, Status: report.StatusCompleted, Summary: "done", Payload: map[string]any{}, Errors: []string{}}
	artifact, err := st.SaveReportArtifact(ctx, rep)
	if err != nil {
		t.Fatalf("save report artifact: %v", err)
	}
	_, content, err := st.GetArtifact(ctx, artifact.ID)
	if err != nil {
		t.Fatalf("get artifact: %v", err)
	}
	if !strings.Contains(string(content), "noop") {
		t.Fatalf("artifact content missing report: %s", content)
	}
	bundle, err := st.RunBundle(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("run bundle: %v", err)
	}
	if len(bundle.Stages) != len(workflow.DefaultTemplate().Stages) || len(bundle.Events) != 1 || len(bundle.Artifacts) != 2 {
		t.Fatalf("unexpected bundle counts: stages=%d events=%d artifacts=%d", len(bundle.Stages), len(bundle.Events), len(bundle.Artifacts))
	}
}

func TestStageCanReferenceStageBriefArtifact(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "build context")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	artifact, err := st.SaveArtifact(ctx, wr.Run.ID, "stage_brief", "text/markdown", []byte("# Stage brief\n"), ".md")
	if err != nil {
		t.Fatalf("save stage brief: %v", err)
	}
	if err := st.UpdateStageBriefArtifactID(ctx, wr.ImplementationStage.ID, artifact.ID); err != nil {
		t.Fatalf("update stage brief ref: %v", err)
	}
	stages, err := st.ListStages(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("list stages: %v", err)
	}
	for _, stage := range stages {
		if stage.ID == wr.ImplementationStage.ID {
			if stage.StageBriefArtifactID != artifact.ID {
				t.Fatalf("stage brief ref = %s, want %s", stage.StageBriefArtifactID, artifact.ID)
			}
			return
		}
	}
	t.Fatal("implementation stage not found")
}

func TestConversationMessagesPersistAndTasksLink(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	conversation, err := st.EnsureProjectConversation(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}
	message, err := st.AddMessage(ctx, conversation.ID, MessageRoleUser, "Build chat from project home")
	if err != nil {
		t.Fatalf("add message: %v", err)
	}
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: message.Body, RefinementLevel: contract.RefinementLevelDirect, ConversationID: conversation.ID})
	if err != nil {
		t.Fatalf("create linked run: %v", err)
	}
	if wr.Task.ConversationID != conversation.ID {
		t.Fatalf("task conversation = %q, want %q", wr.Task.ConversationID, conversation.ID)
	}
	standalone, err := st.CreateWorkflowRun(ctx, "standalone task")
	if err != nil {
		t.Fatalf("create standalone run: %v", err)
	}
	if standalone.Task.ConversationID != "" {
		t.Fatalf("standalone task conversation = %q, want empty", standalone.Task.ConversationID)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	persistedConversation, err := st.EnsureProjectConversation(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("ensure persisted conversation: %v", err)
	}
	if persistedConversation.ID != conversation.ID {
		t.Fatalf("conversation id = %q, want %q", persistedConversation.ID, conversation.ID)
	}
	messages, err := st.ListMessagesForConversation(ctx, conversation.ID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].ID != message.ID || messages[0].Body != message.Body || messages[0].Role != MessageRoleUser {
		t.Fatalf("messages = %#v, want persisted user message", messages)
	}
	linkedTasks, err := st.ListTasksForConversation(ctx, conversation.ID)
	if err != nil {
		t.Fatalf("list linked tasks: %v", err)
	}
	if len(linkedTasks) != 1 || linkedTasks[0].ID != wr.Task.ID || linkedTasks[0].ConversationID != conversation.ID {
		t.Fatalf("linked tasks = %#v, want task %s", linkedTasks, wr.Task.ID)
	}
	loaded, err := st.GetWorkflowRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("get workflow run: %v", err)
	}
	if loaded.Task.ConversationID != conversation.ID {
		t.Fatalf("loaded task conversation = %q, want %q", loaded.Task.ConversationID, conversation.ID)
	}
}

func TestRunCoercesLegacyRefinementLevelAndStageCanReferenceTaskPlanArtifact(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "build context", RefinementLevel: "deep"})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if wr.Run.RefinementLevel != contract.RefinementLevelStandard || wr.Task.RefinementLevel != contract.RefinementLevelStandard {
		t.Fatalf("refinement not coerced on create: run=%q task=%q", wr.Run.RefinementLevel, wr.Task.RefinementLevel)
	}
	artifact, err := st.SaveArtifact(ctx, wr.Run.ID, "task_plan", "text/markdown", []byte("# Task Plan\n"), ".md")
	if err != nil {
		t.Fatalf("save task plan: %v", err)
	}
	if err := st.UpdateStageTaskPlanArtifactID(ctx, wr.IdeaIntakeStage.ID, artifact.ID); err != nil {
		t.Fatalf("update task plan ref: %v", err)
	}
	loaded, err := st.GetWorkflowRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("get workflow run: %v", err)
	}
	if loaded.Run.RefinementLevel != contract.RefinementLevelStandard || loaded.Task.RefinementLevel != contract.RefinementLevelStandard {
		t.Fatalf("refinement not loaded as standard: run=%q task=%q", loaded.Run.RefinementLevel, loaded.Task.RefinementLevel)
	}
	if loaded.IdeaIntakeStage.TaskPlanArtifactID != artifact.ID {
		t.Fatalf("task plan ref = %s, want %s", loaded.IdeaIntakeStage.TaskPlanArtifactID, artifact.ID)
	}
}

func TestWorkflowTemplatesSeedCopyEditAndRejectMidRunEdit(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	templates, err := st.ListWorkflowTemplates(ctx)
	if err != nil {
		t.Fatalf("list workflow templates: %v", err)
	}
	if len(templates) != 4 {
		t.Fatalf("template count = %d, want 4", len(templates))
	}
	if templates[0].ID != workflow.BalancedPRDeliveryID || !templates[0].Recommended {
		t.Fatalf("first template = %+v, want recommended Balanced PR", templates[0])
	}

	copyTemplate, err := st.CopyWorkflowTemplate(ctx, workflow.BalancedPRDeliveryID, "team_balanced", "Team Balanced")
	if err != nil {
		t.Fatalf("copy template: %v", err)
	}
	copyTemplate.Description = "Team editable template"
	copyTemplate.Settings["review_depth"] = "team"
	if err := st.UpdateWorkflowTemplate(ctx, copyTemplate); err != nil {
		t.Fatalf("update copied template before run: %v", err)
	}
	updated, err := st.GetWorkflowTemplate(ctx, "team_balanced")
	if err != nil {
		t.Fatalf("get copied template: %v", err)
	}
	if updated.Description != "Team editable template" || updated.Settings["review_depth"] != "team" {
		t.Fatalf("updated template not persisted: %+v", updated)
	}

	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "build with template", WorkflowTemplateID: updated.ID})
	if err != nil {
		t.Fatalf("create templated run: %v", err)
	}
	if wr.Run.WorkflowTemplateID != updated.ID {
		t.Fatalf("run template id = %s, want %s", wr.Run.WorkflowTemplateID, updated.ID)
	}
	updated.Description = "should be rejected"
	if err := st.UpdateWorkflowTemplate(ctx, updated); !errors.Is(err, ErrWorkflowTemplateInUse) {
		t.Fatalf("update active template error = %v, want ErrWorkflowTemplateInUse", err)
	}
	if err := st.UpdateRunStatus(ctx, wr.Run.ID, RunStatusCompleted); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	if err := st.UpdateWorkflowTemplate(ctx, updated); err != nil {
		t.Fatalf("update template after run completes: %v", err)
	}
}

func TestProjectWorkspaceAndNoOrphanWorkflowRecords(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	project, err := st.GetProject(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("get default project: %v", err)
	}
	for _, child := range []string{"artifacts", "drafts", "memory"} {
		if info, err := os.Stat(filepath.Join(project.WorkspacePath, child)); err != nil || !info.IsDir() {
			t.Fatalf("workspace child %s stat=%v err=%v", child, info, err)
		}
	}
	wr, err := st.CreateWorkflowRun(ctx, "project rooted")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if wr.Project.ID != DefaultProjectID || wr.Task.ProjectID != wr.Project.ID || wr.Run.ProjectID != wr.Project.ID || wr.Run.TaskID != wr.Task.ID || wr.Attempt.ProjectID != wr.Project.ID {
		t.Fatalf("workflow not project-rooted: %+v", wr)
	}
	artifact, err := st.SaveArtifact(ctx, wr.Run.ID, "note", "text/plain", []byte("private"), ".txt")
	if err != nil {
		t.Fatalf("save artifact: %v", err)
	}
	if !strings.HasPrefix(artifact.Path, filepath.Join(project.WorkspacePath, "artifacts")) {
		t.Fatalf("artifact path = %s, want under project workspace %s", artifact.Path, project.WorkspacePath)
	}
	if _, err := st.DB().ExecContext(ctx, `INSERT INTO tasks(id, project_id, idea, status, created_at, updated_at) VALUES ('task_orphan', 'missing_project', 'x', 'pending', 'now', 'now')`); err == nil {
		t.Fatal("insert orphan task succeeded, want foreign-key failure")
	}
	if _, err := st.DB().ExecContext(ctx, `INSERT INTO runs(id, project_id, task_id, idea, status, event_log_artifact_id, created_at, updated_at) VALUES ('run_orphan', ?, 'missing_task', 'x', 'pending', 'artifact_orphan', 'now', 'now')`, wr.Project.ID); err == nil {
		t.Fatal("insert orphan run succeeded, want foreign-key failure")
	}
}

func TestProjectRulesPreferencesMigrationAddsColumnsToExistingProjects(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "parley.db"))
	if err != nil {
		t.Fatalf("open legacy sqlite: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE projects (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  queue_auto_when_ready INTEGER NOT NULL DEFAULT 1,
  queue_max_concurrent INTEGER NOT NULL DEFAULT 1,
  queue_backlog_cap INTEGER NOT NULL DEFAULT 100,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
)`); err != nil {
		t.Fatalf("create legacy projects table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO projects(id, name, description, queue_auto_when_ready, queue_max_concurrent, queue_backlog_cap, created_at, updated_at) VALUES (?, ?, '', 1, 1, 100, 'legacy', 'legacy')`, DefaultProjectID, "Legacy project"); err != nil {
		t.Fatalf("insert legacy project: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy sqlite: %v", err)
	}

	st, err := Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	defer st.Close()
	project, err := st.GetProject(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("get migrated project: %v", err)
	}
	if project.ProjectRules != "" || project.ProjectPreferences != "" {
		t.Fatalf("migrated project rules/preferences = %q/%q, want empty defaults", project.ProjectRules, project.ProjectPreferences)
	}
	if !project.NotificationOnlyWhenNeeded || !project.NotificationWhenFinished {
		t.Fatalf("migrated notification prefs = %+v, want default on", project)
	}
	updated, err := st.UpdateProjectRules(ctx, DefaultProjectID, "Migrated DB accepts rules.\n")
	if err != nil {
		t.Fatalf("update migrated project rules: %v", err)
	}
	if updated.ProjectRules != "Migrated DB accepts rules.\n" {
		t.Fatalf("updated migrated project rules = %q", updated.ProjectRules)
	}
}

func TestProjectNotificationPreferencesDefaultOnAndAppState(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	prefs, err := st.GetProjectNotificationPreferences(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("get notification prefs: %v", err)
	}
	if !prefs.OnlyWhenNeeded || !prefs.WhenFinished {
		t.Fatalf("default notification prefs = %+v, want both on", prefs)
	}

	updated, err := st.UpdateProjectNotificationPreferences(ctx, DefaultProjectID, ProjectNotificationPreferences{OnlyWhenNeeded: false, WhenFinished: true})
	if err != nil {
		t.Fatalf("update notification prefs: %v", err)
	}
	if updated.NotificationOnlyWhenNeeded || !updated.NotificationWhenFinished {
		t.Fatalf("updated project notification prefs = %+v", updated)
	}

	if _, err := st.EnsureProject(ctx, DefaultProjectSpec(dataDir)); err != nil {
		t.Fatalf("ensure project after notification prefs update: %v", err)
	}
	persisted, err := st.GetProjectNotificationPreferences(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("get persisted notification prefs: %v", err)
	}
	if persisted.OnlyWhenNeeded || !persisted.WhenFinished {
		t.Fatalf("ensure project erased notification prefs: %+v", persisted)
	}
}

func TestNotificationsPersistListAndAcknowledge(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "ship notifications")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	first, err := st.InsertNotification(ctx, NotificationInput{ProjectID: wr.Project.ID, RunID: wr.Run.ID, Class: NotificationClassNeedsYou, Title: "Review needed: ship notifications"})
	if err != nil {
		t.Fatalf("insert first notification: %v", err)
	}
	if _, err := st.InsertNotification(ctx, NotificationInput{ProjectID: wr.Project.ID, RunID: wr.Run.ID, Class: NotificationClassFinished, Title: "Run completed: ship notifications"}); err != nil {
		t.Fatalf("insert second notification: %v", err)
	}
	unread, err := st.CountUnreadNotifications(ctx)
	if err != nil {
		t.Fatalf("count unread: %v", err)
	}
	if unread != 2 {
		t.Fatalf("unread = %d, want 2", unread)
	}
	items, err := st.ListNotifications(ctx, 10)
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	if len(items) != 2 || items[0].Title == "" || items[0].RunID != wr.Run.ID {
		t.Fatalf("listed notifications = %+v", items)
	}

	acked, err := st.AcknowledgeNotification(ctx, first.ID)
	if err != nil {
		t.Fatalf("ack notification: %v", err)
	}
	if acked.AcknowledgedAt == "" {
		t.Fatalf("acked notification missing acknowledged_at: %+v", acked)
	}
	unread, err = st.CountUnreadNotifications(ctx)
	if err != nil {
		t.Fatalf("count unread after ack: %v", err)
	}
	if unread != 1 {
		t.Fatalf("unread after ack = %d, want 1", unread)
	}
	if err := st.AcknowledgeAllNotifications(ctx); err != nil {
		t.Fatalf("ack all notifications: %v", err)
	}
	unread, err = st.CountUnreadNotifications(ctx)
	if err != nil {
		t.Fatalf("count unread after ack all: %v", err)
	}
	if unread != 0 {
		t.Fatalf("unread after ack all = %d, want 0", unread)
	}
}

func TestProjectRulesPreferencesCanBeUpdatedAndPromotedFromRepoCandidates(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	updated, err := st.UpdateProjectRules(ctx, DefaultProjectID, "Run targeted validation before commit.\n")
	if err != nil {
		t.Fatalf("update project rules: %v", err)
	}
	if updated.ProjectRules != "Run targeted validation before commit.\n" {
		t.Fatalf("project rules = %q", updated.ProjectRules)
	}
	rules, err := st.GetProjectRules(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("get project rules: %v", err)
	}
	if rules != updated.ProjectRules {
		t.Fatalf("get project rules = %q, want %q", rules, updated.ProjectRules)
	}

	updated, err = st.UpdateProjectPreferences(ctx, DefaultProjectID, "Prefer short reports.\n")
	if err != nil {
		t.Fatalf("update project preferences: %v", err)
	}
	if updated.ProjectPreferences != "Prefer short reports.\n" {
		t.Fatalf("project preferences = %q", updated.ProjectPreferences)
	}
	preferences, err := st.GetProjectPreferences(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("get project preferences: %v", err)
	}
	if preferences != updated.ProjectPreferences {
		t.Fatalf("get project preferences = %q, want %q", preferences, updated.ProjectPreferences)
	}

	repo := t.TempDir()
	for rel, content := range map[string]string{
		ProjectRulesCandidatePath:       "Never bypass human approval gates.\n",
		ProjectPreferencesCandidatePath: "Prefer concise status updates.\n",
	} {
		path := filepath.Join(repo, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	rulesCandidate, err := ReadProjectRulesCandidate(repo)
	if err != nil {
		t.Fatalf("read project rules candidate: %v", err)
	}
	if rulesCandidate != "Never bypass human approval gates.\n" {
		t.Fatalf("rules candidate = %q", rulesCandidate)
	}
	preferencesCandidate, err := ReadProjectPreferencesCandidate(repo)
	if err != nil {
		t.Fatalf("read project preferences candidate: %v", err)
	}
	if preferencesCandidate != "Prefer concise status updates.\n" {
		t.Fatalf("preferences candidate = %q", preferencesCandidate)
	}
	beforePromote, err := st.GetProject(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("get project before promote: %v", err)
	}
	if beforePromote.ProjectRules != "Run targeted validation before commit.\n" || beforePromote.ProjectPreferences != "Prefer short reports.\n" {
		t.Fatalf("read-only candidates changed app state before promote: %+v", beforePromote)
	}

	promotedRules, err := st.PromoteProjectRulesFromRepository(ctx, DefaultProjectID, repo)
	if err != nil {
		t.Fatalf("promote project rules: %v", err)
	}
	if promotedRules.ProjectRules != "Never bypass human approval gates.\n" {
		t.Fatalf("promoted rules = %q", promotedRules.ProjectRules)
	}
	promotedPreferences, err := st.PromoteProjectPreferencesFromRepository(ctx, DefaultProjectID, repo)
	if err != nil {
		t.Fatalf("promote project preferences: %v", err)
	}
	if promotedPreferences.ProjectPreferences != "Prefer concise status updates.\n" {
		t.Fatalf("promoted preferences = %q", promotedPreferences.ProjectPreferences)
	}

	if _, err := st.EnsureProject(ctx, DefaultProjectSpec(dataDir)); err != nil {
		t.Fatalf("ensure project after promote: %v", err)
	}
	persisted, err := st.GetProject(ctx, DefaultProjectID)
	if err != nil {
		t.Fatalf("get project after ensure: %v", err)
	}
	if persisted.ProjectRules != promotedRules.ProjectRules || persisted.ProjectPreferences != promotedPreferences.ProjectPreferences {
		t.Fatalf("ensure project erased app-state rules/preferences: %+v", persisted)
	}
}

func TestProjectMemoryIsSQLiteOnlyAndCuratorGated(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "learn from a run", WorkflowTemplateID: workflow.AutonomousPRDeliveryID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if wr.MemoryUpdateStage.ID == "" {
		t.Fatal("autonomous workflow did not persist a memory update stage")
	}
	sourceReport := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       wr.ImplementationStage.ID,
		StageType:     wr.ImplementationStage.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: "noop"},
		Status:        report.StatusCompleted,
		Summary:       "implementation found a reusable gotcha",
		Payload:       map[string]any{},
		Errors:        []string{},
	}
	sourceArtifact, err := st.SaveReportArtifact(ctx, sourceReport)
	if err != nil {
		t.Fatalf("save source report: %v", err)
	}
	valid := ProjectMemoryInput{Kind: ProjectMemoryKindGotcha, Title: "Validation image needs git", Body: "Validation containers need git available before worktree snapshots can be inspected.", SourceStageID: wr.ImplementationStage.ID, SourceArtifactID: sourceArtifact.ID, SourceSummary: "implementation report"}
	if _, err := st.ApplyProjectMemoryUpdate(ctx, ProjectMemoryUpdate{ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, CuratorStageID: wr.ImplementationStage.ID, Entries: []ProjectMemoryInput{valid}}); !errors.Is(err, ErrProjectMemoryCuratorStage) {
		t.Fatalf("non-curator memory update error = %v, want ErrProjectMemoryCuratorStage", err)
	}
	result, err := st.ApplyProjectMemoryUpdate(ctx, ProjectMemoryUpdate{ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, CuratorStageID: wr.MemoryUpdateStage.ID, Entries: []ProjectMemoryInput{
		valid,
		{Kind: ProjectMemoryKindLesson, Title: "Token leak", Body: "password=super-secret", SourceStageID: wr.ImplementationStage.ID, SourceArtifactID: sourceArtifact.ID, SourceSummary: "bad candidate"},
		{Kind: "standing_instruction", Title: "Always run broad checks", Body: "Always run every validation command", SourceStageID: wr.ImplementationStage.ID, SourceArtifactID: sourceArtifact.ID, SourceSummary: "bad candidate"},
		{Kind: "current_code_truth", Title: "Current code truth", Body: "current code truth belongs in repo evidence, not memory", SourceStageID: wr.ImplementationStage.ID, SourceArtifactID: sourceArtifact.ID, SourceSummary: "bad candidate"},
	}})
	if err != nil {
		t.Fatalf("apply memory update: %v", err)
	}
	if len(result.Entries) != 1 || len(result.Rejections) != 3 {
		t.Fatalf("memory update result entries=%d rejections=%d: %#v", len(result.Entries), len(result.Rejections), result)
	}
	entry := result.Entries[0]
	if entry.ProjectID != wr.Run.ProjectID || entry.SourceArtifactID != sourceArtifact.ID || entry.SourceStageID != wr.ImplementationStage.ID || entry.CuratorStageID != wr.MemoryUpdateStage.ID {
		t.Fatalf("memory entry is not source-linked/curator-linked: %+v", entry)
	}
	entries, err := st.ListProjectMemoryEntries(ctx, wr.Run.ProjectID)
	if err != nil {
		t.Fatalf("list memory entries: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != entry.ID {
		t.Fatalf("persisted memory entries = %#v, want %s", entries, entry.ID)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "projects", DefaultProjectID, "workspace", "memory.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("project memory file stat err = %v, want not exist", err)
	}
}

func TestRunlessRunnerEventPersistsWithNullRunIDAndScopedSequence(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	first, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, Type: "runner.registered", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "registered", Data: map[string]any{"runner_id": "runner_a"}})
	if err != nil {
		t.Fatalf("append first runner event: %v", err)
	}
	second, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, Type: "runner.ready", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "ready", Data: map[string]any{"runner_id": "runner_a"}})
	if err != nil {
		t.Fatalf("append second runner event: %v", err)
	}
	other, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, Type: "runner.registered", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "registered", Data: map[string]any{"runner_id": "runner_b"}})
	if err != nil {
		t.Fatalf("append other runner event: %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 2 || other.Sequence != 1 {
		t.Fatalf("sequences = %d,%d,%d; want 1,2,1", first.Sequence, second.Sequence, other.Sequence)
	}
	var nullRunID int
	if err := st.DB().QueryRowContext(ctx, `SELECT run_id IS NULL FROM events WHERE id = ?`, first.ID).Scan(&nullRunID); err != nil {
		t.Fatalf("query null run_id: %v", err)
	}
	if nullRunID != 1 {
		t.Fatal("runner event run_id is not NULL")
	}
	events, err := st.ListRunnerEvents(ctx, "runner_a")
	if err != nil {
		t.Fatalf("list runner events: %v", err)
	}
	if len(events) != 2 || events[0].RunID != "" || events[1].Type != "runner.ready" {
		t.Fatalf("unexpected runner events: %#v", events)
	}
}

func TestSystemEventsUseAppendOrderNotPerScopeSequenceOrID(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	first, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, ID: "evt_z", Timestamp: "2026-06-04T00:00:00Z", Type: "runner.registered", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "registered", Data: map[string]any{"runner_id": "runner_a"}})
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	second, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, ID: "evt_a", Timestamp: "2026-06-04T00:00:00Z", Type: "runner.ready", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "ready", Data: map[string]any{"runner_id": "runner_b"}})
	if err != nil {
		t.Fatalf("append second: %v", err)
	}
	third, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, ID: "evt_m", Timestamp: "2026-06-04T00:00:00Z", Type: "runner.down", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "down", Data: map[string]any{"runner_id": "runner_c"}})
	if err != nil {
		t.Fatalf("append third: %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 1 || third.Sequence != 1 {
		t.Fatalf("per-runner sequences = %d,%d,%d; want all 1", first.Sequence, second.Sequence, third.Sequence)
	}
	page, err := st.ListSystemEventsPage(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list system events: %v", err)
	}
	if len(page.Events) != 3 || page.Events[0].Event.ID != first.ID || page.Events[1].Event.ID != second.ID || page.Events[2].Event.ID != third.ID {
		t.Fatalf("system events = %#v, want append order %s, %s, %s", page.Events, first.ID, second.ID, third.ID)
	}

	latest, err := st.ListSystemEventsPage(ctx, 0, 2)
	if err != nil {
		t.Fatalf("list latest system events: %v", err)
	}
	if !latest.HasOlder || latest.OlderCursor == 0 {
		t.Fatalf("latest page cursor = %d hasOlder=%v, want older cursor", latest.OlderCursor, latest.HasOlder)
	}
	if len(latest.Events) != 2 || latest.Events[0].Event.ID != second.ID || latest.Events[1].Event.ID != third.ID {
		t.Fatalf("latest page = %#v, want %s then %s", latest.Events, second.ID, third.ID)
	}
	older, err := st.ListSystemEventsPage(ctx, latest.OlderCursor, 2)
	if err != nil {
		t.Fatalf("list older system events: %v", err)
	}
	if older.HasOlder || len(older.Events) != 1 || older.Events[0].Event.ID != first.ID {
		t.Fatalf("older page = %#v hasOlder=%v, want only %s", older.Events, older.HasOlder, first.ID)
	}
}

func TestMigrateLegacyEventsBackfillsRunScope(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	artifactDir := filepath.Join(dataDir, "artifacts")
	if err := os.MkdirAll(artifactDir, 0o700); err != nil {
		t.Fatalf("mkdir artifacts: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "parley.db"))
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	legacy := `
CREATE TABLE runs (id TEXT PRIMARY KEY, idea TEXT NOT NULL, status TEXT NOT NULL, event_log_artifact_id TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE tasks (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), idea TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE attempts (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), task_id TEXT NOT NULL REFERENCES tasks(id), status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE stages (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), task_id TEXT NOT NULL REFERENCES tasks(id), attempt_id TEXT NOT NULL REFERENCES attempts(id), stage_type TEXT NOT NULL, adapter TEXT, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE workflow_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL REFERENCES runs(id), snapshot_json TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE artifacts (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), kind TEXT NOT NULL, media_type TEXT NOT NULL, path TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE events (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), sequence INTEGER NOT NULL, timestamp TEXT NOT NULL, task_id TEXT NOT NULL, attempt_id TEXT NOT NULL, type TEXT NOT NULL, actor_kind TEXT NOT NULL, actor_id TEXT NOT NULL, summary TEXT NOT NULL, data_json TEXT NOT NULL, envelope_json TEXT NOT NULL, UNIQUE(run_id, sequence));
CREATE INDEX idx_events_run_sequence ON events(run_id, sequence);
INSERT INTO runs(id, idea, status, event_log_artifact_id, created_at, updated_at) VALUES ('run_legacy', 'idea', 'running', 'artifact_log', '2026-06-04T00:00:00Z', '2026-06-04T00:00:00Z');
INSERT INTO tasks(id, run_id, idea, status, created_at, updated_at) VALUES ('task_legacy', 'run_legacy', 'idea', 'running', '2026-06-04T00:00:00Z', '2026-06-04T00:00:00Z');
INSERT INTO attempts(id, run_id, task_id, status, created_at, updated_at) VALUES ('attempt_legacy', 'run_legacy', 'task_legacy', 'running', '2026-06-04T00:00:00Z', '2026-06-04T00:00:00Z');
INSERT INTO stages(id, run_id, task_id, attempt_id, stage_type, adapter, status, created_at, updated_at) VALUES ('stage_legacy', 'run_legacy', 'task_legacy', 'attempt_legacy', 'implementation', 'noop', 'running', '2026-06-04T00:00:00Z', '2026-06-04T00:00:00Z');
INSERT INTO workflow_snapshots(run_id, snapshot_json, created_at) VALUES ('run_legacy', '{}', '2026-06-04T00:00:00Z');
INSERT INTO artifacts(id, run_id, kind, media_type, path, created_at) VALUES ('artifact_log', 'run_legacy', 'event_log', 'application/x-jsonlines', '` + filepath.Join(artifactDir, "artifact_log.jsonl") + `', '2026-06-04T00:00:00Z');
INSERT INTO events(id, run_id, sequence, timestamp, task_id, attempt_id, type, actor_kind, actor_id, summary, data_json, envelope_json) VALUES ('evt_legacy', 'run_legacy', 1, '2026-06-04T00:00:00Z', 'task_legacy', 'attempt_legacy', 'run.created', 'user', 'test', 'created', '{}', '{"schema_version":1,"id":"evt_legacy","sequence":1,"timestamp":"2026-06-04T00:00:00Z","run_id":"run_legacy","task_id":"task_legacy","attempt_id":"attempt_legacy","type":"run.created","actor":{"kind":"user","id":"test"},"summary":"created","data":{}}');
`
	if _, err := db.ExecContext(ctx, legacy); err != nil {
		_ = db.Close()
		t.Fatalf("seed legacy db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy db: %v", err)
	}

	st, err := Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	defer st.Close()
	var scope string
	var runIDNotNull int
	if err := st.DB().QueryRowContext(ctx, `SELECT scope, run_id IS NOT NULL FROM events WHERE id = 'evt_legacy'`).Scan(&scope, &runIDNotNull); err != nil {
		t.Fatalf("read migrated event row: %v", err)
	}
	if scope != "run:run_legacy" || runIDNotNull != 1 {
		t.Fatalf("migrated scope/run_id = %q/%d, want run:run_legacy/non-null", scope, runIDNotNull)
	}
	persisted, err := st.AppendEvent(ctx, event.Event{SchemaVersion: event.SchemaVersion, RunID: "run_legacy", TaskID: "task_legacy", AttemptID: "attempt_legacy", Type: "run.started", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "started", Data: map[string]any{}})
	if err != nil {
		t.Fatalf("append after migration: %v", err)
	}
	if persisted.Sequence != 2 {
		t.Fatalf("post-migration sequence = %d, want 2", persisted.Sequence)
	}
}

func TestGetWorkflowRunSelectsLatestAttemptStages(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "retry a thing")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	later := "2099-01-01T00:00:00Z"
	attemptID := "attempt_later"
	ideaStageID := "stage_idea_later"
	implStageID := "stage_impl_later"
	validationStageID := "stage_validation_later"
	commitStageID := "stage_commit_later"
	prReadyStageID := "stage_pr_ready_later"
	if _, err := st.DB().ExecContext(ctx, `INSERT INTO attempts(id, project_id, run_id, task_id, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, attemptID, wr.Run.ProjectID, wr.Run.ID, wr.Task.ID, RunStatusPending, later, later); err != nil {
		t.Fatalf("insert later attempt: %v", err)
	}
	for _, stage := range []Stage{
		{ID: ideaStageID, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypeIdeaIntake, Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
		{ID: implStageID, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypeImplementation, Adapter: "noop", Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
		{ID: validationStageID, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypeValidation, Adapter: "validation", Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
		{ID: commitStageID, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypeCommit, Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
		{ID: prReadyStageID, ProjectID: wr.Run.ProjectID, RunID: wr.Run.ID, TaskID: wr.Task.ID, AttemptID: attemptID, StageType: contract.StageTypePRReady, Status: StageStatusPending, CreatedAt: later, UpdatedAt: later},
	} {
		if _, err := st.DB().ExecContext(ctx, `INSERT INTO stages(id, project_id, run_id, task_id, attempt_id, stage_type, adapter, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, stage.ID, stage.ProjectID, stage.RunID, stage.TaskID, stage.AttemptID, stage.StageType, stage.Adapter, stage.Status, stage.CreatedAt, stage.UpdatedAt); err != nil {
			t.Fatalf("insert later stage %s: %v", stage.ID, err)
		}
	}

	got, err := st.GetWorkflowRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("GetWorkflowRun() error = %v", err)
	}
	if got.Attempt.ID != attemptID {
		t.Fatalf("Attempt.ID = %q, want %q", got.Attempt.ID, attemptID)
	}
	if got.IdeaIntakeStage.ID != ideaStageID || got.IdeaIntakeStage.AttemptID != attemptID {
		t.Fatalf("IdeaIntakeStage = %+v, want latest attempt stage", got.IdeaIntakeStage)
	}
	if got.ImplementationStage.ID != implStageID || got.ImplementationStage.AttemptID != attemptID {
		t.Fatalf("ImplementationStage = %+v, want latest attempt stage", got.ImplementationStage)
	}
	if got.ValidationStage.ID != validationStageID || got.ValidationStage.AttemptID != attemptID {
		t.Fatalf("ValidationStage = %+v, want latest attempt stage", got.ValidationStage)
	}
	if got.CommitStage.ID != commitStageID || got.CommitStage.AttemptID != attemptID {
		t.Fatalf("CommitStage = %+v, want latest attempt stage", got.CommitStage)
	}
	if got.PRReadyStage.ID != prReadyStageID || got.PRReadyStage.AttemptID != attemptID {
		t.Fatalf("PRReadyStage = %+v, want latest attempt stage", got.PRReadyStage)
	}
}

func TestUpdateRunStatusFromAndAppendSystemEventIsAtomic(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	wr, err := st.CreateWorkflowRun(ctx, "dispatch atomically")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	ev, changed, err := st.UpdateRunStatusFromAndAppendSystemEvent(ctx, wr.Run.ID, RunStatusPending, RunStatusRunning, event.Event{Type: "queue.dispatched", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "queued run dispatched", Data: map[string]any{"run_id": wr.Run.ID}})
	if err != nil {
		t.Fatalf("transition with event: %v", err)
	}
	if !changed || ev.Sequence != 1 || ev.RunID != "" {
		t.Fatalf("event=%+v changed=%v, want system event sequence 1 with changed=true", ev, changed)
	}
	run, err := st.GetRun(ctx, wr.Run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != RunStatusRunning {
		t.Fatalf("status = %s, want running", run.Status)
	}
	_, changed, err = st.UpdateRunStatusFromAndAppendSystemEvent(ctx, wr.Run.ID, RunStatusPending, RunStatusRunning, event.Event{Type: "queue.dispatched", Actor: event.Actor{Kind: event.ActorKindWorkflowEngine, ID: "manager"}, Summary: "should not persist", Data: map[string]any{"run_id": wr.Run.ID}})
	if err != nil {
		t.Fatalf("unchanged transition: %v", err)
	}
	if changed {
		t.Fatal("changed=true for stale pending->running transition")
	}
	page, err := st.ListSystemEventsPage(ctx, 0, 20)
	if err != nil {
		t.Fatalf("list system events: %v", err)
	}
	if len(page.Events) != 1 || page.Events[0].Event.Type != "queue.dispatched" {
		t.Fatalf("system events = %#v, want exactly one queue.dispatched", page.Events)
	}
}
