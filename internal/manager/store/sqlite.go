package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/ids"
)

//go:embed schema.sql
var schemaFS embed.FS

const (
	DefaultProjectID    = "default"
	DefaultRepositoryID = "repo_default"

	RunStatusPending       = "pending"
	RunStatusRunning       = "running"
	RunStatusCompleted     = "completed"
	RunStatusFailed        = "failed"
	RunStatusInvalid       = "invalid"
	RunStatusNeedsInput    = "needs_input"
	RunStatusAwaitingHuman = "awaiting_human"
	RunStatusCancelled     = "cancelled"

	StageStatusPending = "pending"
	StageStatusRunning = "running"

	RunnerStatusConnected = "connected"
	RunnerStatusSuspect   = "suspect"
	RunnerStatusDown      = "down"

	RunnerOriginSpawned    = "spawned"
	RunnerOriginRegistered = "registered"

	MessageRoleUser      = "user"
	MessageRoleAssistant = "assistant"
	MessageRoleSystem    = "system"

	ProjectMemoryKindLesson                 = "lesson"
	ProjectMemoryKindRepoFact               = "repo_fact"
	ProjectMemoryKindGotcha                 = "gotcha"
	ProjectMemoryKindImplementationLandmark = "implementation_landmark"
	ProjectMemoryKindPriorResult            = "prior_result"
	ProjectMemoryKindDecision               = "decision"
	ProjectMemoryKindFreshnessNote          = "freshness_note"

	ProjectMemoryMaxEntriesPerUpdate = 20
	ProjectMemoryMaxKindRunes        = 64
	ProjectMemoryMaxTitleRunes       = 160
	ProjectMemoryMaxBodyRunes        = 2000
	ProjectMemoryMaxSourceRunes      = 500
	ProjectMemoryExportDir           = ".parley/memory"
)

var (
	ErrWorkflowTemplateInUse       = errors.New("workflow template is used by an active run")
	ErrWorkflowTemplateNotEditable = errors.New("workflow template is not editable")
	ErrProjectMemoryCuratorStage   = errors.New("project memory writes require a memory_update curator stage")
)

type Store struct {
	db      *sql.DB
	dataDir string
	mu      sync.Mutex
}

type ProjectSpec struct {
	ID                 string
	Name               string
	Description        string
	WorkspacePath      string
	RepositoryPath     string
	QueueAutoWhenReady bool
	QueueMaxConcurrent int
	QueueBacklogCap    int
}

type Project struct {
	ID                         string
	Name                       string
	Description                string
	ProjectRules               string
	ProjectPreferences         string
	WorkflowTemplateDefaultID  string
	WorkflowTemplateSmallFixID string
	NotificationOnlyWhenNeeded bool
	NotificationWhenFinished   bool
	WorkspacePath              string
	QueueAutoWhenReady         bool
	QueueMaxConcurrent         int
	QueueBacklogCap            int
	CreatedAt                  string
	UpdatedAt                  string
}

type Workspace struct {
	ProjectID string
	Path      string
	CreatedAt string
	UpdatedAt string
}

type Repository struct {
	ID        string
	ProjectID string
	Name      string
	Path      string
	IsDefault bool
	CreatedAt string
	UpdatedAt string
}

type Conversation struct {
	ID        string
	ProjectID string
	Title     string
	CreatedAt string
	UpdatedAt string
}

type Message struct {
	ID             string
	ProjectID      string
	ConversationID string
	Role           string
	Body           string
	CreatedAt      string
}

type Run struct {
	ID                 string
	ProjectID          string
	TaskID             string
	Idea               string
	RefinementLevel    string
	WorkflowTemplateID string
	Status             string
	EventLogArtifactID string
	CreatedAt          string
	UpdatedAt          string
}

type Task struct {
	ID              string
	ProjectID       string
	RepositoryID    string
	ConversationID  string
	Idea            string
	RefinementLevel string
	Status          string
	CreatedAt       string
	UpdatedAt       string
}

type Attempt struct {
	ID        string
	ProjectID string
	RunID     string
	TaskID    string
	Status    string
	CreatedAt string
	UpdatedAt string
}

type Stage struct {
	ID                   string
	ProjectID            string
	RunID                string
	TaskID               string
	AttemptID            string
	WorkflowStageID      string
	StageType            string
	Adapter              string
	Status               string
	StageBriefArtifactID string
	TaskPlanArtifactID   string
	CreatedAt            string
	UpdatedAt            string
}

type Artifact struct {
	ID        string
	ProjectID string
	RunID     string
	TaskID    string
	Kind      string
	MediaType string
	Path      string
	CreatedAt string
}

type ProjectMemoryEntry struct {
	ID               string
	ProjectID        string
	Kind             string
	Title            string
	Body             string
	SourceRunID      string
	SourceTaskID     string
	SourceStageID    string
	SourceArtifactID string
	CuratorStageID   string
	SourceSummary    string
	CreatedAt        string
	UpdatedAt        string
}

type ProjectMemoryInput struct {
	Kind             string
	Title            string
	Body             string
	SourceStageID    string
	SourceArtifactID string
	SourceSummary    string
}

type ProjectMemoryUpdate struct {
	ProjectID      string
	RunID          string
	TaskID         string
	CuratorStageID string
	Entries        []ProjectMemoryInput
}

type ProjectMemoryRejection struct {
	Title            string `json:"title"`
	Reason           string `json:"reason"`
	SourceStageID    string `json:"source_stage_id,omitempty"`
	SourceArtifactID string `json:"source_artifact_id,omitempty"`
}

type ProjectMemoryUpdateResult struct {
	Entries    []ProjectMemoryEntry
	Rejections []ProjectMemoryRejection
	Outcomes   []ProjectMemoryWriteOutcome
}

type ProjectMemoryWriteOutcome struct {
	Entry     *ProjectMemoryEntry     `json:"entry,omitempty"`
	Rejection *ProjectMemoryRejection `json:"rejection,omitempty"`
}

type ProjectMemoryExportRequest struct {
	ProjectID      string
	RepositoryPath string
	EntryIDs       []string
}

type ProjectMemoryExportFile struct {
	EntryID      string
	RelativePath string
	Path         string
	Sanitized    bool
}

type ProjectMemoryExportResult struct {
	Files []ProjectMemoryExportFile
}

type Runner struct {
	ID               string
	Status           string
	Origin           string
	CapabilitiesJSON string
	MissedHeartbeats int
	ConnectedAt      string
	UpdatedAt        string
}

type SystemEvent struct {
	Cursor int64
	Event  event.Event
}

type SystemEventPage struct {
	Events      []SystemEvent
	OlderCursor int64
	HasOlder    bool
	Limit       int
}

type WorkflowRun struct {
	Project             Project
	Run                 Run
	Task                Task
	Attempt             Attempt
	IdeaIntakeStage     Stage
	ImplementationStage Stage
	ValidationStage     Stage
	CommitStage         Stage
	PRReadyStage        Stage
	MemoryUpdateStage   Stage
}

type RunBundle struct {
	Project   Project
	Run       Run
	Task      Task
	Attempt   Attempt
	Stages    []Stage
	Events    []event.Event
	Artifacts []Artifact
}

func Open(ctx context.Context, dataDir string) (*Store, error) {
	if dataDir == "" {
		dataDir = ".parley-data"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "parley.db"))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable sqlite foreign keys: %w", err)
	}
	st := &Store{db: db, dataDir: dataDir}
	if err := st.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) DataDir() string { return s.dataDir }

func (s *Store) migrate(ctx context.Context) error {
	schema, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, schemaWithoutIndexes(string(schema))); err != nil {
		return fmt.Errorf("migrate sqlite: %w", err)
	}
	if err := s.ensureWorkflowTemplates(ctx); err != nil {
		return err
	}
	if err := s.ensureProjectRulesPreferencesSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureProjectWorkflowTemplatePolicySchema(ctx); err != nil {
		return err
	}
	if err := s.ensureProjectNotificationPreferencesSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureAgentRegistrySchema(ctx); err != nil {
		return err
	}
	if err := s.ensureDefaultProject(ctx); err != nil {
		return err
	}
	if err := s.ensureEventsSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureProjectWorkflowSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureConversationSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureRefinementSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureWorkflowTemplateRefSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureWorkflowStageSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureStageBriefSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureTaskPlanSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureRunnerRegistrySchema(ctx); err != nil {
		return err
	}
	if err := s.ensureSecretsSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureNotificationSinksSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureForgeCredentialsSchema(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, string(schema)); err != nil {
		return fmt.Errorf("refresh sqlite indexes: %w", err)
	}
	return s.checkForeignKeys(ctx)
}

type sqliteColumn struct {
	Name    string
	NotNull bool
}

func (s *Store) tableColumns(ctx context.Context, table string) (map[string]sqliteColumn, error) {
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return nil, fmt.Errorf("pragma table_info %s: %w", table, err)
	}
	defer rows.Close()
	cols := map[string]sqliteColumn{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &defaultValue, &pk); err != nil {
			return nil, fmt.Errorf("scan table_info %s: %w", table, err)
		}
		cols[name] = sqliteColumn{Name: name, NotNull: notnull != 0}
	}
	return cols, rows.Err()
}

func (s *Store) ensureDefaultProject(ctx context.Context) error {
	_, err := s.EnsureProject(ctx, DefaultProjectSpec(s.dataDir))
	return err
}

func (s *Store) ensureProjectRulesPreferencesSchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "projects")
	if err != nil {
		return err
	}
	if _, ok := cols["project_rules"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE projects ADD COLUMN project_rules TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("add project rules column: %w", err)
		}
	}
	if _, ok := cols["project_preferences"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE projects ADD COLUMN project_preferences TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("add project preferences column: %w", err)
		}
	}
	return nil
}

func (s *Store) ensureProjectWorkflowTemplatePolicySchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "projects")
	if err != nil {
		return err
	}
	if _, ok := cols["workflow_template_default_id"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE projects ADD COLUMN workflow_template_default_id TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("add workflow template default column: %w", err)
		}
	}
	if _, ok := cols["workflow_template_small_fix_id"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE projects ADD COLUMN workflow_template_small_fix_id TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("add workflow template small-fix column: %w", err)
		}
	}
	return nil
}

func (s *Store) ensureProjectNotificationPreferencesSchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "projects")
	if err != nil {
		return err
	}
	if _, ok := cols["notification_only_when_needed"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE projects ADD COLUMN notification_only_when_needed INTEGER NOT NULL DEFAULT 1`); err != nil {
			return fmt.Errorf("add notification only-when-needed column: %w", err)
		}
	}
	if _, ok := cols["notification_when_finished"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE projects ADD COLUMN notification_when_finished INTEGER NOT NULL DEFAULT 1`); err != nil {
			return fmt.Errorf("add notification when-finished column: %w", err)
		}
	}
	return nil
}

func (s *Store) ensureAgentRegistrySchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS agent_registry_overrides (
		scope TEXT PRIMARY KEY CHECK (scope = 'global'),
		overrides_json TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("create agent registry overrides table: %w", err)
	}
	cols, err := s.tableColumns(ctx, "projects")
	if err != nil {
		return err
	}
	if _, ok := cols["agent_registry_overrides_json"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE projects ADD COLUMN agent_registry_overrides_json TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("add project agent registry overrides column: %w", err)
		}
	}
	return nil
}

func (s *Store) ensureNotificationSinksSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS notification_sinks (
		id TEXT PRIMARY KEY,
		type TEXT NOT NULL CHECK (type IN ('gotify', 'webhook')),
		enabled INTEGER NOT NULL DEFAULT 1,
		base_url TEXT NOT NULL DEFAULT '',
		url TEXT NOT NULL DEFAULT '',
		http_method TEXT NOT NULL DEFAULT 'POST',
		priority INTEGER NOT NULL DEFAULT 5,
		secret_ciphertext BLOB NOT NULL,
		allow_insecure_http INTEGER NOT NULL DEFAULT 0,
		send_needs_you INTEGER NOT NULL DEFAULT 1,
		send_finished INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("create notification sinks table: %w", err)
	}
	cols, err := s.tableColumns(ctx, "notification_sinks")
	if err != nil {
		return err
	}
	if _, ok := cols["allow_insecure_http"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE notification_sinks ADD COLUMN allow_insecure_http INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("add notification sink allow-insecure-http column: %w", err)
		}
	}
	if _, ok := cols["send_needs_you"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE notification_sinks ADD COLUMN send_needs_you INTEGER NOT NULL DEFAULT 1`); err != nil {
			return fmt.Errorf("add notification sink needs-you class column: %w", err)
		}
	}
	if _, ok := cols["send_finished"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE notification_sinks ADD COLUMN send_finished INTEGER NOT NULL DEFAULT 1`); err != nil {
			return fmt.Errorf("add notification sink finished class column: %w", err)
		}
	}
	return nil
}

func (s *Store) ensureForgeCredentialsSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS forge_credentials (
		id TEXT PRIMARY KEY,
		host TEXT NOT NULL DEFAULT 'github.com',
		secret_ciphertext BLOB NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("create forge credentials table: %w", err)
	}
	cols, err := s.tableColumns(ctx, "forge_credentials")
	if err != nil {
		return err
	}
	if _, ok := cols["host"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE forge_credentials ADD COLUMN host TEXT NOT NULL DEFAULT 'github.com'`); err != nil {
			return fmt.Errorf("add forge credential host column: %w", err)
		}
	}
	return nil
}

func DefaultProjectSpec(dataDir string) ProjectSpec {
	return ProjectSpec{
		ID:                 DefaultProjectID,
		Name:               "Default project",
		WorkspacePath:      defaultWorkspacePath(dataDir, DefaultProjectID),
		QueueAutoWhenReady: true,
		QueueMaxConcurrent: 1,
		QueueBacklogCap:    100,
	}
}

func (s *Store) EnsureProject(ctx context.Context, spec ProjectSpec) (Project, error) {
	spec = s.normalizeProjectSpec(spec)
	if err := ensureWorkspaceDirs(spec.WorkspacePath); err != nil {
		return Project{}, err
	}
	now := nowRFC3339()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Project{}, fmt.Errorf("begin ensure project: %w", err)
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `INSERT INTO projects(id, name, description, queue_auto_when_ready, queue_max_concurrent, queue_backlog_cap, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET name = excluded.name, description = excluded.description, queue_auto_when_ready = excluded.queue_auto_when_ready, queue_max_concurrent = excluded.queue_max_concurrent, queue_backlog_cap = excluded.queue_backlog_cap, updated_at = excluded.updated_at`, spec.ID, spec.Name, spec.Description, boolInt(spec.QueueAutoWhenReady), spec.QueueMaxConcurrent, spec.QueueBacklogCap, now, now); err != nil {
		return Project{}, fmt.Errorf("upsert project: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO workspaces(project_id, path, created_at, updated_at) VALUES (?, ?, ?, ?)
ON CONFLICT(project_id) DO UPDATE SET path = excluded.path, updated_at = excluded.updated_at`, spec.ID, spec.WorkspacePath, now, now); err != nil {
		return Project{}, fmt.Errorf("upsert workspace: %w", err)
	}
	if strings.TrimSpace(spec.RepositoryPath) != "" {
		repoPath := filepath.Clean(spec.RepositoryPath)
		repositoryID := defaultRepositoryID(spec.ID)
		if _, err := tx.ExecContext(ctx, `INSERT INTO repositories(id, project_id, name, path, is_default, created_at, updated_at) VALUES (?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(id) DO UPDATE SET project_id = excluded.project_id, path = excluded.path, is_default = 1, updated_at = excluded.updated_at`, repositoryID, spec.ID, "Default repository", repoPath, now, now); err != nil {
			return Project{}, fmt.Errorf("upsert default repository: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Project{}, fmt.Errorf("commit ensure project: %w", err)
	}
	return s.GetProject(ctx, spec.ID)
}

func (s *Store) normalizeProjectSpec(spec ProjectSpec) ProjectSpec {
	if strings.TrimSpace(spec.ID) == "" {
		spec.ID = DefaultProjectID
	}
	if strings.TrimSpace(spec.Name) == "" {
		spec.Name = "Default project"
	}
	if strings.TrimSpace(spec.WorkspacePath) == "" {
		spec.WorkspacePath = defaultWorkspacePath(s.dataDir, spec.ID)
	}
	if spec.QueueMaxConcurrent < 1 {
		spec.QueueMaxConcurrent = 1
	}
	if spec.QueueBacklogCap < 1 {
		spec.QueueBacklogCap = 100
	}
	return spec
}

func (s *Store) GetProject(ctx context.Context, projectID string) (Project, error) {
	var project Project
	var auto, notifyNeeded, notifyFinished int
	err := s.db.QueryRowContext(ctx, `SELECT p.id, p.name, p.description, p.project_rules, p.project_preferences, p.workflow_template_default_id, p.workflow_template_small_fix_id, p.notification_only_when_needed, p.notification_when_finished, w.path, p.queue_auto_when_ready, p.queue_max_concurrent, p.queue_backlog_cap, p.created_at, p.updated_at
FROM projects p JOIN workspaces w ON w.project_id = p.id WHERE p.id = ?`, projectID).Scan(&project.ID, &project.Name, &project.Description, &project.ProjectRules, &project.ProjectPreferences, &project.WorkflowTemplateDefaultID, &project.WorkflowTemplateSmallFixID, &notifyNeeded, &notifyFinished, &project.WorkspacePath, &auto, &project.QueueMaxConcurrent, &project.QueueBacklogCap, &project.CreatedAt, &project.UpdatedAt)
	if err != nil {
		return Project{}, fmt.Errorf("get project %s: %w", projectID, err)
	}
	project.NotificationOnlyWhenNeeded = notifyNeeded != 0
	project.NotificationWhenFinished = notifyFinished != 0
	project.QueueAutoWhenReady = auto != 0
	return project, nil
}

func (s *Store) ListProjects(ctx context.Context) ([]Project, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT p.id, p.name, p.description, p.project_rules, p.project_preferences, p.workflow_template_default_id, p.workflow_template_small_fix_id, p.notification_only_when_needed, p.notification_when_finished, w.path, p.queue_auto_when_ready, p.queue_max_concurrent, p.queue_backlog_cap, p.created_at, p.updated_at
FROM projects p JOIN workspaces w ON w.project_id = p.id ORDER BY p.created_at DESC, p.id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()
	var projects []Project
	for rows.Next() {
		var project Project
		var auto, notifyNeeded, notifyFinished int
		if err := rows.Scan(&project.ID, &project.Name, &project.Description, &project.ProjectRules, &project.ProjectPreferences, &project.WorkflowTemplateDefaultID, &project.WorkflowTemplateSmallFixID, &notifyNeeded, &notifyFinished, &project.WorkspacePath, &auto, &project.QueueMaxConcurrent, &project.QueueBacklogCap, &project.CreatedAt, &project.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan project: %w", err)
		}
		project.NotificationOnlyWhenNeeded = notifyNeeded != 0
		project.NotificationWhenFinished = notifyFinished != 0
		project.QueueAutoWhenReady = auto != 0
		projects = append(projects, project)
	}
	return projects, rows.Err()
}

func (s *Store) DefaultRepositoryID(ctx context.Context, projectID string) (string, error) {
	var repositoryID string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM repositories WHERE project_id = ? AND is_default = 1 ORDER BY created_at ASC, id ASC LIMIT 1`, projectID).Scan(&repositoryID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("get default repository: %w", err)
	}
	return repositoryID, nil
}

func (s *Store) GetRepository(ctx context.Context, repositoryID string) (Repository, error) {
	var repo Repository
	var isDefault int
	err := s.db.QueryRowContext(ctx, `SELECT id, project_id, name, path, is_default, created_at, updated_at FROM repositories WHERE id = ?`, repositoryID).Scan(&repo.ID, &repo.ProjectID, &repo.Name, &repo.Path, &isDefault, &repo.CreatedAt, &repo.UpdatedAt)
	if err != nil {
		return Repository{}, fmt.Errorf("get repository %s: %w", repositoryID, err)
	}
	repo.IsDefault = isDefault != 0
	return repo, nil
}

func (s *Store) CreateConversation(ctx context.Context, projectID, title string) (Conversation, error) {
	projectID = normalizeProjectID(projectID)
	if _, err := s.GetProject(ctx, projectID); err != nil {
		return Conversation{}, err
	}
	title = strings.TrimSpace(title)
	if title == "" {
		title = "Project chat"
	}
	now := nowRFC3339()
	conversation := Conversation{ID: ids.New("conv"), ProjectID: projectID, Title: title, CreatedAt: now, UpdatedAt: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO conversations(id, project_id, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`, conversation.ID, conversation.ProjectID, conversation.Title, now, now); err != nil {
		return Conversation{}, fmt.Errorf("insert conversation: %w", err)
	}
	return conversation, nil
}

func (s *Store) EnsureProjectConversation(ctx context.Context, projectID string) (Conversation, error) {
	projectID = normalizeProjectID(projectID)
	conversation, err := s.FirstConversationForProject(ctx, projectID)
	if err == nil {
		return conversation, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Conversation{}, err
	}
	return s.CreateConversation(ctx, projectID, "Project chat")
}

func (s *Store) FirstConversationForProject(ctx context.Context, projectID string) (Conversation, error) {
	projectID = normalizeProjectID(projectID)
	var conversation Conversation
	err := s.db.QueryRowContext(ctx, `SELECT id, project_id, title, created_at, updated_at FROM conversations WHERE project_id = ? ORDER BY created_at ASC, id ASC LIMIT 1`, projectID).Scan(&conversation.ID, &conversation.ProjectID, &conversation.Title, &conversation.CreatedAt, &conversation.UpdatedAt)
	if err != nil {
		return Conversation{}, fmt.Errorf("get project conversation: %w", err)
	}
	return conversation, nil
}

func (s *Store) GetConversation(ctx context.Context, conversationID string) (Conversation, error) {
	conversationID = strings.TrimSpace(conversationID)
	var conversation Conversation
	err := s.db.QueryRowContext(ctx, `SELECT id, project_id, title, created_at, updated_at FROM conversations WHERE id = ?`, conversationID).Scan(&conversation.ID, &conversation.ProjectID, &conversation.Title, &conversation.CreatedAt, &conversation.UpdatedAt)
	if err != nil {
		return Conversation{}, fmt.Errorf("get conversation %s: %w", conversationID, err)
	}
	return conversation, nil
}

func (s *Store) ListConversationsForProject(ctx context.Context, projectID string) ([]Conversation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, title, created_at, updated_at FROM conversations WHERE project_id = ? ORDER BY created_at ASC, id ASC`, normalizeProjectID(projectID))
	if err != nil {
		return nil, fmt.Errorf("list conversations: %w", err)
	}
	defer rows.Close()
	var conversations []Conversation
	for rows.Next() {
		var conversation Conversation
		if err := rows.Scan(&conversation.ID, &conversation.ProjectID, &conversation.Title, &conversation.CreatedAt, &conversation.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan conversation: %w", err)
		}
		conversations = append(conversations, conversation)
	}
	return conversations, rows.Err()
}

func (s *Store) AddMessage(ctx context.Context, conversationID, role, body string) (Message, error) {
	conversation, err := s.GetConversation(ctx, conversationID)
	if err != nil {
		return Message{}, err
	}
	role = strings.ToLower(strings.TrimSpace(role))
	if role == "" {
		role = MessageRoleUser
	}
	body = strings.TrimSpace(body)
	if body == "" {
		return Message{}, fmt.Errorf("message body is required")
	}
	now := nowRFC3339()
	message := Message{ID: ids.New("msg"), ProjectID: conversation.ProjectID, ConversationID: conversation.ID, Role: role, Body: body, CreatedAt: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO messages(id, project_id, conversation_id, role, body, created_at) VALUES (?, ?, ?, ?, ?, ?)`, message.ID, message.ProjectID, message.ConversationID, message.Role, message.Body, message.CreatedAt); err != nil {
		return Message{}, fmt.Errorf("insert message: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE conversations SET updated_at = ? WHERE id = ?`, now, conversation.ID); err != nil {
		return Message{}, fmt.Errorf("touch conversation: %w", err)
	}
	return message, nil
}

func (s *Store) ListMessagesForConversation(ctx context.Context, conversationID string) ([]Message, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, conversation_id, role, body, created_at FROM messages WHERE conversation_id = ? ORDER BY created_at ASC, rowid ASC`, strings.TrimSpace(conversationID))
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	defer rows.Close()
	var messages []Message
	for rows.Next() {
		var message Message
		if err := rows.Scan(&message.ID, &message.ProjectID, &message.ConversationID, &message.Role, &message.Body, &message.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

func (s *Store) ListTasksForConversation(ctx context.Context, conversationID string) ([]Task, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, repository_id, conversation_id, idea, refinement_level, status, created_at, updated_at FROM tasks WHERE conversation_id = ? ORDER BY created_at DESC, id DESC`, strings.TrimSpace(conversationID))
	if err != nil {
		return nil, fmt.Errorf("list conversation tasks: %w", err)
	}
	return scanTasks(rows)
}

func (s *Store) GetTask(ctx context.Context, taskID string) (Task, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, repository_id, conversation_id, idea, refinement_level, status, created_at, updated_at FROM tasks WHERE id = ?`, strings.TrimSpace(taskID))
	if err != nil {
		return Task{}, fmt.Errorf("get task %s: %w", taskID, err)
	}
	tasks, err := scanTasks(rows)
	if err != nil {
		return Task{}, err
	}
	if len(tasks) == 0 {
		return Task{}, fmt.Errorf("get task %s: %w", taskID, sql.ErrNoRows)
	}
	return tasks[0], nil
}

func scanTasks(rows *sql.Rows) ([]Task, error) {
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		var task Task
		var repo, conversation sql.NullString
		if err := rows.Scan(&task.ID, &task.ProjectID, &repo, &conversation, &task.Idea, &task.RefinementLevel, &task.Status, &task.CreatedAt, &task.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		if repo.Valid {
			task.RepositoryID = repo.String
		}
		if conversation.Valid {
			task.ConversationID = conversation.String
		}
		task.RefinementLevel = contract.NormalizeRefinementLevel(task.RefinementLevel)
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s *Store) ensureWorkflowTemplates(ctx context.Context) error {
	for _, template := range workflow.PredefinedTemplates() {
		if err := s.upsertPredefinedWorkflowTemplate(ctx, template); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) upsertPredefinedWorkflowTemplate(ctx context.Context, template workflow.Template) error {
	template = prepareWorkflowTemplateForSave(template)
	if err := workflow.ValidateTemplate(template); err != nil {
		return fmt.Errorf("validate predefined workflow template %s: %w", template.ID, err)
	}
	content, err := json.Marshal(template)
	if err != nil {
		return fmt.Errorf("marshal predefined workflow template %s: %w", template.ID, err)
	}
	now := nowRFC3339()
	_, err = s.db.ExecContext(ctx, `INSERT INTO workflow_templates(id, name, description, is_predefined, is_recommended, is_editable, template_json, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET name = excluded.name, description = excluded.description, is_predefined = excluded.is_predefined, is_recommended = excluded.is_recommended, is_editable = excluded.is_editable, template_json = excluded.template_json, updated_at = excluded.updated_at`,
		template.ID, template.Name, template.Description, boolInt(template.Predefined), boolInt(template.Recommended), boolInt(template.Editable), string(content), now, now)
	if err != nil {
		return fmt.Errorf("upsert predefined workflow template %s: %w", template.ID, err)
	}
	return nil
}

func (s *Store) ListWorkflowTemplates(ctx context.Context) ([]workflow.Template, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT template_json FROM workflow_templates ORDER BY is_recommended DESC, name ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list workflow templates: %w", err)
	}
	defer rows.Close()
	var templates []workflow.Template
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan workflow template: %w", err)
		}
		template, err := decodeWorkflowTemplate(raw)
		if err != nil {
			return nil, err
		}
		templates = append(templates, template)
	}
	return templates, rows.Err()
}

func (s *Store) GetWorkflowTemplate(ctx context.Context, templateID string) (workflow.Template, error) {
	if strings.TrimSpace(templateID) == "" {
		templateID = workflow.DefaultTemplateID
	}
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT template_json FROM workflow_templates WHERE id = ?`, templateID).Scan(&raw); err != nil {
		return workflow.Template{}, fmt.Errorf("get workflow template %s: %w", templateID, err)
	}
	return decodeWorkflowTemplate(raw)
}

func (s *Store) CopyWorkflowTemplate(ctx context.Context, sourceID, newID, name string) (workflow.Template, error) {
	source, err := s.GetWorkflowTemplate(ctx, sourceID)
	if err != nil {
		return workflow.Template{}, err
	}
	copied := source
	copied.ID = strings.TrimSpace(newID)
	copied.Name = strings.TrimSpace(name)
	copied.Predefined = false
	copied.Recommended = false
	copied.Editable = true
	if copied.Name == "" {
		copied.Name = source.Name + " copy"
	}
	if err := s.CreateWorkflowTemplate(ctx, copied); err != nil {
		return workflow.Template{}, err
	}
	return copied, nil
}

func (s *Store) CreateWorkflowTemplate(ctx context.Context, template workflow.Template) error {
	template = prepareWorkflowTemplateForSave(template)
	if !template.Editable {
		return ErrWorkflowTemplateNotEditable
	}
	if err := workflow.ValidateTemplate(template); err != nil {
		return err
	}
	content, err := json.Marshal(template)
	if err != nil {
		return fmt.Errorf("marshal workflow template: %w", err)
	}
	now := nowRFC3339()
	_, err = s.db.ExecContext(ctx, `INSERT INTO workflow_templates(id, name, description, is_predefined, is_recommended, is_editable, template_json, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, template.ID, template.Name, template.Description, boolInt(template.Predefined), boolInt(template.Recommended), boolInt(template.Editable), string(content), now, now)
	if err != nil {
		return fmt.Errorf("insert workflow template: %w", err)
	}
	return nil
}

func (s *Store) UpdateWorkflowTemplate(ctx context.Context, template workflow.Template) error {
	return s.UpdateWorkflowTemplateWithRegistry(ctx, template, agentregistry.Defaults())
}

func (s *Store) UpdateWorkflowTemplateWithRegistry(ctx context.Context, template workflow.Template, registry agentregistry.Registry) error {
	template = prepareWorkflowTemplateForSaveWithRegistry(template, registry)
	if !template.Editable || template.Predefined {
		return ErrWorkflowTemplateNotEditable
	}
	if err := workflow.ValidateTemplateWithRegistry(template, registry); err != nil {
		return err
	}
	var active int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE workflow_template_id = ? AND status NOT IN (?, ?, ?, ?, ?)`, template.ID, RunStatusCompleted, RunStatusFailed, RunStatusInvalid, RunStatusNeedsInput, RunStatusCancelled).Scan(&active); err != nil {
		return fmt.Errorf("count active runs for workflow template %s: %w", template.ID, err)
	}
	if active > 0 {
		return ErrWorkflowTemplateInUse
	}
	content, err := json.Marshal(template)
	if err != nil {
		return fmt.Errorf("marshal workflow template: %w", err)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE workflow_templates SET name = ?, description = ?, is_predefined = ?, is_recommended = ?, is_editable = ?, template_json = ?, updated_at = ? WHERE id = ?`,
		template.Name, template.Description, boolInt(template.Predefined), boolInt(template.Recommended), boolInt(template.Editable), string(content), nowRFC3339(), template.ID)
	if err != nil {
		return fmt.Errorf("update workflow template: %w", err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read workflow template update rows affected: %w", err)
	}
	if changed == 0 {
		return fmt.Errorf("workflow template %s not found", template.ID)
	}
	return nil
}

func decodeWorkflowTemplate(raw string) (workflow.Template, error) {
	var template workflow.Template
	if err := json.Unmarshal([]byte(raw), &template); err != nil {
		return workflow.Template{}, fmt.Errorf("decode workflow template: %w", err)
	}
	template = workflow.NormalizeTemplate(template)
	if err := workflow.ValidateTemplateWithRegistry(template, storedWorkflowTemplateRegistry(template)); err != nil {
		return workflow.Template{}, fmt.Errorf("stored workflow template %s is invalid: %w", template.ID, err)
	}
	return template, nil
}

func storedWorkflowTemplateRegistry(template workflow.Template) agentregistry.Registry {
	registry := agentregistry.Defaults()
	for _, stage := range template.Stages {
		if stage.Actor != workflow.ActorAgent || stage.ProfileID == "" {
			continue
		}
		if _, ok := agentregistry.ProfileByID(registry, stage.ProfileID); ok {
			continue
		}
		registry.Profiles = append(registry.Profiles, agentregistry.Profile{
			ID:            stage.ProfileID,
			FamilyID:      agentregistry.FamilyPi,
			Name:          stage.ProfileID,
			Role:          "stored-template-reference",
			Headless:      true,
			ContextPolicy: "task_contract_only",
			OutputStyle:   "structured_report",
		})
	}
	return registry
}

func prepareWorkflowTemplateForSave(template workflow.Template) workflow.Template {
	return prepareWorkflowTemplateForSaveWithRegistry(template, agentregistry.Defaults())
}

func prepareWorkflowTemplateForSaveWithRegistry(template workflow.Template, registry agentregistry.Registry) workflow.Template {
	template = workflow.NormalizeTemplateWithRegistry(template, registry)
	template.Edges = workflow.DeriveTemplateEdges(template)
	return workflow.NormalizeTemplateWithRegistry(template, registry)
}

func (s *Store) ensureRunnerRegistrySchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "runner_registry")
	if err != nil {
		return err
	}
	if _, ok := cols["origin"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE runner_registry ADD COLUMN origin TEXT NOT NULL DEFAULT 'registered'`); err != nil {
			return fmt.Errorf("add runner origin column: %w", err)
		}
	}
	if _, ok := cols["missed_heartbeats"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE runner_registry ADD COLUMN missed_heartbeats INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("add runner missed_heartbeats column: %w", err)
		}
	}
	return nil
}

func (s *Store) ensureSecretsSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS secrets_keymeta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  kek_version INTEGER NOT NULL,
  wrapped_dek BLOB NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
)`)
	if err != nil {
		return fmt.Errorf("ensure secrets schema: %w", err)
	}
	return nil
}

func (s *Store) ensureEventsSchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "events")
	if err != nil {
		return err
	}
	needsRebuild := false
	if _, ok := cols["project_id"]; !ok {
		needsRebuild = true
	}
	if _, ok := cols["scope"]; !ok {
		needsRebuild = true
	}
	if runID, ok := cols["run_id"]; ok && runID.NotNull {
		needsRebuild = true
	}
	if needsRebuild {
		if err := s.rebuildEventsTable(ctx, cols); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_events_run_sequence ON events(run_id, sequence)`); err != nil {
		return fmt.Errorf("create events run index: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_events_scope_sequence ON events(scope, sequence)`); err != nil {
		return fmt.Errorf("create events scope index: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_events_project_sequence ON events(project_id, sequence)`); err != nil {
		return fmt.Errorf("create events project index: %w", err)
	}
	return nil
}

func (s *Store) rebuildEventsTable(ctx context.Context, cols map[string]sqliteColumn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	projectExpr := "NULL"
	if _, ok := cols["project_id"]; ok {
		projectExpr = "project_id"
	}
	scopeExpr := "CASE WHEN run_id IS NOT NULL THEN 'run:' || run_id ELSE 'system' END"
	if _, ok := cols["scope"]; ok {
		scopeExpr = "COALESCE(NULLIF(scope, ''), CASE WHEN run_id IS NOT NULL THEN 'run:' || run_id ELSE 'system' END)"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin events rebuild: %w", err)
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `ALTER TABLE events RENAME TO events_legacy`); err != nil {
		return fmt.Errorf("rename legacy events: %w", err)
	}
	if err := createEventsTableTx(ctx, tx); err != nil {
		return err
	}
	query := fmt.Sprintf(`INSERT INTO events(id, project_id, run_id, scope, sequence, timestamp, task_id, attempt_id, type, actor_kind, actor_id, summary, data_json, envelope_json)
SELECT id, %s, run_id, %s, sequence, timestamp, task_id, attempt_id, type, actor_kind, actor_id, summary, data_json, envelope_json
FROM events_legacy ORDER BY rowid`, projectExpr, scopeExpr)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("copy legacy events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE events_legacy`); err != nil {
		return fmt.Errorf("drop legacy events: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit events rebuild: %w", err)
	}
	return nil
}

func (s *Store) ensureProjectWorkflowSchema(ctx context.Context) error {
	runCols, err := s.tableColumns(ctx, "runs")
	if err != nil {
		return err
	}
	taskCols, err := s.tableColumns(ctx, "tasks")
	if err != nil {
		return err
	}
	artifactCols, err := s.tableColumns(ctx, "artifacts")
	if err != nil {
		return err
	}
	snapshotCols, err := s.tableColumns(ctx, "workflow_snapshots")
	if err != nil {
		return err
	}
	if _, ok := runCols["project_id"]; ok {
		if _, ok := runCols["task_id"]; ok {
			if _, ok := taskCols["project_id"]; ok {
				if _, legacy := taskCols["run_id"]; !legacy {
					if _, ok := artifactCols["project_id"]; ok {
						if _, ok := snapshotCols["project_id"]; ok {
							return nil
						}
					}
				}
			}
		}
	}
	return s.rebuildWorkflowTablesForProjects(ctx)
}

func (s *Store) ensureConversationSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS conversations (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  title TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(project_id, id)
)`,
		`CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  body TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(project_id, conversation_id) REFERENCES conversations(project_id, id) ON DELETE CASCADE
)`,
		`CREATE INDEX IF NOT EXISTS idx_conversations_project_created ON conversations(project_id, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_conversation_created ON messages(conversation_id, created_at ASC)`,
	}
	for _, stmt := range statements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure conversation schema: %w", err)
		}
	}
	cols, err := s.tableColumns(ctx, "tasks")
	if err != nil {
		return err
	}
	if _, ok := cols["conversation_id"]; !ok {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE tasks ADD COLUMN conversation_id TEXT REFERENCES conversations(id)`); err != nil {
			return fmt.Errorf("add task conversation_id column: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_tasks_conversation_id ON tasks(conversation_id)`); err != nil {
		return fmt.Errorf("create task conversation index: %w", err)
	}
	return nil
}

func (s *Store) ensureRefinementSchema(ctx context.Context) error {
	for _, table := range []string{"tasks", "runs"} {
		cols, err := s.tableColumns(ctx, table)
		if err != nil {
			return err
		}
		if _, ok := cols["refinement_level"]; ok {
			continue
		}
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN refinement_level TEXT NOT NULL DEFAULT '%s'`, table, contract.RefinementLevelStandard)); err != nil {
			return fmt.Errorf("add %s refinement_level column: %w", table, err)
		}
	}
	return nil
}

func (s *Store) ensureWorkflowTemplateRefSchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "runs")
	if err != nil {
		return err
	}
	if _, ok := cols["workflow_template_id"]; ok {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN workflow_template_id TEXT NOT NULL DEFAULT 'balanced_pr_delivery'`); err != nil {
		return fmt.Errorf("add workflow template id column: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_runs_workflow_template_id ON runs(workflow_template_id)`); err != nil {
		return fmt.Errorf("create run workflow template index: %w", err)
	}
	return nil
}

func (s *Store) ensureWorkflowStageSchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "stages")
	if err != nil {
		return err
	}
	if _, ok := cols["workflow_stage_id"]; ok {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE stages ADD COLUMN workflow_stage_id TEXT`); err != nil {
		return fmt.Errorf("add workflow stage id column: %w", err)
	}
	return nil
}

func (s *Store) ensureStageBriefSchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "stages")
	if err != nil {
		return err
	}
	if _, ok := cols["stage_brief_artifact_id"]; ok {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE stages ADD COLUMN stage_brief_artifact_id TEXT REFERENCES artifacts(id)`); err != nil {
		return fmt.Errorf("add stage brief artifact column: %w", err)
	}
	return nil
}

func (s *Store) ensureTaskPlanSchema(ctx context.Context) error {
	cols, err := s.tableColumns(ctx, "stages")
	if err != nil {
		return err
	}
	if _, ok := cols["task_plan_artifact_id"]; ok {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE stages ADD COLUMN task_plan_artifact_id TEXT REFERENCES artifacts(id)`); err != nil {
		return fmt.Errorf("add task plan artifact column: %w", err)
	}
	return nil
}

func (s *Store) rebuildWorkflowTablesForProjects(ctx context.Context) error {
	defaultRepoID, err := s.DefaultRepositoryID(ctx, DefaultProjectID)
	if err != nil {
		return err
	}
	var repoValue any
	if defaultRepoID != "" {
		repoValue = defaultRepoID
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		return fmt.Errorf("disable foreign keys for workflow rebuild: %w", err)
	}
	defer func() { _, _ = s.db.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`) }()

	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin workflow rebuild: %w", err)
	}
	defer rollback(tx)
	for _, table := range []string{"tasks", "runs", "attempts", "stages", "workflow_snapshots", "artifacts", "events"} {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s RENAME TO %s_legacy`, table, table)); err != nil {
			return fmt.Errorf("rename legacy %s: %w", table, err)
		}
	}
	if err := createWorkflowTablesTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tasks(id, project_id, repository_id, idea, status, created_at, updated_at)
SELECT id, ?, ?, idea, status, created_at, updated_at FROM tasks_legacy`, DefaultProjectID, repoValue); err != nil {
		return fmt.Errorf("copy legacy tasks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tasks(id, project_id, repository_id, idea, status, created_at, updated_at)
SELECT 'task_' || id, ?, ?, idea, status, created_at, updated_at FROM runs_legacy r
WHERE NOT EXISTS (SELECT 1 FROM tasks_legacy t WHERE t.run_id = r.id)
  AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.id = 'task_' || r.id)`, DefaultProjectID, repoValue); err != nil {
		return fmt.Errorf("create synthetic tasks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runs(id, project_id, task_id, idea, refinement_level, workflow_template_id, status, event_log_artifact_id, created_at, updated_at)
SELECT r.id, ?, COALESCE((SELECT t.id FROM tasks_legacy t WHERE t.run_id = r.id ORDER BY t.created_at DESC, t.id DESC LIMIT 1), 'task_' || r.id), r.idea, 'standard', ?, r.status, r.event_log_artifact_id, r.created_at, r.updated_at
FROM runs_legacy r`, DefaultProjectID, workflow.DefaultTemplateID); err != nil {
		return fmt.Errorf("copy legacy runs: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempts(id, project_id, run_id, task_id, status, created_at, updated_at)
SELECT a.id, ?, a.run_id,
  CASE WHEN EXISTS (SELECT 1 FROM tasks t WHERE t.id = a.task_id) THEN a.task_id ELSE (SELECT r.task_id FROM runs r WHERE r.id = a.run_id) END,
  a.status, a.created_at, a.updated_at
FROM attempts_legacy a`, DefaultProjectID); err != nil {
		return fmt.Errorf("copy legacy attempts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO stages(id, project_id, run_id, task_id, attempt_id, workflow_stage_id, stage_type, adapter, status, created_at, updated_at)
SELECT s.id, ?, s.run_id,
  CASE WHEN EXISTS (SELECT 1 FROM tasks t WHERE t.id = s.task_id) THEN s.task_id ELSE (SELECT r.task_id FROM runs r WHERE r.id = s.run_id) END,
  s.attempt_id, NULL, s.stage_type, s.adapter, s.status, s.created_at, s.updated_at
FROM stages_legacy s`, DefaultProjectID); err != nil {
		return fmt.Errorf("copy legacy stages: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO workflow_snapshots(id, project_id, run_id, task_id, snapshot_json, created_at)
SELECT ws.id, ?, ws.run_id, (SELECT r.task_id FROM runs r WHERE r.id = ws.run_id), ws.snapshot_json, ws.created_at
FROM workflow_snapshots_legacy ws WHERE EXISTS (SELECT 1 FROM runs r WHERE r.id = ws.run_id)`, DefaultProjectID); err != nil {
		return fmt.Errorf("copy legacy workflow snapshots: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifacts(id, project_id, run_id, task_id, kind, media_type, path, created_at)
SELECT a.id, ?, a.run_id, (SELECT r.task_id FROM runs r WHERE r.id = a.run_id), a.kind, a.media_type, a.path, a.created_at
FROM artifacts_legacy a WHERE EXISTS (SELECT 1 FROM runs r WHERE r.id = a.run_id)`, DefaultProjectID); err != nil {
		return fmt.Errorf("copy legacy artifacts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(id, project_id, run_id, scope, sequence, timestamp, task_id, attempt_id, type, actor_kind, actor_id, summary, data_json, envelope_json)
SELECT e.id, CASE WHEN e.run_id IS NULL THEN NULL ELSE ? END, e.run_id, e.scope, e.sequence, e.timestamp, e.task_id, e.attempt_id, e.type, e.actor_kind, e.actor_id, e.summary, e.data_json, e.envelope_json
FROM events_legacy e`, DefaultProjectID); err != nil {
		return fmt.Errorf("copy legacy events: %w", err)
	}
	for _, table := range []string{"events", "artifacts", "workflow_snapshots", "stages", "attempts", "runs", "tasks"} {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DROP TABLE %s_legacy`, table)); err != nil {
			return fmt.Errorf("drop legacy %s: %w", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit workflow rebuild: %w", err)
	}
	return nil
}

func createWorkflowTablesTx(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  repository_id TEXT REFERENCES repositories(id),
  conversation_id TEXT REFERENCES conversations(id),
  idea TEXT NOT NULL,
  refinement_level TEXT NOT NULL DEFAULT 'standard',
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(project_id, id)
)`,
		`CREATE TABLE runs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  idea TEXT NOT NULL,
  refinement_level TEXT NOT NULL DEFAULT 'standard',
  workflow_template_id TEXT NOT NULL DEFAULT 'balanced_pr_delivery',
  status TEXT NOT NULL,
  event_log_artifact_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id),
  UNIQUE(project_id, id)
)`,
		`CREATE TABLE attempts (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(project_id, run_id) REFERENCES runs(project_id, id),
  FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id),
  UNIQUE(project_id, run_id, id)
)`,
		`CREATE TABLE stages (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  workflow_stage_id TEXT,
  stage_type TEXT NOT NULL,
  adapter TEXT,
  status TEXT NOT NULL,
  stage_brief_artifact_id TEXT REFERENCES artifacts(id),
  task_plan_artifact_id TEXT REFERENCES artifacts(id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(project_id, run_id, attempt_id) REFERENCES attempts(project_id, run_id, id),
  FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id)
)`,
		`CREATE TABLE workflow_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id TEXT NOT NULL REFERENCES projects(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  snapshot_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(project_id, run_id) REFERENCES runs(project_id, id),
  FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id)
)`,
		`CREATE TABLE artifacts (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  kind TEXT NOT NULL,
  media_type TEXT NOT NULL,
  path TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(project_id, run_id) REFERENCES runs(project_id, id),
  FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id)
)`,
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("create workflow table: %w", err)
		}
	}
	return createEventsTableTx(ctx, tx)
}

func createEventsTableTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE events (
  id TEXT PRIMARY KEY,
  project_id TEXT REFERENCES projects(id),
  run_id TEXT REFERENCES runs(id),
  scope TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  timestamp TEXT NOT NULL,
  task_id TEXT,
  attempt_id TEXT,
  type TEXT NOT NULL,
  actor_kind TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  summary TEXT NOT NULL,
  data_json TEXT NOT NULL,
  envelope_json TEXT NOT NULL,
  UNIQUE(scope, sequence)
)`)
	if err != nil {
		return fmt.Errorf("create events table: %w", err)
	}
	return nil
}

func (s *Store) checkForeignKeys(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return fmt.Errorf("foreign key check: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("foreign key check failed after migration")
	}
	return rows.Err()
}

func stagesFromTemplate(projectID, runID, taskID, attemptID string, template workflow.Template, now string) []Stage {
	stages := make([]Stage, 0, len(template.Stages))
	for _, templateStage := range template.Stages {
		stage := Stage{
			ID:              ids.New("stage"),
			ProjectID:       projectID,
			RunID:           runID,
			TaskID:          taskID,
			AttemptID:       attemptID,
			WorkflowStageID: templateStage.ID,
			StageType:       templateStage.Type,
			Adapter:         defaultStageAdapter(templateStage),
			Status:          StageStatusPending,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		stages = append(stages, stage)
	}
	return stages
}

func defaultStageAdapter(stage workflow.StageTemplate) string {
	switch {
	case stage.Type == workflow.StageTypeValidation:
		return "validation"
	case stage.Actor == workflow.ActorAgent:
		return "noop"
	default:
		return ""
	}
}

func assignWorkflowRunStages(wr *WorkflowRun, stages []Stage) {
	for _, stage := range stages {
		switch stage.StageType {
		case contract.StageTypeIdeaIntake, contract.StageTypeIdeaRefinement:
			if wr.IdeaIntakeStage.ID == "" {
				wr.IdeaIntakeStage = stage
			}
		case contract.StageTypeImplementation:
			if wr.ImplementationStage.ID == "" {
				wr.ImplementationStage = stage
			}
		case contract.StageTypeValidation:
			if wr.ValidationStage.ID == "" {
				wr.ValidationStage = stage
			}
		case contract.StageTypeCommit:
			if wr.CommitStage.ID == "" {
				wr.CommitStage = stage
			}
		case contract.StageTypePRReady, contract.StageTypePRCreation:
			if wr.PRReadyStage.ID == "" {
				wr.PRReadyStage = stage
			}
		case contract.StageTypeMemoryUpdate:
			if wr.MemoryUpdateStage.ID == "" {
				wr.MemoryUpdateStage = stage
			}
		}
	}
}

func (s *Store) CreateWorkflowRun(ctx context.Context, idea string) (WorkflowRun, error) {
	return s.CreateWorkflowRunForProject(ctx, DefaultProjectID, idea)
}

func (s *Store) CreateWorkflowRunForProject(ctx context.Context, projectID, idea string) (WorkflowRun, error) {
	return s.CreateWorkflowRunForProjectInput(ctx, projectID, contract.TaskInput{Idea: idea})
}

func (s *Store) CreateWorkflowRunInput(ctx context.Context, input contract.TaskInput) (WorkflowRun, error) {
	return s.CreateWorkflowRunForProjectInput(ctx, DefaultProjectID, input)
}

func (s *Store) CreateWorkflowRunForProjectInput(ctx context.Context, projectID string, input contract.TaskInput) (WorkflowRun, error) {
	project, err := s.GetProject(ctx, projectID)
	if err != nil {
		return WorkflowRun{}, err
	}
	input.RefinementLevel = contract.NormalizeRefinementLevel(input.RefinementLevel)
	if strings.TrimSpace(input.Idea) == "" {
		return WorkflowRun{}, fmt.Errorf("idea is required")
	}
	if err := contract.ValidateRefinementLevel(input.RefinementLevel); err != nil {
		return WorkflowRun{}, err
	}
	input.WorkflowTemplateID = strings.TrimSpace(input.WorkflowTemplateID)
	if input.WorkflowTemplateID == "" {
		input.WorkflowTemplateID = workflow.DefaultTemplateID
	}
	input.ConversationID = strings.TrimSpace(input.ConversationID)
	if input.ConversationID != "" {
		conversation, err := s.GetConversation(ctx, input.ConversationID)
		if err != nil {
			return WorkflowRun{}, err
		}
		if conversation.ProjectID != project.ID {
			return WorkflowRun{}, fmt.Errorf("conversation %s does not belong to project %s", input.ConversationID, project.ID)
		}
	}
	template, err := s.GetWorkflowTemplate(ctx, input.WorkflowTemplateID)
	if err != nil {
		return WorkflowRun{}, err
	}
	repositoryID, err := s.DefaultRepositoryID(ctx, projectID)
	if err != nil {
		return WorkflowRun{}, err
	}
	now := nowRFC3339()
	wr := WorkflowRun{
		Project: project,
		Run:     Run{ID: ids.New("run"), ProjectID: project.ID, Idea: input.Idea, RefinementLevel: input.RefinementLevel, WorkflowTemplateID: input.WorkflowTemplateID, Status: RunStatusPending, EventLogArtifactID: ids.New("artifact"), CreatedAt: now, UpdatedAt: now},
	}
	wr.Task = Task{ID: ids.New("task"), ProjectID: project.ID, RepositoryID: repositoryID, ConversationID: input.ConversationID, Idea: input.Idea, RefinementLevel: input.RefinementLevel, Status: RunStatusPending, CreatedAt: now, UpdatedAt: now}
	wr.Run.TaskID = wr.Task.ID
	wr.Attempt = Attempt{ID: ids.New("attempt"), ProjectID: project.ID, RunID: wr.Run.ID, TaskID: wr.Task.ID, Status: RunStatusPending, CreatedAt: now, UpdatedAt: now}
	stages := stagesFromTemplate(project.ID, wr.Run.ID, wr.Task.ID, wr.Attempt.ID, template, now)
	assignWorkflowRunStages(&wr, stages)

	eventLogPath := artifactPathForWorkspace(project.WorkspacePath, wr.Run.EventLogArtifactID, ".jsonl")
	if err := os.MkdirAll(filepath.Dir(eventLogPath), 0o700); err != nil {
		return WorkflowRun{}, fmt.Errorf("create event log artifact dir: %w", err)
	}
	if err := os.WriteFile(eventLogPath, nil, 0o600); err != nil {
		return WorkflowRun{}, fmt.Errorf("create event log artifact: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("begin create run: %w", err)
	}
	defer rollback(tx)
	var repoValue any
	if wr.Task.RepositoryID != "" {
		repoValue = wr.Task.RepositoryID
	}
	var conversationValue any
	if wr.Task.ConversationID != "" {
		conversationValue = wr.Task.ConversationID
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tasks(id, project_id, repository_id, conversation_id, idea, refinement_level, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, wr.Task.ID, wr.Task.ProjectID, repoValue, conversationValue, wr.Task.Idea, wr.Task.RefinementLevel, wr.Task.Status, now, now); err != nil {
		return WorkflowRun{}, fmt.Errorf("insert task: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runs(id, project_id, task_id, idea, refinement_level, workflow_template_id, status, event_log_artifact_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, wr.Run.ID, wr.Run.ProjectID, wr.Run.TaskID, wr.Run.Idea, wr.Run.RefinementLevel, wr.Run.WorkflowTemplateID, wr.Run.Status, wr.Run.EventLogArtifactID, now, now); err != nil {
		return WorkflowRun{}, fmt.Errorf("insert run: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempts(id, project_id, run_id, task_id, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, wr.Attempt.ID, wr.Attempt.ProjectID, wr.Attempt.RunID, wr.Attempt.TaskID, wr.Attempt.Status, now, now); err != nil {
		return WorkflowRun{}, fmt.Errorf("insert attempt: %w", err)
	}
	for _, stage := range stages {
		if _, err := tx.ExecContext(ctx, `INSERT INTO stages(id, project_id, run_id, task_id, attempt_id, workflow_stage_id, stage_type, adapter, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, stage.ID, stage.ProjectID, stage.RunID, stage.TaskID, stage.AttemptID, stage.WorkflowStageID, stage.StageType, stage.Adapter, stage.Status, now, now); err != nil {
			return WorkflowRun{}, fmt.Errorf("insert stage: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifacts(id, project_id, run_id, task_id, kind, media_type, path, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, wr.Run.EventLogArtifactID, wr.Run.ProjectID, wr.Run.ID, wr.Task.ID, "event_log", "application/x-jsonlines", eventLogPath, now); err != nil {
		return WorkflowRun{}, fmt.Errorf("insert event log artifact: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return WorkflowRun{}, fmt.Errorf("commit create run: %w", err)
	}
	return wr, nil
}

func (s *Store) CreateAttemptForRun(ctx context.Context, runID string, template workflow.Template) (Attempt, []Stage, error) {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return Attempt{}, nil, err
	}
	var taskID string
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM tasks WHERE id = ? AND project_id = ?`, run.TaskID, run.ProjectID).Scan(&taskID); err != nil {
		return Attempt{}, nil, fmt.Errorf("get task for run %s: %w", runID, err)
	}
	template = workflow.NormalizeTemplate(template)
	if err := workflow.ValidateTemplate(template); err != nil {
		return Attempt{}, nil, err
	}
	now := nowRFC3339()
	attempt := Attempt{ID: ids.New("attempt"), ProjectID: run.ProjectID, RunID: run.ID, TaskID: taskID, Status: RunStatusPending, CreatedAt: now, UpdatedAt: now}
	stages := stagesFromTemplate(run.ProjectID, run.ID, taskID, attempt.ID, template, now)

	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Attempt{}, nil, fmt.Errorf("begin create attempt: %w", err)
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempts(id, project_id, run_id, task_id, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, attempt.ID, attempt.ProjectID, attempt.RunID, attempt.TaskID, attempt.Status, now, now); err != nil {
		return Attempt{}, nil, fmt.Errorf("insert attempt: %w", err)
	}
	for _, stage := range stages {
		if _, err := tx.ExecContext(ctx, `INSERT INTO stages(id, project_id, run_id, task_id, attempt_id, workflow_stage_id, stage_type, adapter, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, stage.ID, stage.ProjectID, stage.RunID, stage.TaskID, stage.AttemptID, stage.WorkflowStageID, stage.StageType, stage.Adapter, stage.Status, now, now); err != nil {
			return Attempt{}, nil, fmt.Errorf("insert stage: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Attempt{}, nil, fmt.Errorf("commit create attempt: %w", err)
	}
	return attempt, stages, nil
}

func (s *Store) CountAttemptsForRun(ctx context.Context, runID string) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempts WHERE run_id = ?`, runID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count attempts for run %s: %w", runID, err)
	}
	return count, nil
}

func (s *Store) UpdateAttemptStatus(ctx context.Context, attemptID, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE attempts SET status = ?, updated_at = ? WHERE id = ?`, status, nowRFC3339(), attemptID)
	if err != nil {
		return fmt.Errorf("update attempt status: %w", err)
	}
	return nil
}

func (s *Store) ListRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, task_id, idea, refinement_level, workflow_template_id, status, event_log_artifact_id, created_at, updated_at FROM runs ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	return scanRuns(rows)
}

func (s *Store) ListRunsForProject(ctx context.Context, projectID string) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, task_id, idea, refinement_level, workflow_template_id, status, event_log_artifact_id, created_at, updated_at FROM runs WHERE project_id = ? ORDER BY created_at DESC`, projectID)
	if err != nil {
		return nil, fmt.Errorf("list project runs: %w", err)
	}
	return scanRuns(rows)
}

func (s *Store) ListRunsByStatus(ctx context.Context, status string, limit int) ([]Run, error) {
	query := `SELECT id, project_id, task_id, idea, refinement_level, workflow_template_id, status, event_log_artifact_id, created_at, updated_at FROM runs WHERE status = ? ORDER BY created_at ASC`
	args := []any{status}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs by status: %w", err)
	}
	return scanRuns(rows)
}

func (s *Store) ListRunsByProjectStatus(ctx context.Context, projectID, status string, limit int) ([]Run, error) {
	query := `SELECT id, project_id, task_id, idea, refinement_level, workflow_template_id, status, event_log_artifact_id, created_at, updated_at FROM runs WHERE project_id = ? AND status = ? ORDER BY created_at ASC`
	args := []any{projectID, status}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project runs by status: %w", err)
	}
	return scanRuns(rows)
}

func (s *Store) CountRunsByStatus(ctx context.Context, status string) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE status = ?`, status).Scan(&count); err != nil {
		return 0, fmt.Errorf("count runs by status: %w", err)
	}
	return count, nil
}

func (s *Store) CountRunsByProjectStatus(ctx context.Context, projectID, status string) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE project_id = ? AND status = ?`, projectID, status).Scan(&count); err != nil {
		return 0, fmt.Errorf("count project runs by status: %w", err)
	}
	return count, nil
}

func scanRuns(rows *sql.Rows) ([]Run, error) {
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		var run Run
		if err := rows.Scan(&run.ID, &run.ProjectID, &run.TaskID, &run.Idea, &run.RefinementLevel, &run.WorkflowTemplateID, &run.Status, &run.EventLogArtifactID, &run.CreatedAt, &run.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		run.RefinementLevel = contract.NormalizeRefinementLevel(run.RefinementLevel)
		if run.WorkflowTemplateID == "" {
			run.WorkflowTemplateID = workflow.DefaultTemplateID
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *Store) GetRun(ctx context.Context, runID string) (Run, error) {
	var run Run
	err := s.db.QueryRowContext(ctx, `SELECT id, project_id, task_id, idea, refinement_level, workflow_template_id, status, event_log_artifact_id, created_at, updated_at FROM runs WHERE id = ?`, runID).Scan(&run.ID, &run.ProjectID, &run.TaskID, &run.Idea, &run.RefinementLevel, &run.WorkflowTemplateID, &run.Status, &run.EventLogArtifactID, &run.CreatedAt, &run.UpdatedAt)
	if err != nil {
		return Run{}, fmt.Errorf("get run %s: %w", runID, err)
	}
	run.RefinementLevel = contract.NormalizeRefinementLevel(run.RefinementLevel)
	if run.WorkflowTemplateID == "" {
		run.WorkflowTemplateID = workflow.DefaultTemplateID
	}
	return run, nil
}

func (s *Store) GetWorkflowRun(ctx context.Context, runID string) (WorkflowRun, error) {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return WorkflowRun{}, err
	}
	project, err := s.GetProject(ctx, run.ProjectID)
	if err != nil {
		return WorkflowRun{}, err
	}
	var task Task
	var repo, conversation sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT id, project_id, repository_id, conversation_id, idea, refinement_level, status, created_at, updated_at FROM tasks WHERE id = ? AND project_id = ?`, run.TaskID, run.ProjectID).Scan(&task.ID, &task.ProjectID, &repo, &conversation, &task.Idea, &task.RefinementLevel, &task.Status, &task.CreatedAt, &task.UpdatedAt); err != nil {
		return WorkflowRun{}, fmt.Errorf("get task for run %s: %w", runID, err)
	}
	task.RefinementLevel = contract.NormalizeRefinementLevel(task.RefinementLevel)
	if repo.Valid {
		task.RepositoryID = repo.String
	}
	if conversation.Valid {
		task.ConversationID = conversation.String
	}
	var attempt Attempt
	if err := s.db.QueryRowContext(ctx, `SELECT id, project_id, run_id, task_id, status, created_at, updated_at FROM attempts WHERE project_id = ? AND run_id = ? AND task_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1`, run.ProjectID, run.ID, task.ID).Scan(&attempt.ID, &attempt.ProjectID, &attempt.RunID, &attempt.TaskID, &attempt.Status, &attempt.CreatedAt, &attempt.UpdatedAt); err != nil {
		return WorkflowRun{}, fmt.Errorf("get attempt for run %s: %w", runID, err)
	}
	stages, err := s.ListStagesForAttempt(ctx, run.ID, attempt.ID)
	if err != nil {
		return WorkflowRun{}, err
	}
	wr := WorkflowRun{Project: project, Run: run, Task: task, Attempt: attempt}
	assignWorkflowRunStages(&wr, stages)
	return wr, nil
}

func (s *Store) ListStages(ctx context.Context, runID string) ([]Stage, error) {
	return s.listStages(ctx, `SELECT id, project_id, run_id, task_id, attempt_id, COALESCE(workflow_stage_id,''), stage_type, COALESCE(adapter,''), status, COALESCE(stage_brief_artifact_id,''), COALESCE(task_plan_artifact_id,''), created_at, updated_at FROM stages WHERE run_id = ? ORDER BY created_at ASC`, runID)
}

func (s *Store) ListStagesForAttempt(ctx context.Context, runID, attemptID string) ([]Stage, error) {
	return s.listStages(ctx, `SELECT id, project_id, run_id, task_id, attempt_id, COALESCE(workflow_stage_id,''), stage_type, COALESCE(adapter,''), status, COALESCE(stage_brief_artifact_id,''), COALESCE(task_plan_artifact_id,''), created_at, updated_at FROM stages WHERE run_id = ? AND attempt_id = ? ORDER BY created_at ASC`, runID, attemptID)
}

func (s *Store) listStages(ctx context.Context, query string, args ...any) ([]Stage, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list stages: %w", err)
	}
	defer rows.Close()
	var stages []Stage
	for rows.Next() {
		var stage Stage
		if err := rows.Scan(&stage.ID, &stage.ProjectID, &stage.RunID, &stage.TaskID, &stage.AttemptID, &stage.WorkflowStageID, &stage.StageType, &stage.Adapter, &stage.Status, &stage.StageBriefArtifactID, &stage.TaskPlanArtifactID, &stage.CreatedAt, &stage.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan stage: %w", err)
		}
		stages = append(stages, stage)
	}
	return stages, rows.Err()
}

func (s *Store) UpdateRunStatus(ctx context.Context, runID, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET status = ?, updated_at = ? WHERE id = ?`, status, nowRFC3339(), runID)
	if err != nil {
		return fmt.Errorf("update run status: %w", err)
	}
	return nil
}

func (s *Store) UpdateRunStatusIfOpen(ctx context.Context, runID, status string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET status = ?, updated_at = ? WHERE id = ? AND status NOT IN (?, ?, ?, ?, ?)`, status, nowRFC3339(), runID, RunStatusCompleted, RunStatusFailed, RunStatusInvalid, RunStatusNeedsInput, RunStatusCancelled)
	if err != nil {
		return false, fmt.Errorf("update open run status: %w", err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read rows affected: %w", err)
	}
	return changed > 0, nil
}

func (s *Store) UpdateRunStatusFrom(ctx context.Context, runID, fromStatus, toStatus string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET status = ?, updated_at = ? WHERE id = ? AND status = ?`, toStatus, nowRFC3339(), runID, fromStatus)
	if err != nil {
		return false, fmt.Errorf("update run status from %s to %s: %w", fromStatus, toStatus, err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read rows affected: %w", err)
	}
	return changed > 0, nil
}

func RunStatusIsTerminal(status string) bool {
	switch status {
	case RunStatusCompleted, RunStatusFailed, RunStatusInvalid, RunStatusNeedsInput, RunStatusCancelled:
		return true
	default:
		return false
	}
}

func (s *Store) UpdateStageStatus(ctx context.Context, stageID, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE stages SET status = ?, updated_at = ? WHERE id = ?`, status, nowRFC3339(), stageID)
	if err != nil {
		return fmt.Errorf("update stage status: %w", err)
	}
	return nil
}

func (s *Store) UpdateStageAdapter(ctx context.Context, stageID, adapter string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE stages SET adapter = ?, updated_at = ? WHERE id = ?`, adapter, nowRFC3339(), stageID)
	if err != nil {
		return fmt.Errorf("update stage adapter: %w", err)
	}
	return nil
}

func (s *Store) UpdateStageBriefArtifactID(ctx context.Context, stageID, artifactID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE stages SET stage_brief_artifact_id = ?, updated_at = ? WHERE id = ?`, artifactID, nowRFC3339(), stageID)
	if err != nil {
		return fmt.Errorf("update stage brief artifact: %w", err)
	}
	return nil
}

func (s *Store) UpdateStageTaskPlanArtifactID(ctx context.Context, stageID, artifactID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE stages SET task_plan_artifact_id = ?, updated_at = ? WHERE id = ?`, artifactID, nowRFC3339(), stageID)
	if err != nil {
		return fmt.Errorf("update stage task plan artifact: %w", err)
	}
	return nil
}

func (s *Store) SaveArtifact(ctx context.Context, runID, kind, mediaType string, content []byte, ext string) (Artifact, error) {
	return s.SaveArtifactWithID(ctx, ids.New("artifact"), runID, kind, mediaType, content, ext)
}

func (s *Store) SaveArtifactWithID(ctx context.Context, artifactID, runID, kind, mediaType string, content []byte, ext string) (Artifact, error) {
	if ext == "" {
		ext = ".json"
	}
	projectID, taskID, workspacePath, err := s.runArtifactContext(ctx, runID)
	if err != nil {
		return Artifact{}, err
	}
	artifact := Artifact{ID: artifactID, ProjectID: projectID, RunID: runID, TaskID: taskID, Kind: kind, MediaType: mediaType, CreatedAt: nowRFC3339()}
	artifact.Path = artifactPathForWorkspace(workspacePath, artifact.ID, ext)
	if err := os.MkdirAll(filepath.Dir(artifact.Path), 0o700); err != nil {
		return Artifact{}, fmt.Errorf("create artifact dir: %w", err)
	}
	if err := os.WriteFile(artifact.Path, content, 0o600); err != nil {
		return Artifact{}, fmt.Errorf("write artifact: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO artifacts(id, project_id, run_id, task_id, kind, media_type, path, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, artifact.ID, artifact.ProjectID, artifact.RunID, artifact.TaskID, artifact.Kind, artifact.MediaType, artifact.Path, artifact.CreatedAt); err != nil {
		_ = os.Remove(artifact.Path)
		return Artifact{}, fmt.Errorf("insert artifact: %w", err)
	}
	return artifact, nil
}

func (s *Store) GetArtifact(ctx context.Context, artifactID string) (Artifact, []byte, error) {
	var artifact Artifact
	err := s.db.QueryRowContext(ctx, `SELECT id, project_id, run_id, task_id, kind, media_type, path, created_at FROM artifacts WHERE id = ?`, artifactID).Scan(&artifact.ID, &artifact.ProjectID, &artifact.RunID, &artifact.TaskID, &artifact.Kind, &artifact.MediaType, &artifact.Path, &artifact.CreatedAt)
	if err != nil {
		return Artifact{}, nil, fmt.Errorf("get artifact: %w", err)
	}
	content, err := os.ReadFile(artifact.Path)
	if err != nil {
		return Artifact{}, nil, fmt.Errorf("read artifact: %w", err)
	}
	return artifact, content, nil
}

func (s *Store) AppendEvent(ctx context.Context, ev event.Event) (event.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return event.Event{}, fmt.Errorf("begin append event: %w", err)
	}
	defer rollback(tx)
	ev, err = s.appendEventTx(ctx, tx, ev)
	if err != nil {
		return event.Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return event.Event{}, fmt.Errorf("commit append event: %w", err)
	}
	return ev, nil
}

func (s *Store) UpdateRunStatusFromAndAppendSystemEvent(ctx context.Context, runID, fromStatus, toStatus string, ev event.Event) (event.Event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ev.RunID != "" {
		return event.Event{}, false, fmt.Errorf("system event must not carry run_id")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return event.Event{}, false, fmt.Errorf("begin status/event transaction: %w", err)
	}
	defer rollback(tx)
	var projectID string
	if err := tx.QueryRowContext(ctx, `SELECT project_id FROM runs WHERE id = ?`, runID).Scan(&projectID); err != nil {
		return event.Event{}, false, fmt.Errorf("get run project: %w", err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE runs SET status = ?, updated_at = ? WHERE id = ? AND status = ?`, toStatus, nowRFC3339(), runID, fromStatus)
	if err != nil {
		return event.Event{}, false, fmt.Errorf("update run status from %s to %s: %w", fromStatus, toStatus, err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return event.Event{}, false, fmt.Errorf("read rows affected: %w", err)
	}
	if changed == 0 {
		if err := tx.Commit(); err != nil {
			return event.Event{}, false, fmt.Errorf("commit unchanged status/event transaction: %w", err)
		}
		return event.Event{}, false, nil
	}
	if ev.ProjectID == "" {
		ev.ProjectID = projectID
	}
	ev, err = s.appendEventTx(ctx, tx, ev)
	if err != nil {
		return event.Event{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return event.Event{}, false, fmt.Errorf("commit status/event transaction: %w", err)
	}
	return ev, true, nil
}

func (s *Store) UpdateRunStatusIfOpenAndAppendEvent(ctx context.Context, runID, toStatus string, ev event.Event) (event.Event, bool, error) {
	return s.updateRunStatusAndAppendEvent(ctx, runID, toStatus, ev, `status NOT IN (?, ?, ?, ?, ?)`, RunStatusCompleted, RunStatusFailed, RunStatusInvalid, RunStatusNeedsInput, RunStatusCancelled)
}

func (s *Store) UpdateRunStatusFromAndAppendEvent(ctx context.Context, runID, fromStatus, toStatus string, ev event.Event) (event.Event, bool, error) {
	return s.updateRunStatusAndAppendEvent(ctx, runID, toStatus, ev, `status = ?`, fromStatus)
}

func (s *Store) updateRunStatusAndAppendEvent(ctx context.Context, runID, toStatus string, ev event.Event, condition string, conditionArgs ...any) (event.Event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ev.RunID != "" && ev.RunID != runID {
		return event.Event{}, false, fmt.Errorf("event run_id %q does not match status run_id %q", ev.RunID, runID)
	}
	ev.RunID = runID
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return event.Event{}, false, fmt.Errorf("begin status/event transaction: %w", err)
	}
	defer rollback(tx)
	args := append([]any{toStatus, nowRFC3339(), runID}, conditionArgs...)
	res, err := tx.ExecContext(ctx, `UPDATE runs SET status = ?, updated_at = ? WHERE id = ? AND `+condition, args...)
	if err != nil {
		return event.Event{}, false, fmt.Errorf("update run status to %s: %w", toStatus, err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return event.Event{}, false, fmt.Errorf("read rows affected: %w", err)
	}
	if changed == 0 {
		if err := tx.Commit(); err != nil {
			return event.Event{}, false, fmt.Errorf("commit unchanged status/event transaction: %w", err)
		}
		return event.Event{}, false, nil
	}
	ev, err = s.appendEventTx(ctx, tx, ev)
	if err != nil {
		return event.Event{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return event.Event{}, false, fmt.Errorf("commit status/event transaction: %w", err)
	}
	return ev, true, nil
}

func (s *Store) appendEventTx(ctx context.Context, tx *sql.Tx, ev event.Event) (event.Event, error) {
	if ev.SchemaVersion == 0 {
		ev.SchemaVersion = event.SchemaVersion
	}
	if ev.ID == "" {
		ev.ID = ids.New("evt")
	}
	if ev.Timestamp == "" {
		ev.Timestamp = nowRFC3339()
	}
	if ev.Data == nil {
		ev.Data = map[string]any{}
	}
	if ev.ProjectID == "" && ev.RunID != "" {
		if err := tx.QueryRowContext(ctx, `SELECT project_id FROM runs WHERE id = ?`, ev.RunID).Scan(&ev.ProjectID); err != nil {
			return event.Event{}, fmt.Errorf("get event project: %w", err)
		}
	}
	scope := eventScope(ev)
	var last sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(sequence) FROM events WHERE scope = ?`, scope).Scan(&last); err != nil {
		return event.Event{}, fmt.Errorf("query last event sequence: %w", err)
	}
	if last.Valid {
		ev.Sequence = last.Int64 + 1
	} else {
		ev.Sequence = 1
	}
	dataJSON, err := json.Marshal(ev.Data)
	if err != nil {
		return event.Event{}, fmt.Errorf("marshal event data: %w", err)
	}
	envelopeJSON, err := json.Marshal(ev)
	if err != nil {
		return event.Event{}, fmt.Errorf("marshal event envelope: %w", err)
	}
	var projectID any
	if ev.ProjectID != "" {
		projectID = ev.ProjectID
	}
	var runID any
	if ev.RunID != "" {
		runID = ev.RunID
	}
	var taskID any
	if ev.TaskID != "" {
		taskID = ev.TaskID
	}
	var attemptID any
	if ev.AttemptID != "" {
		attemptID = ev.AttemptID
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(id, project_id, run_id, scope, sequence, timestamp, task_id, attempt_id, type, actor_kind, actor_id, summary, data_json, envelope_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, ev.ID, projectID, runID, scope, ev.Sequence, ev.Timestamp, taskID, attemptID, ev.Type, ev.Actor.Kind, ev.Actor.ID, ev.Summary, string(dataJSON), string(envelopeJSON)); err != nil {
		return event.Event{}, fmt.Errorf("insert event: %w", err)
	}
	if ev.RunID != "" {
		var eventLogPath string
		if err := tx.QueryRowContext(ctx, `SELECT artifacts.path FROM runs JOIN artifacts ON artifacts.id = runs.event_log_artifact_id WHERE runs.id = ?`, ev.RunID).Scan(&eventLogPath); err != nil {
			return event.Event{}, fmt.Errorf("get event log artifact path: %w", err)
		}
		f, err := os.OpenFile(eventLogPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
		if err != nil {
			return event.Event{}, fmt.Errorf("open event jsonl artifact: %w", err)
		}
		if _, err := f.Write(append(envelopeJSON, '\n')); err != nil {
			_ = f.Close()
			return event.Event{}, fmt.Errorf("append event jsonl artifact: %w", err)
		}
		if err := f.Close(); err != nil {
			return event.Event{}, fmt.Errorf("close event jsonl artifact: %w", err)
		}
	}
	return ev, nil
}

func (s *Store) ListEvents(ctx context.Context, runID string) ([]event.Event, error) {
	return s.ListEventsAfter(ctx, runID, 0)
}

func (s *Store) ListEventsAfter(ctx context.Context, runID string, after int64) ([]event.Event, error) {
	return s.listEventsWhere(ctx, `run_id = ? AND sequence > ?`, []any{runID, after})
}

func (s *Store) ListRunnerEvents(ctx context.Context, runnerID string) ([]event.Event, error) {
	return s.ListRunnerEventsAfter(ctx, runnerID, 0)
}

func (s *Store) ListRunnerEventsAfter(ctx context.Context, runnerID string, after int64) ([]event.Event, error) {
	return s.listEventsWhere(ctx, `scope = ? AND sequence > ?`, []any{"runner:" + runnerID, after})
}

func (s *Store) ListSystemEventsPage(ctx context.Context, before int64, limit int) (SystemEventPage, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	where := `run_id IS NULL`
	args := []any{}
	if before > 0 {
		where += ` AND rowid < ?`
		args = append(args, before)
	}
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, `SELECT rowid, project_id, envelope_json FROM events WHERE `+where+` ORDER BY rowid DESC LIMIT ?`, args...)
	if err != nil {
		return SystemEventPage{}, fmt.Errorf("list system events page: %w", err)
	}
	defer rows.Close()
	entries := make([]SystemEvent, 0, limit+1)
	for rows.Next() {
		var entry SystemEvent
		var project sql.NullString
		var raw string
		if err := rows.Scan(&entry.Cursor, &project, &raw); err != nil {
			return SystemEventPage{}, fmt.Errorf("scan system event: %w", err)
		}
		if err := json.Unmarshal([]byte(raw), &entry.Event); err != nil {
			return SystemEventPage{}, fmt.Errorf("decode system event envelope: %w", err)
		}
		if entry.Event.ProjectID == "" && project.Valid {
			entry.Event.ProjectID = project.String
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return SystemEventPage{}, err
	}
	page := SystemEventPage{Limit: limit}
	if len(entries) > limit {
		page.HasOlder = true
		entries = entries[:limit]
	}
	if len(entries) > 0 {
		page.OlderCursor = entries[len(entries)-1].Cursor
	}
	for i := len(entries) - 1; i >= 0; i-- {
		page.Events = append(page.Events, entries[i])
	}
	return page, nil
}

func (s *Store) listEventsWhere(ctx context.Context, where string, args []any) ([]event.Event, error) {
	return s.listEventsWhereOrdered(ctx, where, args, `sequence ASC`)
}

func (s *Store) listEventsWhereOrdered(ctx context.Context, where string, args []any, orderBy string) ([]event.Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT project_id, envelope_json FROM events WHERE `+where+` ORDER BY `+orderBy, args...)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	var events []event.Event
	for rows.Next() {
		var project sql.NullString
		var raw string
		if err := rows.Scan(&project, &raw); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		var ev event.Event
		if err := json.Unmarshal([]byte(raw), &ev); err != nil {
			return nil, fmt.Errorf("decode event envelope: %w", err)
		}
		if ev.ProjectID == "" && project.Valid {
			ev.ProjectID = project.String
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

func (s *Store) ListArtifacts(ctx context.Context, runID string) ([]Artifact, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, run_id, task_id, kind, media_type, path, created_at FROM artifacts WHERE run_id = ? ORDER BY created_at ASC`, runID)
	if err != nil {
		return nil, fmt.Errorf("list artifacts: %w", err)
	}
	defer rows.Close()
	var artifacts []Artifact
	for rows.Next() {
		var artifact Artifact
		if err := rows.Scan(&artifact.ID, &artifact.ProjectID, &artifact.RunID, &artifact.TaskID, &artifact.Kind, &artifact.MediaType, &artifact.Path, &artifact.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan artifact: %w", err)
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

func (s *Store) RunBundle(ctx context.Context, runID string) (RunBundle, error) {
	wr, err := s.GetWorkflowRun(ctx, runID)
	if err != nil {
		return RunBundle{}, err
	}
	stages, err := s.ListStages(ctx, runID)
	if err != nil {
		return RunBundle{}, err
	}
	events, err := s.ListEvents(ctx, runID)
	if err != nil {
		return RunBundle{}, err
	}
	artifacts, err := s.ListArtifacts(ctx, runID)
	if err != nil {
		return RunBundle{}, err
	}
	return RunBundle{Project: wr.Project, Run: wr.Run, Task: wr.Task, Attempt: wr.Attempt, Stages: stages, Events: events, Artifacts: artifacts}, nil
}

func (s *Store) SaveWorkflowSnapshot(ctx context.Context, runID string, snapshot any) error {
	content, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal workflow snapshot: %w", err)
	}
	projectID, taskID, err := s.runProjectTask(ctx, runID)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO workflow_snapshots(project_id, run_id, task_id, snapshot_json, created_at) VALUES (?, ?, ?, ?, ?)`, projectID, runID, taskID, string(content), nowRFC3339())
	if err != nil {
		return fmt.Errorf("insert workflow snapshot: %w", err)
	}
	return nil
}

func (s *Store) LatestWorkflowSnapshot(ctx context.Context, runID string) (map[string]any, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT snapshot_json FROM workflow_snapshots WHERE run_id = ? ORDER BY id DESC LIMIT 1`, runID).Scan(&raw); err != nil {
		return nil, fmt.Errorf("get latest workflow snapshot: %w", err)
	}
	var snapshot map[string]any
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return nil, fmt.Errorf("decode latest workflow snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *Store) LatestWorkflowTemplateSnapshot(ctx context.Context, runID string) (workflow.Template, error) {
	snapshot, err := s.LatestWorkflowSnapshot(ctx, runID)
	if err != nil {
		return workflow.Template{}, err
	}
	raw, ok := snapshot["workflow_template_snapshot"]
	if !ok {
		return workflow.Template{}, fmt.Errorf("workflow snapshot missing workflow_template_snapshot")
	}
	content, err := json.Marshal(raw)
	if err != nil {
		return workflow.Template{}, fmt.Errorf("marshal workflow template snapshot: %w", err)
	}
	var template workflow.Template
	if err := json.Unmarshal(content, &template); err != nil {
		return workflow.Template{}, fmt.Errorf("decode workflow template snapshot: %w", err)
	}
	template = workflow.NormalizeTemplate(template)
	if err := workflow.ValidateTemplate(template); err != nil {
		return workflow.Template{}, fmt.Errorf("workflow template snapshot is invalid: %w", err)
	}
	return template, nil
}

func (s *Store) UpsertRunner(ctx context.Context, runnerID, status string, capabilities any) error {
	return s.UpsertRunnerWithOrigin(ctx, runnerID, status, RunnerOriginRegistered, capabilities)
}

func (s *Store) UpsertRunnerWithOrigin(ctx context.Context, runnerID, status, origin string, capabilities any) error {
	if origin == "" {
		origin = RunnerOriginRegistered
	}
	capJSON, err := json.Marshal(capabilities)
	if err != nil {
		return fmt.Errorf("marshal runner capabilities: %w", err)
	}
	now := nowRFC3339()
	_, err = s.db.ExecContext(ctx, `INSERT INTO runner_registry(runner_id, status, origin, capabilities_json, missed_heartbeats, connected_at, updated_at) VALUES (?, ?, ?, ?, 0, ?, ?) ON CONFLICT(runner_id) DO UPDATE SET status = excluded.status, origin = excluded.origin, capabilities_json = excluded.capabilities_json, missed_heartbeats = 0, updated_at = excluded.updated_at`, runnerID, status, origin, string(capJSON), now, now)
	if err != nil {
		return fmt.Errorf("upsert runner: %w", err)
	}
	return nil
}

func (s *Store) UpdateRunnerHealth(ctx context.Context, runnerID, status string, missedHeartbeats int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runner_registry SET status = ?, missed_heartbeats = ?, updated_at = ? WHERE runner_id = ?`, status, missedHeartbeats, nowRFC3339(), runnerID)
	if err != nil {
		return fmt.Errorf("update runner health: %w", err)
	}
	return nil
}

func (s *Store) GetRunner(ctx context.Context, runnerID string) (Runner, error) {
	var runner Runner
	err := s.db.QueryRowContext(ctx, `SELECT runner_id, status, origin, capabilities_json, missed_heartbeats, connected_at, updated_at FROM runner_registry WHERE runner_id = ?`, runnerID).Scan(&runner.ID, &runner.Status, &runner.Origin, &runner.CapabilitiesJSON, &runner.MissedHeartbeats, &runner.ConnectedAt, &runner.UpdatedAt)
	if err != nil {
		return Runner{}, fmt.Errorf("get runner %s: %w", runnerID, err)
	}
	return runner, nil
}

func (s *Store) ListRunners(ctx context.Context) ([]Runner, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT runner_id, status, origin, capabilities_json, missed_heartbeats, connected_at, updated_at FROM runner_registry ORDER BY updated_at DESC, runner_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list runners: %w", err)
	}
	defer rows.Close()
	var runners []Runner
	for rows.Next() {
		var runner Runner
		if err := rows.Scan(&runner.ID, &runner.Status, &runner.Origin, &runner.CapabilitiesJSON, &runner.MissedHeartbeats, &runner.ConnectedAt, &runner.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan runner: %w", err)
		}
		runners = append(runners, runner)
	}
	return runners, rows.Err()
}

func eventScope(ev event.Event) string {
	if ev.RunID != "" {
		return "run:" + ev.RunID
	}
	if runnerID, ok := ev.Data["runner_id"].(string); ok && runnerID != "" {
		return "runner:" + runnerID
	}
	if strings.HasPrefix(ev.Type, "runner.") && ev.Actor.ID != "" {
		return "runner:" + ev.Actor.ID
	}
	if ev.ProjectID != "" {
		return "project:" + ev.ProjectID
	}
	return "system"
}

func (s *Store) runProjectTask(ctx context.Context, runID string) (string, string, error) {
	var projectID, taskID string
	if err := s.db.QueryRowContext(ctx, `SELECT project_id, task_id FROM runs WHERE id = ?`, runID).Scan(&projectID, &taskID); err != nil {
		return "", "", fmt.Errorf("get run project/task: %w", err)
	}
	return projectID, taskID, nil
}

func (s *Store) runArtifactContext(ctx context.Context, runID string) (string, string, string, error) {
	var projectID, taskID, workspacePath string
	err := s.db.QueryRowContext(ctx, `SELECT r.project_id, r.task_id, w.path FROM runs r JOIN workspaces w ON w.project_id = r.project_id WHERE r.id = ?`, runID).Scan(&projectID, &taskID, &workspacePath)
	if err != nil {
		return "", "", "", fmt.Errorf("get artifact project context: %w", err)
	}
	return projectID, taskID, workspacePath, nil
}

func defaultWorkspacePath(dataDir, projectID string) string {
	if dataDir == "" {
		dataDir = ".parley-data"
	}
	if projectID == "" {
		projectID = DefaultProjectID
	}
	return filepath.Join(dataDir, "projects", projectID, "workspace")
}

func ensureWorkspaceDirs(workspacePath string) error {
	for _, path := range []string{workspacePath, filepath.Join(workspacePath, "artifacts"), filepath.Join(workspacePath, "drafts"), filepath.Join(workspacePath, "memory")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create workspace dir %s: %w", path, err)
		}
	}
	return nil
}

func artifactPathForWorkspace(workspacePath, id, ext string) string {
	return filepath.Join(workspacePath, "artifacts", id+ext)
}

func schemaWithoutIndexes(schema string) string {
	lines := strings.Split(schema, "\n")
	kept := lines[:0]
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "CREATE INDEX") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

func defaultRepositoryID(projectID string) string {
	if projectID == "" || projectID == DefaultProjectID {
		return DefaultRepositoryID
	}
	return "repo_" + projectID
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func rollback(tx *sql.Tx) { _ = tx.Rollback() }

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }
