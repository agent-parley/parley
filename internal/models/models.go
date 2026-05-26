package models

import "time"

// Executor is the persisted runner registry record.
// The UI calls these records "runners". The in-process attempt implementation
// lives behind executor.Runner and is separate from this persisted record.
type Executor struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Endpoint         string    `json:"endpoint"`
	Status           string    `json:"status"`
	Kind             string    `json:"kind"`
	Description      string    `json:"description"`
	ResourceClass    string    `json:"resource_class"`
	ContainerRuntime string    `json:"container_runtime"`
	RepoAvailability string    `json:"repo_availability"`
	MaxSlots         int       `json:"max_slots"`
	Capabilities     []string  `json:"capabilities"`
	AgentProfiles    []string  `json:"agent_profiles"`
	Notes            string    `json:"notes"`
	LastSeenAt       time.Time `json:"last_seen_at"`
}

type Project struct {
	ID                        string    `json:"id"`
	Name                      string    `json:"name"`
	Description               string    `json:"description"`
	RepoPath                  string    `json:"repo_path"`
	DefaultBranch             string    `json:"default_branch"`
	DefaultExecutorID         string    `json:"default_executor_id"`
	AgentContext              string    `json:"agent_context"`
	DefaultAgentProfile       string    `json:"default_agent_profile"`
	DefaultWorkflowTemplateID string    `json:"default_workflow_template_id"`
	ReviewLoopCount           int       `json:"review_loop_count"`
	RetryCount                int       `json:"retry_count"`
	CreatedAt                 time.Time `json:"created_at"`
	UpdatedAt                 time.Time `json:"updated_at"`
}

type WorkflowTemplate struct {
	ID          string         `json:"id"`
	ProjectID   string         `json:"project_id"`
	Name        string         `json:"name"`
	Summary     string         `json:"summary"`
	UseCase     string         `json:"use_case"`
	Steps       []WorkflowStep `json:"steps"`
	ReviewLoops int            `json:"review_loops"`
	RetryCount  int            `json:"retry_count"`
	IsDefault   bool           `json:"is_default"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

type WorkflowStep struct {
	Name        string `json:"name"`
	Role        string `json:"role"`
	Description string `json:"description"`
	Optional    bool   `json:"optional"`
}

type PlannerSession struct {
	ID             string    `json:"id"`
	ProjectID      string    `json:"project_id"`
	Title          string    `json:"title"`
	Prompt         string    `json:"prompt"`
	Status         string    `json:"status"`
	DraftTitle     string    `json:"draft_title"`
	DraftObjective string    `json:"draft_objective"`
	DraftFocus     string    `json:"draft_focus"`
	DraftBoundaries string   `json:"draft_boundaries"`
	DraftDoneWhen  string    `json:"draft_done_when"`
	Assumptions    []string  `json:"assumptions"`
	Risks          []string  `json:"risks"`
	GraphPreview       []string `json:"graph_preview"`
	PlannerRevision   int      `json:"planner_revision"`
	ActiveGenerationID string   `json:"active_generation_id"`
	AgentMode          string     `json:"agent_mode"`
	AgentStatus        string     `json:"agent_status"`
	AgentSummary    string     `json:"agent_summary"`
	PlannerProfile  string     `json:"planner_profile"`
	CriticProfile   string     `json:"critic_profile"`
	AgentExecutedAt *time.Time `json:"agent_executed_at,omitempty"`
	ApprovedRunID   string     `json:"approved_run_id"`
	ApprovedTaskID string    `json:"approved_task_id"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type PlannerMessage struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id"`
	Role      string    `json:"role"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

type PlannerGeneration struct {
	ID              string     `json:"id"`
	ProjectID       string     `json:"project_id"`
	SessionID       string     `json:"session_id"`
	Status          string     `json:"status"`
	Mode            string     `json:"mode"`
	PlannerProfile  string     `json:"planner_profile"`
	CriticProfile   string     `json:"critic_profile"`
	PlannerRevision int        `json:"planner_revision"`
	Summary         string     `json:"summary"`
	Diagnostic      string     `json:"diagnostic"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type PlannerDiagnostic struct {
	ID           string    `json:"id"`
	ProjectID    string    `json:"project_id"`
	SessionID    string    `json:"session_id"`
	GenerationID string    `json:"generation_id"`
	Kind         string    `json:"kind"`
	Path         string    `json:"path"`
	MediaType    string    `json:"media_type"`
	Sensitivity  string    `json:"sensitivity"`
	SizeBytes    int64     `json:"size_bytes"`
	SHA256       string    `json:"sha256"`
	CreatedAt    time.Time `json:"created_at"`
}

type Run struct {
	ID        string    `json:"id"`
	ProjectID string    `json:"project_id"`
	Title     string    `json:"title"`
	Goal      string    `json:"goal"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Task struct {
	ID                 string    `json:"id"`
	RunID              string    `json:"run_id"`
	ProjectID          string    `json:"project_id"`
	AssignedExecutorID string    `json:"assigned_executor_id"`
	LeaseID            string    `json:"lease_id"`
	Title              string    `json:"title"`
	Objective          string    `json:"objective"`
	Focus              string    `json:"focus"`
	ExcludedPaths      string    `json:"excluded_paths"`
	AcceptanceCriteria string    `json:"acceptance_criteria"`
	Status             string    `json:"status"`
	RiskLevel          string    `json:"risk_level"`
	Adapter            string    `json:"adapter"`
	Role               string    `json:"role"`
	BranchName         string    `json:"branch_name"`
	Attempts           int       `json:"attempts"`
	MaxAttempts        int       `json:"max_attempts"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type Attempt struct {
	ID        string    `json:"id"`
	ProjectID string    `json:"project_id"`
	RunID     string    `json:"run_id"`
	TaskID    string    `json:"task_id"`
	Number    int       `json:"number"`
	Kind      string    `json:"kind"`
	Status    string    `json:"status"`
	Summary   string    `json:"summary"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Lease struct {
	ID         string     `json:"id"`
	TaskID     string     `json:"task_id"`
	ExecutorID string     `json:"executor_id"`
	Status     string     `json:"status"`
	GrantedAt  time.Time  `json:"granted_at"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	ReleasedAt *time.Time `json:"released_at,omitempty"`
	Reason     string     `json:"reason"`
}

type Handoff struct {
	ID                    string             `json:"id"`
	ProjectID             string             `json:"project_id"`
	RunID                 string             `json:"run_id"`
	TaskID                string             `json:"task_id"`
	SourceExecutorID      string             `json:"source_executor_id"`
	DestinationExecutorID string             `json:"destination_executor_id"`
	Status                string             `json:"status"`
	BranchName            string             `json:"branch_name"`
	CommitCheck           string             `json:"commit_check"`
	RemoteCheck           string             `json:"remote_check"`
	Included              []HandoffItem      `json:"included"`
	Excluded              []HandoffExclusion `json:"excluded"`
	ParleyIgnorePreview   string             `json:"parleyignore_preview"`
	ManifestPreview       string             `json:"manifest_preview"`
	ResultSummary         string             `json:"result_summary"`
	CreatedAt             time.Time          `json:"created_at"`
	UpdatedAt             time.Time          `json:"updated_at"`
	CompletedAt           *time.Time         `json:"completed_at,omitempty"`
}

type HandoffItem struct {
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	RelativePath string `json:"relative_path"`
	Sensitivity string `json:"sensitivity"`
	SHA256      string `json:"sha256"`
}

type HandoffExclusion struct {
	RelativePath string `json:"relative_path"`
	Reason       string `json:"reason"`
}

type Artifact struct {
	ID            string    `json:"id"`
	RunID         string    `json:"run_id"`
	TaskID        string    `json:"task_id"`
	AttemptNumber int       `json:"attempt_number"`
	Kind          string    `json:"kind"`
	Path          string    `json:"path"`
	MediaType     string    `json:"media_type"`
	Sensitivity   string    `json:"sensitivity"`
	SizeBytes     int64     `json:"size_bytes"`
	SHA256        string    `json:"sha256"`
	CreatedAt     time.Time `json:"created_at"`
}

type Event struct {
	ID         string         `json:"id"`
	Sequence   int            `json:"sequence"`
	Timestamp  time.Time      `json:"timestamp"`
	RunID      string         `json:"run_id"`
	TaskID     string         `json:"task_id,omitempty"`
	ExecutorID string         `json:"executor_id,omitempty"`
	LeaseID    string         `json:"lease_id,omitempty"`
	Type       string         `json:"type"`
	ActorKind  string         `json:"actor_kind"`
	ActorID    string         `json:"actor_id"`
	Summary    string         `json:"summary"`
	Data       map[string]any `json:"data,omitempty"`
}
