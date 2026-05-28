package models

const (
	LocalExecutorID = "local"
)

const (
	ModeAllInOne = "all-in-one"
	ModeManager  = "manager"
	ModeExecutor = "executor"
)

const (
	ExecutorStatusOnline  = "online"
	ExecutorStatusOffline = "offline"
	ExecutorKindLocal     = "local"
	ExecutorKindRemote    = "remote"
)

const (
	PlannerStatusPlanning  = "planning"
	PlannerStatusApproved  = "approved"
	PlannerStatusDismissed = "dismissed"
)

const (
	PlannerAgentStatusRunning   = "running"
	PlannerAgentStatusCompleted = "completed"
	PlannerAgentStatusFailed    = "failed"
	PlannerAgentStatusDiscarded = "discarded"
)

const (
	PlannerGenerationStatusRunning   = "running"
	PlannerGenerationStatusCompleted = "completed"
	PlannerGenerationStatusFailed    = "failed"
	PlannerGenerationStatusDiscarded = "discarded"
)

const (
	PlannerGenerationEventStarted          = "planner_generation.started"
	PlannerGenerationEventPreflightStarted = "planner_generation.preflight_started"
	PlannerGenerationEventPreflightFailed  = "planner_generation.preflight_failed"
	PlannerGenerationEventPlannerStarted   = "planner_generation.planner_started"
	PlannerGenerationEventPlannerFinished  = "planner_generation.planner_finished"
	PlannerGenerationEventCriticStarted    = "planner_generation.critic_started"
	PlannerGenerationEventCriticFinished   = "planner_generation.critic_finished"
	PlannerGenerationEventDiagnosticsSaved = "planner_generation.diagnostics_saved"
	PlannerGenerationEventResultApplied    = "planner_generation.result_applied"
	PlannerGenerationEventResultDiscarded  = "planner_generation.result_discarded"
	PlannerGenerationEventResultFailed     = "planner_generation.result_failed"
)

const (
	RunStatusAwaitingApproval = "AWAITING_APPROVAL"
	RunStatusQueued           = "QUEUED"
	RunStatusRunning          = "RUNNING"
	RunStatusAwaitingReview   = "AWAITING_REVIEW"
	RunStatusNeedsFix         = "NEEDS_FIX"
	RunStatusCompleted        = "COMPLETED"
	RunStatusFailed           = "FAILED"
)

const (
	TaskStatusDraft          = "DRAFT"
	TaskStatusQueued         = "QUEUED"
	TaskStatusRunning        = "RUNNING"
	TaskStatusAwaitingReview = "AWAITING_REVIEW"
	TaskStatusNeedsFix       = "NEEDS_FIX"
	TaskStatusDone           = "DONE"
	TaskStatusFailed         = "FAILED"
)

const (
	AttemptKindWorker = "worker"
	AttemptKindFix    = "fix"
)

const (
	AttemptStatusRequested = "requested"
	AttemptStatusQueued    = "queued"
	AttemptStatusRunning   = "running"
	AttemptStatusReviewed  = "reviewed"
	AttemptStatusFailed    = "failed"
	AttemptStatusExpired   = "expired"
)

const (
	LeaseStatusActive   = "active"
	LeaseStatusReleased = "released"
	LeaseStatusExpired  = "expired"
)

const (
	HandoffStatusPreview  = "preview"
	HandoffStatusRecorded = "recorded"
)

const (
	ActorKindManager = "manager"
	ActorKindRunner  = "runner"
	ActorKindUser    = "user"
)

const (
	EventPlannerPromptReceived   = "planner.prompt_received"
	EventPlannerDraftCreated     = "planner.draft_created"
	EventPlannerSessionStarted   = "planner.session_started"
	EventTaskPlanCreated         = "task_plan.created"
	EventTaskContractCreated     = "task_contract.created"
	EventLeaseGranted            = "lease.granted"
	EventLeaseReleased           = "lease.released"
	EventLeaseExpired            = "lease.expired"
	EventTaskStarted             = "task.started"
	EventAttemptPreflightStarted = "attempt.preflight_started"
	EventAttemptPreflightFailed  = "attempt.preflight_failed"
	EventAttemptWorkerStarted    = "attempt.worker_started"
	EventAttemptWorkerFinished   = "attempt.worker_finished"
	EventAttemptReviewerStarted  = "attempt.reviewer_started"
	EventAttemptReviewerFinished = "attempt.reviewer_finished"
	EventArtifactCreated         = "artifact.created"
	EventReviewCompleted         = "review.completed"
	EventTaskCompleted           = "task.completed"
	EventTaskStateChanged        = "task.state_changed"
	EventHandoffApproved         = "handoff.approved"
	EventHandoffCompleted        = "handoff.completed"
)

const (
	PlannerDiagnosticKindInput      = "planner-input"
	PlannerDiagnosticKindOutput     = "planner-output"
	PlannerDiagnosticKindRuntimeLog = "planner-runtime-log"
	PlannerDiagnosticKindError      = "planner-error"
	PlannerDiagnosticKindTrace      = "planner-trace"
)

const (
	ArtifactKindWorkerInput  = "worker-input"
	ArtifactKindWorkerOutput = "worker-output"
	ArtifactKindChangedFiles = "changed-files"
	ArtifactKindSummary      = "summary"
	ArtifactKindReview       = "review"
	ArtifactKindFindings     = "findings"
	ArtifactKindDiff         = "diff"
	ArtifactKindPlan         = "plan"
	ArtifactKindContract     = "contract"
	ArtifactKindCheckpoint   = "checkpoint"
)

const (
	SensitivityNormal = "normal"
	SensitivityInternal = "internal"
	SensitivitySecret = "secret"
)

const (
	QueuePolicyManual        = "manual"
	QueuePolicyAutoWhenReady = "auto_when_ready"
)
