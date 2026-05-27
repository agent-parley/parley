package app

import (
	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/models"
)

func templateConstants() map[string]string {
	return map[string]string{
		"TaskStatusDraft":          models.TaskStatusDraft,
		"TaskStatusQueued":         models.TaskStatusQueued,
		"TaskStatusAwaitingReview": models.TaskStatusAwaitingReview,
		"TaskStatusNeedsFix":       models.TaskStatusNeedsFix,
		"TaskStatusFailed":         models.TaskStatusFailed,
		"AttemptStatusRequested":   models.AttemptStatusRequested,
		"AttemptStatusFailed":      models.AttemptStatusFailed,
		"ArtifactKindWorkerInput":  models.ArtifactKindWorkerInput,
		"ArtifactKindWorkerOutput": models.ArtifactKindWorkerOutput,
		"ArtifactKindSummary":      models.ArtifactKindSummary,
		"ArtifactKindChangedFiles": models.ArtifactKindChangedFiles,
		"ArtifactKindDiff":         models.ArtifactKindDiff,
		"ArtifactKindReview":       models.ArtifactKindReview,
		"ArtifactKindFindings":     models.ArtifactKindFindings,
		"LocalExecutorID":          models.LocalExecutorID,
		"ExecutorStatusOnline":     models.ExecutorStatusOnline,
		"ExecutorKindLocal":        models.ExecutorKindLocal,
		"HandoffStatusRecorded":    models.HandoffStatusRecorded,
		"SensitivityNormal":        models.SensitivityNormal,
		"PlannerStatusPlanning":    models.PlannerStatusPlanning,
		"ExecutionModeLocalPi":     config.ExecutionModeLocalPi,
		"QueuePolicyManual":        models.QueuePolicyManual,
		"QueuePolicyAutoWhenReady": models.QueuePolicyAutoWhenReady,
	}
}
