package app

import (
	"path/filepath"
	"strings"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/profiles"
)

func eventDetail(event models.Event) string {
	summary := strings.TrimSpace(event.Summary)
	if summary == "" || summary == eventLabel(event.Type) {
		return ""
	}
	return summary
}

func eventLabel(kind string) string {
	switch kind {
	case models.EventPlannerPromptReceived:
		return "Prompt captured"
	case models.EventPlannerDraftCreated:
		return "Plan drafted"
	case models.EventPlannerSessionStarted:
		return "Planning started"
	case models.EventTaskPlanCreated, models.EventTaskContractCreated:
		return "Plan saved"
	case models.EventLeaseGranted:
		return "Runner slot reserved"
	case models.EventLeaseReleased:
		return "Runner slot released"
	case models.EventLeaseExpired:
		return "Runner slot expired"
	case models.EventTaskStarted:
		return "Worker started"
	case models.EventArtifactCreated:
		return "Outputs saved"
	case models.EventReviewCompleted:
		return "Fresh review complete"
	case models.EventTaskCompleted:
		return "Task accepted"
	case models.EventTaskStateChanged:
		return "Task updated"
	case models.EventHandoffApproved:
		return "Handoff preview approved"
	case models.EventHandoffCompleted:
		return "Handoff recorded"
	default:
		label := strings.ReplaceAll(kind, "_", " ")
		label = strings.ReplaceAll(label, ".", " · ")
		return label
	}
}

func statusLabel(status string) string {
	switch status {
	case models.TaskStatusDraft:
		return "Draft plan"
	case models.RunStatusAwaitingApproval:
		return "Awaiting approval"
	case models.TaskStatusQueued:
		return "Queued"
	case models.TaskStatusRunning:
		return "Running"
	case models.TaskStatusAwaitingReview:
		return "Waiting for review"
	case models.TaskStatusNeedsFix:
		return "Fix requested"
	case models.TaskStatusDone:
		return "Accepted"
	case models.RunStatusCompleted:
		return "Completed"
	case models.TaskStatusFailed:
		return "Failed"
	default:
		return strings.ReplaceAll(strings.ToLower(status), "_", " ")
	}
}

func artifactName(path string) string {
	name := filepath.Base(path)
	if name == "." || name == string(filepath.Separator) {
		return path
	}
	return name
}

func agentProfileLabel(profile string) string {
	if profile == "" {
		profile = "pi-standard"
	}
	if known, ok := profiles.Lookup(profile); ok && known.Label != "" {
		return known.Label
	}
	return artifactLabel(profile)
}

func artifactLabel(kind string) string {
	switch kind {
	case models.ArtifactKindWorkerInput:
		return "Worker input"
	case models.ArtifactKindWorkerOutput:
		return "Worker output"
	case models.ArtifactKindChangedFiles:
		return "Changed files"
	case models.ArtifactKindSummary:
		return "Summary"
	case models.ArtifactKindReview:
		return "Review"
	case models.ArtifactKindFindings:
		return "Findings"
	case models.ArtifactKindDiff:
		return "Diff"
	case models.ArtifactKindPlan:
		return "Plan"
	case models.ArtifactKindContract:
		return "Task contract"
	default:
		label := strings.ReplaceAll(kind, "-", " ")
		label = strings.ReplaceAll(label, "_", " ")
		if label == "" {
			return "Output"
		}
		return strings.ToUpper(label[:1]) + label[1:]
	}
}
