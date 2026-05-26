package executor

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type checkpointDocument struct {
	SchemaVersion int      `json:"schema_version"`
	ProjectID     string   `json:"project_id"`
	RunID         string   `json:"run_id"`
	TaskID        string   `json:"task_id"`
	AttemptID     string   `json:"attempt_id"`
	AttemptNumber int      `json:"attempt_number"`
	Step          string   `json:"step"`
	Role          string   `json:"role"`
	Profile       string   `json:"profile"`
	Summary       string   `json:"summary"`
	Status        string   `json:"status"`
	ExitCode      *int     `json:"exit_code,omitempty"`
	Artifacts     []string `json:"artifacts,omitempty"`
	NextAction    string   `json:"next_action"`
	CreatedAt     string   `json:"created_at"`
}

func CheckpointBody(input AttemptInput, step, role, profile, status, summary, nextAction string, exitCode *int, artifactNames []string) string {
	step = safeStep(step)
	role = roleOrFallback(step, role)
	profile = strings.TrimSpace(profile)
	if profile == "" {
		profile = input.Task.Adapter
	}
	doc := checkpointDocument{
		SchemaVersion: 1,
		ProjectID: input.Project.ID, RunID: input.Run.ID, TaskID: input.Task.ID, AttemptID: input.Attempt.ID, AttemptNumber: input.Attempt.Number,
		Step: step, Role: role, Profile: profile,
		Summary: strings.TrimSpace(summary), Status: strings.TrimSpace(status), ExitCode: exitCode, Artifacts: artifactNames,
		NextAction: strings.TrimSpace(nextAction), CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "{}\n"
	}
	return string(data) + "\n"
}

func ResumeCheckpointSection(checkpoints []Checkpoint) string {
	if len(checkpoints) == 0 {
		return "## Resume checkpoints\n\nNo prior checkpoint metadata is available.\n\n"
	}
	var b strings.Builder
	b.WriteString("## Resume checkpoints\n\n")
	for _, checkpoint := range checkpoints {
		b.WriteString(fmt.Sprintf("- Attempt %d %s checkpoint", checkpoint.AttemptNumber, checkpoint.Step))
		if checkpoint.Summary != "" {
			b.WriteString(": ")
			b.WriteString(checkpoint.Summary)
		}
		if checkpoint.Path != "" {
			b.WriteString(fmt.Sprintf(" (`%s`)", checkpoint.Path))
		}
		b.WriteString("\n")
	}
	b.WriteString("\n")
	return b.String()
}

func safeStep(step string) string {
	switch strings.TrimSpace(strings.ToLower(step)) {
	case "reviewer", "fresh-review", "fresh_review", "review":
		return "reviewer"
	default:
		return "worker"
	}
}

func roleOrFallback(step, role string) string {
	step = safeStep(step)
	if step == "reviewer" {
		return "reviewer"
	}
	role = strings.TrimSpace(role)
	if role != "" {
		return role
	}
	return step
}
