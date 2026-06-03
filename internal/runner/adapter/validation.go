package adapter

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/agent-parley/parley/internal/runner/provider"
	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/runner/worktree"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/ids"
	"github.com/agent-parley/parley/internal/shared/report"
)

const (
	validationName       = "validation"
	defaultValidationCmd = "go build ./... && go test ./..."
	defaultValidationImg = "docker.io/library/golang:1.26"
)

type ValidationOptions struct {
	Provider       provider.SandboxProvider
	DataRoot       string
	ProjectID      string
	SourceRepo     string
	ArtifactDir    string
	ReferenceRoot  string
	AgentStateRoot string
	Image          string
	Command        string
	Network        provider.Network
	ContainerName  string
}

type Validation struct {
	opts ValidationOptions
}

type ValidationPreparedRun struct {
	Invocation   provider.PreparedInvocation
	WorktreePath string
	ArtifactDir  string
	Command      string
}

func NewValidation(opts ValidationOptions) Validation {
	return Validation{opts: opts}
}

func (a Validation) Name() string { return validationName }

func (a Validation) Run(ctx context.Context, disp contract.Dispatch, sink runnerio.Sink) (report.Report, error) {
	prepared, err := a.Prepare(disp)
	if err != nil {
		return validationFailureReport(disp, "validation setup failed", err, map[string]any{"adapter": validationName}), nil
	}
	if err := sink.Emit(ctx, validationEvent(disp, "sandboxed validation started", map[string]any{"command": prepared.Command, "network": string(prepared.Invocation.Network)})); err != nil {
		return report.Report{}, err
	}

	result, runErr := a.opts.Provider.Run(ctx, prepared.Invocation, sink)
	if ctx.Err() != nil {
		return report.Report{}, fmt.Errorf("validation canceled: %w", ctx.Err())
	}
	if runErr != nil && result.StartedAt.IsZero() {
		return validationFailureReport(disp, "validation sandbox failed to start", runErr, validationPayload(prepared, result, runErr, "")), nil
	}

	diffPath := filepath.Join(prepared.ArtifactDir, "diff.patch")
	diff, diffErr := worktree.CaptureDiff(ctx, prepared.WorktreePath, diffPath)
	if diffErr != nil {
		return validationFailureReport(disp, "validation diff capture failed", diffErr, validationPayload(prepared, result, runErr, "")), nil
	}
	diffID := ids.New("artifact")
	if err := sink.Artifact(ctx, runnerio.Artifact{ID: diffID, Name: "diff.patch", Kind: "diff_patch", MediaType: "text/x-diff", Content: diff}); err != nil {
		return report.Report{}, err
	}

	payload := validationPayload(prepared, result, runErr, diffID)
	exitZero := result.ExitCode == 0
	diffNonEmpty := len(bytes.TrimSpace(diff)) > 0
	passed := exitZero && diffNonEmpty && runErr == nil
	payload["gate"] = map[string]any{"exit_zero": exitZero, "diff_non_empty": diffNonEmpty, "passed": passed}
	payload["diff_bytes"] = len(diff)

	status := report.StatusCompleted
	summary := "validation passed"
	errors := []string{}
	if !passed {
		status = report.StatusFailed
		summary = "validation failed"
		if !exitZero {
			errors = append(errors, fmt.Sprintf("validation command exited with code %d", result.ExitCode))
		}
		if !diffNonEmpty {
			errors = append(errors, "diff.patch was empty")
		}
		if runErr != nil {
			errors = append(errors, runErr.Error())
		}
	}
	if err := sink.Emit(ctx, validationEvent(disp, "sandboxed validation completed", map[string]any{"status": status, "diff_artifact_id": diffID})); err != nil {
		return report.Report{}, err
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: validationName},
		Status:        status,
		Summary:       summary,
		EvidenceRefs:  []string{diffID},
		Payload:       payload,
		Errors:        errors,
	}, nil
}

func (a Validation) Prepare(disp contract.Dispatch) (ValidationPreparedRun, error) {
	if a.opts.Provider == nil {
		return ValidationPreparedRun{}, fmt.Errorf("validation sandbox provider is required")
	}
	if a.opts.DataRoot == "" {
		return ValidationPreparedRun{}, fmt.Errorf("validation data root is required")
	}
	projectID := a.opts.ProjectID
	if projectID == "" {
		projectID = "default"
	}
	worktreePath, err := worktree.Locate(a.opts.DataRoot, projectID, disp.RunID, disp.TaskID, disp.AttemptID)
	if err != nil {
		return ValidationPreparedRun{}, err
	}
	artifactDir := a.opts.ArtifactDir
	if artifactDir == "" {
		artifactDir = filepath.Join(a.opts.DataRoot, "projects", projectID, "artifacts", disp.RunID, disp.TaskID, disp.AttemptID, "validation")
	}
	if err := os.MkdirAll(artifactDir, 0o700); err != nil {
		return ValidationPreparedRun{}, fmt.Errorf("create validation artifact dir: %w", err)
	}
	command := a.opts.Command
	if command == "" {
		command = defaultValidationCmd
	}
	image := a.opts.Image
	if image == "" {
		image = defaultValidationImg
	}
	mounts := []provider.Mount{
		{Host: worktreePath, Container: containerRepoPath, Mode: "rw", Relabel: "Z"},
		{Host: artifactDir, Container: containerWorkspacePath, Mode: "rw", Relabel: "Z"},
	}
	if a.opts.ReferenceRoot != "" {
		mounts = append(mounts, provider.Mount{Host: a.opts.ReferenceRoot, Container: containerReferencePath, Mode: "ro"})
	}
	inv := provider.PreparedInvocation{
		Adapter:        validationName,
		Profile:        "validation",
		Role:           "validator",
		ContainerImage: image,
		Mounts:         mounts,
		Env: map[string]string{
			"HARNESS_RUN_ID":  disp.RunID,
			"HARNESS_TASK_ID": disp.TaskID,
			// Validation runs as a non-root keep-id user, but stock toolchain
			// images (e.g. golang) default HOME=/ and GOPATH=/go — neither
			// writable by the mapped uid — which breaks the Go build/test cache.
			// Redirect writable state into the rw workspace mount. Harmless for
			// non-Go validation commands.
			"HOME":    containerWorkspacePath,
			"GOCACHE": containerWorkspacePath + "/.gocache",
			"GOPATH":  containerWorkspacePath + "/.gopath",
		},
		Command:       []string{"sh", "-lc", command},
		WorkDir:       containerRepoPath,
		Network:       validationNetwork(a.opts.Network),
		UserNS:        "keep-id",
		ContainerName: a.opts.ContainerName,
	}
	return ValidationPreparedRun{Invocation: inv, WorktreePath: worktreePath, ArtifactDir: artifactDir, Command: command}, nil
}

func validationNetwork(network provider.Network) provider.Network {
	if network != "" {
		return network
	}
	return provider.NetworkNone
}

func validationPayload(prepared ValidationPreparedRun, result provider.Result, runErr error, diffID string) map[string]any {
	payload := map[string]any{
		"adapter":          validationName,
		"provider":         prepared.Invocation.Adapter,
		"command":          prepared.Command,
		"network":          string(prepared.Invocation.Network),
		"exit_code":        result.ExitCode,
		"started_at":       result.StartedAt,
		"ended_at":         result.EndedAt,
		"workdir":          containerRepoPath,
		"diff_artifact_id": diffID,
	}
	if runErr != nil {
		payload["provider_run_error"] = runErr.Error()
	}
	return payload
}

func validationFailureReport(disp contract.Dispatch, summary string, err error, payload map[string]any) report.Report {
	if payload == nil {
		payload = map[string]any{}
	}
	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindHarness, ID: validationName},
		Status:        report.StatusFailed,
		Summary:       summary,
		EvidenceRefs:  []string{},
		Payload:       payload,
		Errors:        []string{err.Error()},
	}
}

func validationEvent(disp contract.Dispatch, summary string, data map[string]any) event.Event {
	if data == nil {
		data = map[string]any{}
	}
	data["stage_id"] = disp.StageID
	data["stage_type"] = disp.StageType
	return event.Event{
		SchemaVersion: event.SchemaVersion,
		ID:            ids.New("evt"),
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		Type:          "harness.progress",
		Actor:         event.Actor{Kind: event.ActorKindHarness, ID: validationName},
		Summary:       summary,
		Data:          data,
	}
}
