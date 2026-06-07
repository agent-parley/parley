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

	// Snapshot untracked files so we can strip whatever the validation command
	// writes into the worktree (e.g. `go build ./...` drops a compiled binary)
	// before capturing the validation-stage diff. Otherwise that build output
	// pollutes diff.patch. A failed snapshot disables cleanup — never strip
	// against an empty baseline, or worker files go too.
	baseline, baselineErr := worktree.ListUntrackedFiles(ctx, prepared.WorktreePath)

	result, runErr := a.opts.Provider.Run(ctx, prepared.Invocation, sink)
	if ctx.Err() != nil {
		return report.Report{}, fmt.Errorf("validation canceled: %w", ctx.Err())
	}
	if runErr != nil && result.StartedAt.IsZero() {
		return validationFailureReport(disp, "validation sandbox failed to start", runErr, validationPayload(prepared, result, runErr, "")), nil
	}

	var removed []string
	if baselineErr == nil {
		var cleanupErr error
		removed, cleanupErr = worktree.RemoveCreatedUntracked(ctx, prepared.WorktreePath, baseline)
		if cleanupErr != nil {
			if emitErr := sink.Emit(ctx, validationEvent(disp, "validation worktree cleanup incomplete", map[string]any{"error": cleanupErr.Error()})); emitErr != nil {
				return report.Report{}, emitErr
			}
		}
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
	if len(removed) > 0 {
		payload["worktree_artifacts_removed"] = removed
	}

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
	projectID := disp.ProjectID
	if projectID == "" {
		projectID = a.opts.ProjectID
	}
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
			// Point writable state at container-local /tmp (world-writable 1777,
			// not bind-mounted) rather than the SELinux :Z-relabeled workspace
			// mount, so the build cache's many small files never touch the host
			// artifact dir. Harmless for non-Go validation commands.
			"HOME":    "/tmp",
			"GOCACHE": "/tmp/gocache",
			"GOPATH":  "/tmp/gopath",
		},
		// Use a non-login shell: `sh -lc` sources /etc/profile, which resets
		// PATH to the Debian default and drops the toolchain image's
		// /usr/local/go/bin (so `go` becomes "not found"). `sh -c` preserves the
		// image's ENV PATH.
		Command:       []string{"sh", "-c", command},
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
