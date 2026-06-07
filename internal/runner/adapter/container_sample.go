package adapter

import (
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

const containerSampleName = "container_sample"

type ContainerSampleOptions struct {
	Provider       provider.SandboxProvider
	DataRoot       string
	ProjectID      string
	SourceRepo     string
	ArtifactDir    string
	ReferenceRoot  string
	AgentStateRoot string
	Image          string
	Command        []string
	ContainerName  string
}

// ContainerSample is M2's provider-backed noop-equivalent. It creates a task
// worktree, runs a trivial command through SandboxProvider, transfers the
// workspace output and harness-captured diff.patch as first-class artifacts,
// and returns a normal 0068 report.
type ContainerSample struct {
	opts ContainerSampleOptions
}

func NewContainerSample(opts ContainerSampleOptions) ContainerSample {
	return ContainerSample{opts: opts}
}

func (a ContainerSample) Name() string { return containerSampleName }

func (a ContainerSample) Run(ctx context.Context, disp contract.Dispatch, sink runnerio.Sink) (report.Report, error) {
	if a.opts.Provider == nil {
		return report.Report{}, fmt.Errorf("container sample provider is required")
	}
	if a.opts.DataRoot == "" {
		return report.Report{}, fmt.Errorf("container sample data root is required")
	}
	if a.opts.SourceRepo == "" {
		return report.Report{}, fmt.Errorf("container sample source repo is required")
	}
	projectID := disp.ProjectID
	if projectID == "" {
		projectID = a.opts.ProjectID
	}
	if projectID == "" {
		projectID = "default"
	}
	if err := sink.Emit(ctx, containerSampleEvent(disp, "container sample started", map[string]any{"step": "start"})); err != nil {
		return report.Report{}, err
	}

	wt, err := worktree.Create(ctx, worktree.CreateOptions{
		DataRoot:   a.opts.DataRoot,
		ProjectID:  projectID,
		RunID:      disp.RunID,
		TaskID:     disp.TaskID,
		SourceRepo: a.opts.SourceRepo,
	})
	if err != nil {
		return report.Report{}, err
	}

	artifactDir := a.opts.ArtifactDir
	if artifactDir == "" {
		artifactDir = filepath.Join(a.opts.DataRoot, "projects", projectID, "artifacts", disp.RunID, disp.TaskID, disp.AttemptID)
	}
	if err := os.MkdirAll(artifactDir, 0o700); err != nil {
		return report.Report{}, fmt.Errorf("create artifact dir: %w", err)
	}

	inv := a.invocation(disp, wt.Path, artifactDir)
	result, err := a.opts.Provider.Run(ctx, inv, sink)
	if err != nil {
		return report.Report{}, err
	}

	outputID, err := transferFile(ctx, sink, filepath.Join(artifactDir, "out"), "out", "adapter_output", "text/plain")
	if err != nil {
		return report.Report{}, err
	}
	diffPath := filepath.Join(artifactDir, "diff.patch")
	diff, err := worktree.CaptureDiff(ctx, wt.Path, diffPath)
	if err != nil {
		return report.Report{}, err
	}
	diffID := ids.New("artifact")
	if err := sink.Artifact(ctx, runnerio.Artifact{
		ID:        diffID,
		Name:      "diff.patch",
		Kind:      "diff_patch",
		MediaType: "text/x-diff",
		Content:   diff,
	}); err != nil {
		return report.Report{}, err
	}

	if err := sink.Emit(ctx, containerSampleEvent(disp, "container sample produced artifacts", map[string]any{"output_artifact_id": outputID, "diff_artifact_id": diffID})); err != nil {
		return report.Report{}, err
	}

	return report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         disp.RunID,
		TaskID:        disp.TaskID,
		AttemptID:     disp.AttemptID,
		StageID:       disp.StageID,
		StageType:     disp.StageType,
		Actor:         report.Actor{Kind: report.ActorKindAgent, ID: containerSampleName},
		Status:        report.StatusCompleted,
		Verdict:       nil,
		Summary:       "container sample completed",
		EvidenceRefs:  []string{outputID, diffID},
		Payload: map[string]any{
			"adapter":   containerSampleName,
			"provider":  a.opts.Provider.Name(),
			"exit_code": result.ExitCode,
		},
		Errors: []string{},
	}, nil
}

func (a ContainerSample) invocation(disp contract.Dispatch, worktreePath, artifactDir string) provider.PreparedInvocation {
	image := a.opts.Image
	if image == "" {
		image = "docker.io/library/alpine:3.20"
	}
	command := a.opts.Command
	if len(command) == 0 {
		command = []string{"sh", "-c", "echo hi > /project/workspace/out && echo hi > /project/repo/container-sample.txt && cat /project/workspace/out"}
	}
	mounts := []provider.Mount{
		{Host: worktreePath, Container: "/project/repo", Mode: "rw", Relabel: "Z"},
		{Host: artifactDir, Container: "/project/workspace", Mode: "rw", Relabel: "Z"},
	}
	if a.opts.ReferenceRoot != "" {
		mounts = append(mounts, provider.Mount{Host: a.opts.ReferenceRoot, Container: "/project/reference", Mode: "ro"})
	}
	if a.opts.AgentStateRoot != "" {
		mounts = append(mounts, provider.Mount{Host: a.opts.AgentStateRoot, Container: "/project/creds", Mode: "ro", Credential: true})
	}
	return provider.PreparedInvocation{
		Adapter:        containerSampleName,
		Profile:        "worker",
		Role:           "worker",
		ContainerImage: image,
		Mounts:         mounts,
		Env: map[string]string{
			"HARNESS_RUN_ID":     disp.RunID,
			"HARNESS_TASK_ID":    disp.TaskID,
			"HARNESS_ATTEMPT_ID": disp.AttemptID,
		},
		Command:       command,
		WorkDir:       "/project/repo",
		Network:       provider.NetworkNone,
		UserNS:        "keep-id",
		ContainerName: a.opts.ContainerName,
	}
}

func transferFile(ctx context.Context, sink runnerio.Sink, path, name, kind, mediaType string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read artifact %s: %w", path, err)
	}
	artifactID := ids.New("artifact")
	if err := sink.Artifact(ctx, runnerio.Artifact{
		ID:        artifactID,
		Name:      name,
		Kind:      kind,
		MediaType: mediaType,
		Content:   content,
	}); err != nil {
		return "", err
	}
	return artifactID, nil
}

func containerSampleEvent(disp contract.Dispatch, summary string, data map[string]any) event.Event {
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
		Type:          "adapter.progress",
		Actor:         event.Actor{Kind: event.ActorKindAdapter, ID: containerSampleName},
		Summary:       summary,
		Data:          data,
	}
}
