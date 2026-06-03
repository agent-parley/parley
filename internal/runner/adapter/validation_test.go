package adapter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/runner/provider"
	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/runner/worktree"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestValidationGateTruthTable(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	tests := []struct {
		name        string
		exitCode    int
		withDiff    bool
		providerErr error
		wantStatus  string
	}{
		{name: "exit zero and diff passes", exitCode: 0, withDiff: true, wantStatus: report.StatusCompleted},
		{name: "exit nonzero and diff fails", exitCode: 1, withDiff: true, providerErr: fmt.Errorf("exit 1"), wantStatus: report.StatusFailed},
		{name: "exit zero and no diff fails", exitCode: 0, withDiff: false, wantStatus: report.StatusFailed},
		{name: "exit nonzero and no diff fails", exitCode: 1, withDiff: false, providerErr: fmt.Errorf("exit 1"), wantStatus: report.StatusFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			source := initValidationSourceRepo(t, ctx)
			dataRoot := t.TempDir()
			wt, err := worktree.Create(ctx, worktree.CreateOptions{DataRoot: dataRoot, ProjectID: "p1", RunID: "run1", TaskID: "task1", AttemptID: "attempt1", SourceRepo: source})
			if err != nil {
				t.Fatalf("create worktree: %v", err)
			}
			if tt.withDiff {
				if err := os.WriteFile(filepath.Join(wt.Path, "changed.txt"), []byte("changed\n"), 0o600); err != nil {
					t.Fatalf("write diff file: %v", err)
				}
			}
			fp := &fakeValidationProvider{result: provider.Result{ExitCode: tt.exitCode, StartedAt: time.Now(), EndedAt: time.Now()}, err: tt.providerErr}
			adapter := NewValidation(ValidationOptions{Provider: fp, DataRoot: dataRoot, ProjectID: "p1", Image: "validation-image", Command: "echo validate"})
			sink := &validationRecordingSink{}
			rep, err := adapter.Run(ctx, validationDispatch(), sink)
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if rep.Status != tt.wantStatus {
				t.Fatalf("status = %s, want %s; errors=%v", rep.Status, tt.wantStatus, rep.Errors)
			}
			gate, ok := rep.Payload["gate"].(map[string]any)
			if !ok {
				t.Fatalf("missing gate payload: %+v", rep.Payload)
			}
			if gate["exit_zero"] != (tt.exitCode == 0) || gate["diff_non_empty"] != tt.withDiff {
				t.Fatalf("gate payload = %+v", gate)
			}
			if len(sink.artifacts) != 1 || sink.artifacts[0].Kind != "diff_patch" {
				t.Fatalf("expected diff patch artifact, got %+v", sink.artifacts)
			}
			if fp.inv.Network != provider.NetworkNone {
				t.Fatalf("validation network = %s, want none", fp.inv.Network)
			}
			if strings.Join(fp.inv.Command, " ") != "sh -lc echo validate" {
				t.Fatalf("validation command = %#v", fp.inv.Command)
			}
		})
	}
}

func validationDispatch() contract.Dispatch {
	return contract.Dispatch{RunID: "run1", TaskID: "task1", AttemptID: "attempt1", StageID: "stage_validation", StageType: contract.StageTypeValidation, Adapter: validationName, Input: map[string]any{}}
}

type fakeValidationProvider struct {
	inv    provider.PreparedInvocation
	result provider.Result
	err    error
}

func (p *fakeValidationProvider) Name() string { return "fake" }

func (p *fakeValidationProvider) Run(_ context.Context, inv provider.PreparedInvocation, _ runnerio.Sink) (provider.Result, error) {
	p.inv = inv
	return p.result, p.err
}

type validationRecordingSink struct {
	events    []event.Event
	artifacts []runnerio.Artifact
}

func (s *validationRecordingSink) Emit(_ context.Context, ev event.Event) error {
	s.events = append(s.events, ev)
	return nil
}

func (s *validationRecordingSink) Artifact(_ context.Context, art runnerio.Artifact) error {
	s.artifacts = append(s.artifacts, art)
	return nil
}

func initValidationSourceRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	runValidationGit(t, ctx, dir, "init")
	runValidationGit(t, ctx, dir, "config", "user.email", "test@example.invalid")
	runValidationGit(t, ctx, dir, "config", "user.name", "Parley Test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runValidationGit(t, ctx, dir, "add", "README.md")
	runValidationGit(t, ctx, dir, "commit", "-m", "initial")
	return dir
}

func runValidationGit(t *testing.T, ctx context.Context, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}
