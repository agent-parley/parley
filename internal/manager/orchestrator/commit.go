package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const (
	defaultGitAuthorName  = "Parley Agent"
	defaultGitAuthorEmail = "agent@agent-parley.dev"
)

type commitOptions struct {
	WorktreePath   string
	RunID          string
	TaskID         string
	Idea           string
	ReportSummary  string
	DiffArtifactID string
	Git            string
	AuthorName     string
	AuthorEmail    string
}

type commitResult struct {
	Branch      string
	CommitSHA   string
	AuthorName  string
	AuthorEmail string
}

type gitIdentity struct {
	Name  string
	Email string
}

func commitWorktree(ctx context.Context, opts commitOptions) (commitResult, error) {
	if opts.WorktreePath == "" {
		return commitResult{}, fmt.Errorf("worktree path is required")
	}
	if opts.RunID == "" || opts.TaskID == "" {
		return commitResult{}, fmt.Errorf("run_id and task_id are required")
	}
	git := opts.Git
	if git == "" {
		git = "git"
	}
	branch := "agent/" + opts.RunID + "/" + opts.TaskID
	identity := configuredGitIdentity(opts.AuthorName, opts.AuthorEmail)

	if _, err := runGitCommand(ctx, git, opts.WorktreePath, "checkout", "-B", branch); err != nil {
		return commitResult{}, fmt.Errorf("checkout commit branch: %w", err)
	}
	if _, err := runGitCommand(ctx, git, opts.WorktreePath, "add", "-A"); err != nil {
		return commitResult{}, fmt.Errorf("stage worktree changes: %w", err)
	}
	hasChanges, err := gitHasStagedChanges(ctx, git, opts.WorktreePath)
	if err != nil {
		return commitResult{}, err
	}
	if !hasChanges {
		return commitResult{}, fmt.Errorf("no changes to commit")
	}

	hookDir, err := os.MkdirTemp("", "parley-hooks-disabled-*")
	if err != nil {
		return commitResult{}, fmt.Errorf("create disabled hooks dir: %w", err)
	}
	defer os.RemoveAll(hookDir)

	subject := commitSubject(opts.Idea)
	body := commitBody(opts.ReportSummary, opts.RunID, opts.TaskID, opts.DiffArtifactID)
	args := gitCommitArgs(identity, hookDir, subject, body)
	if _, err := runGitCommand(ctx, git, opts.WorktreePath, args...); err != nil {
		return commitResult{}, fmt.Errorf("git commit --no-verify: %w", err)
	}
	shaBytes, err := runGitCommand(ctx, git, opts.WorktreePath, "rev-parse", "HEAD")
	if err != nil {
		return commitResult{}, fmt.Errorf("read commit sha: %w", err)
	}
	return commitResult{Branch: branch, CommitSHA: strings.TrimSpace(string(shaBytes)), AuthorName: identity.Name, AuthorEmail: identity.Email}, nil
}

func configuredGitIdentity(name, email string) gitIdentity {
	if name == "" {
		name = os.Getenv("PARLEY_GIT_AUTHOR_NAME")
	}
	if email == "" {
		email = os.Getenv("PARLEY_GIT_AUTHOR_EMAIL")
	}
	if name == "" {
		name = defaultGitAuthorName
	}
	if email == "" {
		email = defaultGitAuthorEmail
	}
	return gitIdentity{Name: name, Email: email}
}

func commitSubject(idea string) string {
	first := "update"
	for _, line := range strings.Split(strings.TrimSpace(idea), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			first = line
			break
		}
	}
	return "parley: " + first
}

func commitBody(summary, runID, taskID, diffArtifactID string) string {
	if strings.TrimSpace(summary) == "" {
		summary = "No validation summary provided."
	}
	var b strings.Builder
	b.WriteString(strings.TrimSpace(summary))
	b.WriteString("\n\nRun ID: ")
	b.WriteString(runID)
	b.WriteString("\nTask ID: ")
	b.WriteString(taskID)
	if diffArtifactID != "" {
		b.WriteString("\nDiff artifact ID: ")
		b.WriteString(diffArtifactID)
	}
	b.WriteString("\n")
	return b.String()
}

func gitCommitArgs(identity gitIdentity, hooksPath, subject, body string) []string {
	return []string{
		"-c", "user.name=" + identity.Name,
		"-c", "user.email=" + identity.Email,
		"-c", "core.hooksPath=" + hooksPath,
		"commit", "--no-verify", "-m", subject, "-m", body,
	}
}

func gitHasStagedChanges(ctx context.Context, git, dir string) (bool, error) {
	cmd := exec.CommandContext(ctx, git, "diff", "--cached", "--quiet", "--no-ext-diff", "--")
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return false, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return true, nil
	}
	if stderr.Len() > 0 {
		return false, fmt.Errorf("git diff --cached --quiet: %w: %s", err, stderr.String())
	}
	return false, fmt.Errorf("git diff --cached --quiet: %w", err)
}

func runGitCommand(ctx context.Context, git, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, git, args...)
	cmd.Dir = dir
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("git %v: %w: %s", args, err, stderr.String())
		}
		return nil, fmt.Errorf("git %v: %w", args, err)
	}
	return stdout.Bytes(), nil
}
