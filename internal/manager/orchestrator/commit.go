package orchestrator

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	defaultGitAuthorName  = "Parley Agent"
	defaultGitAuthorEmail = "agent@agent-parley.dev"
)

type commitOptions struct {
	WorktreePath   string
	BaseSHA        string
	BaseTreeSHA    string
	WorkerTreeSHA  string
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
	if opts.BaseSHA == "" || opts.BaseTreeSHA == "" || opts.WorkerTreeSHA == "" {
		return commitResult{}, fmt.Errorf("base sha, base tree sha, and worker tree sha are required")
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

	if opts.WorkerTreeSHA == opts.BaseTreeSHA {
		return commitResult{}, fmt.Errorf("no changes to commit")
	}

	subject := commitSubject(opts.Idea)
	body := commitBody(opts.ReportSummary, opts.RunID, opts.TaskID, opts.DiffArtifactID)
	commitEnv := []string{
		"GIT_AUTHOR_NAME=" + identity.Name,
		"GIT_AUTHOR_EMAIL=" + identity.Email,
		"GIT_COMMITTER_NAME=" + identity.Name,
		"GIT_COMMITTER_EMAIL=" + identity.Email,
	}
	shaBytes, err := runGitCommandEnv(ctx, git, opts.WorktreePath, commitEnv, "commit-tree", opts.WorkerTreeSHA, "-p", opts.BaseSHA, "-m", subject, "-m", body)
	if err != nil {
		return commitResult{}, fmt.Errorf("git commit-tree: %w", err)
	}
	commitSHA := strings.TrimSpace(string(shaBytes))
	if commitSHA == "" {
		return commitResult{}, fmt.Errorf("git commit-tree returned empty commit sha")
	}
	if _, err := runGitCommand(ctx, git, opts.WorktreePath, "update-ref", "refs/heads/"+branch, commitSHA); err != nil {
		return commitResult{}, fmt.Errorf("update commit branch: %w", err)
	}
	return commitResult{Branch: branch, CommitSHA: commitSHA, AuthorName: identity.Name, AuthorEmail: identity.Email}, nil
}

func snapshotGitWorktree(ctx context.Context, git, worktreePath string) (string, string, string, error) {
	if worktreePath == "" {
		return "", "", "", fmt.Errorf("worktree path is required")
	}
	if git == "" {
		git = "git"
	}
	baseBytes, err := runGitCommand(ctx, git, worktreePath, "rev-parse", "HEAD")
	if err != nil {
		return "", "", "", fmt.Errorf("read base sha: %w", err)
	}
	baseSHA := strings.TrimSpace(string(baseBytes))
	baseTreeBytes, err := runGitCommand(ctx, git, worktreePath, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return "", "", "", fmt.Errorf("read base tree sha: %w", err)
	}
	baseTreeSHA := strings.TrimSpace(string(baseTreeBytes))

	tempDir, err := os.MkdirTemp("", "parley-worker-index-*")
	if err != nil {
		return "", "", "", fmt.Errorf("create worker snapshot index dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	tempIndex := filepath.Join(tempDir, "index")
	env := []string{"GIT_INDEX_FILE=" + tempIndex}
	if _, err := runGitCommandEnv(ctx, git, worktreePath, env, "add", "-A"); err != nil {
		return "", "", "", fmt.Errorf("snapshot worker tree: %w", err)
	}
	workerTreeBytes, err := runGitCommandEnv(ctx, git, worktreePath, env, "write-tree")
	if err != nil {
		return "", "", "", fmt.Errorf("write worker tree: %w", err)
	}
	workerTreeSHA := strings.TrimSpace(string(workerTreeBytes))
	return baseSHA, baseTreeSHA, workerTreeSHA, nil
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

func runGitCommand(ctx context.Context, git, dir string, args ...string) ([]byte, error) {
	return runGitCommandEnv(ctx, git, dir, nil, args...)
}

func runGitCommandEnv(ctx context.Context, git, dir string, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, git, args...)
	cmd.Dir = dir
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
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
