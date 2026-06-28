package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/agent-parley/parley/internal/shared/report"
)

type ForgeDeliveryClient interface {
	CompletePR(context.Context, ForgeDeliveryRequest) (ForgeDeliveryResult, error)
}

type ForgeDeliveryRequest struct {
	WorktreePath   string
	Branch         string
	TargetBranch   string
	CommitSHA      string
	Title          string
	Body           string
	MergePolicy    string
	MergeMethod    string
	CredentialRef  string
	RequiredChecks []string
}

type ForgeDeliveryResult struct {
	Remote         string
	PRURL          string
	PRNumber       string
	MergeCommitSHA string
	PushPerformed  bool
	PRCreated      bool
	MergeAttempted bool
	Merged         bool
	ChecksPassed   []string
}

type deliveryGateError struct {
	status string
	reason string
}

func (e deliveryGateError) Error() string { return e.reason }

func newDeliveryGateError(status, reason string) deliveryGateError {
	if status == "" {
		status = report.StatusNeedsInput
	}
	return deliveryGateError{status: status, reason: strings.TrimSpace(reason)}
}

func asDeliveryGateError(err error) (deliveryGateError, bool) {
	var gateErr deliveryGateError
	if errors.As(err, &gateErr) {
		return gateErr, true
	}
	return deliveryGateError{}, false
}

type cliForgeDeliveryClient struct {
	git string
	gh  string
}

func newCLIForgeDeliveryClient(git string) ForgeDeliveryClient {
	if strings.TrimSpace(git) == "" {
		git = "git"
	}
	return cliForgeDeliveryClient{git: git, gh: "gh"}
}

func (c cliForgeDeliveryClient) CompletePR(ctx context.Context, req ForgeDeliveryRequest) (ForgeDeliveryResult, error) {
	if req.WorktreePath == "" {
		return ForgeDeliveryResult{}, newDeliveryGateError(report.StatusFailed, "auto-merge requires a worktree path")
	}
	if req.Branch == "" || req.CommitSHA == "" {
		return ForgeDeliveryResult{}, newDeliveryGateError(report.StatusFailed, "auto-merge requires branch and commit_sha from the commit stage")
	}
	if req.TargetBranch == "" {
		return ForgeDeliveryResult{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge requires a configured target branch")
	}
	if req.MergeMethod == "" {
		req.MergeMethod = "merge"
	}
	credentialEnv, err := githubCredentialEnv(req.CredentialRef)
	if err != nil {
		return ForgeDeliveryResult{}, err
	}
	if _, err := exec.LookPath(c.gh); err != nil {
		return ForgeDeliveryResult{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge requires GitHub CLI credentials, but gh is unavailable")
	}
	if out, err := c.run(ctx, req.WorktreePath, credentialEnv, c.gh, "auth", "status"); err != nil {
		return ForgeDeliveryResult{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge forge credentials are unavailable for credential reference "+req.CredentialRef+": "+cleanCommandError(out, err))
	}

	remote := "origin"
	if out, err := c.run(ctx, req.WorktreePath, nil, c.git, "remote"); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if strings.TrimSpace(line) == "origin" {
				remote = "origin"
				break
			}
			if strings.TrimSpace(line) != "" {
				remote = strings.TrimSpace(line)
			}
		}
	}
	if out, err := c.run(ctx, req.WorktreePath, nil, c.git, "push", "--set-upstream", remote, req.Branch+":"+req.Branch); err != nil {
		return ForgeDeliveryResult{}, fmt.Errorf("push auto-merge branch: %s", cleanCommandError(out, err))
	}
	result := ForgeDeliveryResult{Remote: remote, PushPerformed: true}

	prURL, created, err := c.ensurePullRequest(ctx, req, credentialEnv)
	if err != nil {
		return result, err
	}
	result.PRURL = prURL
	result.PRCreated = created
	if prURL == "" {
		return result, fmt.Errorf("create pull request: gh returned no pull request URL")
	}

	checks, err := c.requiredChecksPassed(ctx, req.WorktreePath, credentialEnv, prURL, req.RequiredChecks)
	if err != nil {
		return result, err
	}
	result.ChecksPassed = checks
	result.MergeAttempted = true

	mergeFlag := "--merge"
	switch strings.ToLower(strings.TrimSpace(req.MergeMethod)) {
	case "squash":
		mergeFlag = "--squash"
	case "rebase":
		mergeFlag = "--rebase"
	case "merge", "":
		mergeFlag = "--merge"
	default:
		return result, newDeliveryGateError(report.StatusNeedsInput, "unsupported auto-merge method "+req.MergeMethod)
	}
	if out, err := c.run(ctx, req.WorktreePath, credentialEnv, c.gh, "pr", "merge", prURL, mergeFlag); err != nil {
		return result, fmt.Errorf("merge pull request: %s", cleanCommandError(out, err))
	}
	result.Merged = true
	return result, nil
}

func (c cliForgeDeliveryClient) ensurePullRequest(ctx context.Context, req ForgeDeliveryRequest, credentialEnv []string) (string, bool, error) {
	if out, err := c.run(ctx, req.WorktreePath, credentialEnv, c.gh, "pr", "view", req.Branch, "--json", "url", "--jq", ".url"); err == nil {
		if url := strings.TrimSpace(string(out)); url != "" {
			return url, false, nil
		}
	}
	args := []string{"pr", "create", "--base", req.TargetBranch, "--head", req.Branch, "--title", req.Title, "--body", req.Body}
	out, err := c.run(ctx, req.WorktreePath, credentialEnv, c.gh, args...)
	if err != nil {
		return "", false, fmt.Errorf("create pull request: %s", cleanCommandError(out, err))
	}
	return firstURL(strings.TrimSpace(string(out))), true, nil
}

func (c cliForgeDeliveryClient) requiredChecksPassed(ctx context.Context, dir string, credentialEnv []string, prURL string, required []string) ([]string, error) {
	out, err := c.runAllowExit(ctx, dir, credentialEnv, c.gh, "pr", "checks", prURL, "--json", "name,state,bucket")
	if len(strings.TrimSpace(string(out))) == 0 {
		if err != nil {
			return nil, fmt.Errorf("read pull request checks: %s", cleanCommandError(out, err))
		}
		return nil, newDeliveryGateError(report.StatusFailed, "auto-merge required checks are missing from the pull request")
	}
	var checks []struct {
		Name   string `json:"name"`
		State  string `json:"state"`
		Bucket string `json:"bucket"`
	}
	if parseErr := json.Unmarshal(out, &checks); parseErr != nil {
		if err != nil {
			return nil, fmt.Errorf("read pull request checks: %s", cleanCommandError(out, err))
		}
		return nil, fmt.Errorf("parse pull request checks: %w", parseErr)
	}
	byName := map[string]struct {
		State  string
		Bucket string
	}{}
	for _, check := range checks {
		name := strings.TrimSpace(check.Name)
		if name == "" {
			continue
		}
		byName[name] = struct {
			State  string
			Bucket string
		}{State: check.State, Bucket: check.Bucket}
	}
	passed := make([]string, 0, len(required))
	for _, name := range required {
		check, ok := byName[name]
		if !ok {
			return passed, newDeliveryGateError(report.StatusFailed, "auto-merge required check "+name+" is missing from the pull request")
		}
		if !checkPassed(check.State, check.Bucket) {
			return passed, newDeliveryGateError(report.StatusFailed, "auto-merge required check "+name+" is not passing (state="+check.State+", bucket="+check.Bucket+")")
		}
		passed = append(passed, name)
	}
	return passed, nil
}

func (c cliForgeDeliveryClient) run(ctx context.Context, dir string, extraEnv []string, name string, args ...string) ([]byte, error) {
	out, err := c.runAllowExit(ctx, dir, extraEnv, name, args...)
	return out, err
}

func (c cliForgeDeliveryClient) runAllowExit(ctx context.Context, dir string, extraEnv []string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	return cmd.CombinedOutput()
}

func githubCredentialEnv(ref string) ([]string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, newDeliveryGateError(report.StatusNeedsInput, "auto-merge requires a forge credential reference")
	}
	lower := strings.ToLower(ref)
	switch lower {
	case "gh", "github", "github-cli", "gh-cli":
		return nil, nil
	case "tea", "gitea", "gitea-cli", "tea-cli":
		return nil, newDeliveryGateError(report.StatusNeedsInput, "auto-merge currently requires a GitHub CLI credential reference; Gitea merge support is not configured")
	}
	if strings.HasPrefix(lower, "env:") {
		name := strings.TrimSpace(ref[len("env:"):])
		if name == "" {
			return nil, newDeliveryGateError(report.StatusNeedsInput, "auto-merge forge credential env reference is empty")
		}
		value := os.Getenv(name)
		if value == "" {
			return nil, newDeliveryGateError(report.StatusNeedsInput, "auto-merge forge credential env var "+name+" is not set")
		}
		if name == "GH_TOKEN" || name == "GITHUB_TOKEN" {
			return nil, nil
		}
		return []string{"GH_TOKEN=" + value}, nil
	}
	return nil, newDeliveryGateError(report.StatusNeedsInput, "unsupported auto-merge forge credential reference "+ref)
}

func checkPassed(state, bucket string) bool {
	state = strings.ToUpper(strings.TrimSpace(state))
	bucket = strings.ToUpper(strings.TrimSpace(bucket))
	return state == "PASS" || state == "PASSED" || state == "SUCCESS" || state == "SUCCESSFUL" || bucket == "PASS" || bucket == "PASSED" || bucket == "SUCCESS"
}

func firstURL(out string) string {
	for _, field := range strings.Fields(out) {
		if strings.HasPrefix(field, "http://") || strings.HasPrefix(field, "https://") {
			return strings.TrimSpace(field)
		}
	}
	if strings.HasPrefix(out, "http://") || strings.HasPrefix(out, "https://") {
		return strings.TrimSpace(out)
	}
	return ""
}

func cleanCommandError(out []byte, err error) string {
	text := strings.TrimSpace(string(out))
	if text == "" && err != nil {
		text = err.Error()
	}
	if text == "" {
		text = "command failed"
	}
	if len(text) > 500 {
		text = text[:500] + "…"
	}
	return text
}
