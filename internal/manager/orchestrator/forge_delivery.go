package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/agent-parley/parley/internal/manager/secrets"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/report"
)

const (
	DefaultMergeWaitTimeout = 5 * time.Minute
	MaxMergeWaitTimeout     = 30 * time.Minute
	defaultCheckPollDelay   = 5 * time.Second
)

type ForgeDeliveryClient interface {
	CompletePR(context.Context, ForgeDeliveryRequest) (ForgeDeliveryResult, error)
}

type ForgeDeliveryRequest struct {
	WorktreePath     string
	Branch           string
	TargetBranch     string
	CommitSHA        string
	Title            string
	Body             string
	MergePolicy      string
	MergeMethod      string
	MergeWaitTimeout time.Duration
	CredentialRef    string
	RequiredChecks   []string
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
	git               string
	gh                string
	store             *store.Store
	secrets           *secrets.Service
	checkPollInterval time.Duration
}

func newCLIForgeDeliveryClient(git string, st *store.Store, secretService *secrets.Service) ForgeDeliveryClient {
	if strings.TrimSpace(git) == "" {
		git = "git"
	}
	return cliForgeDeliveryClient{git: git, gh: "gh", store: st, secrets: secretService, checkPollInterval: defaultCheckPollDelay}
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
	if req.MergeWaitTimeout <= 0 {
		req.MergeWaitTimeout = DefaultMergeWaitTimeout
	}
	if req.MergeWaitTimeout > MaxMergeWaitTimeout {
		req.MergeWaitTimeout = MaxMergeWaitTimeout
	}
	credential, err := c.openForgeCredential(ctx, req.CredentialRef)
	if err != nil {
		return ForgeDeliveryResult{}, err
	}
	if _, err := exec.LookPath(c.gh); err != nil && !filepathIsExecutable(c.gh) {
		return ForgeDeliveryResult{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge requires GitHub CLI credentials, but gh is unavailable")
	}
	ghEnv := credential.ghEnv()
	if out, err := c.run(ctx, req.WorktreePath, ghEnv, c.gh, "auth", "status"); err != nil {
		return ForgeDeliveryResult{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge forge credentials are unavailable for credential reference "+req.CredentialRef+": "+cleanCommandError(out, err))
	}

	remote := c.defaultRemote(ctx, req.WorktreePath)
	pushTarget := c.authenticatedPushTarget(ctx, req.WorktreePath, remote, credential)
	if out, err := c.run(ctx, req.WorktreePath, credential.gitEnv(), c.git, "push", "--set-upstream", pushTarget, req.Branch+":"+req.Branch); err != nil {
		return ForgeDeliveryResult{}, fmt.Errorf("push auto-merge branch: %s", cleanCommandError(out, err))
	}
	result := ForgeDeliveryResult{Remote: remote, PushPerformed: true}

	prURL, created, err := c.ensurePullRequest(ctx, req, ghEnv)
	if err != nil {
		return result, err
	}
	result.PRURL = prURL
	result.PRCreated = created
	if prURL == "" {
		return result, fmt.Errorf("create pull request: gh returned no pull request URL")
	}

	checks, err := c.requiredChecksPassed(ctx, req.WorktreePath, ghEnv, prURL, req.RequiredChecks, req.MergeWaitTimeout)
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
	if out, err := c.run(ctx, req.WorktreePath, ghEnv, c.gh, "pr", "merge", prURL, mergeFlag); err != nil {
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

func (c cliForgeDeliveryClient) requiredChecksPassed(ctx context.Context, dir string, credentialEnv []string, prURL string, required []string, timeout time.Duration) ([]string, error) {
	if timeout <= 0 {
		timeout = DefaultMergeWaitTimeout
	}
	if timeout > MaxMergeWaitTimeout {
		timeout = MaxMergeWaitTimeout
	}
	poll := c.checkPollInterval
	if poll <= 0 {
		poll = defaultCheckPollDelay
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		passed, pending, err := c.evaluateRequiredChecks(ctx, dir, credentialEnv, prURL, required)
		if err != nil {
			return passed, err
		}
		if !pending {
			return passed, nil
		}
		select {
		case <-time.After(poll):
		case <-deadline.C:
			return passed, newDeliveryGateError(report.StatusNeedsInput, "auto-merge timed out waiting for required checks to pass")
		case <-ctx.Done():
			return passed, ctx.Err()
		}
	}
}

func (c cliForgeDeliveryClient) evaluateRequiredChecks(ctx context.Context, dir string, credentialEnv []string, prURL string, required []string) ([]string, bool, error) {
	out, err := c.runAllowExit(ctx, dir, credentialEnv, c.gh, "pr", "checks", prURL, "--json", "name,state,bucket")
	if len(strings.TrimSpace(string(out))) == 0 {
		if err != nil {
			return nil, false, fmt.Errorf("read pull request checks: %s", cleanCommandError(out, err))
		}
		return nil, false, newDeliveryGateError(report.StatusFailed, "auto-merge required checks are missing from the pull request")
	}
	var checks []struct {
		Name   string `json:"name"`
		State  string `json:"state"`
		Bucket string `json:"bucket"`
	}
	if parseErr := json.Unmarshal(out, &checks); parseErr != nil {
		if err != nil {
			return nil, false, fmt.Errorf("read pull request checks: %s", cleanCommandError(out, err))
		}
		return nil, false, fmt.Errorf("parse pull request checks: %w", parseErr)
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
	pending := false
	for _, name := range required {
		check, ok := byName[name]
		if !ok {
			return passed, false, newDeliveryGateError(report.StatusFailed, "auto-merge required check "+name+" is missing from the pull request")
		}
		if checkPassed(check.State, check.Bucket) {
			passed = append(passed, name)
			continue
		}
		if checkFailed(check.State, check.Bucket) {
			return passed, false, newDeliveryGateError(report.StatusFailed, "auto-merge required check "+name+" is not passing (state="+check.State+", bucket="+check.Bucket+")")
		}
		pending = true
	}
	return passed, pending, nil
}

func (c cliForgeDeliveryClient) openForgeCredential(ctx context.Context, credentialID string) (forgeCredentialAuth, error) {
	credentialID = strings.TrimSpace(credentialID)
	if credentialID == "" {
		return forgeCredentialAuth{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge requires a forge credential reference")
	}
	if c.store == nil {
		return forgeCredentialAuth{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge forge credential store is unavailable")
	}
	if c.secrets == nil || !c.secrets.Available() {
		state := "unavailable"
		if c.secrets != nil {
			state = string(c.secrets.State())
		}
		return forgeCredentialAuth{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge forge credentials require the secrets facility (state="+state+")")
	}
	credential, err := c.store.GetForgeCredential(ctx, credentialID)
	if err != nil {
		return forgeCredentialAuth{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge forge credential "+credentialID+" was not found")
	}
	table, column, rowID := store.ForgeCredentialSecretAD(credential.ID)
	plaintext, err := c.secrets.Open(ctx, credential.SecretCiphertext, secrets.AssociatedData{Table: table, Column: column, RowID: rowID})
	if err != nil {
		return forgeCredentialAuth{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge forge credential "+credentialID+" could not be opened: "+err.Error())
	}
	token := strings.TrimSpace(string(plaintext))
	if token == "" {
		return forgeCredentialAuth{}, newDeliveryGateError(report.StatusNeedsInput, "auto-merge forge credential "+credentialID+" is empty")
	}
	host := strings.TrimSpace(credential.Host)
	if host == "" {
		host = "github.com"
	}
	return forgeCredentialAuth{host: host, token: token}, nil
}

func (c cliForgeDeliveryClient) defaultRemote(ctx context.Context, dir string) string {
	remote := "origin"
	if out, err := c.run(ctx, dir, nil, c.git, "remote"); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if strings.TrimSpace(line) == "origin" {
				return "origin"
			}
			if strings.TrimSpace(line) != "" {
				remote = strings.TrimSpace(line)
			}
		}
	}
	return remote
}

func (c cliForgeDeliveryClient) authenticatedPushTarget(ctx context.Context, dir, remote string, credential forgeCredentialAuth) string {
	out, err := c.run(ctx, dir, nil, c.git, "remote", "get-url", "--push", remote)
	if err != nil || strings.TrimSpace(string(out)) == "" {
		out, err = c.run(ctx, dir, nil, c.git, "remote", "get-url", remote)
	}
	if err != nil {
		return remote
	}
	if httpsURL := githubHTTPSRemote(strings.TrimSpace(string(out)), credential.host); httpsURL != "" {
		return httpsURL
	}
	return remote
}

func (c cliForgeDeliveryClient) run(ctx context.Context, dir string, extraEnv []string, name string, args ...string) ([]byte, error) {
	out, err := c.runAllowExit(ctx, dir, extraEnv, name, args...)
	return out, err
}

func (c cliForgeDeliveryClient) runAllowExit(ctx context.Context, dir string, extraEnv []string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	if len(extraEnv) > 0 {
		cmd.Env = mergeEnv(os.Environ(), extraEnv)
	}
	return cmd.CombinedOutput()
}

type forgeCredentialAuth struct {
	host  string
	token string
}

func (c forgeCredentialAuth) ghEnv() []string {
	host := strings.TrimSpace(c.host)
	if host == "" {
		host = "github.com"
	}
	return []string{"GH_TOKEN=" + c.token, "GH_HOST=" + host}
}

func (c forgeCredentialAuth) gitEnv() []string {
	host := strings.TrimSpace(c.host)
	if host == "" {
		host = "github.com"
	}
	return []string{
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=http.https://" + host + "/.extraheader",
		"GIT_CONFIG_VALUE_0=AUTHORIZATION: bearer " + c.token,
	}
}

func githubHTTPSRemote(raw, host string) string {
	raw = strings.TrimSpace(raw)
	host = strings.TrimSpace(host)
	if host == "" {
		host = "github.com"
	}
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "git@"+host+":") {
		path := strings.TrimPrefix(raw, "git@"+host+":")
		if path != "" {
			return "https://" + host + "/" + path
		}
	}
	if strings.HasPrefix(raw, "ssh://git@"+host+"/") {
		path := strings.TrimPrefix(raw, "ssh://git@"+host+"/")
		if path != "" {
			return "https://" + host + "/" + path
		}
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if (parsed.Scheme == "https" || parsed.Scheme == "http") && strings.EqualFold(parsed.Host, host) {
		parsed.Scheme = "https"
		parsed.User = nil
		return parsed.String()
	}
	return ""
}

func checkPassed(state, bucket string) bool {
	state = strings.ToUpper(strings.TrimSpace(state))
	bucket = strings.ToUpper(strings.TrimSpace(bucket))
	return state == "PASS" || state == "PASSED" || state == "SUCCESS" || state == "SUCCESSFUL" || bucket == "PASS" || bucket == "PASSED" || bucket == "SUCCESS"
}

func checkFailed(state, bucket string) bool {
	state = strings.ToUpper(strings.TrimSpace(state))
	bucket = strings.ToUpper(strings.TrimSpace(bucket))
	switch state {
	case "FAIL", "FAILED", "FAILURE", "ERROR", "CANCELLED", "CANCELED", "SKIPPED", "TIMED_OUT", "ACTION_REQUIRED":
		return true
	}
	switch bucket {
	case "FAIL", "FAILED", "FAILURE", "ERROR", "CANCELLED", "CANCELED", "SKIPPED", "TIMED_OUT", "ACTION_REQUIRED":
		return true
	default:
		return false
	}
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

func mergeEnv(base, overrides []string) []string {
	positions := map[string]int{}
	merged := append([]string{}, base...)
	for i, value := range merged {
		if key := envKey(value); key != "" {
			positions[key] = i
		}
	}
	for _, value := range overrides {
		key := envKey(value)
		if key == "" {
			continue
		}
		if pos, ok := positions[key]; ok {
			merged[pos] = value
			continue
		}
		positions[key] = len(merged)
		merged = append(merged, value)
	}
	return merged
}

func envKey(value string) string {
	key, _, ok := strings.Cut(value, "=")
	if !ok {
		return ""
	}
	return key
}

func filepathIsExecutable(path string) bool {
	if strings.ContainsRune(path, os.PathSeparator) {
		info, err := os.Stat(path)
		return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
	}
	return false
}
