package orchestrator

import (
	"context"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/runner/worktree"
	"github.com/agent-parley/parley/internal/shared/report"
)

type prDeliveryOutcome struct {
	Status             string
	Reason             string
	BranchPolicy       string
	PRBehavior         string
	MergePolicy        string
	TargetBranch       string
	RequiredChecks     []string
	CredentialRef      string
	PushPerformed      bool
	PRCreated          bool
	AutoMergeAttempted bool
	AutoMergeCompleted bool
	PRURL              string
	PRNumber           string
	MergeCommitSHA     string
	ChecksPassed       []string
}

func (e *Engine) completePRDelivery(ctx context.Context, wr store.WorkflowRun, branch, commitSHA, diffID string, template workflow.Template) prDeliveryOutcome {
	settings := template.Settings
	out := prDeliveryOutcome{
		Status:       report.StatusCompleted,
		BranchPolicy: settingString(settings, "branch_policy"),
		PRBehavior:   settingString(settings, "pr_behavior"),
		MergePolicy:  settingString(settings, "merge_policy"),
		TargetBranch: settingString(settings, "target_branch"),
	}
	if !workflow.MergePolicyAllowsAutoMerge(out.MergePolicy) {
		out.Reason = "merge_policy does not enable auto-merge"
		return out
	}
	out.RequiredChecks = settingStringList(settings, "required_checks")
	out.CredentialRef = firstSettingString(settings, "forge_credential", "forge_credential_id", "credential_ref")
	if out.BranchPolicy != "feature_branch" {
		out.Status = report.StatusNeedsInput
		out.Reason = "auto-merge requires branch_policy=feature_branch"
		return out
	}
	if out.PRBehavior != "create_pr" {
		out.Status = report.StatusNeedsInput
		out.Reason = "auto-merge requires pr_behavior=create_pr"
		return out
	}
	if branch == "" || commitSHA == "" {
		out.Status = report.StatusFailed
		out.Reason = "auto-merge requires branch and commit_sha from the commit stage"
		return out
	}
	if out.TargetBranch == "" {
		out.Status = report.StatusNeedsInput
		out.Reason = "auto-merge requires a configured target branch"
		return out
	}
	if len(out.RequiredChecks) == 0 {
		out.Status = report.StatusNeedsInput
		out.Reason = "auto-merge requires at least one configured required check"
		return out
	}
	if out.CredentialRef == "" {
		out.Status = report.StatusNeedsInput
		out.Reason = "auto-merge requires a configured forge credential reference"
		return out
	}
	if e.forgeDeliveryClient == nil {
		out.Status = report.StatusNeedsInput
		out.Reason = "auto-merge requires a configured forge delivery client"
		return out
	}

	out.AutoMergeAttempted = true
	result, err := e.forgeDeliveryClient.CompletePR(ctx, ForgeDeliveryRequest{
		WorktreePath:   e.worktreePathForDelivery(wr),
		Branch:         branch,
		TargetBranch:   out.TargetBranch,
		CommitSHA:      commitSHA,
		Title:          deliveryTitle(wr),
		Body:           deliveryBody(wr, diffID),
		MergePolicy:    out.MergePolicy,
		MergeMethod:    settingString(settings, "merge_method"),
		CredentialRef:  out.CredentialRef,
		RequiredChecks: out.RequiredChecks,
	})
	if err != nil {
		out.Status = report.StatusFailed
		out.Reason = "auto-merge failed: " + err.Error()
		if gateErr, ok := asDeliveryGateError(err); ok {
			out.Status = gateErr.status
			out.Reason = gateErr.reason
		}
		out.PushPerformed = result.PushPerformed
		out.PRCreated = result.PRCreated
		out.PRURL = result.PRURL
		out.PRNumber = result.PRNumber
		out.ChecksPassed = result.ChecksPassed
		return out
	}
	out.PushPerformed = result.PushPerformed
	out.PRCreated = result.PRCreated
	out.PRURL = result.PRURL
	out.PRNumber = result.PRNumber
	out.MergeCommitSHA = result.MergeCommitSHA
	out.ChecksPassed = result.ChecksPassed
	out.AutoMergeCompleted = result.Merged
	if !result.Merged {
		out.Status = report.StatusFailed
		out.Reason = "auto-merge did not complete"
	}
	return out
}

func (e *Engine) worktreePathForDelivery(wr store.WorkflowRun) string {
	path, err := worktree.Locate(e.dataRoot, wr.Run.ProjectID, wr.Run.ID, wr.Task.ID, wr.Attempt.ID)
	if err != nil {
		return ""
	}
	return path
}

func deliveryTitle(wr store.WorkflowRun) string {
	return commitSubject(wr.Run.Idea)
}

func deliveryBody(wr store.WorkflowRun, diffID string) string {
	var b strings.Builder
	b.WriteString("Created by Parley.\n\n")
	b.WriteString("Run ID: ")
	b.WriteString(wr.Run.ID)
	b.WriteString("\nTask ID: ")
	b.WriteString(wr.Task.ID)
	if diffID != "" {
		b.WriteString("\nDiff artifact ID: ")
		b.WriteString(diffID)
	}
	b.WriteString("\n")
	return b.String()
}

func firstSettingString(settings map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(settingString(settings, key)); value != "" {
			return value
		}
	}
	return ""
}

func settingStringList(settings map[string]any, key string) []string {
	if settings == nil {
		return nil
	}
	value, ok := settings[key]
	if !ok || value == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	add := func(raw string) {
		for _, part := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == '\n' || r == '\r' || r == ';' }) {
			part = strings.TrimSpace(part)
			if part == "" || seen[part] {
				continue
			}
			seen[part] = true
			out = append(out, part)
		}
	}
	switch v := value.(type) {
	case []string:
		for _, item := range v {
			add(item)
		}
	case []any:
		for _, item := range v {
			add(fmt.Sprint(item))
		}
	default:
		add(fmt.Sprint(v))
	}
	return out
}
