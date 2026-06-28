package orchestrator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestPRReadyStageAutoMergeAttemptsWithChecksAndCredentials(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	client := &fakeForgeDeliveryClient{result: ForgeDeliveryResult{
		PushPerformed:  true,
		PRCreated:      true,
		MergeAttempted: true,
		Merged:         true,
		PRURL:          "https://github.com/acme/repo/pull/42",
		PRNumber:       "42",
		ChecksPassed:   []string{"ci/test", "lint"},
	}}
	engine := newRecordingEngine(t, st, nil, EngineOptions{ForgeDeliveryClient: client})
	template := createDeliveryTemplate(t, ctx, st, "auto_delivery", map[string]any{
		"branch_policy":      "feature_branch",
		"pr_behavior":        "create_pr",
		"merge_policy":       "auto_merge",
		"required_checks":    []string{"ci/test", "lint"},
		"forge_credential":   "fcr_test",
		"target_branch":      "main",
		"merge_wait_timeout": "2s",
	})
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "ship auto merge", WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rep, err := engine.runPRReadyStage(ctx, wr, wr.PRReadyStage, commitReportForDelivery(), template, stageTemplateByID(t, template, "pr_creation"))
	if err != nil {
		t.Fatalf("runPRReadyStage() error = %v", err)
	}
	if rep.Status != report.StatusCompleted {
		t.Fatalf("status = %s, want completed; report=%#v", rep.Status, rep)
	}
	if len(client.requests) != 1 {
		t.Fatalf("forge requests = %d, want 1", len(client.requests))
	}
	req := client.requests[0]
	if req.Branch != "agent/test" || req.CommitSHA != "abc123" || req.TargetBranch != "main" || req.CredentialRef != "fcr_test" {
		t.Fatalf("forge request = %+v", req)
	}
	if strings.Join(req.RequiredChecks, ",") != "ci/test,lint" {
		t.Fatalf("required checks = %#v", req.RequiredChecks)
	}
	if req.MergeWaitTimeout != 2*time.Second {
		t.Fatalf("merge wait timeout = %s, want 2s", req.MergeWaitTimeout)
	}
	if rep.Payload["auto_merge_attempted"] != true || rep.Payload["auto_merge_completed"] != true {
		t.Fatalf("auto-merge payload = %+v", rep.Payload)
	}
	if rep.Payload["push_performed"] != true || rep.Payload["pr_created"] != true || payloadString(rep.Payload, "pr_url") == "" {
		t.Fatalf("PR delivery payload = %+v", rep.Payload)
	}
}

func TestPRReadyStageDoesNotAutoMergeHumanGatedPolicy(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	client := &fakeForgeDeliveryClient{err: newDeliveryGateError(report.StatusFailed, "should not be called")}
	engine := newRecordingEngine(t, st, nil, EngineOptions{ForgeDeliveryClient: client})
	template := createDeliveryTemplate(t, ctx, st, "human_gated_delivery", map[string]any{
		"branch_policy":    "feature_branch",
		"pr_behavior":      "create_pr",
		"merge_policy":     "human_stop",
		"required_checks":  []string{"ci/test"},
		"forge_credential": "fcr_test",
	})
	wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "stop for human", WorkflowTemplateID: template.ID})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rep, err := engine.runPRReadyStage(ctx, wr, wr.PRReadyStage, commitReportForDelivery(), template, stageTemplateByID(t, template, "pr_creation"))
	if err != nil {
		t.Fatalf("runPRReadyStage() error = %v", err)
	}
	if rep.Status != report.StatusCompleted {
		t.Fatalf("status = %s, want completed", rep.Status)
	}
	if len(client.requests) != 0 {
		t.Fatalf("forge client was called under human-gated policy: %+v", client.requests)
	}
	if rep.Payload["auto_merge_attempted"] != false || rep.Payload["auto_merge_completed"] != false {
		t.Fatalf("auto-merge payload = %+v", rep.Payload)
	}
	if got := payloadString(rep.Payload, "auto_merge_skipped_reason"); !strings.Contains(got, "does not enable auto-merge") {
		t.Fatalf("skip reason = %q", got)
	}
}

func TestPRReadyStageAutoMergeMissingChecksOrCredentialsNeedsInput(t *testing.T) {
	cases := []struct {
		name     string
		settings map[string]any
		want     string
	}{
		{
			name: "missing branch policy",
			settings: map[string]any{
				"pr_behavior":      "create_pr",
				"merge_policy":     "auto_merge",
				"target_branch":    "main",
				"required_checks":  []string{"ci/test"},
				"forge_credential": "fcr_test",
			},
			want: "branch_policy",
		},
		{
			name: "missing PR behavior",
			settings: map[string]any{
				"branch_policy":    "feature_branch",
				"merge_policy":     "auto_merge",
				"target_branch":    "main",
				"required_checks":  []string{"ci/test"},
				"forge_credential": "fcr_test",
			},
			want: "pr_behavior",
		},
		{
			name: "missing target branch",
			settings: map[string]any{
				"branch_policy":    "feature_branch",
				"pr_behavior":      "create_pr",
				"merge_policy":     "auto_merge",
				"required_checks":  []string{"ci/test"},
				"forge_credential": "fcr_test",
			},
			want: "target branch",
		},
		{
			name: "missing checks",
			settings: map[string]any{
				"branch_policy":    "feature_branch",
				"pr_behavior":      "create_pr",
				"merge_policy":     "auto_merge",
				"target_branch":    "main",
				"forge_credential": "fcr_test",
			},
			want: "required check",
		},
		{
			name: "missing credential",
			settings: map[string]any{
				"branch_policy":   "feature_branch",
				"pr_behavior":     "create_pr",
				"merge_policy":    "auto_merge",
				"target_branch":   "main",
				"required_checks": []string{"ci/test"},
			},
			want: "credential",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, t.TempDir())
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			client := &fakeForgeDeliveryClient{err: newDeliveryGateError(report.StatusFailed, "should not be called")}
			engine := newRecordingEngine(t, st, nil, EngineOptions{ForgeDeliveryClient: client})
			template := createDeliveryTemplate(t, ctx, st, "auto_missing_"+strings.ReplaceAll(tc.name, " ", "_"), tc.settings)
			wr, err := st.CreateWorkflowRunInput(ctx, contract.TaskInput{Idea: "ship incomplete auto merge", WorkflowTemplateID: template.ID})
			if err != nil {
				t.Fatalf("create run: %v", err)
			}

			rep, err := engine.runPRReadyStage(ctx, wr, wr.PRReadyStage, commitReportForDelivery(), template, stageTemplateByID(t, template, "pr_creation"))
			if err != nil {
				t.Fatalf("runPRReadyStage() error = %v", err)
			}
			if rep.Status != report.StatusNeedsInput {
				t.Fatalf("status = %s, want needs_input; report=%#v", rep.Status, rep)
			}
			if len(client.requests) != 0 {
				t.Fatalf("forge client called despite missing prerequisite: %+v", client.requests)
			}
			if len(rep.Errors) == 0 || !strings.Contains(rep.Errors[0], tc.want) {
				t.Fatalf("errors = %#v, want reason containing %q", rep.Errors, tc.want)
			}
			if rep.Payload["auto_merge_attempted"] != false || payloadString(rep.Payload, "auto_merge_skipped_reason") == "" {
				t.Fatalf("auto-merge payload = %+v", rep.Payload)
			}
		})
	}
}

type fakeForgeDeliveryClient struct {
	requests []ForgeDeliveryRequest
	result   ForgeDeliveryResult
	err      error
}

func (f *fakeForgeDeliveryClient) CompletePR(_ context.Context, req ForgeDeliveryRequest) (ForgeDeliveryResult, error) {
	f.requests = append(f.requests, req)
	return f.result, f.err
}

func createDeliveryTemplate(t *testing.T, ctx context.Context, st *store.Store, id string, settings map[string]any) workflow.Template {
	t.Helper()
	template := workflow.Template{
		SchemaVersion: workflow.SchemaVersion,
		ID:            id,
		Name:          "Delivery " + id,
		Editable:      true,
		Stages: []workflow.StageTemplate{
			{ID: "idea_refinement", Type: workflow.StageTypeIdeaRefinement, Label: "Idea refinement", Actor: workflow.ActorHarness},
			{ID: "implementation", Type: workflow.StageTypeImplementation, Label: "Implementation", Actor: workflow.ActorAgent},
			{ID: "commit_feature_branch", Type: workflow.StageTypeCommit, Label: "Commit", Actor: workflow.ActorHarness},
			{ID: "pr_creation", Type: workflow.StageTypePRCreation, Label: "PR creation", Actor: workflow.ActorHarness},
			{ID: "stop_report", Type: workflow.StageTypeStopReport, Label: "Stop/report", Actor: workflow.ActorHarness},
		},
		Settings: settings,
	}
	template.Edges = workflow.DeriveTemplateEdges(template)
	if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
		t.Fatalf("create template: %v", err)
	}
	return template
}

func stageTemplateByID(t *testing.T, template workflow.Template, id string) workflow.StageTemplate {
	t.Helper()
	for _, stage := range template.Stages {
		if stage.ID == id {
			return stage
		}
	}
	t.Fatalf("missing template stage %s", id)
	return workflow.StageTemplate{}
}

func commitReportForDelivery() report.Report {
	return report.Report{Payload: map[string]any{
		"branch":           "agent/test",
		"commit_sha":       "abc123",
		"diff_artifact_id": "implementation_diff",
	}}
}
