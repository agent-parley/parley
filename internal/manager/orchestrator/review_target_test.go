package orchestrator

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestReviewBaseInputBuildsPacketsForNewTargets(t *testing.T) {
	wr := store.WorkflowRun{Run: store.Run{ID: "run1", ProjectID: "p1", Idea: "ship it"}, Task: store.Task{ID: "task1"}, Attempt: store.Attempt{ID: "attempt1"}}
	validationReport := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       "stage_validation",
		StageType:     contract.StageTypeValidation,
		Status:        report.StatusCompleted,
		Summary:       "validation passed",
		EvidenceRefs:  []string{"validation-log"},
		Payload:       validationEvidencePayload(),
	}
	deliveryReport := report.Report{
		SchemaVersion: report.SchemaVersion,
		RunID:         wr.Run.ID,
		TaskID:        wr.Task.ID,
		AttemptID:     wr.Attempt.ID,
		StageID:       "stage_pr",
		StageType:     contract.StageTypePRCreation,
		Status:        report.StatusCompleted,
		Summary:       "PR-ready handoff reached",
		EvidenceRefs:  []string{"diff-artifact"},
		Payload: map[string]any{
			"branch":           "agent/issue-159",
			"commit_sha":       "abc123",
			"diff_artifact_id": "diff-artifact",
			"pr_created":       false,
		},
	}
	snapshot := workerSnapshot{BaseSHA: "base", WorkerTreeSHA: "worker", DiffArtifactID: "diff-artifact"}

	validationInput := reviewBaseInput(wr, reviewTargetStage(contract.ReviewTargetValidationEvidence), deliveryReport, validationReport, report.Report{}, snapshot, nil)
	validationPacket := mustPacket(t, validationInput["review_target_packet"])
	if validationPacket["target"] != contract.ReviewTargetValidationEvidence {
		t.Fatalf("validation target packet target = %#v", validationPacket["target"])
	}
	if validationPacket["validation_report"] == nil || validationPacket["expected_evidence_fields"] == nil {
		t.Fatalf("validation target packet missing evidence: %#v", validationPacket)
	}

	deliveryInput := reviewBaseInput(wr, reviewTargetStage(contract.ReviewTargetDeliveryResult), deliveryReport, validationReport, deliveryReport, snapshot, nil)
	deliveryPacket := mustPacket(t, deliveryInput["review_target_packet"])
	if deliveryPacket["target"] != contract.ReviewTargetDeliveryResult {
		t.Fatalf("delivery target packet target = %#v", deliveryPacket["target"])
	}
	if deliveryPacket["delivery_report"] == nil || deliveryPacket["validation_report"] == nil || deliveryPacket["implementation_snapshot"] == nil {
		t.Fatalf("delivery target packet missing handoff evidence: %#v", deliveryPacket)
	}

	memoryReport := report.Report{StageID: "stage_memory", StageType: contract.StageTypeMemoryUpdate, Status: report.StatusCompleted, Summary: "memory updated"}
	deliveryAfterMemoryInput := reviewBaseInput(wr, reviewTargetStage(contract.ReviewTargetDeliveryResult), memoryReport, validationReport, deliveryReport, snapshot, nil)
	deliveryAfterMemoryPacket := mustPacket(t, deliveryAfterMemoryInput["review_target_packet"])
	if deliveryAfterMemoryPacket["delivery_report"] == nil {
		t.Fatalf("delivery target packet did not preserve prior delivery evidence after memory stage: %#v", deliveryAfterMemoryPacket)
	}
}

func TestHumanReviewPacketsIncludeNewTargetEvidence(t *testing.T) {
	cases := []struct {
		name              string
		target            string
		includePRCreation bool
		wantPacketKey     string
	}{
		{name: "validation evidence", target: workflow.TargetValidationEvidence, wantPacketKey: "validation_report"},
		{name: "delivery result", target: workflow.TargetDeliveryResult, includePRCreation: true, wantPacketKey: "delivery_report"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, t.TempDir())
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			template := reviewTargetHumanTemplate("human_"+tc.target, tc.target, tc.includePRCreation)
			if err := st.CreateWorkflowTemplate(ctx, template); err != nil {
				t.Fatalf("create workflow template: %v", err)
			}
			engine := newRecordingEngine(t, st, reviewTargetPacketRunner{}, EngineOptions{})
			runID, err := engine.StartRunInput(ctx, contract.TaskInput{Idea: "review " + tc.target, RefinementLevel: contract.RefinementLevelDirect, WorkflowTemplateID: template.ID})
			if err != nil {
				t.Fatalf("StartRunInput() error = %v", err)
			}
			freezeRunWorkflowSnapshot(t, engine, st, runID)
			waitForWorkflowStageAwaiting(t, st, runID, "human_review")

			awaitingEvent := eventByWorkflowStage(t, st, runID, "stage.awaiting_human", "human_review")
			packetArtifactID, _ := awaitingEvent.Data["human_review_packet_id"].(string)
			if packetArtifactID == "" {
				t.Fatalf("awaiting event missing packet id: %#v", awaitingEvent.Data)
			}
			_, content, err := st.GetArtifact(ctx, packetArtifactID)
			if err != nil {
				t.Fatalf("get packet artifact: %v", err)
			}
			var packet map[string]any
			if err := json.Unmarshal(content, &packet); err != nil {
				t.Fatalf("decode packet: %v", err)
			}
			targetPacket := mustPacket(t, packet["review_target_packet"])
			if targetPacket["target"] != tc.target {
				t.Fatalf("target packet target = %#v, want %q", targetPacket["target"], tc.target)
			}
			if targetPacket[tc.wantPacketKey] == nil {
				t.Fatalf("target packet missing %s: %#v", tc.wantPacketKey, targetPacket)
			}
		})
	}
}

func reviewTargetStage(target string) workflow.StageTemplate {
	return workflow.StageTemplate{
		ID:     "review_" + target,
		Type:   workflow.StageTypeReview,
		Label:  "Review " + target,
		Actor:  workflow.ActorAgent,
		Target: target,
		Settings: map[string]any{
			"profile":   contract.ReviewProfileGeneralist,
			"intensity": contract.ReviewIntensityNormal,
		},
	}
}

func reviewTargetHumanTemplate(id, target string, includePRCreation bool) workflow.Template {
	stages := []workflow.StageTemplate{
		{ID: "idea_refinement", Type: workflow.StageTypeIdeaRefinement, Label: "Idea refinement", Actor: workflow.ActorHarness},
		{ID: "implementation", Type: workflow.StageTypeImplementation, Label: "Implementation", Actor: workflow.ActorAgent},
		{ID: "validation", Type: workflow.StageTypeValidation, Label: "Validation", Actor: workflow.ActorHarness},
	}
	if includePRCreation {
		stages = append(stages, workflow.StageTemplate{ID: "pr_creation", Type: workflow.StageTypePRCreation, Label: "PR creation", Actor: workflow.ActorHarness})
	}
	stages = append(stages,
		workflow.StageTemplate{ID: "human_review", Type: workflow.StageTypeReview, Label: "Human review", Actor: workflow.ActorHuman, Target: target, Settings: map[string]any{"profile": contract.ReviewProfileGeneralist, "intensity": contract.ReviewIntensityNormal}},
		workflow.StageTemplate{ID: "stop_report", Type: workflow.StageTypeStopReport, Label: "Stop/report", Actor: workflow.ActorHarness},
	)
	template := workflow.Template{SchemaVersion: workflow.SchemaVersion, ID: id, Name: id, Editable: true, Stages: stages, Settings: map[string]any{"fix_loop": false, "pr_behavior": "create_pr"}}
	template.Edges = workflow.DeriveTemplateEdges(template)
	return template
}

type reviewTargetPacketRunner struct{}

func (reviewTargetPacketRunner) Dispatch(_ context.Context, disp contract.Dispatch) (report.Report, error) {
	rep := validAdapterReport(disp, "completed")
	switch disp.StageType {
	case contract.StageTypeImplementation:
		rep.Payload = map[string]any{"diff_artifact_id": "diff-artifact"}
	case contract.StageTypeValidation:
		rep.Summary = "validation passed"
		rep.EvidenceRefs = []string{"validation-log"}
		rep.Payload = validationEvidencePayload()
	}
	return rep, nil
}

func (reviewTargetPacketRunner) CancelAttempt(context.Context, string, string, string) error {
	return nil
}

func validationEvidencePayload() map[string]any {
	return map[string]any{
		report.ValidationOutputPayloadKey: report.ValidationOutput{
			Result: report.ValidationResultPassed,
			ChecksRun: []report.ValidationCheck{{
				Name:    "go test ./internal/manager/orchestrator",
				Status:  report.ValidationCheckPassed,
				Summary: "orchestrator tests passed",
			}},
			Outputs:             []report.ValidationOutputRef{{ID: "validation-log", Name: "validation.log", Kind: "log"}},
			Failures:            []report.ValidationFailure{},
			Skipped:             []report.ValidationSkippedCheck{},
			EnvNotes:            []string{},
			Confidence:          report.ValidationConfidenceHigh,
			SuggestedNextAction: "deliver",
		},
	}
}

func mustPacket(t *testing.T, value any) map[string]any {
	t.Helper()
	packet, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("packet = %#v, want map", value)
	}
	return packet
}
