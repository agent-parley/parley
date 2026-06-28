package orchestrator

import (
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/report"
)

const memoryCapturePayloadKey = "learning_opportunities"

var memoryCaptureCandidatePayloadKeys = []string{
	"memory_candidates",
	"project_memory_candidates",
	memoryCapturePayloadKey,
}

func addMemoryCaptureInput(input map[string]any, template workflow.Template, stage workflow.StageTemplate) {
	memoryStageID := memoryCaptureStageID(template)
	if memoryStageID == "" || !memoryCaptureProducerStage(stage) {
		return
	}
	input["memory_capture_enabled"] = true
	input["memory_capture_payload_key"] = memoryCapturePayloadKey
	input["memory_update_workflow_stage_id"] = memoryStageID
	input["memory_capture_policy"] = map[string]any{
		"payload_key":       memoryCapturePayloadKey,
		"candidate_kinds":   projectMemoryCandidateKinds(),
		"max_candidates":    store.ProjectMemoryMaxEntriesPerUpdate,
		"source_linking":    "the manager links accepted candidates to this stage report artifact; include source_summary explaining the stage evidence",
		"durable_writes":    "do not write project memory directly; the Memory update stage curates and writes accepted candidates",
		"forbidden_content": []string{"secrets or credentials", "standing instructions", "raw logs or transcripts", "speculative plans", "current code truth"},
	}
}

func memoryCaptureStageID(template workflow.Template) string {
	for _, stage := range template.Stages {
		if stage.Type == workflow.StageTypeMemoryUpdate {
			return stage.ID
		}
	}
	return ""
}

func memoryCaptureProducerStage(stage workflow.StageTemplate) bool {
	if stage.Actor != workflow.ActorAgent {
		return false
	}
	switch stage.Type {
	case workflow.StageTypeImplementation, workflow.StageTypeReview:
		return true
	default:
		return false
	}
}

func projectMemoryCandidateKinds() []string {
	return []string{
		store.ProjectMemoryKindLesson,
		store.ProjectMemoryKindRepoFact,
		store.ProjectMemoryKindGotcha,
		store.ProjectMemoryKindImplementationLandmark,
		store.ProjectMemoryKindPriorResult,
		store.ProjectMemoryKindDecision,
		store.ProjectMemoryKindFreshnessNote,
	}
}

func memoryCaptureInputEnabled(input map[string]any) bool {
	enabled, _ := input["memory_capture_enabled"].(bool)
	return enabled
}

func enforceMemoryCapturePayloadPolicy(rep report.Report, enabled bool) report.Report {
	if !enabled {
		if rep.Payload == nil {
			return rep
		}
		for _, key := range memoryCaptureCandidatePayloadKeys {
			delete(rep.Payload, key)
		}
		return rep
	}
	if rep.Payload == nil {
		rep.Payload = map[string]any{}
	}
	if _, ok := rep.Payload[memoryCapturePayloadKey]; ok {
		return rep
	}
	if raw, ok := firstPayloadValue(rep.Payload, "memory_candidates", "project_memory_candidates"); ok {
		rep.Payload[memoryCapturePayloadKey] = raw
		return rep
	}
	rep.Payload[memoryCapturePayloadKey] = []any{}
	return rep
}
