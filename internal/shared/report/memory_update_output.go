package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const MemoryUpdateOutputPayloadKey = "memory_update_output"

type MemoryCandidateState string

const (
	MemoryCandidateApplied  MemoryCandidateState = "applied"
	MemoryCandidateRejected MemoryCandidateState = "rejected"
	MemoryCandidateEdited   MemoryCandidateState = "edited"
	MemoryCandidateMerged   MemoryCandidateState = "merged"
	MemoryCandidateDeferred MemoryCandidateState = "deferred"
)

type MemoryChangeAction string

const (
	MemoryChangeApplied MemoryChangeAction = "applied"
)

// MemoryUpdateOutput is the typed audit payload for memory_update stage reports.
// Reports carry it at Payload[MemoryUpdateOutputPayloadKey].
type MemoryUpdateOutput struct {
	InboxSummary      MemoryInboxSummary        `json:"inbox_summary"`
	Applied           []MemoryCandidateDecision `json:"applied"`
	Rejected          []MemoryCandidateDecision `json:"rejected"`
	Edited            []MemoryCandidateDecision `json:"edited"`
	Merged            []MemoryCandidateDecision `json:"merged"`
	Deferred          []MemoryCandidateDecision `json:"deferred"`
	MemoryChanges     []MemoryChange            `json:"memory_changes"`
	ActorAuthority    MemoryActorAuthority      `json:"actor_authority"`
	SafetyNotes       []string                  `json:"safety_notes"`
	StopReportSummary string                    `json:"stop_report_summary"`
}

type MemoryInboxSummary struct {
	LearningOpportunities int      `json:"learning_opportunities"`
	CandidatesGenerated   int      `json:"candidates_generated"`
	CandidatesCurated     int      `json:"candidates_curated"`
	SourceArtifactRefs    []string `json:"source_artifact_refs"`
}

type MemoryActorAuthority struct {
	Kind      string `json:"kind"`
	ID        string `json:"id"`
	Authority string `json:"authority"`
}

type MemoryCandidateDecision struct {
	CandidateID        string               `json:"candidate_id,omitempty"`
	CandidateIDs       []string             `json:"candidate_ids,omitempty"`
	State              MemoryCandidateState `json:"state"`
	Kind               string               `json:"kind,omitempty"`
	Title              string               `json:"title,omitempty"`
	Body               string               `json:"body,omitempty"`
	Rationale          string               `json:"rationale"`
	EntryID            string               `json:"entry_id,omitempty"`
	EntryIDs           []string             `json:"entry_ids,omitempty"`
	SourceArtifactRefs []string             `json:"source_artifact_refs,omitempty"`
	Freshness          MemoryFreshness      `json:"freshness"`
}

type MemoryChange struct {
	Action             MemoryChangeAction `json:"action"`
	EntryID            string             `json:"entry_id"`
	CandidateIDs       []string           `json:"candidate_ids,omitempty"`
	Kind               string             `json:"kind"`
	Title              string             `json:"title"`
	SourceArtifactRefs []string           `json:"source_artifact_refs"`
	Freshness          MemoryFreshness    `json:"freshness"`
}

type MemoryFreshness struct {
	SourceRunID        string   `json:"source_run_id,omitempty"`
	SourceTaskID       string   `json:"source_task_id,omitempty"`
	SourceStageID      string   `json:"source_stage_id,omitempty"`
	SourceArtifactRefs []string `json:"source_artifact_refs,omitempty"`
	VerifiedAt         string   `json:"verified_at,omitempty"`
	UpdatedAt          string   `json:"updated_at,omitempty"`
}

func (o *MemoryUpdateOutput) Normalize() {
	if o == nil {
		return
	}
	o.InboxSummary.SourceArtifactRefs = cleanMemoryStrings(o.InboxSummary.SourceArtifactRefs)
	for i := range o.MemoryChanges {
		change := &o.MemoryChanges[i]
		if change.Action == "" {
			change.Action = MemoryChangeApplied
		}
		change.CandidateIDs = cleanMemoryStrings(change.CandidateIDs)
		refs := memoryChangeSourceArtifactRefs(*change)
		change.SourceArtifactRefs = refs
		if len(change.Freshness.SourceArtifactRefs) == 0 {
			change.Freshness.SourceArtifactRefs = refs
		} else {
			change.Freshness.SourceArtifactRefs = cleanMemoryStrings(change.Freshness.SourceArtifactRefs)
		}
	}
}

func (o MemoryUpdateOutput) Validate() error {
	var errs []error
	if o.InboxSummary.CandidatesGenerated < 0 {
		errs = append(errs, errors.New("inbox_summary.candidates_generated must be non-negative"))
	}
	if o.InboxSummary.CandidatesCurated < 0 {
		errs = append(errs, errors.New("inbox_summary.candidates_curated must be non-negative"))
	}
	if !validActorKind(o.ActorAuthority.Kind) || strings.TrimSpace(o.ActorAuthority.ID) == "" {
		errs = append(errs, errors.New("actor_authority.kind and actor_authority.id are required"))
	}
	if strings.TrimSpace(o.ActorAuthority.Authority) == "" {
		errs = append(errs, errors.New("actor_authority.authority is required"))
	}
	validateMemoryDecisions(&errs, "applied", o.Applied, MemoryCandidateApplied)
	validateMemoryDecisions(&errs, "rejected", o.Rejected, MemoryCandidateRejected)
	validateMemoryDecisions(&errs, "edited", o.Edited, MemoryCandidateEdited)
	validateMemoryDecisions(&errs, "merged", o.Merged, MemoryCandidateMerged)
	validateMemoryDecisions(&errs, "deferred", o.Deferred, MemoryCandidateDeferred)
	for i, change := range o.MemoryChanges {
		if change.Action == "" {
			change.Action = MemoryChangeApplied
		}
		if change.Action != MemoryChangeApplied {
			errs = append(errs, fmt.Errorf("memory_changes[%d].action %q is invalid", i, change.Action))
		}
		if strings.TrimSpace(change.EntryID) == "" {
			errs = append(errs, fmt.Errorf("memory_changes[%d].entry_id is required", i))
		}
		if strings.TrimSpace(change.Kind) == "" {
			errs = append(errs, fmt.Errorf("memory_changes[%d].kind is required", i))
		}
		if strings.TrimSpace(change.Title) == "" {
			errs = append(errs, fmt.Errorf("memory_changes[%d].title is required", i))
		}
		if len(memoryChangeSourceArtifactRefs(change)) == 0 {
			errs = append(errs, fmt.Errorf("memory_changes[%d].source_artifact_refs must contain at least one artifact ref", i))
		}
	}
	if strings.TrimSpace(o.StopReportSummary) == "" {
		errs = append(errs, errors.New("stop_report_summary is required"))
	}
	return errors.Join(errs...)
}

func MemoryUpdateOutputFromPayload(payload map[string]any) (MemoryUpdateOutput, error) {
	if payload == nil {
		return MemoryUpdateOutput{}, fmt.Errorf("%s is required", MemoryUpdateOutputPayloadKey)
	}
	raw, ok := payload[MemoryUpdateOutputPayloadKey]
	if !ok || raw == nil {
		return MemoryUpdateOutput{}, fmt.Errorf("%s is required", MemoryUpdateOutputPayloadKey)
	}
	var out MemoryUpdateOutput
	switch typed := raw.(type) {
	case MemoryUpdateOutput:
		out = typed
	case *MemoryUpdateOutput:
		if typed == nil {
			return MemoryUpdateOutput{}, fmt.Errorf("%s is required", MemoryUpdateOutputPayloadKey)
		}
		out = *typed
	default:
		content, err := json.Marshal(raw)
		if err != nil {
			return MemoryUpdateOutput{}, fmt.Errorf("marshal %s: %w", MemoryUpdateOutputPayloadKey, err)
		}
		if err := json.Unmarshal(content, &out); err != nil {
			return MemoryUpdateOutput{}, fmt.Errorf("parse %s: %w", MemoryUpdateOutputPayloadKey, err)
		}
	}
	out.Normalize()
	if err := out.Validate(); err != nil {
		return MemoryUpdateOutput{}, fmt.Errorf("%s is invalid: %w", MemoryUpdateOutputPayloadKey, err)
	}
	return out, nil
}

func validateMemoryDecisions(errs *[]error, field string, decisions []MemoryCandidateDecision, state MemoryCandidateState) {
	for i, decision := range decisions {
		if decision.State == "" {
			decision.State = state
		}
		if decision.State != state {
			*errs = append(*errs, fmt.Errorf("%s[%d].state must be %q", field, i, state))
		}
		if strings.TrimSpace(decision.CandidateID) == "" && len(cleanMemoryStrings(decision.CandidateIDs)) == 0 {
			*errs = append(*errs, fmt.Errorf("%s[%d] must identify candidate_id or candidate_ids", field, i))
		}
		if strings.TrimSpace(decision.Rationale) == "" {
			*errs = append(*errs, fmt.Errorf("%s[%d].rationale is required", field, i))
		}
		if state == MemoryCandidateApplied || state == MemoryCandidateEdited || state == MemoryCandidateMerged {
			if strings.TrimSpace(decision.Kind) == "" {
				*errs = append(*errs, fmt.Errorf("%s[%d].kind is required", field, i))
			}
			if strings.TrimSpace(decision.Title) == "" {
				*errs = append(*errs, fmt.Errorf("%s[%d].title is required", field, i))
			}
			if strings.TrimSpace(decision.Body) == "" {
				*errs = append(*errs, fmt.Errorf("%s[%d].body is required", field, i))
			}
		}
	}
}

func memoryChangeSourceArtifactRefs(change MemoryChange) []string {
	refs := cleanMemoryStrings(change.SourceArtifactRefs)
	if len(refs) == 0 {
		refs = cleanMemoryStrings(change.Freshness.SourceArtifactRefs)
	}
	return refs
}

func cleanMemoryStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
