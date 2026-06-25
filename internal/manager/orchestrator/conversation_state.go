package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/report"
)

const (
	conversationStateRunLimit      = 8
	conversationStateOpenRunLimit  = 6
	conversationStatePayloadMaxLen = 600
	conversationStateListMaxItems  = 12
	conversationStateMapMaxKeys    = 24
)

type conversationOrchestrationState struct {
	ProjectID             string                        `json:"project_id"`
	ConversationID        string                        `json:"conversation_id"`
	Scope                 string                        `json:"scope"`
	TotalProjectRuns      int                           `json:"total_project_runs"`
	IncludedProjectRuns   int                           `json:"included_project_runs"`
	OmittedProjectRuns    int                           `json:"omitted_project_runs"`
	ConversationTaskCount int                           `json:"conversation_task_count"`
	OpenProjectRuns       []conversationOpenRunState    `json:"open_project_runs"`
	ConversationTasks     []conversationTaskState       `json:"conversation_tasks"`
	IncludedRunTaskLinks  []conversationTaskState       `json:"included_run_task_links"`
	Runs                  []conversationRunState        `json:"runs"`
	ReportReadErrors      []conversationReportReadError `json:"report_read_errors,omitempty"`
}

type conversationTaskState struct {
	ID              string `json:"id"`
	ProjectID       string `json:"project_id"`
	RepositoryID    string `json:"repository_id,omitempty"`
	ConversationID  string `json:"conversation_id,omitempty"`
	Idea            string `json:"idea"`
	RefinementLevel string `json:"refinement_level"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

type conversationOpenRunState struct {
	ID                 string `json:"id"`
	TaskID             string `json:"task_id"`
	Idea               string `json:"idea"`
	Status             string `json:"status"`
	RefinementLevel    string `json:"refinement_level"`
	WorkflowTemplateID string `json:"workflow_template_id"`
	UpdatedAt          string `json:"updated_at"`
}

type conversationRunState struct {
	ID                 string                   `json:"id"`
	TaskID             string                   `json:"task_id"`
	TaskConversationID string                   `json:"task_conversation_id,omitempty"`
	Idea               string                   `json:"idea"`
	RefinementLevel    string                   `json:"refinement_level"`
	WorkflowTemplateID string                   `json:"workflow_template_id"`
	Status             string                   `json:"status"`
	CreatedAt          string                   `json:"created_at"`
	UpdatedAt          string                   `json:"updated_at"`
	AttemptID          string                   `json:"attempt_id,omitempty"`
	Stages             []conversationStageState `json:"stages"`
}

type conversationStageState struct {
	ID                   string                         `json:"id"`
	WorkflowStageID      string                         `json:"workflow_stage_id,omitempty"`
	Type                 string                         `json:"type"`
	Adapter              string                         `json:"adapter,omitempty"`
	Status               string                         `json:"status"`
	Verdict              string                         `json:"verdict,omitempty"`
	StageBriefArtifactID string                         `json:"stage_brief_artifact_id,omitempty"`
	TaskPlanArtifactID   string                         `json:"task_plan_artifact_id,omitempty"`
	UpdatedAt            string                         `json:"updated_at"`
	Reports              []conversationStageReportState `json:"reports,omitempty"`
}

type conversationStageReportState struct {
	ArtifactID   string         `json:"artifact_id"`
	ActorKind    string         `json:"actor_kind,omitempty"`
	ActorID      string         `json:"actor_id,omitempty"`
	Status       string         `json:"status"`
	Verdict      string         `json:"verdict,omitempty"`
	Summary      string         `json:"summary"`
	Errors       []string       `json:"errors,omitempty"`
	EvidenceRefs []string       `json:"evidence_refs,omitempty"`
	Payload      map[string]any `json:"payload,omitempty"`
}

type conversationReportReadError struct {
	ArtifactID string `json:"artifact_id"`
	Error      string `json:"error"`
}

func (e *Engine) conversationOrchestrationEvidence(ctx context.Context, projectID, conversationID string) (conversationOrchestrationState, string, string, error) {
	state, err := e.conversationOrchestrationState(ctx, projectID, conversationID)
	if err != nil {
		return conversationOrchestrationState{}, "", "", err
	}
	summary := conversationOrchestrationSummary(state)
	markdown := conversationOrchestrationMarkdown(state, summary)
	return state, summary, markdown, nil
}

func (e *Engine) conversationOrchestrationState(ctx context.Context, projectID, conversationID string) (conversationOrchestrationState, error) {
	conversationTasks, err := e.store.ListTasksForConversation(ctx, conversationID)
	if err != nil {
		return conversationOrchestrationState{}, err
	}
	projectRuns, err := e.store.ListRunsForProject(ctx, projectID)
	if err != nil {
		return conversationOrchestrationState{}, err
	}

	state := conversationOrchestrationState{
		ProjectID:             projectID,
		ConversationID:        conversationID,
		Scope:                 "read_only orchestration snapshot: conversation-linked tasks plus recent project runs; state changes require the allow-listed create-Task action",
		TotalProjectRuns:      len(projectRuns),
		ConversationTaskCount: len(conversationTasks),
		ConversationTasks:     taskStates(conversationTasks),
		OpenProjectRuns:       openRunStates(projectRuns, conversationStateOpenRunLimit),
	}

	selected := selectConversationRuns(projectRuns, conversationTasks, conversationStateRunLimit)
	state.IncludedProjectRuns = len(selected)
	if omitted := len(projectRuns) - len(selected); omitted > 0 {
		state.OmittedProjectRuns = omitted
	}

	includedTasks := map[string]conversationTaskState{}
	for _, run := range selected {
		bundle, err := e.store.RunBundle(ctx, run.ID)
		if err != nil {
			return conversationOrchestrationState{}, err
		}
		state.Runs = append(state.Runs, e.conversationRunState(ctx, bundle, &state.ReportReadErrors))
		includedTasks[bundle.Task.ID] = taskState(bundle.Task)
	}
	state.IncludedRunTaskLinks = sortedTaskStates(includedTasks)
	return state, nil
}

func selectConversationRuns(projectRuns []store.Run, conversationTasks []store.Task, limit int) []store.Run {
	if limit <= 0 {
		return nil
	}
	conversationTaskIDs := map[string]bool{}
	for _, task := range conversationTasks {
		conversationTaskIDs[task.ID] = true
	}
	selectedIDs := map[string]bool{}
	for _, run := range projectRuns {
		if !conversationTaskIDs[run.TaskID] {
			continue
		}
		selectedIDs[run.ID] = true
		if len(selectedIDs) == limit {
			break
		}
	}
	for _, run := range projectRuns {
		if selectedIDs[run.ID] {
			continue
		}
		selectedIDs[run.ID] = true
		if len(selectedIDs) == limit {
			break
		}
	}
	selected := make([]store.Run, 0, conversationMinInt(limit, len(projectRuns)))
	for _, run := range projectRuns {
		if selectedIDs[run.ID] {
			selected = append(selected, run)
		}
	}
	return selected
}

func (e *Engine) conversationRunState(ctx context.Context, bundle store.RunBundle, reportErrors *[]conversationReportReadError) conversationRunState {
	reportsByStage := e.conversationReportsByStage(ctx, bundle.Artifacts, reportErrors)
	run := conversationRunState{
		ID:                 bundle.Run.ID,
		TaskID:             bundle.Run.TaskID,
		TaskConversationID: bundle.Task.ConversationID,
		Idea:               bundle.Run.Idea,
		RefinementLevel:    bundle.Run.RefinementLevel,
		WorkflowTemplateID: bundle.Run.WorkflowTemplateID,
		Status:             bundle.Run.Status,
		CreatedAt:          bundle.Run.CreatedAt,
		UpdatedAt:          bundle.Run.UpdatedAt,
		AttemptID:          bundle.Attempt.ID,
		Stages:             make([]conversationStageState, 0, len(bundle.Stages)),
	}
	for _, stage := range bundle.Stages {
		reports := reportsByStage[stage.ID]
		stageState := conversationStageState{
			ID:                   stage.ID,
			WorkflowStageID:      stage.WorkflowStageID,
			Type:                 stage.StageType,
			Adapter:              stage.Adapter,
			Status:               stage.Status,
			StageBriefArtifactID: stage.StageBriefArtifactID,
			TaskPlanArtifactID:   stage.TaskPlanArtifactID,
			UpdatedAt:            stage.UpdatedAt,
			Reports:              reports,
		}
		for i := len(reports) - 1; i >= 0; i-- {
			if reports[i].Verdict != "" {
				stageState.Verdict = reports[i].Verdict
				break
			}
		}
		run.Stages = append(run.Stages, stageState)
	}
	return run
}

func (e *Engine) conversationReportsByStage(ctx context.Context, artifacts []store.Artifact, reportErrors *[]conversationReportReadError) map[string][]conversationStageReportState {
	reportsByStage := map[string][]conversationStageReportState{}
	for _, artifact := range artifacts {
		if artifact.Kind != "report" {
			continue
		}
		_, content, err := e.store.GetArtifact(ctx, artifact.ID)
		if err != nil {
			*reportErrors = append(*reportErrors, conversationReportReadError{ArtifactID: artifact.ID, Error: err.Error()})
			continue
		}
		var rep report.Report
		if err := json.Unmarshal(content, &rep); err != nil {
			*reportErrors = append(*reportErrors, conversationReportReadError{ArtifactID: artifact.ID, Error: err.Error()})
			continue
		}
		reportsByStage[rep.StageID] = append(reportsByStage[rep.StageID], conversationReportState(artifact.ID, rep))
	}
	return reportsByStage
}

func conversationReportState(artifactID string, rep report.Report) conversationStageReportState {
	state := conversationStageReportState{
		ArtifactID:   artifactID,
		ActorKind:    rep.Actor.Kind,
		ActorID:      rep.Actor.ID,
		Status:       rep.Status,
		Summary:      rep.Summary,
		Errors:       rep.Errors,
		EvidenceRefs: rep.EvidenceRefs,
		Payload:      compactPayload(rep.Payload),
	}
	if rep.Verdict != nil {
		state.Verdict = string(*rep.Verdict)
	}
	return state
}

func taskStates(tasks []store.Task) []conversationTaskState {
	states := make([]conversationTaskState, 0, len(tasks))
	for _, task := range tasks {
		states = append(states, taskState(task))
	}
	return states
}

func taskState(task store.Task) conversationTaskState {
	return conversationTaskState{
		ID:              task.ID,
		ProjectID:       task.ProjectID,
		RepositoryID:    task.RepositoryID,
		ConversationID:  task.ConversationID,
		Idea:            task.Idea,
		RefinementLevel: task.RefinementLevel,
		Status:          task.Status,
		CreatedAt:       task.CreatedAt,
		UpdatedAt:       task.UpdatedAt,
	}
}

func sortedTaskStates(tasks map[string]conversationTaskState) []conversationTaskState {
	states := make([]conversationTaskState, 0, len(tasks))
	for _, task := range tasks {
		states = append(states, task)
	}
	sort.Slice(states, func(i, j int) bool {
		if states[i].CreatedAt == states[j].CreatedAt {
			return states[i].ID > states[j].ID
		}
		return states[i].CreatedAt > states[j].CreatedAt
	})
	return states
}

func openRunStates(runs []store.Run, limit int) []conversationOpenRunState {
	states := []conversationOpenRunState{}
	for _, run := range runs {
		if store.RunStatusIsTerminal(run.Status) {
			continue
		}
		states = append(states, conversationOpenRunState{
			ID:                 run.ID,
			TaskID:             run.TaskID,
			Idea:               run.Idea,
			Status:             run.Status,
			RefinementLevel:    run.RefinementLevel,
			WorkflowTemplateID: run.WorkflowTemplateID,
			UpdatedAt:          run.UpdatedAt,
		})
		if limit > 0 && len(states) == limit {
			break
		}
	}
	return states
}

func compactPayload(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	compact, _ := compactAny(payload).(map[string]any)
	return compact
}

func compactAny(value any) any {
	switch v := value.(type) {
	case nil, bool, float64, int, int64, json.Number:
		return v
	case string:
		return truncateString(v, conversationStatePayloadMaxLen)
	case []any:
		limit := conversationMinInt(len(v), conversationStateListMaxItems)
		out := make([]any, 0, limit+1)
		for i := 0; i < limit; i++ {
			out = append(out, compactAny(v[i]))
		}
		if len(v) > limit {
			out = append(out, fmt.Sprintf("... %d more items omitted", len(v)-limit))
		}
		return out
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		limit := conversationMinInt(len(keys), conversationStateMapMaxKeys)
		out := make(map[string]any, limit+1)
		for i := 0; i < limit; i++ {
			key := keys[i]
			out[key] = compactAny(v[key])
		}
		if len(keys) > limit {
			out["_omitted_keys"] = len(keys) - limit
		}
		return out
	case []string:
		limit := conversationMinInt(len(v), conversationStateListMaxItems)
		out := make([]any, 0, limit+1)
		for i := 0; i < limit; i++ {
			out = append(out, truncateString(v[i], conversationStatePayloadMaxLen))
		}
		if len(v) > limit {
			out = append(out, fmt.Sprintf("... %d more items omitted", len(v)-limit))
		}
		return out
	default:
		return truncateString(fmt.Sprint(v), conversationStatePayloadMaxLen)
	}
}

func truncateString(value string, max int) string {
	if max <= 0 || len(value) <= max {
		return value
	}
	return value[:max] + fmt.Sprintf("... [truncated %d bytes]", len(value)-max)
}

func conversationOrchestrationSummary(state conversationOrchestrationState) string {
	var b strings.Builder
	fmt.Fprintf(&b, "- Conversation-linked tasks: %d.\n", state.ConversationTaskCount)
	fmt.Fprintf(&b, "- Project runs included: %d of %d", state.IncludedProjectRuns, state.TotalProjectRuns)
	if state.OmittedProjectRuns > 0 {
		fmt.Fprintf(&b, " (%d older runs omitted)", state.OmittedProjectRuns)
	}
	b.WriteString(".\n")
	if len(state.OpenProjectRuns) == 0 {
		b.WriteString("- Open project runs: none.\n")
	} else {
		parts := make([]string, 0, len(state.OpenProjectRuns))
		for _, run := range state.OpenProjectRuns {
			parts = append(parts, fmt.Sprintf("%s (%s, task %s)", run.ID, run.Status, run.TaskID))
		}
		fmt.Fprintf(&b, "- Open project runs: %s.\n", strings.Join(parts, "; "))
	}
	if len(state.Runs) == 0 {
		b.WriteString("- No project runs are available in this snapshot.\n")
		return strings.TrimSpace(b.String())
	}
	last := state.Runs[0]
	fmt.Fprintf(&b, "- Most recent included run: %s is %s (task %s, refinement %s, template %s).\n", last.ID, last.Status, last.TaskID, last.RefinementLevel, last.WorkflowTemplateID)
	if stage, rep, ok := lastStageReport(last); ok {
		verdict := rep.Verdict
		if verdict == "" {
			verdict = stage.Verdict
		}
		if verdict != "" {
			fmt.Fprintf(&b, "- Latest report on that run: stage %s/%s status %s verdict %s — %s.\n", stage.WorkflowStageID, stage.Type, rep.Status, verdict, rep.Summary)
		} else {
			fmt.Fprintf(&b, "- Latest report on that run: stage %s/%s status %s — %s.\n", stage.WorkflowStageID, stage.Type, rep.Status, rep.Summary)
		}
	}
	if len(state.ReportReadErrors) > 0 {
		fmt.Fprintf(&b, "- Report read warnings: %d artifact(s) could not be decoded; see full snapshot.\n", len(state.ReportReadErrors))
	}
	return strings.TrimSpace(b.String())
}

func lastStageReport(run conversationRunState) (conversationStageState, conversationStageReportState, bool) {
	for i := len(run.Stages) - 1; i >= 0; i-- {
		stage := run.Stages[i]
		if len(stage.Reports) == 0 {
			continue
		}
		return stage, stage.Reports[len(stage.Reports)-1], true
	}
	return conversationStageState{}, conversationStageReportState{}, false
}

func conversationOrchestrationMarkdown(state conversationOrchestrationState, summary string) string {
	var b strings.Builder
	b.WriteString("# Parley Orchestration State Snapshot\n\n")
	b.WriteString("Read-only evidence for this conversation turn. This file is generated from persisted Tasks, Runs, Stages, report artifacts, and event-backed run bundles. It is not an action surface.\n\n")
	fmt.Fprintf(&b, "- Project ID: `%s`\n", state.ProjectID)
	fmt.Fprintf(&b, "- Conversation ID: `%s`\n", state.ConversationID)
	fmt.Fprintf(&b, "- Scope: %s\n", state.Scope)
	fmt.Fprintf(&b, "- Included runs: `%d` of `%d`\n", state.IncludedProjectRuns, state.TotalProjectRuns)
	if state.OmittedProjectRuns > 0 {
		fmt.Fprintf(&b, "- Omitted older runs: `%d`\n", state.OmittedProjectRuns)
	}
	b.WriteString("\n## Compact Summary\n\n")
	b.WriteString(summary)
	b.WriteString("\n\n")
	appendConversationTasksMarkdown(&b, "Conversation-Linked Tasks", state.ConversationTasks)
	appendConversationTasksMarkdown(&b, "Tasks for Included Runs", state.IncludedRunTaskLinks)
	appendOpenRunsMarkdown(&b, state.OpenProjectRuns)
	b.WriteString("## Included Runs\n\n")
	if len(state.Runs) == 0 {
		b.WriteString("No runs are included in this snapshot.\n")
	} else {
		for _, run := range state.Runs {
			appendRunMarkdown(&b, run)
		}
	}
	if len(state.ReportReadErrors) > 0 {
		b.WriteString("## Report Read Warnings\n\n")
		for _, readErr := range state.ReportReadErrors {
			fmt.Fprintf(&b, "- `%s`: %s\n", readErr.ArtifactID, readErr.Error)
		}
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String()) + "\n"
}

func appendConversationTasksMarkdown(b *strings.Builder, title string, tasks []conversationTaskState) {
	fmt.Fprintf(b, "## %s\n\n", title)
	if len(tasks) == 0 {
		b.WriteString("None.\n\n")
		return
	}
	for _, task := range tasks {
		fmt.Fprintf(b, "- `%s` — status `%s`, refinement `%s`, conversation `%s`\n", task.ID, task.Status, task.RefinementLevel, emptyDash(task.ConversationID))
		fmt.Fprintf(b, "  - Idea: %s\n", oneLine(task.Idea))
	}
	b.WriteString("\n")
}

func appendOpenRunsMarkdown(b *strings.Builder, runs []conversationOpenRunState) {
	b.WriteString("## Open Project Runs\n\n")
	if len(runs) == 0 {
		b.WriteString("None.\n\n")
		return
	}
	for _, run := range runs {
		fmt.Fprintf(b, "- `%s` — status `%s`, task `%s`, refinement `%s`, template `%s`\n", run.ID, run.Status, run.TaskID, run.RefinementLevel, run.WorkflowTemplateID)
		fmt.Fprintf(b, "  - Idea: %s\n", oneLine(run.Idea))
	}
	b.WriteString("\n")
}

func appendRunMarkdown(b *strings.Builder, run conversationRunState) {
	fmt.Fprintf(b, "### Run `%s`\n\n", run.ID)
	fmt.Fprintf(b, "- Status: `%s`\n", run.Status)
	fmt.Fprintf(b, "- Task: `%s` (conversation `%s`)\n", run.TaskID, emptyDash(run.TaskConversationID))
	fmt.Fprintf(b, "- Refinement/template: `%s` / `%s`\n", run.RefinementLevel, run.WorkflowTemplateID)
	fmt.Fprintf(b, "- Idea: %s\n", oneLine(run.Idea))
	b.WriteString("\n#### Stages\n\n")
	if len(run.Stages) == 0 {
		b.WriteString("No stages recorded.\n\n")
		return
	}
	for _, stage := range run.Stages {
		fmt.Fprintf(b, "- Stage `%s` (`%s` `%s`) — status `%s`", stage.ID, emptyDash(stage.WorkflowStageID), stage.Type, stage.Status)
		if stage.Verdict != "" {
			fmt.Fprintf(b, ", verdict `%s`", stage.Verdict)
		}
		b.WriteString("\n")
		for _, rep := range stage.Reports {
			fmt.Fprintf(b, "  - Report `%s`: status `%s`", rep.ArtifactID, rep.Status)
			if rep.Verdict != "" {
				fmt.Fprintf(b, ", verdict `%s`", rep.Verdict)
			}
			fmt.Fprintf(b, " — %s\n", oneLine(rep.Summary))
			if len(rep.Errors) > 0 {
				fmt.Fprintf(b, "    - Errors: %s\n", strings.Join(rep.Errors, "; "))
			}
			if len(rep.Payload) > 0 {
				b.WriteString("    - Payload excerpt:\n\n")
				b.WriteString(indentMarkdownCodeBlock(jsonForMarkdown(rep.Payload), "      "))
			}
		}
	}
	b.WriteString("\n")
}

func jsonForMarkdown(value any) string {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "null"
	}
	return string(content)
}

func indentMarkdownCodeBlock(content, indent string) string {
	var b strings.Builder
	b.WriteString(indent)
	b.WriteString("```json\n")
	for _, line := range strings.Split(content, "\n") {
		b.WriteString(indent)
		b.WriteString(line)
		b.WriteString("\n")
	}
	b.WriteString(indent)
	b.WriteString("```\n")
	return b.String()
}

func emptyDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "—"
	}
	return value
}

func oneLine(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\n", " "))
	if value == "" {
		return "—"
	}
	return truncateString(value, 220)
}

func conversationMinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
