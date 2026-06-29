package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/ids"
)

type projectMemoryEntryUniqueKey struct {
	ProjectID string
	Kind      string
	Title     string
}

func (s *Store) ApplyProjectMemoryUpdate(ctx context.Context, update ProjectMemoryUpdate) (ProjectMemoryUpdateResult, error) {
	update.ProjectID = strings.TrimSpace(update.ProjectID)
	update.RunID = strings.TrimSpace(update.RunID)
	update.TaskID = strings.TrimSpace(update.TaskID)
	update.CuratorStageID = strings.TrimSpace(update.CuratorStageID)

	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProjectMemoryUpdateResult{}, fmt.Errorf("begin project memory update: %w", err)
	}
	defer rollback(tx)

	if err := validateProjectMemoryCuratorStageTx(ctx, tx, update); err != nil {
		return ProjectMemoryUpdateResult{}, err
	}

	result := ProjectMemoryUpdateResult{}
	entryRevertByKey := map[projectMemoryEntryUniqueKey]int{}
	appliedByCandidate := map[string]string{}
	rejectedByCandidate := map[string]ProjectMemoryRejection{}
	appendRejection := func(rejection ProjectMemoryRejection) {
		result.Rejections = append(result.Rejections, rejection)
		if rejection.CandidateID != "" {
			rejectedByCandidate[rejection.CandidateID] = rejection
		}
	}
	now := nowRFC3339()
	for i, raw := range update.Entries {
		raw.CandidateID = strings.TrimSpace(raw.CandidateID)
		if i >= ProjectMemoryMaxEntriesPerUpdate {
			rejection := projectMemoryRejectionForInput(raw, fmt.Sprintf("memory update is bounded to %d entries", ProjectMemoryMaxEntriesPerUpdate))
			appendRejection(rejection)
			result.Outcomes = append(result.Outcomes, ProjectMemoryWriteOutcome{Rejection: &rejection})
			continue
		}
		entry, err := normalizeProjectMemoryInput(raw)
		if err != nil {
			rejection := projectMemoryRejectionForInput(raw, err.Error())
			appendRejection(rejection)
			result.Outcomes = append(result.Outcomes, ProjectMemoryWriteOutcome{Rejection: &rejection})
			continue
		}
		if err := validateProjectMemorySourceTx(ctx, tx, update, entry); err != nil {
			rejection := projectMemoryRejectionForInput(entry, err.Error())
			appendRejection(rejection)
			result.Outcomes = append(result.Outcomes, ProjectMemoryWriteOutcome{Rejection: &rejection})
			continue
		}
		key := projectMemoryEntryUniqueKey{ProjectID: update.ProjectID, Kind: entry.Kind, Title: entry.Title}
		revertIndex, ok := entryRevertByKey[key]
		if !ok {
			previous, found, err := lookupProjectMemoryEntryTx(ctx, tx, update.ProjectID, entry.Kind, entry.Title)
			if err != nil {
				return ProjectMemoryUpdateResult{}, err
			}
			var previousPtr *ProjectMemoryEntry
			if found {
				previousCopy := previous
				previousPtr = &previousCopy
			}
			result.Revert.Entries = append(result.Revert.Entries, ProjectMemoryEntryRevert{ProjectID: update.ProjectID, Kind: entry.Kind, Title: entry.Title, Previous: previousPtr})
			revertIndex = len(result.Revert.Entries) - 1
			entryRevertByKey[key] = revertIndex
		}
		id := ids.New("memory")
		_, err = tx.ExecContext(ctx, `INSERT INTO project_memory_entries(id, project_id, kind, title, body, source_run_id, source_task_id, source_stage_id, source_artifact_id, curator_stage_id, source_summary, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(project_id, kind, title) DO UPDATE SET body = excluded.body, source_run_id = excluded.source_run_id, source_task_id = excluded.source_task_id, source_stage_id = excluded.source_stage_id, source_artifact_id = excluded.source_artifact_id, curator_stage_id = excluded.curator_stage_id, source_summary = excluded.source_summary, updated_at = excluded.updated_at`, id, update.ProjectID, entry.Kind, entry.Title, entry.Body, update.RunID, update.TaskID, entry.SourceStageID, entry.SourceArtifactID, update.CuratorStageID, entry.SourceSummary, now, now)
		if err != nil {
			return ProjectMemoryUpdateResult{}, fmt.Errorf("upsert project memory entry: %w", err)
		}
		persisted, err := getProjectMemoryEntryTx(ctx, tx, update.ProjectID, entry.Kind, entry.Title)
		if err != nil {
			return ProjectMemoryUpdateResult{}, err
		}
		result.Revert.Entries[revertIndex].AppliedID = persisted.ID
		result.Entries = append(result.Entries, persisted)
		result.Outcomes = append(result.Outcomes, ProjectMemoryWriteOutcome{Entry: &persisted})
		if entry.CandidateID != "" {
			appliedByCandidate[entry.CandidateID] = persisted.ID
		}
	}
	if len(update.Decisions) > 0 {
		decisions, err := recordProjectMemoryDecisionsTx(ctx, tx, update, update.Decisions, appliedByCandidate, rejectedByCandidate, now, &result.Revert)
		if err != nil {
			return ProjectMemoryUpdateResult{}, err
		}
		result.Decisions = decisions
	}
	if err := tx.Commit(); err != nil {
		return ProjectMemoryUpdateResult{}, fmt.Errorf("commit project memory update: %w", err)
	}
	return result, nil
}

func (s *Store) RollbackProjectMemoryUpdate(ctx context.Context, revert ProjectMemoryUpdateRevert) error {
	if len(revert.Entries) == 0 && len(revert.Decisions) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin project memory update rollback: %w", err)
	}
	defer rollback(tx)

	for i := len(revert.Decisions) - 1; i >= 0; i-- {
		decision := revert.Decisions[i]
		if decision.Previous == nil {
			if err := deleteProjectMemoryDecisionRevertTx(ctx, tx, decision); err != nil {
				return err
			}
			continue
		}
		if err := restoreProjectMemoryDecisionTx(ctx, tx, *decision.Previous); err != nil {
			return err
		}
	}
	for i := len(revert.Entries) - 1; i >= 0; i-- {
		entry := revert.Entries[i]
		if entry.Previous == nil {
			if err := deleteProjectMemoryEntryRevertTx(ctx, tx, entry); err != nil {
				return err
			}
			continue
		}
		if err := restoreProjectMemoryEntryTx(ctx, tx, *entry.Previous); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit project memory update rollback: %w", err)
	}
	return nil
}

func (s *Store) ListProjectMemoryEntries(ctx context.Context, projectID string) ([]ProjectMemoryEntry, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		projectID = DefaultProjectID
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, kind, title, body, source_run_id, source_task_id, source_stage_id, source_artifact_id, curator_stage_id, source_summary, created_at, updated_at FROM project_memory_entries WHERE project_id = ? ORDER BY updated_at DESC, title ASC`, projectID)
	if err != nil {
		return nil, fmt.Errorf("list project memory entries: %w", err)
	}
	defer rows.Close()
	var entries []ProjectMemoryEntry
	for rows.Next() {
		entry, err := scanProjectMemoryEntry(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func (s *Store) ListProjectMemoryDecisions(ctx context.Context, runID string) ([]ProjectMemoryDecisionRecord, error) {
	runID = strings.TrimSpace(runID)
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, run_id, task_id, curator_stage_id, candidate_id, action, outcome, kind, title, body, reason, source_stage_id, source_artifact_id, source_summary, COALESCE(entry_id, ''), created_at FROM project_memory_candidate_decisions WHERE run_id = ? ORDER BY created_at ASC, candidate_id ASC`, runID)
	if err != nil {
		return nil, fmt.Errorf("list project memory decisions: %w", err)
	}
	defer rows.Close()
	var decisions []ProjectMemoryDecisionRecord
	for rows.Next() {
		decision, err := scanProjectMemoryDecision(rows)
		if err != nil {
			return nil, err
		}
		decisions = append(decisions, decision)
	}
	return decisions, rows.Err()
}

func (s *Store) ExportProjectMemoryEntries(ctx context.Context, req ProjectMemoryExportRequest) (ProjectMemoryExportResult, error) {
	projectID := normalizeProjectID(req.ProjectID)
	repositoryPath := filepath.Clean(strings.TrimSpace(req.RepositoryPath))
	if strings.TrimSpace(req.RepositoryPath) == "" {
		return ProjectMemoryExportResult{}, fmt.Errorf("repository path is required for project memory export")
	}
	info, err := os.Stat(repositoryPath)
	if err != nil {
		return ProjectMemoryExportResult{}, fmt.Errorf("stat project memory export repository: %w", err)
	}
	if !info.IsDir() {
		return ProjectMemoryExportResult{}, fmt.Errorf("project memory export repository is not a directory: %s", repositoryPath)
	}
	entryIDs := normalizeProjectMemoryExportEntryIDs(req.EntryIDs)
	if len(entryIDs) == 0 {
		return ProjectMemoryExportResult{}, fmt.Errorf("select at least one memory entry to export")
	}

	entries, err := s.ListProjectMemoryEntries(ctx, projectID)
	if err != nil {
		return ProjectMemoryExportResult{}, err
	}
	entriesByID := make(map[string]ProjectMemoryEntry, len(entries))
	for _, entry := range entries {
		entriesByID[entry.ID] = entry
	}
	selected := make([]ProjectMemoryEntry, 0, len(entryIDs))
	for _, entryID := range entryIDs {
		entry, ok := entriesByID[entryID]
		if !ok {
			return ProjectMemoryExportResult{}, fmt.Errorf("memory entry %s was not found for project %s", entryID, projectID)
		}
		selected = append(selected, entry)
	}

	exportedAt := nowRFC3339()
	result := ProjectMemoryExportResult{Files: make([]ProjectMemoryExportFile, 0, len(selected))}
	for _, entry := range selected {
		sanitized, notes, changed := sanitizeProjectMemoryExportEntry(entry)
		relativePath := projectMemoryExportRelativePath(sanitized)
		absolutePath := filepath.Join(repositoryPath, filepath.FromSlash(relativePath))
		if err := os.MkdirAll(filepath.Dir(absolutePath), 0o755); err != nil {
			return ProjectMemoryExportResult{}, fmt.Errorf("create project memory export dir: %w", err)
		}
		content := renderProjectMemoryExport(sanitized, exportedAt, notes)
		if err := os.WriteFile(absolutePath, []byte(content), 0o644); err != nil {
			return ProjectMemoryExportResult{}, fmt.Errorf("write project memory export: %w", err)
		}
		result.Files = append(result.Files, ProjectMemoryExportFile{EntryID: entry.ID, RelativePath: relativePath, Path: absolutePath, Sanitized: changed})
	}
	return result, nil
}

func normalizeProjectMemoryExportEntryIDs(raw []string) []string {
	seen := map[string]bool{}
	ids := make([]string, 0, len(raw))
	for _, value := range raw {
		id := strings.TrimSpace(value)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids
}

func sanitizeProjectMemoryExportEntry(entry ProjectMemoryEntry) (ProjectMemoryEntry, []string, bool) {
	var notes []string
	changed := false
	kind := normalizeProjectMemoryKind(entry.Kind)
	if kind == "" || !validProjectMemoryKind(kind) || projectMemoryExportForbiddenText(kind) {
		kind = ProjectMemoryKindLesson
		notes = append(notes, "kind contained unsupported or forbidden content and was replaced")
		changed = true
	}
	entry.Kind = kind

	if title, fieldChanged := sanitizeProjectMemoryExportText(entry.Title, "Untitled sanitized memory entry"); fieldChanged {
		entry.Title = projectMemoryExportOneLine(title)
		notes = append(notes, "title contained forbidden content and was sanitized")
		changed = true
	} else {
		entry.Title = projectMemoryExportOneLine(title)
	}
	if body, fieldChanged := sanitizeProjectMemoryExportText(entry.Body, "All body content was removed by project-memory export sanitization."); fieldChanged {
		entry.Body = body
		notes = append(notes, "body lines containing secrets or forbidden content were removed")
		changed = true
	} else {
		entry.Body = body
	}
	if summary, fieldChanged := sanitizeProjectMemoryExportText(entry.SourceSummary, "Sanitized source summary"); fieldChanged {
		entry.SourceSummary = projectMemoryExportOneLine(summary)
		notes = append(notes, "source summary contained forbidden content and was sanitized")
		changed = true
	} else {
		entry.SourceSummary = projectMemoryExportOneLine(summary)
	}
	return entry, notes, changed
}

func sanitizeProjectMemoryExportText(value, fallback string) (string, bool) {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	if strings.TrimSpace(value) == "" {
		return "", false
	}
	lines := strings.Split(value, "\n")
	kept := make([]string, 0, len(lines))
	removed := false
	inSecretBlock := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if inSecretBlock {
			removed = true
			if projectMemoryExportSecretBlockEnd(trimmed) {
				inSecretBlock = false
			}
			continue
		}
		if projectMemoryExportSecretBlockStart(trimmed) {
			removed = true
			if !projectMemoryExportSecretBlockEnd(trimmed) {
				inSecretBlock = true
			}
			continue
		}
		if trimmed != "" && projectMemoryExportForbiddenText(line) {
			removed = true
			continue
		}
		kept = append(kept, line)
	}
	result := strings.TrimSpace(strings.Join(kept, "\n"))
	if result == "" {
		return fallback, true
	}
	return result, removed || result != strings.TrimSpace(value)
}

func projectMemoryExportSecretBlockStart(line string) bool {
	lower := strings.ToLower(line)
	return strings.Contains(lower, "-----begin ") && strings.Contains(lower, "private key-----")
}

func projectMemoryExportSecretBlockEnd(line string) bool {
	lower := strings.ToLower(line)
	return strings.Contains(lower, "-----end ") && strings.Contains(lower, "private key-----")
}

func projectMemoryExportForbiddenText(text string) bool {
	return looksLikeSecret(text) || looksLikeStandingInstruction("", text) || looksLikeCurrentCodeTruth("", text)
}

func projectMemoryExportRelativePath(entry ProjectMemoryEntry) string {
	kind := projectMemoryExportSlug(entry.Kind)
	if kind == "" {
		kind = "memory"
	}
	base := projectMemoryExportSlug(entry.Title)
	if base == "" {
		base = "memory"
	}
	if id := projectMemoryExportSlug(entry.ID); id != "" {
		base += "-" + id
	}
	return filepath.ToSlash(filepath.Join(ProjectMemoryExportDir, kind, base+".md"))
}

func projectMemoryExportSlug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash && b.Len() > 0 {
			b.WriteByte('-')
			lastDash = true
		}
		if b.Len() >= 80 {
			break
		}
	}
	return strings.Trim(b.String(), "-")
}

func renderProjectMemoryExport(entry ProjectMemoryEntry, exportedAt string, notes []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", entry.Title)
	b.WriteString("> [!info] Exported project memory\n")
	projectMemoryExportMetadata(&b, "Memory ID", entry.ID)
	projectMemoryExportMetadata(&b, "Kind", entry.Kind)
	projectMemoryExportMetadata(&b, "Source run", entry.SourceRunID)
	projectMemoryExportMetadata(&b, "Source task", entry.SourceTaskID)
	projectMemoryExportMetadata(&b, "Source stage", entry.SourceStageID)
	projectMemoryExportMetadata(&b, "Source artifact", entry.SourceArtifactID)
	projectMemoryExportMetadata(&b, "Curator stage", entry.CuratorStageID)
	projectMemoryExportMetadata(&b, "Source summary", entry.SourceSummary)
	projectMemoryExportMetadata(&b, "Created at", entry.CreatedAt)
	projectMemoryExportMetadata(&b, "Updated at", entry.UpdatedAt)
	if entry.UpdatedAt != "" {
		projectMemoryExportMetadata(&b, "Freshness", "memory updated at "+entry.UpdatedAt)
	}
	projectMemoryExportMetadata(&b, "Exported at", exportedAt)
	if len(notes) == 0 {
		projectMemoryExportMetadata(&b, "Sanitization", "no forbidden content detected by export sanitizer")
	} else {
		projectMemoryExportMetadata(&b, "Sanitization", strings.Join(notes, "; "))
	}
	b.WriteString("\n")
	b.WriteString(entry.Body)
	if !strings.HasSuffix(entry.Body, "\n") {
		b.WriteString("\n")
	}
	return b.String()
}

func projectMemoryExportMetadata(b *strings.Builder, label, value string) {
	value = projectMemoryExportOneLine(value)
	if value == "" {
		return
	}
	fmt.Fprintf(b, "> - %s: `%s`\n", label, strings.ReplaceAll(value, "`", "'"))
}

func projectMemoryExportOneLine(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func validateProjectMemoryCuratorStageTx(ctx context.Context, tx *sql.Tx, update ProjectMemoryUpdate) error {
	if update.ProjectID == "" || update.RunID == "" || update.TaskID == "" || update.CuratorStageID == "" {
		return fmt.Errorf("%w: project_id, run_id, task_id, and curator_stage_id are required", ErrProjectMemoryCuratorStage)
	}
	var projectID, runID, taskID, stageType string
	if err := tx.QueryRowContext(ctx, `SELECT project_id, run_id, task_id, stage_type FROM stages WHERE id = ?`, update.CuratorStageID).Scan(&projectID, &runID, &taskID, &stageType); err != nil {
		return fmt.Errorf("%w: get curator stage: %v", ErrProjectMemoryCuratorStage, err)
	}
	if projectID != update.ProjectID || runID != update.RunID || taskID != update.TaskID || stageType != workflow.StageTypeMemoryUpdate {
		return fmt.Errorf("%w: stage %s is %s for project=%s run=%s task=%s", ErrProjectMemoryCuratorStage, update.CuratorStageID, stageType, projectID, runID, taskID)
	}
	return nil
}

func recordProjectMemoryDecisionsTx(ctx context.Context, tx *sql.Tx, update ProjectMemoryUpdate, decisions []ProjectMemoryDecisionInput, appliedByCandidate map[string]string, rejectedByCandidate map[string]ProjectMemoryRejection, now string, revert *ProjectMemoryUpdateRevert) ([]ProjectMemoryDecisionRecord, error) {
	records := make([]ProjectMemoryDecisionRecord, 0, len(decisions))
	seen := map[string]bool{}
	for _, raw := range decisions {
		decision, err := normalizeProjectMemoryDecisionInput(raw)
		if err != nil {
			return nil, err
		}
		if seen[decision.CandidateID] {
			return nil, fmt.Errorf("memory decision candidate %q is duplicated", decision.CandidateID)
		}
		seen[decision.CandidateID] = true
		if err := validateProjectMemorySourceRefsTx(ctx, tx, update, decision.SourceStageID, decision.SourceArtifactID); err != nil {
			return nil, fmt.Errorf("memory decision %s source invalid: %w", decision.CandidateID, err)
		}
		revertIndex := -1
		if revert != nil {
			previous, found, err := lookupProjectMemoryDecisionTx(ctx, tx, update.CuratorStageID, decision.CandidateID)
			if err != nil {
				return nil, err
			}
			var previousPtr *ProjectMemoryDecisionRecord
			if found {
				previousCopy := previous
				previousPtr = &previousCopy
			}
			revert.Decisions = append(revert.Decisions, ProjectMemoryDecisionRevert{CuratorStageID: update.CuratorStageID, CandidateID: decision.CandidateID, Previous: previousPtr})
			revertIndex = len(revert.Decisions) - 1
		}
		outcome := ProjectMemoryDecisionOutcomeRejected
		entryID := ""
		reason := decision.Reason
		switch decision.Action {
		case ProjectMemoryDecisionReject:
			outcome = ProjectMemoryDecisionOutcomeRejected
			if reason == "" {
				reason = "rejected by human"
			}
		case ProjectMemoryDecisionDefer:
			outcome = ProjectMemoryDecisionOutcomeDeferred
			if reason == "" {
				reason = "deferred by human"
			}
		case ProjectMemoryDecisionApprove, ProjectMemoryDecisionEdit:
			if applied := appliedByCandidate[decision.CandidateID]; applied != "" {
				outcome = ProjectMemoryDecisionOutcomeApplied
				entryID = applied
			} else if rejection, ok := rejectedByCandidate[decision.CandidateID]; ok {
				outcome = ProjectMemoryDecisionOutcomeRejected
				reason = rejection.Reason
			} else {
				outcome = ProjectMemoryDecisionOutcomeRejected
				reason = "approved memory candidate was not applied"
			}
		}
		body := ""
		if outcome == ProjectMemoryDecisionOutcomeApplied {
			body = decision.Body
		}
		id := ids.New("memory_decision")
		_, err = tx.ExecContext(ctx, `INSERT INTO project_memory_candidate_decisions(id, project_id, run_id, task_id, curator_stage_id, candidate_id, action, outcome, kind, title, body, reason, source_stage_id, source_artifact_id, source_summary, entry_id, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?)
ON CONFLICT(curator_stage_id, candidate_id) DO UPDATE SET action = excluded.action, outcome = excluded.outcome, kind = excluded.kind, title = excluded.title, body = excluded.body, reason = excluded.reason, source_stage_id = excluded.source_stage_id, source_artifact_id = excluded.source_artifact_id, source_summary = excluded.source_summary, entry_id = excluded.entry_id`, id, update.ProjectID, update.RunID, update.TaskID, update.CuratorStageID, decision.CandidateID, decision.Action, outcome, decision.Kind, decision.Title, body, reason, decision.SourceStageID, decision.SourceArtifactID, decision.SourceSummary, entryID, now)
		if err != nil {
			return nil, fmt.Errorf("record project memory decision: %w", err)
		}
		record, err := getProjectMemoryDecisionTx(ctx, tx, update.CuratorStageID, decision.CandidateID)
		if err != nil {
			return nil, err
		}
		if revert != nil && revertIndex >= 0 {
			revert.Decisions[revertIndex].AppliedID = record.ID
		}
		records = append(records, record)
	}
	return records, nil
}

func normalizeProjectMemoryDecisionInput(input ProjectMemoryDecisionInput) (ProjectMemoryDecisionInput, error) {
	input.CandidateID = strings.TrimSpace(input.CandidateID)
	input.Action = strings.ToLower(strings.TrimSpace(input.Action))
	input.Kind = strings.TrimSpace(input.Kind)
	input.Title = strings.TrimSpace(input.Title)
	input.Body = strings.TrimSpace(input.Body)
	input.Reason = strings.TrimSpace(input.Reason)
	input.SourceStageID = strings.TrimSpace(input.SourceStageID)
	input.SourceArtifactID = strings.TrimSpace(input.SourceArtifactID)
	input.SourceSummary = strings.TrimSpace(input.SourceSummary)
	if input.SourceSummary == "" {
		input.SourceSummary = input.Title
	}
	if input.CandidateID == "" {
		return ProjectMemoryDecisionInput{}, fmt.Errorf("memory decision candidate_id is required")
	}
	switch input.Action {
	case ProjectMemoryDecisionApprove, ProjectMemoryDecisionEdit, ProjectMemoryDecisionReject, ProjectMemoryDecisionDefer:
	default:
		return ProjectMemoryDecisionInput{}, fmt.Errorf("memory decision %s has invalid action %q", input.CandidateID, input.Action)
	}
	if input.Kind == "" {
		input.Kind = ProjectMemoryKindLesson
	}
	if input.Title == "" {
		input.Title = "untitled memory candidate"
	}
	if input.SourceStageID == "" || input.SourceArtifactID == "" {
		return ProjectMemoryDecisionInput{}, fmt.Errorf("memory decision %s must link to a source stage and source artifact", input.CandidateID)
	}
	return input, nil
}

func validateProjectMemorySourceTx(ctx context.Context, tx *sql.Tx, update ProjectMemoryUpdate, entry ProjectMemoryInput) error {
	return validateProjectMemorySourceRefsTx(ctx, tx, update, entry.SourceStageID, entry.SourceArtifactID)
}

func validateProjectMemorySourceRefsTx(ctx context.Context, tx *sql.Tx, update ProjectMemoryUpdate, sourceStageID, sourceArtifactID string) error {
	var sourceProjectID, sourceRunID, sourceTaskID string
	if err := tx.QueryRowContext(ctx, `SELECT project_id, run_id, task_id FROM stages WHERE id = ?`, sourceStageID).Scan(&sourceProjectID, &sourceRunID, &sourceTaskID); err != nil {
		return fmt.Errorf("source stage must exist: %v", err)
	}
	if sourceProjectID != update.ProjectID || sourceRunID != update.RunID || sourceTaskID != update.TaskID {
		return fmt.Errorf("source stage must belong to the same project/run/task")
	}
	if err := tx.QueryRowContext(ctx, `SELECT project_id, run_id, task_id FROM artifacts WHERE id = ?`, sourceArtifactID).Scan(&sourceProjectID, &sourceRunID, &sourceTaskID); err != nil {
		return fmt.Errorf("source artifact must exist: %v", err)
	}
	if sourceProjectID != update.ProjectID || sourceRunID != update.RunID || sourceTaskID != update.TaskID {
		return fmt.Errorf("source artifact must belong to the same project/run/task")
	}
	return nil
}

func normalizeProjectMemoryInput(input ProjectMemoryInput) (ProjectMemoryInput, error) {
	input.CandidateID = strings.TrimSpace(input.CandidateID)
	input.Kind = normalizeProjectMemoryKind(input.Kind)
	input.Title = strings.TrimSpace(input.Title)
	input.Body = strings.TrimSpace(input.Body)
	input.SourceStageID = strings.TrimSpace(input.SourceStageID)
	input.SourceArtifactID = strings.TrimSpace(input.SourceArtifactID)
	input.SourceSummary = strings.TrimSpace(input.SourceSummary)
	if input.SourceSummary == "" {
		input.SourceSummary = input.Title
	}
	if !validProjectMemoryKind(input.Kind) {
		return ProjectMemoryInput{}, fmt.Errorf("memory kind %q is not allowed", input.Kind)
	}
	if input.Title == "" {
		return ProjectMemoryInput{}, fmt.Errorf("memory title is required")
	}
	if input.Body == "" {
		return ProjectMemoryInput{}, fmt.Errorf("memory body is required")
	}
	if input.SourceStageID == "" || input.SourceArtifactID == "" {
		return ProjectMemoryInput{}, fmt.Errorf("memory entries must link to a source stage and source artifact")
	}
	if tooLong(input.Kind, ProjectMemoryMaxKindRunes) {
		return ProjectMemoryInput{}, fmt.Errorf("memory kind exceeds %d characters", ProjectMemoryMaxKindRunes)
	}
	if tooLong(input.Title, ProjectMemoryMaxTitleRunes) {
		return ProjectMemoryInput{}, fmt.Errorf("memory title exceeds %d characters", ProjectMemoryMaxTitleRunes)
	}
	if tooLong(input.Body, ProjectMemoryMaxBodyRunes) {
		return ProjectMemoryInput{}, fmt.Errorf("memory body exceeds %d characters", ProjectMemoryMaxBodyRunes)
	}
	if tooLong(input.SourceSummary, ProjectMemoryMaxSourceRunes) {
		return ProjectMemoryInput{}, fmt.Errorf("memory source summary exceeds %d characters", ProjectMemoryMaxSourceRunes)
	}
	combined := strings.Join([]string{input.Kind, input.Title, input.Body, input.SourceSummary}, "\n")
	if looksLikeSecret(combined) {
		return ProjectMemoryInput{}, fmt.Errorf("memory entries must not contain secrets or credentials")
	}
	if looksLikeStandingInstruction(input.Kind, combined) {
		return ProjectMemoryInput{}, fmt.Errorf("memory entries must not contain standing instructions")
	}
	if looksLikeCurrentCodeTruth(input.Kind, combined) {
		return ProjectMemoryInput{}, fmt.Errorf("memory entries must not store current-code-truth")
	}
	return input, nil
}

func normalizeProjectMemoryKind(kind string) string {
	kind = strings.ToLower(strings.TrimSpace(kind))
	kind = strings.NewReplacer("-", "_", " ", "_").Replace(kind)
	if kind == "" {
		return ProjectMemoryKindLesson
	}
	return kind
}

func validProjectMemoryKind(kind string) bool {
	switch normalizeProjectMemoryKind(kind) {
	case ProjectMemoryKindLesson,
		ProjectMemoryKindRepoFact,
		ProjectMemoryKindGotcha,
		ProjectMemoryKindImplementationLandmark,
		ProjectMemoryKindPriorResult,
		ProjectMemoryKindDecision,
		ProjectMemoryKindFreshnessNote:
		return true
	default:
		return false
	}
}

func looksLikeSecret(text string) bool {
	lower := strings.ToLower(text)
	needles := []string{"api_key", "apikey", "access token", "auth token", "bearer token", "credential", "password", "private key", "secret=", "secret:", "ghp_", "github_pat_", "ssh-rsa"}
	for _, needle := range needles {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return strings.Contains(text, "BEGIN RSA PRIVATE KEY") || strings.Contains(text, "BEGIN OPENSSH PRIVATE KEY")
}

func looksLikeStandingInstruction(kind, text string) bool {
	if kind == "standing_instruction" || kind == "instruction" || kind == "instructions" {
		return true
	}
	lower := strings.ToLower(text)
	needles := []string{"standing instruction", "from now on", "for all future", "always ", "never ", "agents must", "agent must", "must always", "should always"}
	for _, needle := range needles {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func looksLikeCurrentCodeTruth(kind, text string) bool {
	if kind == "current_code_truth" || kind == "code_truth" {
		return true
	}
	lower := strings.ToLower(text)
	return strings.Contains(lower, "current-code-truth") || strings.Contains(lower, "current code truth")
}

func tooLong(value string, max int) bool { return len([]rune(value)) > max }

func projectMemoryRejectionForInput(input ProjectMemoryInput, reason string) ProjectMemoryRejection {
	return ProjectMemoryRejection{
		CandidateID:      strings.TrimSpace(input.CandidateID),
		Title:            rejectionTitle(input),
		Reason:           strings.TrimSpace(reason),
		SourceStageID:    strings.TrimSpace(input.SourceStageID),
		SourceArtifactID: strings.TrimSpace(input.SourceArtifactID),
	}
}

func rejectionTitle(input ProjectMemoryInput) string {
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return "untitled memory candidate"
	}
	return title
}

func deleteProjectMemoryEntryRevertTx(ctx context.Context, tx *sql.Tx, revert ProjectMemoryEntryRevert) error {
	if revert.AppliedID != "" {
		_, err := tx.ExecContext(ctx, `DELETE FROM project_memory_entries WHERE id = ?`, revert.AppliedID)
		if err != nil {
			return fmt.Errorf("delete rolled-back project memory entry: %w", err)
		}
		return nil
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM project_memory_entries WHERE project_id = ? AND kind = ? AND title = ?`, revert.ProjectID, revert.Kind, revert.Title)
	if err != nil {
		return fmt.Errorf("delete rolled-back project memory entry: %w", err)
	}
	return nil
}

func restoreProjectMemoryEntryTx(ctx context.Context, tx *sql.Tx, entry ProjectMemoryEntry) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO project_memory_entries(id, project_id, kind, title, body, source_run_id, source_task_id, source_stage_id, source_artifact_id, curator_stage_id, source_summary, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(project_id, kind, title) DO UPDATE SET id = excluded.id, body = excluded.body, source_run_id = excluded.source_run_id, source_task_id = excluded.source_task_id, source_stage_id = excluded.source_stage_id, source_artifact_id = excluded.source_artifact_id, curator_stage_id = excluded.curator_stage_id, source_summary = excluded.source_summary, created_at = excluded.created_at, updated_at = excluded.updated_at`, entry.ID, entry.ProjectID, entry.Kind, entry.Title, entry.Body, entry.SourceRunID, entry.SourceTaskID, entry.SourceStageID, entry.SourceArtifactID, entry.CuratorStageID, entry.SourceSummary, entry.CreatedAt, entry.UpdatedAt)
	if err != nil {
		return fmt.Errorf("restore project memory entry: %w", err)
	}
	return nil
}

func lookupProjectMemoryEntryTx(ctx context.Context, tx *sql.Tx, projectID, kind, title string) (ProjectMemoryEntry, bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT id, project_id, kind, title, body, source_run_id, source_task_id, source_stage_id, source_artifact_id, curator_stage_id, source_summary, created_at, updated_at FROM project_memory_entries WHERE project_id = ? AND kind = ? AND title = ?`, projectID, kind, title)
	entry, err := scanProjectMemoryEntry(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectMemoryEntry{}, false, nil
		}
		return ProjectMemoryEntry{}, false, fmt.Errorf("lookup project memory entry: %w", err)
	}
	return entry, true, nil
}

func getProjectMemoryEntryTx(ctx context.Context, tx *sql.Tx, projectID, kind, title string) (ProjectMemoryEntry, error) {
	entry, found, err := lookupProjectMemoryEntryTx(ctx, tx, projectID, kind, title)
	if err != nil {
		return ProjectMemoryEntry{}, err
	}
	if !found {
		return ProjectMemoryEntry{}, fmt.Errorf("get project memory entry: %w", sql.ErrNoRows)
	}
	return entry, nil
}

type memoryEntryScanner interface {
	Scan(dest ...any) error
}

func scanProjectMemoryEntry(scanner memoryEntryScanner) (ProjectMemoryEntry, error) {
	var entry ProjectMemoryEntry
	if err := scanner.Scan(&entry.ID, &entry.ProjectID, &entry.Kind, &entry.Title, &entry.Body, &entry.SourceRunID, &entry.SourceTaskID, &entry.SourceStageID, &entry.SourceArtifactID, &entry.CuratorStageID, &entry.SourceSummary, &entry.CreatedAt, &entry.UpdatedAt); err != nil {
		return ProjectMemoryEntry{}, fmt.Errorf("scan project memory entry: %w", err)
	}
	return entry, nil
}

func deleteProjectMemoryDecisionRevertTx(ctx context.Context, tx *sql.Tx, revert ProjectMemoryDecisionRevert) error {
	if revert.AppliedID != "" {
		_, err := tx.ExecContext(ctx, `DELETE FROM project_memory_candidate_decisions WHERE id = ?`, revert.AppliedID)
		if err != nil {
			return fmt.Errorf("delete rolled-back project memory decision: %w", err)
		}
		return nil
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM project_memory_candidate_decisions WHERE curator_stage_id = ? AND candidate_id = ?`, revert.CuratorStageID, revert.CandidateID)
	if err != nil {
		return fmt.Errorf("delete rolled-back project memory decision: %w", err)
	}
	return nil
}

func restoreProjectMemoryDecisionTx(ctx context.Context, tx *sql.Tx, decision ProjectMemoryDecisionRecord) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO project_memory_candidate_decisions(id, project_id, run_id, task_id, curator_stage_id, candidate_id, action, outcome, kind, title, body, reason, source_stage_id, source_artifact_id, source_summary, entry_id, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?)
ON CONFLICT(curator_stage_id, candidate_id) DO UPDATE SET id = excluded.id, project_id = excluded.project_id, run_id = excluded.run_id, task_id = excluded.task_id, action = excluded.action, outcome = excluded.outcome, kind = excluded.kind, title = excluded.title, body = excluded.body, reason = excluded.reason, source_stage_id = excluded.source_stage_id, source_artifact_id = excluded.source_artifact_id, source_summary = excluded.source_summary, entry_id = excluded.entry_id, created_at = excluded.created_at`, decision.ID, decision.ProjectID, decision.RunID, decision.TaskID, decision.CuratorStageID, decision.CandidateID, decision.Action, decision.Outcome, decision.Kind, decision.Title, decision.Body, decision.Reason, decision.SourceStageID, decision.SourceArtifactID, decision.SourceSummary, decision.EntryID, decision.CreatedAt)
	if err != nil {
		return fmt.Errorf("restore project memory decision: %w", err)
	}
	return nil
}

func lookupProjectMemoryDecisionTx(ctx context.Context, tx *sql.Tx, curatorStageID, candidateID string) (ProjectMemoryDecisionRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT id, project_id, run_id, task_id, curator_stage_id, candidate_id, action, outcome, kind, title, body, reason, source_stage_id, source_artifact_id, source_summary, COALESCE(entry_id, ''), created_at FROM project_memory_candidate_decisions WHERE curator_stage_id = ? AND candidate_id = ?`, curatorStageID, candidateID)
	decision, err := scanProjectMemoryDecision(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectMemoryDecisionRecord{}, false, nil
		}
		return ProjectMemoryDecisionRecord{}, false, fmt.Errorf("lookup project memory decision: %w", err)
	}
	return decision, true, nil
}

func getProjectMemoryDecisionTx(ctx context.Context, tx *sql.Tx, curatorStageID, candidateID string) (ProjectMemoryDecisionRecord, error) {
	decision, found, err := lookupProjectMemoryDecisionTx(ctx, tx, curatorStageID, candidateID)
	if err != nil {
		return ProjectMemoryDecisionRecord{}, err
	}
	if !found {
		return ProjectMemoryDecisionRecord{}, fmt.Errorf("get project memory decision: %w", sql.ErrNoRows)
	}
	return decision, nil
}

type memoryDecisionScanner interface {
	Scan(dest ...any) error
}

func scanProjectMemoryDecision(scanner memoryDecisionScanner) (ProjectMemoryDecisionRecord, error) {
	var decision ProjectMemoryDecisionRecord
	if err := scanner.Scan(&decision.ID, &decision.ProjectID, &decision.RunID, &decision.TaskID, &decision.CuratorStageID, &decision.CandidateID, &decision.Action, &decision.Outcome, &decision.Kind, &decision.Title, &decision.Body, &decision.Reason, &decision.SourceStageID, &decision.SourceArtifactID, &decision.SourceSummary, &decision.EntryID, &decision.CreatedAt); err != nil {
		return ProjectMemoryDecisionRecord{}, fmt.Errorf("scan project memory decision: %w", err)
	}
	return decision, nil
}
