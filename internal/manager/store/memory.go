package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/ids"
)

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
	if len(update.Entries) > ProjectMemoryMaxEntriesPerUpdate {
		for _, entry := range update.Entries[ProjectMemoryMaxEntriesPerUpdate:] {
			result.Rejections = append(result.Rejections, ProjectMemoryRejection{Title: rejectionTitle(entry), Reason: fmt.Sprintf("memory update is bounded to %d entries", ProjectMemoryMaxEntriesPerUpdate), SourceStageID: strings.TrimSpace(entry.SourceStageID), SourceArtifactID: strings.TrimSpace(entry.SourceArtifactID)})
		}
		update.Entries = update.Entries[:ProjectMemoryMaxEntriesPerUpdate]
	}

	now := nowRFC3339()
	for _, raw := range update.Entries {
		entry, err := normalizeProjectMemoryInput(raw)
		if err != nil {
			result.Rejections = append(result.Rejections, ProjectMemoryRejection{Title: rejectionTitle(raw), Reason: err.Error(), SourceStageID: strings.TrimSpace(raw.SourceStageID), SourceArtifactID: strings.TrimSpace(raw.SourceArtifactID)})
			continue
		}
		if err := validateProjectMemorySourceTx(ctx, tx, update, entry); err != nil {
			result.Rejections = append(result.Rejections, ProjectMemoryRejection{Title: entry.Title, Reason: err.Error(), SourceStageID: entry.SourceStageID, SourceArtifactID: entry.SourceArtifactID})
			continue
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
		result.Entries = append(result.Entries, persisted)
	}
	if err := tx.Commit(); err != nil {
		return ProjectMemoryUpdateResult{}, fmt.Errorf("commit project memory update: %w", err)
	}
	return result, nil
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

func validateProjectMemorySourceTx(ctx context.Context, tx *sql.Tx, update ProjectMemoryUpdate, entry ProjectMemoryInput) error {
	var sourceProjectID, sourceRunID, sourceTaskID string
	if err := tx.QueryRowContext(ctx, `SELECT project_id, run_id, task_id FROM stages WHERE id = ?`, entry.SourceStageID).Scan(&sourceProjectID, &sourceRunID, &sourceTaskID); err != nil {
		return fmt.Errorf("source stage must exist: %v", err)
	}
	if sourceProjectID != update.ProjectID || sourceRunID != update.RunID || sourceTaskID != update.TaskID {
		return fmt.Errorf("source stage must belong to the same project/run/task")
	}
	if err := tx.QueryRowContext(ctx, `SELECT project_id, run_id, task_id FROM artifacts WHERE id = ?`, entry.SourceArtifactID).Scan(&sourceProjectID, &sourceRunID, &sourceTaskID); err != nil {
		return fmt.Errorf("source artifact must exist: %v", err)
	}
	if sourceProjectID != update.ProjectID || sourceRunID != update.RunID || sourceTaskID != update.TaskID {
		return fmt.Errorf("source artifact must belong to the same project/run/task")
	}
	return nil
}

func normalizeProjectMemoryInput(input ProjectMemoryInput) (ProjectMemoryInput, error) {
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

func rejectionTitle(input ProjectMemoryInput) string {
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return "untitled memory candidate"
	}
	return title
}

func getProjectMemoryEntryTx(ctx context.Context, tx *sql.Tx, projectID, kind, title string) (ProjectMemoryEntry, error) {
	row := tx.QueryRowContext(ctx, `SELECT id, project_id, kind, title, body, source_run_id, source_task_id, source_stage_id, source_artifact_id, curator_stage_id, source_summary, created_at, updated_at FROM project_memory_entries WHERE project_id = ? AND kind = ? AND title = ?`, projectID, kind, title)
	entry, err := scanProjectMemoryEntry(row)
	if err != nil {
		return ProjectMemoryEntry{}, fmt.Errorf("get project memory entry: %w", err)
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
