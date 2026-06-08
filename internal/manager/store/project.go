package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	ProjectRulesCandidatePath       = ".parley/rules.md"
	ProjectPreferencesCandidatePath = ".parley/preferences.md"
)

func (s *Store) GetProjectRules(ctx context.Context, projectID string) (string, error) {
	project, err := s.GetProject(ctx, normalizeProjectID(projectID))
	if err != nil {
		return "", err
	}
	return project.ProjectRules, nil
}

func (s *Store) UpdateProjectRules(ctx context.Context, projectID, rules string) (Project, error) {
	return s.updateProjectText(ctx, normalizeProjectID(projectID), "project_rules", rules)
}

func (s *Store) PromoteProjectRulesFromRepository(ctx context.Context, projectID, repositoryPath string) (Project, error) {
	content, err := readProjectCandidateFile(repositoryPath, ProjectRulesCandidatePath)
	if err != nil {
		return Project{}, err
	}
	return s.UpdateProjectRules(ctx, projectID, content)
}

func (s *Store) GetProjectPreferences(ctx context.Context, projectID string) (string, error) {
	project, err := s.GetProject(ctx, normalizeProjectID(projectID))
	if err != nil {
		return "", err
	}
	return project.ProjectPreferences, nil
}

func (s *Store) UpdateProjectPreferences(ctx context.Context, projectID, preferences string) (Project, error) {
	return s.updateProjectText(ctx, normalizeProjectID(projectID), "project_preferences", preferences)
}

func (s *Store) PromoteProjectPreferencesFromRepository(ctx context.Context, projectID, repositoryPath string) (Project, error) {
	content, err := readProjectCandidateFile(repositoryPath, ProjectPreferencesCandidatePath)
	if err != nil {
		return Project{}, err
	}
	return s.UpdateProjectPreferences(ctx, projectID, content)
}

func (s *Store) updateProjectText(ctx context.Context, projectID, column, content string) (Project, error) {
	if column != "project_rules" && column != "project_preferences" {
		return Project{}, fmt.Errorf("unsupported project text column %q", column)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`UPDATE projects SET %s = ?, updated_at = ? WHERE id = ?`, column), content, nowRFC3339(), projectID)
	if err != nil {
		return Project{}, fmt.Errorf("update %s: %w", column, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Project{}, fmt.Errorf("update %s rows affected: %w", column, err)
	}
	if changed == 0 {
		return Project{}, fmt.Errorf("get project %s: %w", projectID, sql.ErrNoRows)
	}
	return s.GetProject(ctx, projectID)
}

func readProjectCandidateFile(repositoryPath, rel string) (string, error) {
	repositoryPath = strings.TrimSpace(repositoryPath)
	if repositoryPath == "" {
		return "", fmt.Errorf("repository path is required to promote %s", rel)
	}
	content, err := os.ReadFile(filepath.Join(filepath.Clean(repositoryPath), rel))
	if err != nil {
		return "", fmt.Errorf("read project candidate %s: %w", rel, err)
	}
	return string(content), nil
}

func normalizeProjectID(projectID string) string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return DefaultProjectID
	}
	return projectID
}
