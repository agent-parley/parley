package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent-parley/parley/internal/manager/workflow"
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

func ReadProjectRulesCandidate(repositoryPath string) (string, error) {
	return readProjectCandidateFile(repositoryPath, ProjectRulesCandidatePath)
}

func (s *Store) PromoteProjectRulesFromRepository(ctx context.Context, projectID, repositoryPath string) (Project, error) {
	content, err := ReadProjectRulesCandidate(repositoryPath)
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

func ReadProjectPreferencesCandidate(repositoryPath string) (string, error) {
	return readProjectCandidateFile(repositoryPath, ProjectPreferencesCandidatePath)
}

func (s *Store) PromoteProjectPreferencesFromRepository(ctx context.Context, projectID, repositoryPath string) (Project, error) {
	content, err := ReadProjectPreferencesCandidate(repositoryPath)
	if err != nil {
		return Project{}, err
	}
	return s.UpdateProjectPreferences(ctx, projectID, content)
}

type ProjectWorkflowTemplatePolicy struct {
	DefaultTemplateID  string
	SmallFixTemplateID string
}

func NormalizeProjectWorkflowTemplatePolicy(policy ProjectWorkflowTemplatePolicy) ProjectWorkflowTemplatePolicy {
	policy.DefaultTemplateID = strings.TrimSpace(policy.DefaultTemplateID)
	policy.SmallFixTemplateID = strings.TrimSpace(policy.SmallFixTemplateID)
	if policy.DefaultTemplateID == "" {
		policy.DefaultTemplateID = workflow.DefaultTemplateID
	}
	return policy
}

func (s *Store) GetProjectWorkflowTemplatePolicy(ctx context.Context, projectID string) (ProjectWorkflowTemplatePolicy, error) {
	project, err := s.GetProject(ctx, normalizeProjectID(projectID))
	if err != nil {
		return ProjectWorkflowTemplatePolicy{}, err
	}
	return NormalizeProjectWorkflowTemplatePolicy(ProjectWorkflowTemplatePolicy{
		DefaultTemplateID:  project.WorkflowTemplateDefaultID,
		SmallFixTemplateID: project.WorkflowTemplateSmallFixID,
	}), nil
}

func (s *Store) UpdateProjectWorkflowTemplatePolicy(ctx context.Context, projectID string, policy ProjectWorkflowTemplatePolicy) (Project, error) {
	projectID = normalizeProjectID(projectID)
	stored := ProjectWorkflowTemplatePolicy{
		DefaultTemplateID:  strings.TrimSpace(policy.DefaultTemplateID),
		SmallFixTemplateID: strings.TrimSpace(policy.SmallFixTemplateID),
	}
	if err := s.validateProjectWorkflowTemplatePolicy(ctx, stored); err != nil {
		return Project{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `UPDATE projects SET workflow_template_default_id = ?, workflow_template_small_fix_id = ?, updated_at = ? WHERE id = ?`, stored.DefaultTemplateID, stored.SmallFixTemplateID, nowRFC3339(), projectID)
	if err != nil {
		return Project{}, fmt.Errorf("update workflow template policy: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Project{}, fmt.Errorf("update workflow template policy rows affected: %w", err)
	}
	if changed == 0 {
		return Project{}, fmt.Errorf("get project %s: %w", projectID, sql.ErrNoRows)
	}
	return s.GetProject(ctx, projectID)
}

func (s *Store) validateProjectWorkflowTemplatePolicy(ctx context.Context, policy ProjectWorkflowTemplatePolicy) error {
	if policy.DefaultTemplateID != "" {
		if err := s.validateProjectWorkflowTemplatePolicyEntry(ctx, "default", policy.DefaultTemplateID); err != nil {
			return err
		}
	}
	if policy.SmallFixTemplateID != "" {
		if err := s.validateProjectWorkflowTemplatePolicyEntry(ctx, "small-fix", policy.SmallFixTemplateID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) validateProjectWorkflowTemplatePolicyEntry(ctx context.Context, label, templateID string) error {
	template, err := s.GetWorkflowTemplate(ctx, templateID)
	if err != nil {
		return fmt.Errorf("%s workflow template %q: %w", label, templateID, err)
	}
	if !workflow.MeetsHumanGateFloor(template) {
		return fmt.Errorf("%s workflow template %q is not selectable by the conversational agent: it lacks a human gate before the target branch", label, template.ID)
	}
	return nil
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
		return "", fmt.Errorf("repository path is required to read %s", rel)
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
