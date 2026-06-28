package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
)

func (s *Store) ResolveGlobalAgentRegistry(ctx context.Context) (agentregistry.Registry, error) {
	global, err := s.GetGlobalAgentRegistryOverrides(ctx)
	if err != nil {
		return agentregistry.Registry{}, err
	}
	return agentregistry.Resolve(global, agentregistry.Overrides{})
}

func (s *Store) ResolveAgentRegistry(ctx context.Context, projectID string) (agentregistry.Registry, error) {
	global, err := s.GetGlobalAgentRegistryOverrides(ctx)
	if err != nil {
		return agentregistry.Registry{}, err
	}
	project, err := s.GetProjectAgentRegistryOverrides(ctx, projectID)
	if err != nil {
		return agentregistry.Registry{}, err
	}
	return agentregistry.Resolve(global, project)
}

func (s *Store) GetGlobalAgentRegistryOverrides(ctx context.Context) (agentregistry.Overrides, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT overrides_json FROM agent_registry_overrides WHERE scope = 'global'`).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return agentregistry.Overrides{}, nil
		}
		return agentregistry.Overrides{}, fmt.Errorf("get global agent registry overrides: %w", err)
	}
	return decodeAgentRegistryOverrides(raw)
}

func (s *Store) UpdateGlobalAgentRegistryOverrides(ctx context.Context, overrides agentregistry.Overrides) (agentregistry.Registry, error) {
	registry, err := agentregistry.Resolve(overrides, agentregistry.Overrides{})
	if err != nil {
		return agentregistry.Registry{}, err
	}
	raw, err := encodeAgentRegistryOverrides(overrides)
	if err != nil {
		return agentregistry.Registry{}, err
	}
	now := nowRFC3339()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO agent_registry_overrides(scope, overrides_json, created_at, updated_at) VALUES ('global', ?, ?, ?)
ON CONFLICT(scope) DO UPDATE SET overrides_json = excluded.overrides_json, updated_at = excluded.updated_at`, raw, now, now); err != nil {
		return agentregistry.Registry{}, fmt.Errorf("update global agent registry overrides: %w", err)
	}
	return registry, nil
}

func (s *Store) GetProjectAgentRegistryOverrides(ctx context.Context, projectID string) (agentregistry.Overrides, error) {
	projectID = normalizeProjectID(projectID)
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT agent_registry_overrides_json FROM projects WHERE id = ?`, projectID).Scan(&raw)
	if err != nil {
		return agentregistry.Overrides{}, fmt.Errorf("get project %s agent registry overrides: %w", projectID, err)
	}
	return decodeAgentRegistryOverrides(raw)
}

func (s *Store) UpdateProjectAgentRegistryOverrides(ctx context.Context, projectID string, overrides agentregistry.Overrides) (agentregistry.Registry, error) {
	projectID = normalizeProjectID(projectID)
	global, err := s.GetGlobalAgentRegistryOverrides(ctx)
	if err != nil {
		return agentregistry.Registry{}, err
	}
	registry, err := agentregistry.Resolve(global, overrides)
	if err != nil {
		return agentregistry.Registry{}, err
	}
	raw, err := encodeAgentRegistryOverrides(overrides)
	if err != nil {
		return agentregistry.Registry{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `UPDATE projects SET agent_registry_overrides_json = ?, updated_at = ? WHERE id = ?`, raw, nowRFC3339(), projectID)
	if err != nil {
		return agentregistry.Registry{}, fmt.Errorf("update project agent registry overrides: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return agentregistry.Registry{}, fmt.Errorf("update project agent registry override rows affected: %w", err)
	}
	if changed == 0 {
		return agentregistry.Registry{}, fmt.Errorf("get project %s: %w", projectID, sql.ErrNoRows)
	}
	return registry, nil
}

func decodeAgentRegistryOverrides(raw string) (agentregistry.Overrides, error) {
	if raw == "" {
		return agentregistry.Overrides{}, nil
	}
	var overrides agentregistry.Overrides
	if err := json.Unmarshal([]byte(raw), &overrides); err != nil {
		return agentregistry.Overrides{}, fmt.Errorf("parse agent registry overrides: %w", err)
	}
	return overrides, nil
}

func encodeAgentRegistryOverrides(overrides agentregistry.Overrides) (string, error) {
	content, err := json.Marshal(overrides)
	if err != nil {
		return "", fmt.Errorf("encode agent registry overrides: %w", err)
	}
	return string(content), nil
}
