package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

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

// UpdateAgentRegistryOverridesAtomically persists a global override update and
// all associated project override updates in one transaction.
func (s *Store) UpdateAgentRegistryOverridesAtomically(ctx context.Context, globalOverrides agentregistry.Overrides, projectOverrides map[string]agentregistry.Overrides) error {
	if _, err := agentregistry.Resolve(globalOverrides, agentregistry.Overrides{}); err != nil {
		return err
	}
	globalRaw, err := encodeAgentRegistryOverrides(globalOverrides)
	if err != nil {
		return err
	}

	projectRaws := make(map[string]string, len(projectOverrides))
	projectIDs := make([]string, 0, len(projectOverrides))
	for rawProjectID, overrides := range projectOverrides {
		projectID := normalizeProjectID(rawProjectID)
		if _, exists := projectRaws[projectID]; exists {
			return fmt.Errorf("duplicate project agent registry update for %s", projectID)
		}
		if _, err := agentregistry.Resolve(globalOverrides, overrides); err != nil {
			return fmt.Errorf("resolve project %s agent registry overrides: %w", projectID, err)
		}
		raw, err := encodeAgentRegistryOverrides(overrides)
		if err != nil {
			return err
		}
		projectRaws[projectID] = raw
		projectIDs = append(projectIDs, projectID)
	}
	sort.Strings(projectIDs)

	now := nowRFC3339()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin atomic agent registry override update: %w", err)
	}
	defer rollback(tx)

	if _, err := tx.ExecContext(ctx, `INSERT INTO agent_registry_overrides(scope, overrides_json, created_at, updated_at) VALUES ('global', ?, ?, ?)
ON CONFLICT(scope) DO UPDATE SET overrides_json = excluded.overrides_json, updated_at = excluded.updated_at`, globalRaw, now, now); err != nil {
		return fmt.Errorf("update global agent registry overrides: %w", err)
	}
	for _, projectID := range projectIDs {
		result, err := tx.ExecContext(ctx, `UPDATE projects SET agent_registry_overrides_json = ?, updated_at = ? WHERE id = ?`, projectRaws[projectID], now, projectID)
		if err != nil {
			return fmt.Errorf("update project %s agent registry overrides: %w", projectID, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("update project %s agent registry override rows affected: %w", projectID, err)
		}
		if changed == 0 {
			return fmt.Errorf("get project %s: %w", projectID, sql.ErrNoRows)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit atomic agent registry override update: %w", err)
	}
	return nil
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
