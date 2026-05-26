package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/agent-parley/parley/internal/models"
)

func (s *Store) loadSQLiteState() error {
	empty, err := s.sqliteIsEmpty()
	if err != nil {
		return err
	}
	if empty {
		imported, err := s.loadLegacyJSONState()
		if err != nil {
			return err
		}
		if imported {
			return nil
		}
		s.state = newState()
		return nil
	}
	loaded := newState()
	if err := loadMap(s.db, "executors", loaded.Executors); err != nil { return err }
	if err := loadMap(s.db, "projects", loaded.Projects); err != nil { return err }
	if err := loadMap(s.db, "workflow_templates", loaded.WorkflowTemplates); err != nil { return err }
	if err := loadMap(s.db, "planner_sessions", loaded.PlannerSessions); err != nil { return err }
	if err := loadMap(s.db, "planner_messages", loaded.PlannerMessages); err != nil { return err }
	if err := loadMap(s.db, "planner_generations", loaded.PlannerGenerations); err != nil { return err }
	if err := loadMap(s.db, "planner_diagnostics", loaded.PlannerDiagnostics); err != nil { return err }
	if err := loadMap(s.db, "runs", loaded.Runs); err != nil { return err }
	if err := loadMap(s.db, "tasks", loaded.Tasks); err != nil { return err }
	if err := loadMap(s.db, "attempts", loaded.Attempts); err != nil { return err }
	if err := loadMap(s.db, "handoffs", loaded.Handoffs); err != nil { return err }
	if err := loadMap(s.db, "leases", loaded.Leases); err != nil { return err }
	if err := loadMap(s.db, "artifacts", loaded.Artifacts); err != nil { return err }
	events, err := loadEvents(s.db)
	if err != nil {
		return err
	}
	loaded.Events = events
	s.state = loaded
	return nil
}

func (s *Store) sqliteIsEmpty() (bool, error) {
	for _, table := range persistedTables() {
		var count int
		if err := s.db.QueryRow("SELECT COUNT(1) FROM " + table).Scan(&count); err != nil {
			return false, err
		}
		if count > 0 {
			return false, nil
		}
	}
	return true, nil
}

func persistedTables() []string {
	return []string{"executors", "projects", "workflow_templates", "planner_sessions", "planner_messages", "planner_generations", "planner_diagnostics", "runs", "tasks", "attempts", "handoffs", "leases", "artifacts", "events"}
}

func loadMap[T any](db *sql.DB, table string, dst map[string]T) error {
	rows, err := db.Query("SELECT id, body FROM " + table)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, body string
		if err := rows.Scan(&id, &body); err != nil {
			return err
		}
		var value T
		if err := json.Unmarshal([]byte(body), &value); err != nil {
			return fmt.Errorf("decode %s %s: %w", table, id, err)
		}
		dst[id] = value
	}
	return rows.Err()
}

func loadEvents(db *sql.DB) ([]models.Event, error) {
	rows, err := db.Query(`SELECT body FROM events ORDER BY event_order ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []models.Event
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var event models.Event
		if err := json.Unmarshal([]byte(body), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) saveSQLiteSnapshotLocked() error {
	if s.db == nil {
		return fmt.Errorf("sqlite store is not open")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := saveSnapshotTx(tx, s.state); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func saveSnapshotTx(tx *sql.Tx, st state) error {
	for _, table := range []string{"events", "artifacts", "leases", "handoffs", "attempts", "tasks", "runs", "planner_diagnostics", "planner_generations", "planner_messages", "planner_sessions", "workflow_templates", "projects", "executors"} {
		if _, err := tx.Exec("DELETE FROM " + table); err != nil {
			return err
		}
	}
	for _, value := range st.Executors { if err := insertExecutor(tx, value); err != nil { return err } }
	for _, value := range st.Projects { if err := insertProject(tx, value); err != nil { return err } }
	for _, value := range st.WorkflowTemplates { if err := insertWorkflowTemplate(tx, value); err != nil { return err } }
	for _, value := range st.PlannerSessions { if err := insertPlannerSession(tx, value); err != nil { return err } }
	for _, value := range st.PlannerMessages { if err := insertPlannerMessage(tx, value); err != nil { return err } }
	for _, value := range st.PlannerGenerations { if err := insertPlannerGeneration(tx, value); err != nil { return err } }
	for _, value := range st.PlannerDiagnostics { if err := insertPlannerDiagnostic(tx, value); err != nil { return err } }
	for _, value := range st.Runs { if err := insertRun(tx, value); err != nil { return err } }
	for _, value := range st.Tasks { if err := insertTask(tx, value); err != nil { return err } }
	for _, value := range st.Attempts { if err := insertAttempt(tx, value); err != nil { return err } }
	for _, value := range st.Handoffs { if err := insertHandoff(tx, value); err != nil { return err } }
	for _, value := range st.Leases { if err := insertLease(tx, value); err != nil { return err } }
	for _, value := range st.Artifacts { if err := insertArtifact(tx, value); err != nil { return err } }
	for i, value := range st.Events { if err := insertEvent(tx, i+1, value); err != nil { return err } }
	return nil
}

func encodeBody(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func timeText(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func ptrTimeText(t *time.Time) string {
	if t == nil {
		return ""
	}
	return timeText(*t)
}

func insertExecutor(tx *sql.Tx, value models.Executor) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO executors(id, status, kind, body) VALUES (?, ?, ?, ?)`, value.ID, value.Status, value.Kind, body)
	return err
}
func insertProject(tx *sql.Tx, value models.Project) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO projects(id, created_at, updated_at, body) VALUES (?, ?, ?, ?)`, value.ID, timeText(value.CreatedAt), timeText(value.UpdatedAt), body)
	return err
}
func insertWorkflowTemplate(tx *sql.Tx, value models.WorkflowTemplate) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO workflow_templates(id, project_id, created_at, updated_at, body) VALUES (?, ?, ?, ?, ?)`, value.ID, value.ProjectID, timeText(value.CreatedAt), timeText(value.UpdatedAt), body)
	return err
}
func insertPlannerSession(tx *sql.Tx, value models.PlannerSession) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO planner_sessions(id, project_id, status, created_at, updated_at, body) VALUES (?, ?, ?, ?, ?, ?)`, value.ID, value.ProjectID, value.Status, timeText(value.CreatedAt), timeText(value.UpdatedAt), body)
	return err
}
func insertPlannerMessage(tx *sql.Tx, value models.PlannerMessage) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO planner_messages(id, session_id, role, created_at, body) VALUES (?, ?, ?, ?, ?)`, value.ID, value.SessionID, value.Role, timeText(value.CreatedAt), body)
	return err
}
func insertPlannerGeneration(tx *sql.Tx, value models.PlannerGeneration) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO planner_generations(id, project_id, session_id, status, created_at, updated_at, body) VALUES (?, ?, ?, ?, ?, ?, ?)`, value.ID, value.ProjectID, value.SessionID, value.Status, timeText(value.CreatedAt), timeText(value.UpdatedAt), body)
	return err
}
func insertPlannerDiagnostic(tx *sql.Tx, value models.PlannerDiagnostic) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO planner_diagnostics(id, project_id, session_id, generation_id, kind, sensitivity, created_at, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, value.ProjectID, value.SessionID, value.GenerationID, value.Kind, value.Sensitivity, timeText(value.CreatedAt), body)
	return err
}
func insertRun(tx *sql.Tx, value models.Run) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO runs(id, project_id, status, created_at, updated_at, body) VALUES (?, ?, ?, ?, ?, ?)`, value.ID, value.ProjectID, value.Status, timeText(value.CreatedAt), timeText(value.UpdatedAt), body)
	return err
}
func insertTask(tx *sql.Tx, value models.Task) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO tasks(id, project_id, run_id, status, created_at, updated_at, body) VALUES (?, ?, ?, ?, ?, ?, ?)`, value.ID, value.ProjectID, value.RunID, value.Status, timeText(value.CreatedAt), timeText(value.UpdatedAt), body)
	return err
}
func insertAttempt(tx *sql.Tx, value models.Attempt) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO attempts(id, project_id, run_id, task_id, attempt_number, kind, status, created_at, updated_at, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, value.ProjectID, value.RunID, value.TaskID, value.Number, value.Kind, value.Status, timeText(value.CreatedAt), timeText(value.UpdatedAt), body)
	return err
}
func insertHandoff(tx *sql.Tx, value models.Handoff) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO handoffs(id, project_id, run_id, task_id, status, created_at, updated_at, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, value.ProjectID, value.RunID, value.TaskID, value.Status, timeText(value.CreatedAt), timeText(value.UpdatedAt), body)
	return err
}
func insertLease(tx *sql.Tx, value models.Lease) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO leases(id, task_id, executor_id, status, granted_at, expires_at, body) VALUES (?, ?, ?, ?, ?, ?, ?)`, value.ID, value.TaskID, value.ExecutorID, value.Status, timeText(value.GrantedAt), ptrTimeText(value.ExpiresAt), body)
	return err
}
func insertArtifact(tx *sql.Tx, value models.Artifact) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO artifacts(id, run_id, task_id, attempt_number, kind, sensitivity, created_at, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, value.RunID, value.TaskID, value.AttemptNumber, value.Kind, value.Sensitivity, timeText(value.CreatedAt), body)
	return err
}
func insertEvent(tx *sql.Tx, order int, value models.Event) error {
	body, err := encodeBody(value); if err != nil { return err }
	_, err = tx.Exec(`INSERT INTO events(id, event_order, run_id, task_id, executor_id, lease_id, sequence, type, actor_kind, actor_id, timestamp, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, order, value.RunID, value.TaskID, value.ExecutorID, value.LeaseID, value.Sequence, value.Type, value.ActorKind, value.ActorID, timeText(value.Timestamp), body)
	return err
}
