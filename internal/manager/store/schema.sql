CREATE TABLE IF NOT EXISTS projects (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  project_rules TEXT NOT NULL DEFAULT '',
  project_preferences TEXT NOT NULL DEFAULT '',
  queue_auto_when_ready INTEGER NOT NULL DEFAULT 1,
  queue_max_concurrent INTEGER NOT NULL DEFAULT 1,
  queue_backlog_cap INTEGER NOT NULL DEFAULT 100,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workspaces (
  project_id TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
  path TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS repositories (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  path TEXT NOT NULL,
  is_default INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_templates (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  is_predefined INTEGER NOT NULL DEFAULT 0,
  is_recommended INTEGER NOT NULL DEFAULT 0,
  is_editable INTEGER NOT NULL DEFAULT 1,
  template_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  repository_id TEXT REFERENCES repositories(id),
  idea TEXT NOT NULL,
  refinement_level TEXT NOT NULL DEFAULT 'standard',
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(project_id, id)
);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  idea TEXT NOT NULL,
  refinement_level TEXT NOT NULL DEFAULT 'standard',
  workflow_template_id TEXT NOT NULL DEFAULT 'balanced_pr_delivery',
  status TEXT NOT NULL,
  event_log_artifact_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id),
  UNIQUE(project_id, id)
);

CREATE TABLE IF NOT EXISTS attempts (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(project_id, run_id) REFERENCES runs(project_id, id),
  FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id),
  UNIQUE(project_id, run_id, id)
);

CREATE TABLE IF NOT EXISTS stages (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  workflow_stage_id TEXT,
  stage_type TEXT NOT NULL,
  adapter TEXT,
  status TEXT NOT NULL,
  stage_brief_artifact_id TEXT REFERENCES artifacts(id),
  task_plan_artifact_id TEXT REFERENCES artifacts(id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(project_id, run_id, attempt_id) REFERENCES attempts(project_id, run_id, id),
  FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id)
);

CREATE TABLE IF NOT EXISTS workflow_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id TEXT NOT NULL REFERENCES projects(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  snapshot_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(project_id, run_id) REFERENCES runs(project_id, id),
  FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id)
);

CREATE TABLE IF NOT EXISTS runner_registry (
  runner_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  origin TEXT NOT NULL DEFAULT 'registered',
  capabilities_json TEXT NOT NULL,
  missed_heartbeats INTEGER NOT NULL DEFAULT 0,
  connected_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  kind TEXT NOT NULL,
  media_type TEXT NOT NULL,
  path TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(project_id, run_id) REFERENCES runs(project_id, id),
  FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id)
);

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  project_id TEXT REFERENCES projects(id),
  run_id TEXT REFERENCES runs(id),
  scope TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  timestamp TEXT NOT NULL,
  task_id TEXT,
  attempt_id TEXT,
  type TEXT NOT NULL,
  actor_kind TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  summary TEXT NOT NULL,
  data_json TEXT NOT NULL,
  envelope_json TEXT NOT NULL,
  UNIQUE(scope, sequence)
);

CREATE TABLE IF NOT EXISTS project_memory_entries (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  source_run_id TEXT NOT NULL REFERENCES runs(id),
  source_task_id TEXT NOT NULL REFERENCES tasks(id),
  source_stage_id TEXT NOT NULL REFERENCES stages(id),
  source_artifact_id TEXT NOT NULL REFERENCES artifacts(id),
  curator_stage_id TEXT NOT NULL REFERENCES stages(id),
  source_summary TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(project_id, source_run_id) REFERENCES runs(project_id, id),
  FOREIGN KEY(project_id, source_task_id) REFERENCES tasks(project_id, id),
  UNIQUE(project_id, kind, title)
);

CREATE INDEX IF NOT EXISTS idx_projects_created_at ON projects(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_workspaces_project_id ON workspaces(project_id);
CREATE INDEX IF NOT EXISTS idx_repositories_project_id ON repositories(project_id);
CREATE INDEX IF NOT EXISTS idx_workflow_templates_recommended ON workflow_templates(is_recommended DESC, name ASC);
CREATE INDEX IF NOT EXISTS idx_runs_project_created ON runs(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_task_id ON runs(task_id);
CREATE INDEX IF NOT EXISTS idx_runs_workflow_template_id ON runs(workflow_template_id);
CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_project_created ON tasks(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_attempts_run_id ON attempts(run_id);
CREATE INDEX IF NOT EXISTS idx_attempts_task_created ON attempts(project_id, task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_stages_run_id ON stages(run_id);
CREATE INDEX IF NOT EXISTS idx_stages_attempt_id ON stages(project_id, run_id, attempt_id);
CREATE INDEX IF NOT EXISTS idx_events_run_sequence ON events(run_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_project_sequence ON events(project_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_scope_sequence ON events(scope, sequence);
CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_project_id ON artifacts(project_id);
CREATE INDEX IF NOT EXISTS idx_project_memory_entries_project ON project_memory_entries(project_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_project_memory_entries_source_run ON project_memory_entries(source_run_id);
