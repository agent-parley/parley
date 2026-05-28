# Parley

> **Open-source multi-agent deliberation for software tasks.**

[![License](https://img.shields.io/badge/license-MIT-green.svg)](#license)
[![Status](https://img.shields.io/badge/status-prototype-orange.svg)](#roadmap)
[![Namespace](https://img.shields.io/badge/namespace-agent--parley-blue.svg)](#project-namespace)

Parley coordinates AI coding agents that **discuss, divide, review, and deliver** software work through an inspectable workflow.

The planned v1 app is a **Go manager/control-plane backend** with a responsive browser UI. It is local-first and currently runs as a local server process, with a supported dry-run app-container serving path for the web/control plane. The design is intended to orchestrate agent runners on the machine/container host where each runner is running.

Instead of asking one model to do everything in one pass, Parley gives a task to a small council of agents. They deliberate on the approach, split the work, run bounded tasks, review each other’s output, and produce artifacts for final approval.

> [!warning]
> Parley is early. This README describes the intended project direction and should be adapted as implementation details stabilize.

## Current v1 premises

The current prototype is local-only and dry-run by default, with an explicit experimental `local-pi` execution mode for guarded local execution plumbing. These premises describe the intended v1 direction after stabilization.

- **Frontend:** responsive web UI, not a TUI-first product and not a heavy SPA in the first slice.
- **Backend/control plane:** Go.
- **Execution:** all-in-one local manager/runner first; guarded local worktree/container execution is experimental and opt-in while real remote runners remain later.
- **Deployment:** local server process first; app-container serving is supported for dry-run web/control-plane use with explicit data/repo mounts.
- **First agent profile:** Pi, with integration boundaries kept generic for future agents.
- **Manager/runner:** `local` all-in-one first; architecture should support one manager registering multiple authenticated runners later, such as a homelab box and a powerful workstation.
- **Repo strategy:** one monorepo first, with clear manager/runner package boundaries and optional separate binaries/modes later.
- **Sync/handoff:** use Git for code state plus Parley sync manifests for safe artifacts/state; no blind workspace copying.
- **Platform posture:** OSS/distro-neutral; no Fedora-only or Ubuntu-only core assumptions.

## Why Parley?

AI coding tools are powerful, but single-agent workflows often hide too much:

- Why did the agent choose this approach?
- What alternatives were considered?
- Which parts of the task were risky?
- Was the output reviewed by fresh context?
- What changed, and what still needs final approval?

Parley makes the workflow explicit.

```text
Task
  ↓
Deliberation
  ↓
Plan
  ↓
Delegated agent work
  ↓
Fresh review
  ↓
Final approval
  ↓
Integration
```

## What Parley does

Parley is designed to support:

- **Multi-agent deliberation** — agents propose, critique, revise, and converge.
- **Task decomposition** — large software goals become bounded work units.
- **Role-based agents** — planner, worker, reviewer, oracle, scout, or custom roles.
- **Review-gated execution** — important transitions require approval.
- **Inspectable artifacts** — plans, outputs, events, diffs, and reviews are preserved.
- **Isolated work** — dry-run remains the default; the experimental `local-pi` mode uses managed worktrees and a guarded container runtime scaffold.
- **Agent-ready orchestration** — start with one agent profile, leave room for more.
- **Manager/runner control path** — start with all-in-one local mode, but avoid assumptions that prevent one manager from coordinating several authenticated runners later.
- **Monorepo before multi-repo** — keep manager/runner interfaces together until APIs stabilize; split binaries/modes before splitting repositories.
- **Safe handoff/sync** — move work between runners with Git plus explicit manifests, not shared databases, direct remote mounts, exposed container sockets, or raw workspace copies.
- **Responsive web access** — monitor and approve work from desktop browsers locally today; intentional network exposure waits for authentication.

## What Parley is not

Parley is not:

- a claim that multi-agent systems always outperform single agents
- a replacement for engineering judgment
- a generic chatbot UI
- a promise of fully autonomous production code
- a black-box coding service

Parley is a workflow layer for making agentic software work more structured, observable, and governable.

## Core concepts

### Parley

A **parley** is a structured discussion between agents around a task. It is not an open-ended chat. It has roles, inputs, constraints, and an expected output.

### Council

A **council** is the group of agents assigned to a task. A council might include a planner, implementer, reviewer, and specialist agents.

### Gate

A **gate** is an approval boundary. Gates prevent planning, execution, integration, or destructive operations from happening invisibly.

### Artifact

An **output** is durable material from a run: plans, logs, diffs, summaries, review notes, or generated files.

### Agent profile

An **agent profile** translates Parley workflow steps into concrete agent invocations.

## Example workflow

```text
1. User describes a software goal.
2. Planner agent drafts a plan.
3. Reviewer/oracle critiques the plan.
4. User approves or edits the plan.
5. Parley saves the plan into durable task instructions.
6. Worker agents run isolated tasks.
7. Reviewer agents inspect diffs and artifacts.
8. User approves fixes or integration.
9. Parley preserves the full run history.
```

## Project namespace

The public product name is **Parley**.

The project uses **agent-parley** for URLs and package namespaces:

```text
GitHub org:    agent-parley
Main repo:     agent-parley/parley
npm scope:     @agent-parley/*
PyPI package:  agent-parley
CLI name:      agent-parley or parley
```

## Quickstart

> [!note]
> The current prototype is a local all-in-one manager/runner web app. It proves project registration, editable project settings/workflow templates, persistent planner sessions, asynchronous planner/critic draft generation, planner generation history/diagnostics, manual task plans, approval, runner slots, de-duplicated activity display, real task detail tabs with attempt history, runner management, mock safe handoff preview, chain visualization, and placeholder outputs. Workflow templates are planning previews while approved tasks run as dry-run attempts by default. Planner/critic generation can update a draft before approval, but task creation and worker execution remain separate approval-gated steps. Approval persists a queued attempt and schedules it through the bounded local dispatcher; queued attempts recover on startup, while attempts interrupted mid-run are marked failed/retryable. Dry-run remains the default; an explicit experimental local Pi mode can run guarded planner/critic and worker/reviewer steps for hardening.

```bash
git clone git@github.com:agent-parley/parley.git
cd parley

go run ./cmd/parley \
  --bind 127.0.0.1:7345
```

Then open:

```text
http://127.0.0.1:7345
```

Optional data-root override:

```bash
PARLEY_DATA_ROOT=/path/to/parley-data \
  go run ./cmd/parley
```

### App-container serving

Parley also ships an OCI app-container serving path for local dry-run use. This serves the web/control plane only; it is not a public Pi runner image, remote runner mode, or container-execution backend.

```bash
podman build -t parley-app:local -f Containerfile .
```

```bash
podman volume create parley-data
podman run --rm \
  -p 127.0.0.1:7345:7345 \
  -v parley-data:/data:Z \
  -v "$PWD:/workspace/repo:ro,Z" \
  parley-app:local
```

Then open `http://127.0.0.1:7345` and register mounted repos by their in-container paths, such as `/workspace/repo`. The container defaults to `PARLEY_APP_CONTAINER=true`, `PARLEY_EXECUTION_MODE=dry-run`, and `--bind 0.0.0.0:7345` so container-internal serving works with port publishing.

> [!warning]
> App-container mode relies on the container runtime's host-port binding for local-only access. Publish the host port to loopback exactly as shown (`-p 127.0.0.1:7345:7345`). Do **not** use broad publishing such as `-p 7345:7345` or `-p 0.0.0.0:7345:7345`; that can expose this unauthenticated prototype beyond localhost. Parley's Host-header allowlist is defense-in-depth, not a network access-control boundary.

Authenticated LAN exposure remains disabled. The server still rejects non-local Host headers in app-container mode, so access by container DNS name, service name, or raw LAN host/IP is intentionally unsupported. `/healthz` returns `ok` for external local health checks; the distroless image does not include curl/wget or an in-container `HEALTHCHECK`.

Dry-run is the default execution mode. The guarded local Pi execution path is experimental, currently Linux-only, requires Podman plus a configured Pi-compatible runner image to already be available locally, and must be selected explicitly. `PARLEY_RUNTIME_PROVIDER` / `--runtime-provider` introduces runtime-provider vocabulary: `podman` is the only active provider today, while `docker` is accepted as a planned name but fails clearly as unsupported instead of silently falling back. Its dispatcher stays bounded in-process; queued attempts are persisted with a hardcoded prototype backlog cap of 100 and recover on startup, while interrupted running attempts are failed and retryable. Capacity-related pre-start failures stay queued/deferred; unrecoverable setup failures fail the queued attempt with sanitized UI events. Repeated restarts can repeat recovery activity events; this is safe pre-alpha observability noise, not repeated execution.

The built-in profile image name is a placeholder until a public Pi runner image is published. For local smoke validation, point `PARLEY_PI_IMAGE` at a local image that provides a `pi --headless --input <file>` command.

```bash
PARLEY_EXECUTION_MODE=local-pi \
  PARLEY_PI_IMAGE=<local-image> \
  PARLEY_DATA_ROOT=/path/to/parley-data \
  go run ./cmd/parley
```

The prototype stores state in SQLite at `<data-root>/parley.db`. If an older `<data-root>/state.json` exists and every persisted SQLite table is empty, Parley imports it once, applies normal startup repairs/defaults, and leaves the JSON file in place as a backup.

### Repo-local non-secret settings

A repository may include `.parley/settings.toml` to suggest non-secret project defaults at registration time. These settings are advisory defaults only; existing project settings keep precedence after registration and approval gates still control execution. `queue_policy = "manual"` is the default; `queue_policy = "auto_when_ready"` only queues durable local attempts after explicit task approval or fix-ready gates, using the same local dispatcher as the manual button.

```toml
runtime_provider = "podman"
default_agent_profile = "pi-standard"
workflow_template = "standard"
queue_policy = "manual"
review_profiles = ["pi-reviewer"]
```

Supported keys are intentionally narrow: `runtime_provider`, `container_backend`, `default_agent_profile`, `workflow_template`, `queue_policy`, and `review_profiles`. Secrets, auth provider config, secret-file references, socket paths, path overrides, container/network/privileged flags, remote runner settings, sync/handoff, and memory export settings are rejected. `docker` remains planned but unsupported and fails clearly if selected. Queue policy does not add cron, GitHub polling, remote scheduling, Docker, auth, sync/handoff, or approval bypass.

### Secret/auth safety contract

Parley has a central safety contract for future repo-sourced credentials, but this prototype does not load or execute auth providers yet. Secret references are names only: `env:VAR_NAME` is the only supported source shape, and the referenced variable must match the selected profile's env-prefix allowlist, look credential-intended (`TOKEN`, `API_KEY`, `SECRET`, `CREDENTIAL`, `PASSWORD`, etc.), and not be a Parley process/config variable such as `PARLEY_BIND` or `PARLEY_DATA_ROOT`. Inline tokens, repo-local secret files, broad env inheritance, keychain references, socket paths, and container/network overrides are rejected or deferred. Secret-like diagnostics, checkpoints, and normal artifact bodies are redacted or classified away from normal preview/handoff paths; approval gates and dry-run/local-pi boundaries are unchanged.

### Metadata-only agent registry

The profile registry can now name future agent families without enabling them. Pi remains the only active execution family. Codex, Claude Code, Gemini, and Copilot entries are visible as planned/disabled metadata for roadmap and UI language only; they are not selectable project defaults, are not included in local-pi invocation allowlists, and have no images, adapters, auth resolution, remote execution, or queue behavior.

## Validation

The pre-alpha Go test suite is intended to protect behavior, not chase random coverage. It runs without Podman, Pi, Docker, remote runners, sync, or scheduling:

```bash
go test ./...
```

Optional static validation:

```bash
go vet ./...
```

Current coverage focuses on store transitions and SQLite persistence, durable dispatcher enqueue/recovery behavior and sanitized events, workflow failure/lease lifecycle, internal checkpoint artifacts/resume metadata, artifact sensitivity and preview/handoff exclusions, diagnostics-route access for internal diagnostic artifacts, asynchronous planner/critic draft generation, planner diagnostics, strict JSON validation, profile and Pi adapter validation, managed worktree cleanup, path confinement, and config defaults/platform gates. Manual `local-pi` smoke validation is a separate Linux-only step. The guarded container/worktree path has been smoke-validated with a local stub runner image; a real published Pi runner image remains a later hardening milestone.

## Configuration

> [!note]
> Placeholder. Final configuration should document local data paths, agent/profile settings, model/provider settings, and execution isolation options.

Example direction:

```toml
[server]
bind = "127.0.0.1:7345"
# app_container = false # explicit container serving mode; dry-run only
# trusted_lan = false  # disabled until authentication is implemented

[data]
root = "~/.local/share/parley"

[runners.local]
type = "local"
resource_class = "workstation"

[agents]
default = "pi"
idle_retention_minutes = 0 # 0 closes local agents after each task/step
max_idle_agents = 1       # guardrail for future idle-agent reuse

[execution]
mode = "dry-run" # explicit local-pi is experimental and opt-in
runtime_provider = "podman" # docker is planned but unsupported
queue_policy = "manual" # auto_when_ready still requires explicit approval gates
worktree_isolation = true
approval_required = true
```

Current agent/session persistence is checkpoint-based: Parley writes compact internal checkpoint artifacts for worker/reviewer steps and passes prior checkpoint metadata into later attempts as resume context. It does not keep live containers or agents alive yet, even when non-zero retention is configured. Malformed integer values for `PARLEY_AGENT_IDLE_RETENTION_MINUTES` or `PARLEY_MAX_IDLE_AGENTS` fail startup with a clear configuration error instead of silently falling back.

## Architecture

Parley is best understood as a Go web/control plane for agent workflows.

```text
Responsive Browser UI
  ├─ runner selector
  ├─ task intake
  ├─ plan review
  ├─ run monitor
  ├─ diff/review viewer
  └─ approval gates

Go Manager / Control Plane
  ├─ runner capability registry
  ├─ project registry
  ├─ planning sessions
  ├─ task compiler
  ├─ workflow engine
  ├─ event log
  ├─ artifact store
  ├─ runner slot manager
  ├─ sync/handoff coordinator
  ├─ agent profile registry
  └─ integration gate

Runner Runtime
  ├─ local repo/workspace cache
  ├─ worktree/container manager
  ├─ agent profile host
  └─ artifact/log collector

Machine-Local Agent Runtime
  ├─ planner agents
  ├─ worker agents
  ├─ reviewer agents
  └─ specialist agents
```

## Design principles

- **Deliberation before execution** — do not rush into code when the task needs a plan.
- **Durable plans over chat drift** — turn intent into explicit task plans.
- **Fresh-context review** — review should not inherit all worker assumptions.
- **Review-gated integration** — risky or irreversible transitions require final approval.
- **Inspectable by default** — preserve plans, decisions, logs, artifacts, and diffs.
- **Local-first where possible** — avoid unnecessary cloud dependency.
- **Machine-local execution** — each runner runs agents on its own host resources; remote control goes through authenticated manager/runner APIs, not shared sockets or remote filesystem mounts.
- **Git for code, manifests for Parley state** — committed code moves through branches/fetch/pull; Parley-owned state moves through checked sync manifests.
- **Portable by default** — support Linux/container-runtime diversity without distro-specific assumptions in core code.
- **Agent-ready, not integration-chaotic** — support extension without losing coherence.

## Prototype roadmap

The public prototype roadmap is tracked inline here. Private `docs/` and `plans/` notes are intentionally ignored by the repo.

Implemented product prototypes:

1. Project settings and workflow templates.
2. Interactive planner session.
3. Task detail tabs and attempt timeline.
4. Runner management.
5. Sync/handoff UI mock.
6. Diagnostics tab for local-only internal logs/checkpoints without making them normal artifacts or handoff content.

Next:

1. Meaningful pre-alpha tests now cover the core local execution foundation and should be kept green for alpha and hardening commits.
2. Durable queued-attempt recovery is in place for queued attempts; interrupted running attempts are conservatively failed and retryable on startup. The persisted backlog cap is currently hardcoded at 100, and repeated restarts can repeat recovery activity events until later de-duplication work.
3. Agent/session persistence is checkpoint-based: internal checkpoint artifacts preserve per-step resume context while live agent/container reuse remains disabled by default.
4. Dry-run UX polish is in place: task details use real local tabs, empty dry-run diffs explain that no diff was produced, activity rows suppress duplicate label/summary text, and handoff copy states that preview/approval is mock-only.
5. The alpha UI path has been manually validated in dry-run mode, the guarded `local-pi` worktree/container path has been smoke-validated with a local stub runner image, and diagnostics UI/internal-log access now uses a task-scoped internal route that keeps raw runtime logs out of normal preview and handoff content. Real Pi runner image publishing/hardening remains separate. Remote runners and real sync/handoff remain deferred.
6. Planner sessions can now generate planner/critic drafts asynchronously before approval. Dry-run produces a local preview; explicit experimental `local-pi` runs real planner/critic Pi profiles against a managed clean Git worktree with read-only repo/scratch mounts before the user approves any task execution. Planner/critic outputs require strict structured JSON, duplicate generation starts are blocked while one is running, stale generation results are discarded if the session is approved/dismissed/revised while they run, and internal planner diagnostics are scoped to the planner session route.

## Roadmap

### Phase 0 — Project skeleton

- [x] Create repo structure
- [x] Choose license
- [x] Define core data model
- [x] Add initial documentation
- [x] Add all-in-one `local` manager/runner capability model
- [x] Keep manager/runner roles in one repo with explicit package boundaries
- [x] Add runner slot placeholder for task attempts
- [x] Add sync manifest/ignore-policy placeholders
- [x] Establish responsive web shell
- [x] Establish supported dry-run app-container serving path

### Phase 1 — Planning and deliberation

- [x] Task intake
- [x] Planner session prototype
- [x] Structured plan artifact
- [x] Approval gate
- [x] Real planner/critic agent execution behind approval gate
- [x] Async planner generation history and internal diagnostics

### Phase 2 — Execution workflow

- [x] Task plans
- [x] Local dry-run worker role
- [x] Event logging
- [x] Artifact store
- [x] Task-scoped internal diagnostics/log preview
- [x] Run monitor
- [x] SQLite persistence foundation
- [x] Real worktree manager
- [x] Podman runner scaffold
- [x] Runtime provider vocabulary and backend seam
- [x] Prepared invocation validation
- [x] Behavior-focused pre-alpha test baseline
- [x] Durable queued-attempt recovery
- [x] Internal checkpoint artifacts for resume context
- [x] Agent idle-retention/max-idle policy settings
- [ ] Live idle-agent/container reuse behind retention policy
- [ ] Pi worker/reviewer invocation hardened beyond experimental `local-pi` scaffold

### Phase 3 — Review and integration

- [x] Placeholder fresh-review artifact
- [x] Diff/artifact review UI
- [x] Fix loop
- [x] Review-gated integration controls
- [ ] Real fresh-reviewer execution hardened beyond experimental `local-pi` scaffold
- [ ] Real diff capture from worktrees hardened beyond experimental `local-pi` scaffold

### Phase 4 — Extensibility

- [x] Agent profile metadata
- [x] Runner registry prototype
- [x] Safe handoff/sync manifest preview
- [ ] Authenticated remote runner design
- [ ] Example custom agent role
- [ ] Workflow extension points that drive execution
- [ ] Provider/model configuration

## Contributing

Parley is early and welcomes focused contributions.

Good first contribution areas:

- documentation and examples
- workflow design
- role definitions
- local execution safety
- event/artifact formats
- agent profile prototypes
- UI sketches

Before opening a large PR, start with an issue or design note.

## Security model

Parley should treat agent execution as untrusted by default.

Expected safety direction:

- no broad host home-directory mounts
- explicit project mounts
- per-task worktrees for edits
- no raw credential leakage to workers
- localhost-only use until authenticated access and Host/Origin/CSRF protections are complete for intentional exposure
- no direct remote container socket or filesystem sharing between runners
- no blind workspace-folder sync; use `.parleyignore`, hard exclusions, checksums, and explicit approval for sensitive/large artifacts
- final approval before destructive operations
- event logs for sensitive transitions

## License

MIT

## Name

**Parley** means a discussion or conference, especially between parties negotiating terms. The name fits the project because software tasks often benefit from structured conversation before action.

Agents should not just act. They should parley.