# Parley

> **Open-source, local-first harness that takes a software idea to a sandboxed, inspectable PR-ready stop.**

[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](#status)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Namespace](https://img.shields.io/badge/namespace-agent--parley-blue.svg)](#name-and-namespace)

Parley is a **control-plane harness**, not a coding agent. It owns run state, runner coordination, sandbox setup, artifacts, events, and delivery policy; agents are the workers it dispatches.

The current build is a local-first **operator app**, not yet the full configurable workflow product. You drive it through a **chat-primary web UI**: a conversation can discuss a repository and, when you ask, author a scoped **Task** that the harness runs through a deterministic delivery loop — idea intake (with a refinement level), implementation in an isolated worktree, containerized validation, and a commit built from the worker snapshot — suspending at human review/approval gates and stopping at a **PR-ready handoff**. Repo-aware chat and real diffs require a diff-producing adapter, the **Pi** agent; the default `noop` adapter exercises the same paths but writes nothing (chat returns a placeholder reply, and a run reaches the commit stage with *no changes to commit*). Parley does **not** push branches or open pull requests yet, and executes one local runner slot at a time.

> [!warning]
> Parley is early/alpha. The operator app and delivery loop run, but the product surface and workflow model are still changing. This README separates what works today from intended direction.

## Status

What runs today:

- **Built, live-validated:** the core control plane — Manager spawns and dials one Runner over a persistent WebSocket; deterministic `idea → implementation → validation → commit → pr_ready` delivery loop; rootless Podman sandbox provider; isolated git worktrees; SQLite + filesystem artifacts; durable events; per-run JSONL logs; one real agent family, **Pi**, behind the runner adapter interface.
- **Built:** chat-primary **operator UI** (embedded hypermedia, Datastar + SSE) with three surfaces — **Chat** (a conversation is a first-class entity), **Run story** (stage/event progress, artifacts, diffs, cancellation, runner health), and **Settings**. A conversation can author an allow-listed **Task** (`create-Task`) that the harness runs, stopping at human plan review.
- **Built:** **idea-intake refinement levels** — `direct` (deterministic) and `standard` (single-shot plan). Legacy `deep` values degrade to `standard`.
- **Built:** **human review/approval stages** that suspend a run to durable state (`awaiting_human`) and resume on a verdict (pass / changes-requested / blocked).
- **Built:** in-app notifications **and** external notification sinks (gotify + signed webhook); a centralized **secrets-at-rest facility** (envelope encryption, XChaCha20-Poly1305, pluggable key provider, fail-closed); optional TOML settings and a local auto-queue — `POST /runs` enqueues runs that auto-dispatch to free slots (approval gates preserved), with a backlog cap and crash/startup recovery.
- **Not yet built:** push/PR creation (stops at PR-ready metadata); concurrent multi-runner execution (single local slot — one run at a time, the rest queue); editable workflow templates; agent profiles; project memory; semantic review verdicts; auto-pickup / issue polling; non-Pi agents and non-Podman substrates.
- **No published release yet.** Expect sharp edges.

There is a single local runner slot, so one run executes at a time; additional submitted runs are queued and auto-dispatched as the slot frees. There is no external scheduler or issue auto-pickup yet.

## What works today

There are two ways to drive Parley:

1. **Chat** — open a project and talk to its conversation. The conversational agent can discuss the repository and, when you ask, emit an allow-listed `create-Task` action; the harness then runs that Task through the delivery loop, suspending at human plan review. (Repo-aware replies need the Pi adapter; with `noop` the agent returns a placeholder.)
2. **Direct run submission** — `POST /runs` (or the UI) enqueues an idea with a refinement level and dispatches it to the runner slot.

Either way, the deterministic delivery loop is:

```text
Idea intake         (manager creates the run and task contract; refinement: direct | standard)
  → Implementation  (Pi adapter in an isolated worktree; noop adapter by default)
  → Validation      (containerized validation command; status gate only)
  → Commit          (commit made from the post-implementation worker snapshot)
  → PR-ready stop   (branch/commit/diff metadata; no forge push, no PR)
```

Human review/approval stages can **suspend** the run to durable state (`awaiting_human`) and resume on a verdict.

The full commit → PR-ready loop requires an adapter that produces changes (the Pi agent). With the default `noop` adapter the implementation step writes nothing, so the run reaches the commit stage and stops there with *no changes to commit*.

Delivery **routing** is deterministic on structured `status` values only — the conversational agent plans and authors Tasks but does not alter that routing. There is no semantic verdict engine, configurable fix loop, or editable workflow template yet.

The web UI lets you hold a conversation, submit and watch runs (stage/event progress over SSE), inspect artifacts and diffs, act on human-review gates, cancel a run, manage notifications and settings, and see runner health. Runs, conversations, and artifacts are persisted under `.parley-data` by default.

Submitted runs are enqueued and auto-dispatched to the local runner slot as it frees; the UI shows queue depth and the effective policy. Optional TOML settings (`.parley/config.toml`) select defaults such as the queue policy (`auto_when_ready`, `max_concurrent`, `backlog_cap`); settings change which defaults apply, never the deterministic routing.

## Intended direction

The long-term product direction is still a configurable workflow harness:

- editable workflow templates and run snapshots
- richer agent-or-human stage configuration across the workflow (planning, review, validation, commit, PR creation, memory update)
- semantic review verdicts and fix loops
- configurable delivery policy, including real push/PR creation
- concurrent multi-runner execution beyond the single local slot
- agent profiles, context packets, auto-pickup, and curated memory
- additional agent families and sandbox substrates beyond Pi/rootless Podman

Those are direction, not current behavior.

## Sandboxed by design

Isolation is a core feature. Parley treats agents as untrusted automation and runs live agent/validation work inside sandboxed containers.

- **Today:** rootless Podman is the implemented sandbox provider.
- **Today:** Pi is the only real agent family; validation runs as its own adapter.
- **Today:** edit work happens in per-run worktrees, not your primary checkout.
- **Today:** credentials are referenced through explicit local paths/volumes; they are not intended to be committed into the repo.
- **Planned, not done:** Docker support, remote runners, and non-Pi agent families.

## Pluggable, adapter-ready

The runner has a generic adapter interface, but the only real supported agent family today is **Pi**. The generic interface is a seam for future adapters, not a claim of broad provider support.

Parley curates the dispatch contract and reads structured reports back from adapters. Agents manage their own working context inside that boundary.

## Build, run, and test

Prerequisites:

- Go 1.26 (see `go.mod`)
- `make`
- rootless Podman — required for the sandboxed run pipeline (validation), the Pi agent, and the live test targets. The web UI and **chat with the default `noop` adapter run without Podman** (useful for demoing the operator surface); the full delivery loop and repo-aware Pi replies need it.

Build both binaries:

```sh
make build
```

Run the Manager and its spawned Runner:

```sh
make run
```

`make run` builds first, starts the web UI at `http://127.0.0.1:8080` by default, and stores local state in `.parley-data`. Override with environment variables such as `PARLEY_ADDR`, `PARLEY_DATA_DIR`, and `PARLEY_RUNNER_BIN`.

The default implementation adapter is `noop`, which makes no file changes — useful as a smoke test, but a run using it stops at the commit stage with *no changes to commit*. To exercise the full commit → PR-ready loop, run the real Pi adapter by providing the Pi worker image/auth configuration and opting in explicitly, for example:

```sh
PARLEY_ADAPTER=pi \
PARLEY_PI_AUTH_JSON=/path/to/auth.json \
PARLEY_PI_IMAGE=localhost/parley-pi-worker:0.78.0 \
make run
```

Validation uses a rootless Podman container. Tune it with `PARLEY_VALIDATION_IMAGE`, `PARLEY_VALIDATION_CMD`, and `PARLEY_VALIDATION_NETWORK`.

Useful test targets:

```sh
make test
make vet
make test-race
make test-integration
make test-live-m4
make test-live-m5
make test-live-m5-loop
```

The live targets are guarded and require the Podman images, Pi auth volume/path, and environment described in the `Makefile`.

## What Parley is not

- not a claim that multiple agents always beat a single agent
- not a replacement for engineering judgment
- not a generic chatbot — chat authors scoped specs that execute under review, it is not free-form conversation for its own sake
- not a black-box "fully autonomous engineer"
- not a hosted cloud service
- not currently a tool that pushes branches or opens pull requests for you

Parley is a workflow layer that makes agentic software work **structured, inspectable, and governable**.

## Name and namespace

**Parley** — a conference between parties negotiating terms. The name fits: plans are reviewed before work begins, and work is reviewed before it ships. Agents shouldn't just act — they should parley.

The product is **Parley**; the project uses **`agent-parley`** for URLs and package namespaces (e.g. `github.com/agent-parley/parley`).

## Contributing

Parley is early and welcomes focused contributions to the current build and its next layers: workflow depth, sandbox safety, event/artifact contracts, runner adapters, web UI, and PR-ready delivery. Open an issue or design note before a large PR.

## License

MIT — see [LICENSE](LICENSE).
