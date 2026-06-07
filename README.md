# Parley

> **Open-source, local-first harness that takes a software idea to a sandboxed, inspectable PR-ready stop.**

[![Status](https://img.shields.io/badge/status-early%20design-orange.svg)](#status)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Namespace](https://img.shields.io/badge/namespace-agent--parley-blue.svg)](#name-and-namespace)

Parley is a **control-plane harness**, not a coding agent. It owns run state, runner coordination, sandbox setup, artifacts, events, and delivery policy; agents are the workers it dispatches.

The current repo contains a live-validated walking skeleton, not the full configurable workflow product. A manually submitted idea runs through a deterministic local path: implementation in an isolated worktree, containerized validation, a commit built from the worker snapshot, and a stop at a **PR-ready human handoff**. The end-to-end commit → PR-ready loop requires a diff-producing adapter (the **Pi** agent); the default `noop` adapter runs the path but makes no changes, so the run stops at the commit stage with *no changes to commit*. Parley does **not** push branches or open pull requests yet.

> [!warning]
> Parley is early. The walking skeleton runs, but the product surface and workflow model will keep changing. This README separates what works today from intended direction.

## Status

Walking skeleton status:

- **Built and live-validated:** Manager spawns and dials one Runner over a persistent WebSocket.
- **Built and live-validated:** deterministic `idea → implementation → validation → commit → pr_ready` path.
- **Built and live-validated:** rootless Podman sandbox provider, isolated git worktrees, SQLite + filesystem artifacts, durable events, and per-run JSONL logs.
- **Built and live-validated:** one real agent family, **Pi**, behind the runner adapter interface.
- **Built and live-validated:** embedded hypermedia web UI using Datastar + SSE, including run events, artifacts, cancellation, and runner-health/supervision surfaces.
- **Built:** optional TOML project/global settings and a local auto-queue — `POST /runs` enqueues runs that auto-dispatch to free runner slots (approval gates preserved), with a backlog cap and crash/startup recovery. Capacity equals the number of local runner slots (currently one).
- **Not yet built:** push/PR creation. The workflow stops at PR-ready metadata for a human/operator.
- **Not yet built:** auto-pickup, issue polling, agent profiles, workflow templates, project memory, semantic review verdicts, or human-stage parity.
- **No published release yet.** Expect sharp edges.

The skeleton runs a single local runner slot, so one run executes at a time; additional submitted runs are queued and auto-dispatched as the slot frees. There is no external scheduler or issue auto-pickup yet.

## What works today

The current deterministic path is:

```text
Idea intake         (manager creates the run and task contract)
  → Implementation  (Pi adapter in an isolated worktree; noop adapter by default)
  → Validation      (containerized validation command; status gate only)
  → Commit          (commit made from the post-implementation worker snapshot)
  → PR-ready stop   (branch/commit/diff metadata; no forge push, no PR)
```

The full commit → PR-ready loop requires an adapter that produces changes (the Pi agent). With the default `noop` adapter the implementation step writes nothing, so the run reaches the commit stage and stops there with *no changes to commit*.

Routing is based on structured `status` values only. There is no resident coordinator LLM, semantic verdict engine, review stage, configurable fix loop, or human approval stage in the skeleton.

The web UI lets you submit a run, watch stage/event progress over SSE, inspect artifacts and diffs, cancel a run, and see runner health. Runs and artifacts are persisted under `.parley-data` by default.

Submitted runs are enqueued and auto-dispatched to the local runner slot as it frees; the UI shows queue depth and the effective policy. Optional TOML settings (`.parley/config.toml`) select defaults such as the queue policy (`auto_when_ready`, `max_concurrent`, `backlog_cap`); settings change which defaults apply, never the deterministic routing.

## Intended direction

The long-term product direction is still a configurable workflow harness:

- editable workflow templates and run snapshots
- agent-or-human stages for planning, implementation, review, validation, commit, PR creation, and memory update
- semantic review verdicts and fix loops
- configurable delivery policy, including real push/PR creation
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
- rootless Podman (required for submitted-run validation and the live test targets)

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
- not a generic chatbot UI
- not a black-box "fully autonomous engineer"
- not a hosted cloud service
- not currently a tool that pushes branches or opens pull requests for you

Parley is a workflow layer that makes agentic software work **structured, inspectable, and governable**.

## Name and namespace

**Parley** — a conference between parties negotiating terms. The name fits: plans are reviewed before work begins, and work is reviewed before it ships. Agents shouldn't just act — they should parley.

The product is **Parley**; the project uses **`agent-parley`** for URLs and package namespaces (e.g. `github.com/agent-parley/parley`).

## Contributing

Parley is early and welcomes focused contributions to the current skeleton and its next layers: workflow depth, sandbox safety, event/artifact contracts, runner adapters, web UI, and PR-ready delivery. Open an issue or design note before a large PR.

## License

MIT — see [LICENSE](LICENSE).
