# Parley

> **Open-source, local-first harness that takes a software idea to an inspectable pull request through a configurable agent workflow.**

[![Status](https://img.shields.io/badge/status-early%20design-orange.svg)](#status)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Namespace](https://img.shields.io/badge/namespace-agent--parley-blue.svg)](#name-and-namespace)

Parley turns a software idea into a branch, commit set, and PR-ready result by running it through a **configurable workflow** of focused, sandboxed agents — planning, implementing, reviewing, validating — and handing you the result to approve.

You stay in control of the *shape* of the work (which stages run, how strict review is, where humans step in) without hand-running each step.

> [!warning]
> Parley is early. This README describes the intended direction; details will change as implementation stabilizes. Today the system targets **local-first, dry-run-by-default** operation, with guarded local execution still experimental.

## What Parley is

Parley is a **control-plane harness**, not a coding agent. It owns the workflow, the run state, context assembly, runner coordination, artifacts and events, and delivery policy. The agents are the workers it dispatches.

The harness is **deterministic and configurable**: you (and your project defaults) decide the workflow; the agents supply the cognition inside each step. Parley does not let an agent improvise the control flow.

```text
Idea
  → Plan               (draft + review the plan)
  → Implement          (isolated, sandboxed work)
  → Review / Validate  (fresh-context review; run tests/checks)
  → (loop back to Implement when changes are requested)
  → Commit → Open PR
```

Every stage is a unit in an editable workflow graph. The human's default touchpoint is **approving the resulting PR** — but any stage's actor can be an agent *or* a human, and you can place an approval gate anywhere.

## How it works

- **Workflow templates & snapshots.** Start from a project-default template, adjust it for a run if you want, then it freezes into a snapshot for a reproducible run.
- **Stages with agent-or-human actors.** Idea intake, Implementation, Review, Validation, Commit, PR creation, Memory update — each performed by an assigned **agent profile** or by a human.
- **Harness-coordinated, not a free-for-all.** Agents are isolated and don't talk to each other directly; the harness dispatches each one a curated brief and reads back a structured result. Runs stay reproducible and each agent's context stays focused.
- **Fresh-context review.** Review runs with its own context and judges a target (the plan, the diff, validation evidence) instead of inheriting the implementer's assumptions.
- **Configurable delivery policy.** Branch + PR is the recommended default; direct integration and human approval gates are *configurable choices*, not mandatory ceremony.

## Sandboxed by design

Isolation is a core feature, not an afterthought. Parley treats agents as untrusted automation and runs them inside isolated containers.

- **You provide one working runtime; Parley owns the rest.** Point Parley at a container runtime you provide — **rootless Podman** today, with **Docker** and **remote runners you operate** planned. Parley owns the isolation recipe, mounts, and lifecycle, so you never hand-configure container internals.
- **Filesystem & credential isolation.** No host home mount; edit work runs in per-task worktrees, not your primary checkout; credentials are brokered, never handed raw to workers.
- **Secure by default, your machine is yours.** Sandboxing is on by default; a clearly-labeled "no sandbox" option exists for users who knowingly want it — never as a silent fallback.
- **Local-first.** Parley runs on infrastructure *you* control — your own machine, or a remote host you operate. It is not a hosted service.

## Pluggable, adapter-ready

- **Agent profiles.** Pi is the first supported agent family, behind a generic agent interface so other families can follow. Core models stay vendor-neutral.
- **Context engineering at the boundary.** Parley curates what each agent is given and reads structured output back; agents manage their own working context. Focused context, more reliable agents.

## What Parley is not

- not a claim that multiple agents always beat a single agent
- not a replacement for engineering judgment
- not a generic chatbot UI
- not a black-box "fully autonomous engineer"
- not a hosted cloud service

Parley is a workflow layer that makes agentic software work **structured, inspectable, and governable**.

## Status

Honest current state:

- **Local-first**, intended to run as a local process with a responsive web UI (not a TUI).
- **Dry-run by default.** Guarded local execution (including an experimental local Pi mode) is opt-in and still maturing.
- **Pi-first**, with the agent interface kept generic for future families.
- No published release yet. Remote runners, real sync/handoff, and broad multi-runtime support are **planned, not done.**

## Name and namespace

**Parley** — a conference between parties negotiating terms. The name fits: plans are reviewed before work begins, and work is reviewed before it ships. Agents shouldn't just act — they should parley.

The product is **Parley**; the project uses **`agent-parley`** for URLs and package namespaces (e.g. `github.com/agent-parley/parley`).

## Contributing

Parley is early and welcomes focused contributions — workflow/stage design, execution-isolation safety, event/artifact formats, agent-profile prototypes, and UI sketches. Open an issue or design note before a large PR.

## License

MIT — see [LICENSE](LICENSE).
