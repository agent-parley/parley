# Auto-merge live validation (operator runbook)

The opt-in auto-merge delivery path is built and covered by tests against fakes, but it
has not yet been exercised against a live forge. This runbook walks an operator through
one controlled live validation on a scratch GitHub repository. When a run completes it,
the README's "not yet live-validated" caveat can be dropped.

Auto-merge only ever runs when a workflow template explicitly opts in; nothing here
changes default behavior (the default delivery stops at a PR-ready handoff).

## Where this must run

Run the manager in **process mode on the host** (`make run`). The delivery client
shells out to `git` and `gh`, and the manager app-container image is a single static
binary with neither tool inside — container deployments cannot complete auto-merge
today. Validation also needs the **Pi adapter**: the default `noop` adapter writes no
changes, so the commit stage produces no branch/commit and auto-merge stops by design.

## Prerequisites

- A **scratch GitHub repository** you own, low stakes, default branch `main`, with at
  least one CI check — a trivial Actions workflow is enough. Note the check's name;
  auto-merge waits for the checks you name before merging.
- `git` and `gh` available on the host.
- A GitHub token scoped to the scratch repository (classic PAT with `repo`, or a
  fine-grained token with contents + pull requests read/write). The token is sealed by
  the secrets facility; it is never stored in plain text.
- `PARLEY_SECRETS_KEK` set in the manager environment — without it, forge credentials
  stay disabled and auto-merge cannot be configured.
- Pi worker image and auth configured (see the README's Pi section).

## Steps

1. **Start the manager** against a local clone of the scratch repository:

   ```sh
   PARLEY_SECRETS_KEK=... \
   PARLEY_ADAPTER=pi \
   PARLEY_PI_AUTH_JSON=/path/to/auth.json \
   PARLEY_PI_IMAGE=localhost/parley-pi-worker:<tag> \
   PARLEY_SOURCE_REPO=/path/to/scratch-repo \
   make run
   ```

2. **Seal the forge credential.** Settings → forge credentials → add the host
   (`github.com`) and the token. The UI answers with a stored credential ID
   (`fcr_...`) — that ID is what workflow templates reference; the token itself is
   sealed at rest.

3. **Create the opt-in template.** In `/templates`, copy **Autonomous PR Delivery**
   (it is already `feature_branch` + `create_pr`, uses an agent code review with a fix
   loop, and has no human gates). In the copy's merge settings set:

   | Setting | Value |
   |---|---|
   | merge policy | `auto_merge` (also accepted: `auto`, `automerge`, `merge_when_green`, `auto_when_green`) |
   | target branch | `main` |
   | required checks | the check name(s) from your Actions workflow, comma-separated |
   | forge credential | the `fcr_...` ID from step 2 |
   | merge wait timeout | default `5m`, capped at `30m` |

4. **Submit a tiny run directly** (new-run form or `POST /runs`) with the copied
   template and a deliberately small idea — a README typo fix is ideal. Submit it
   directly on purpose: chat-authored Tasks and project default templates refuse
   auto-merge templates (the human-gate floor), so direct submission is the only
   intended entry.

5. **Watch the run story.** After the commit and PR-creation stages, the delivery
   result records the outcome.

## Reading the outcome

**Success** — the delivery result shows the push performed, the PR created (URL and
number), the named checks passed, and auto-merge completed with a merge commit SHA.
Confirm on GitHub that the PR merged into `main`.

**Held for configuration** (`needs_input` + a specific reason) — a missing or
inconsistent opt-in setting: branch policy or PR behavior not `feature_branch` /
`create_pr`, no target branch, no required checks, no credential reference, or an
invalid merge wait timeout. Fix the template setting named in the reason and re-run.
A template whose merge policy simply does not enable auto-merge is not an error — the
run completes at the normal PR-ready stop.

**Failed** — the checks did not pass within the merge wait timeout, the merge was
refused by the forge, or `git`/`gh` errored. The reason carries the detail, and the
partial state (pushed? PR created? which checks passed?) is recorded so you can see
exactly how far it got. A run with *no changes to commit* (the `noop` adapter) fails
the branch/commit gate — use Pi.

## Afterward

- Disable the path: set the copied template's merge policy back to a human/manual
  value or delete the copy. Optionally delete the forge credential under Settings.
- Clean up the scratch repository's branches and merged PRs as desired.
- On a successful validation, update the README: move auto-merge from
  "Opt-in, not yet live-validated" to live-validated status (with the container-image
  limitation noted above).
