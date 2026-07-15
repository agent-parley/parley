# Parley testing guide

Welcome — you're testing **Parley**, a harness that takes a software idea to a
sandboxed, inspectable, PR-ready stop. You talk to a project in chat; when you ask, the
conversation turns your idea into a scoped **Task** and runs it through a delivery
loop — planning, implementation in an isolated copy of the repository, containerized
validation, review — pausing for your approval along the way. Nothing lands on a real
branch: runs stop at a "PR-ready" state you can inspect.

You cannot break anything that matters. Agents work in isolated, sandboxed copies;
the worst case is a failed run, which is itself useful feedback.

## Access

The test instance runs at: **`<INSTANCE-URL>`** (shared privately with you).

The instance is **shared** — there are no user accounts yet. Everyone sees the same
projects, conversations, runs, and settings, and anyone can act on any of them. Two
courtesies keep this workable:

- **Sign your review verdicts** — put your name in the reviewer field so we can follow
  who decided what.
- **Don't cancel or resume runs you didn't start** unless a run is clearly stuck and
  its owner isn't around.

## The tour — things to actually try

Work through as many of these as you like, in roughly this order. Break things; that's
the point.

1. **Chat with a project.** Open a project and ask about its repository — structure,
   a file, "what does X do". Replies are grounded in the real repo.
2. **Turn an idea into a Task.** Ask the chat to create a task for a small change
   ("add a --version flag", "fix the typo in the install docs"). It drafts a scoped
   brief; the harness runs it and **stops at plan review** for you.
3. **Review the plan.** Open the run, read the plan, and give a verdict: pass,
   request changes (say what), or block. Try requesting changes at least once to see
   the loop react.
4. **Watch the run story.** Follow the stage timeline live — events, artifacts, the
   diff when implementation finishes. Open the artifacts. Is it clear what happened
   and why?
5. **Act on review gates.** Later review stages show you code changes or validation
   evidence. Judge them like a real reviewer.
6. **Pause and rearrange a running workflow.** On a running run, press pause — it
   suspends at the next stage boundary. Edit the remaining stages (reorder, add,
   remove), then resume. Already-started stages are locked; that's intentional.
7. **Re-run from a stage.** On a finished run, pick a stage and re-run from there —
   e.g. re-run implementation after a plan tweak.
8. **Cancel a run.** Start something and cancel it mid-flight. Does the state make
   sense afterwards?
9. **Edit a workflow template.** In Templates, copy an existing template and change
   it — toggle optional stages, reorder, adjust per-stage settings or instructions.
   Start a run with your copy. Invalid structures should be rejected with a clear
   message; if you can save something broken, that's a bug we want.
10. **Customize one run's workflow.** On the new-run form, opt in to edit the workflow
    for just that run without touching the template.
11. **Approve memory.** If a workflow includes a memory-update stage with human
    approval, the run pauses with learning candidates — approve, edit, reject, or
    defer each. Approved entries quietly inform later runs on the project.
12. **Check notifications and settings.** Watch the in-app notifications as your runs
    progress; skim Settings to see what an operator can configure.

## What to expect (not bugs)

- **One run executes at a time.** Everyone shares a single worker, so submitted runs
  queue and start in order; the queue view shows depth. A queued run under load is
  normal. Chat stays responsive even while runs execute.
- **A stuck chat turn recovers itself.** If a reply hangs, after ~15 minutes it is
  cancelled with a message saying so — the conversation is intact; just send your
  message again. (If you see this often, tell us.)
- **Deliveries stop at PR-ready.** Runs do not push branches or open pull requests;
  you inspect the would-be result. (An opt-in auto-merge path exists but is not part
  of this test unless the operator says otherwise.)
- **Fresh-start replies after long gaps** are expected — the agent may take a moment
  longer on the first message after a while.

## Out of scope for this test

User accounts and per-person permissions; more than one run executing concurrently;
automatic issue pickup; agents other than the current one. Feedback on these is
welcome but they're known and deliberate.

## Reporting what you find

<FEEDBACK-CHANNEL — how and where to report, filled in before testing starts>

A useful report has four parts: what you did, what you expected, what actually
happened, and where to look (run link or ID, conversation, screenshot). Rough notes
are fine — a two-line report beats an unreported bug.

We especially want to hear about: places you felt lost or couldn't tell what the
system was doing; a verdict/pause/cancel that didn't do what you meant; anything that
felt slow; and anywhere the words on screen didn't match what happened.

Thank you — every confused moment you report makes this better.
