package app

const layoutTemplate = `{{define "layout"}}
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{.Title}} · Parley</title>
  <link rel="stylesheet" href="/static/app.css">
</head>
<body>
  <header class="topbar">
    <a class="brand" href="/projects">Parley</a>
    <nav class="topnav" aria-label="Primary"><a href="/projects">Projects</a><a href="/runners">Runners</a></nav>
    <span class="badge">prototype</span>
  </header>
  <main class="container">
    {{template "content" .}}
  </main>
</body>
</html>
{{end}}
`

const projectsTemplate = `{{define "content"}}
<section class="hero">
  <div>
    <h1>Projects</h1>
    <p>Local runner is online. Remote runners and sync/handoff are modeled, not active yet.</p>
  </div>
  <div class="card compact">
    <strong>{{.Executor.Name}}</strong>
    <span class="status">{{.Executor.Status}}</span>
    <small>{{.Executor.ResourceClass}} · {{.Executor.Endpoint}}</small>
  </div>
</section>
{{if .Error}}<p class="error" role="alert">{{.Error}}</p>{{end}}
<section class="grid two">
  <form method="post" action="/projects" class="card form">
    <input type="hidden" name="csrf_token" value="{{.CSRFToken}}">
    <h2>Add project</h2>
    <p class="hint">Register a project that exists on this runner. Containerized installs need explicit repo mounts.</p>
    <p class="hint">Optional <code>.parley/settings.toml</code> files may provide non-secret advisory defaults only; secrets, Docker execution, sync/handoff, and automatic queueing stay disabled.</p>
    <label>Project name <input name="name" placeholder="My project"></label>
    <label>Project description <textarea name="description" rows="4" placeholder="What this project does, important conventions, and what agents should know before working on it."></textarea></label>
    <label>Project folder on local runner <input name="repo_path" placeholder="/path/to/project" required></label>
    <label>Default branch <input name="default_branch" value="main"></label>
    <button type="submit">Register project</button>
  </form>
  <div class="card">
    <h2>Registered projects</h2>
    {{if .Projects}}
      <ul class="list">{{range .Projects}}<li><a href="/projects/{{.ID}}">{{.Name}}</a><small>{{.RepoPath}}</small></li>{{end}}</ul>
    {{else}}
      <p>No projects yet.</p>
    {{end}}
    <p class="muted">Data root: {{.DataRoot}}</p>
    <div class="callout">Next: richer project sources, repo availability checks per runner, and project-level agent/workflow settings.</div>
  </div>
</section>
{{end}}
`

const projectTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / {{.Project.Name}}</nav>
<section class="hero">
  <div>
    <h1>{{.Project.Name}}</h1>
    <p>{{.Project.Description}}</p>
    <small>{{.Project.RepoPath}}</small>
  </div>
  <div class="hero-actions">
    <a class="button" href="/projects/{{.Project.ID}}/planner">Open planner</a>
    <a class="button secondary-link" href="/projects/{{.Project.ID}}/tasks/new">Manual task</a>
    <a class="button secondary-link" href="/projects/{{.Project.ID}}/settings">Settings</a>
    <a class="button secondary-link" href="/runners?project={{.Project.ID}}">Runners</a>
  </div>
</section>
<section class="grid three">
  <div class="card">
    <h2>Project context</h2>
    <p>{{if .Project.AgentContext}}{{.Project.AgentContext}}{{else}}{{if .Project.Description}}{{.Project.Description}}{{else}}No project context yet. Add agent-facing notes in settings.{{end}}{{end}}</p>
  </div>
  <div class="card">
    <h2>Agent profile</h2>
    <dl><dt>Default</dt><dd>{{agentProfileLabel .Project.DefaultAgentProfile}}</dd><dt>Runner</dt><dd>{{.Executor.Name}}</dd><dt>Mode</dt><dd>Review-gated</dd></dl>
    <p class="hint">Settings now store project-level agent and runner defaults.</p>
  </div>
  <div class="card">
    <h2>Workflow</h2>
    <dl><dt>Review loops</dt><dd>{{.Project.ReviewLoopCount}}</dd><dt>Retries</dt><dd>{{.Project.RetryCount}}</dd><dt>Handoff</dt><dd>Git + manifest mock</dd></dl>
  </div>
</section>
<section class="card">
  <div class="section-head">
    <div><h2>Workflow templates</h2><p class="hint">Editable project defaults for planning. The current execution seam uses a fixed chain; dry-run is default and experimental local-pi is process-level opt-in.</p></div>
    <a class="button secondary-link" href="/projects/{{.Project.ID}}/settings">Project settings</a>
  </div>
  <div class="template-grid">
    {{range .Templates}}
      <a class="template-card {{if eq .ID $.Project.DefaultWorkflowTemplateID}}selected{{end}}" href="/projects/{{$.Project.ID}}/templates/{{.ID}}"><strong>{{.Name}}</strong><span>{{.Summary}}</span><small>{{.UseCase}}</small></a>
    {{end}}
  </div>
</section>
<section class="grid two">
  <div class="card">
    <h2>Recent planner sessions</h2>
    {{if .Sessions}}<ul class="list">{{range .Sessions}}<li><a href="/projects/{{$.Project.ID}}/planner/{{.ID}}">{{.Title}}</a><span class="status">{{.Status}}</span><small>{{since .UpdatedAt}}</small></li>{{end}}</ul>{{else}}<p>No planner sessions yet.</p>{{end}}
  </div>
  <div class="card">
    <h2>Runs</h2>
    {{if .Runs}}<ul class="list">{{range .Runs}}<li><a href="/runs/{{.ID}}">{{.Title}}</a><span class="status">{{statusLabel .Status}}</span><small>{{since .CreatedAt}}</small></li>{{end}}</ul>{{else}}<p>No runs yet. Create a task to start the first prototype flow.</p>{{end}}
  </div>
</section>
{{end}}
`

const projectSettingsTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / <a href="/projects/{{.Project.ID}}">{{.Project.Name}}</a> / Settings</nav>
<div class="page-actions"><a class="back-link" href="/projects/{{.Project.ID}}">← Back to project</a></div>
<form method="post" action="/projects/{{.Project.ID}}/settings" class="card form wide">
  <input type="hidden" name="csrf_token" value="{{.CSRFToken}}">
  <h1>Project settings</h1>
  <p class="hint">Prototype settings for agent context, local runner assignment records, and workflow behavior. Dry-run is the default execution path; experimental local-pi can be enabled from process config. Remote runner selections are labels only until real runner APIs exist.</p>
  {{with .Project.RepoSettings}}
    <div class="callout"><strong>Repo-local defaults loaded</strong><br><small>{{.Path}}</small><dl>{{if .DefaultAgentProfile}}<dt>Agent profile</dt><dd>{{agentProfileLabel .DefaultAgentProfile}}</dd>{{end}}{{if .WorkflowTemplate}}<dt>Workflow template</dt><dd>{{.WorkflowTemplate}}</dd>{{end}}{{if .QueuePolicy}}<dt>Queue policy</dt><dd>{{.QueuePolicy}} initial default</dd>{{end}}{{if .RuntimeProvider}}<dt>Runtime provider</dt><dd>{{.RuntimeProvider}}</dd>{{end}}{{if .ReviewProfiles}}<dt>Review profiles</dt><dd>{{range $i, $profile := .ReviewProfiles}}{{if $i}}, {{end}}{{agentProfileLabel $profile}}{{end}}</dd>{{end}}</dl><p class="hint">This file is non-secret and provides registration defaults only. It does not enable Docker execution, remote runners, sync/handoff, secret loading, scheduled polling, or approval bypass; approval gates still apply.</p>{{if .Warnings}}<ul class="list compact-list">{{range .Warnings}}<li>{{.}}</li>{{end}}</ul>{{end}}</div>
  {{end}}
  <label>Project description <textarea name="description" rows="3">{{.Project.Description}}</textarea></label>
  <label>Agent context / project notes <textarea name="agent_context" rows="7" placeholder="Conventions, architecture notes, preferred commands, important docs, risk areas.">{{.Project.AgentContext}}</textarea></label>
  <section class="settings-grid settings-grid-primary">
    <label>Default runner
      <select name="default_runner">{{range .Runners}}<option value="{{.ID}}" {{if eq .ID $.ExecutionRunnerID}}selected{{end}} {{if ne .Kind (index $.K "ExecutorKindLocal")}}disabled{{end}}>{{.Name}} · {{if eq .Kind (index $.K "ExecutorKindLocal")}}local execution target{{else}}registry/handoff preview only{{end}}</option>{{end}}</select>
    </label>
    <label>Agent profile
      <select name="agent_profile">
        {{range .AgentProfiles}}<option value="{{.}}" {{if eq $.Project.DefaultAgentProfile .}}selected{{end}}>{{agentProfileLabel .}}</option>{{end}}
      </select>
      <small>Only active Pi worker profiles are selectable. Codex, Claude Code, Gemini, and Copilot registry entries are planned metadata only.</small>
    </label>
    <label>Default workflow
      <select name="workflow_template">{{range .Templates}}<option value="{{.ID}}" {{if eq .ID $.Project.DefaultWorkflowTemplateID}}selected{{end}}>{{.Name}}</option>{{end}}</select>
    </label>
  </section>
  <section class="settings-grid settings-grid-compact">
    <label>Queue policy
      <select name="queue_policy">{{range .QueuePolicies}}<option value="{{.}}" {{if eq $.Project.QueuePolicy .}}selected{{end}}>{{if eq . (index $.K "QueuePolicyAutoWhenReady")}}Auto-queue when ready{{else}}Manual{{end}}</option>{{end}}</select>
      <small>Manual is default. Auto-queue only creates a local durable queued attempt after explicit task approval or fix-ready gates; it is not scheduling, polling, or remote execution.</small>
    </label>
    <label>Review loops <input name="review_loops" type="number" min="0" max="9" value="{{.Project.ReviewLoopCount}}"></label>
    <label>Retry count <input name="retry_count" type="number" min="0" max="9" value="{{.Project.RetryCount}}"></label>
  </section>
  <button type="submit">Save settings</button>
</form>
{{end}}
`

const workflowTemplateTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / <a href="/projects/{{.Project.ID}}">{{.Project.Name}}</a> / Template</nav>
<div class="page-actions"><a class="back-link" href="/projects/{{.Project.ID}}">← Back to project</a></div>
<form method="post" action="/projects/{{.Project.ID}}/templates/{{.Template.ID}}" class="card form wide">
  <input type="hidden" name="csrf_token" value="{{.CSRFToken}}">
  <h1>Edit workflow template</h1>
  <p class="hint">Prototype editor. These steps shape planning and review UX now; real execution will interpret templates after the workflow engine is wired.</p>
  <label>Name <input name="name" value="{{.Template.Name}}"></label>
  <label>Summary <input name="summary" value="{{.Template.Summary}}"></label>
  <label>Use case <input name="use_case" value="{{.Template.UseCase}}"></label>
  <section class="settings-grid">
    <label>Review loops <input name="review_loops" type="number" min="0" max="9" value="{{.Template.ReviewLoops}}"></label>
    <label>Retry count <input name="retry_count" type="number" min="0" max="9" value="{{.Template.RetryCount}}"></label>
    <label class="checkbox"><input type="checkbox" name="make_default"> Make project default</label>
  </section>
  <h2>Steps</h2>
  <div class="step-editor">
    {{range $i, $step := .Template.Steps}}
      <article class="step-card">
        <strong>Step {{$i}}</strong>
        <label>Name <input name="step_{{$i}}_name" value="{{$step.Name}}"></label>
        <label>Role <input name="step_{{$i}}_role" value="{{$step.Role}}"></label>
        <label>Description <textarea name="step_{{$i}}_description" rows="3">{{$step.Description}}</textarea></label>
        <label class="checkbox"><input type="checkbox" name="step_{{$i}}_optional" {{if $step.Optional}}checked{{end}}> Optional step</label>
      </article>
    {{end}}
  </div>
  <button type="submit">Save template</button>
</form>
{{end}}
`

const plannerTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / <a href="/projects/{{.Project.ID}}">{{.Project.Name}}</a> / Planner</nav>
<div class="page-actions"><a class="back-link" href="/projects/{{.Project.ID}}">← Back to project</a></div>
<section class="hero">
  <div>
    <h1>Planner</h1>
    <p>Start a persistent planning thread, generate a planner/critic draft when needed, then approve the visible draft into a task plan.</p>
  </div>
  <span class="status large">{{.Executor.Name}}</span>
</section>
{{if .Error}}<p class="error" role="alert">{{.Error}}</p>{{end}}
<section class="grid two planner-shell">
  <div class="card chat-panel">
    <div class="bubble agent"><strong>Planner</strong><span>Describe the goal. I will draft assumptions, risks, and a workflow preview before execution starts. Dry-run generates a local preview; experimental local-pi runs real planner/critic agents before approval.</span></div>
    <form method="post" action="/projects/{{.Project.ID}}/planner" class="prompt-box">
      <input type="hidden" name="csrf_token" value="{{.CSRFToken}}">
      <label>What do you want to accomplish?
        <textarea name="prompt" rows="7" placeholder="Example: add account settings, keep security tight, use fresh review, and do not touch migrations yet"></textarea>
      </label>
      <button type="submit">Start planning session</button>
    </form>
  </div>
  <div class="card">
    <h2>Recent sessions</h2>
    {{if .Sessions}}<ul class="list">{{range .Sessions}}<li><a href="/projects/{{$.Project.ID}}/planner/{{.ID}}">{{.Title}}</a><span class="status">{{.Status}}</span><small>{{since .UpdatedAt}}</small></li>{{end}}</ul>{{else}}<p>No sessions yet.</p>{{end}}
  </div>
</section>
{{end}}
`

const plannerGenerationActivityTemplate = `{{define "planner-generation-activity"}}
<div id="planner-generation-activity" class="planner-activity" data-generation-running="{{if .GenerationRunning}}true{{else}}false{{end}}" role="status" aria-live="polite" aria-atomic="false">
  <div class="section-head"><div><h3>Generation progress</h3><p class="hint">Durable planner/critic activity before task approval. Draft updates here never create or queue a task until you approve them.</p></div>{{if .GenerationRunning}}<span class="status">running</span>{{end}}</div>
  {{if .GenerationEvents}}
    <ul class="events planner-events">{{range .GenerationEvents}}<li><strong>{{plannerGenerationEventLabel .Type}}</strong>{{if .Summary}}<span>{{.Summary}}</span>{{end}}<small>{{since .CreatedAt}} · gen {{printf "%.12s" .GenerationID}}{{if .Sequence}} · step {{.Sequence}}{{end}}</small></li>{{end}}</ul>
  {{else}}
    <p class="hint">No planner/critic generation activity has been captured yet.</p>
  {{end}}
</div>
{{end}}
`

const plannerSessionTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / <a href="/projects/{{.Project.ID}}">{{.Project.Name}}</a> / <a href="/projects/{{.Project.ID}}/planner">Planner</a> / Session</nav>
<div class="page-actions"><a class="back-link" href="/projects/{{.Project.ID}}">← Back to project</a><a class="back-link secondary" href="/projects/{{.Project.ID}}/planner">← Back to planner</a></div>
<section class="hero">
  <div><h1>{{.Session.Title}}</h1><p>{{.Session.Prompt}}</p></div>
  <span class="status large">{{.Session.Status}}</span>
</section>
<section class="grid two planner-shell">
  <div class="card chat-panel">
    <h2>Planning thread</h2>
    {{range .Messages}}<div class="bubble {{.Role}}"><strong>{{.Role}}</strong><span>{{.Body}}</span><small>{{since .CreatedAt}}</small></div>{{end}}
    {{if eq .Session.Status (index .K "PlannerStatusPlanning")}}
    <form method="post" action="/projects/{{.Project.ID}}/planner/{{.Session.ID}}/messages" class="prompt-box">
      <input type="hidden" name="csrf_token" value="{{.CSRFToken}}">
      <label>Reply or record a note <textarea name="message" rows="4" placeholder="Add constraints, answer planner questions, or note scope changes. Regenerate the planner/critic draft when it should change."></textarea></label>
      <button type="submit">Send reply</button>
    </form>
    {{end}}
  </div>
  <div class="card draft-panel">
    <h2>Draft plan</h2>
    {{if .Session.AgentSummary}}<div class="callout"><strong>Planner/critic draft: {{.Session.AgentStatus}}</strong><p>{{.Session.AgentSummary}}</p><small>Mode: {{.Session.AgentMode}}{{if .Session.PlannerProfile}} · Planner: {{agentProfileLabel .Session.PlannerProfile}}{{end}}{{if .Session.CriticProfile}} · Critic: {{agentProfileLabel .Session.CriticProfile}}{{end}}</small></div>{{else}}<p class="hint">No planner/critic draft has been generated for this session yet. The draft below is still approval-gated and can be approved manually.</p>{{end}}
    {{if .GenerationRunning}}<div class="callout"><strong>Generation running</strong><p>Planner/critic generation is running asynchronously. Progress updates below; no task will be created until you approve the draft.</p></div>{{end}}
    {{if or .GenerationRunning .GenerationEvents}}{{template "planner-generation-activity" .}}{{end}}
    <dl><dt>Title</dt><dd>{{.Session.DraftTitle}}</dd><dt>Objective</dt><dd>{{.Session.DraftObjective}}</dd><dt>Focus</dt><dd>{{.Session.DraftFocus}}</dd><dt>Boundaries</dt><dd>{{.Session.DraftBoundaries}}</dd><dt>Done when</dt><dd>{{.Session.DraftDoneWhen}}</dd></dl>
    <h3>Assumptions</h3><ul class="list compact-list">{{range .Session.Assumptions}}<li>{{.}}</li>{{end}}</ul>
    <h3>Risks</h3><ul class="list compact-list">{{range .Session.Risks}}<li>{{.}}</li>{{end}}</ul>
    <h3>Task graph preview</h3>
    <div class="mini-chain">{{range .Session.GraphPreview}}<span>{{.}}</span>{{end}}</div>
    {{if and (eq .Session.Status (index .K "PlannerStatusPlanning")) (eq .ExecutionMode (index .K "ExecutionModeLocalPi"))}}<div class="callout"><strong>Experimental local-pi mode</strong><p>Generating a planner/critic draft runs real local Pi agents before approval. Task creation and queueing still require later approvals.</p></div>{{end}}
    {{if eq .Session.Status (index .K "PlannerStatusPlanning")}}
    <div class="actions inline-actions">
      {{if .GenerationRunning}}<button class="secondary" type="button" disabled aria-disabled="true">Planner/critic generation running</button>{{else}}<form method="post" action="/projects/{{.Project.ID}}/planner/{{.Session.ID}}/run-agents"><input type="hidden" name="csrf_token" value="{{.CSRFToken}}"><button class="secondary" type="submit">Generate planner/critic draft</button></form>{{end}}
      <form method="post" action="/projects/{{.Project.ID}}/planner/{{.Session.ID}}/revise"><input type="hidden" name="csrf_token" value="{{.CSRFToken}}"><button class="secondary" type="submit">Record revision note</button></form>
      <form method="post" action="/projects/{{.Project.ID}}/planner/{{.Session.ID}}/approve"><input type="hidden" name="csrf_token" value="{{.CSRFToken}}"><button type="submit">Approve final plan</button></form>
      <p class="hint">Planner approval creates a draft task; task execution still needs the separate queue approval on the task page.</p>
      <form method="post" action="/projects/{{.Project.ID}}/planner/{{.Session.ID}}/dismiss"><input type="hidden" name="csrf_token" value="{{.CSRFToken}}"><button class="secondary" type="submit">Dismiss</button></form>
    </div>
    {{else if .Session.ApprovedTaskID}}
      <a class="button" href="/tasks/{{.Session.ApprovedTaskID}}">Open approved task</a>
    {{end}}
    {{if .GenerationViews}}
    <h3>Planner/critic generations</h3>
    <p class="hint">Internal planner/critic inputs and raw runtime logs are grouped by generation here only; they are not task artifacts or handoff content. Common secret-like values are heuristically redacted, files are capped, and diagnostics are retained for the latest 5 generations per session.</p>
    <div class="generation-history">{{range .GenerationViews}}<article class="attempt-card"><div><strong>{{.Generation.Status}}</strong><span class="status">{{.Generation.Mode}}</span></div><small>{{if .Generation.PlannerProfile}}{{agentProfileLabel .Generation.PlannerProfile}}{{end}}{{if .Generation.CriticProfile}} / {{agentProfileLabel .Generation.CriticProfile}}{{end}} · rev {{.Generation.PlannerRevision}} · {{since .Generation.UpdatedAt}}</small>{{if .Generation.Summary}}<p>{{.Generation.Summary}}</p>{{end}}{{if .Diagnostics}}<ul class="artifact-grid diagnostics-grid">{{range .Diagnostics}}<li><a href="/projects/{{$.Project.ID}}/planner/{{$.Session.ID}}/diagnostics/{{.ID}}">{{artifactLabel .Kind}}</a><small>{{artifactName .Path}} · {{.SizeBytes}} bytes</small></li>{{end}}</ul>{{else}}<p class="hint">No internal diagnostics captured for this generation.</p>{{end}}</article>{{end}}</div>
    {{end}}
  </div>
</section>
{{if .GenerationRunning}}<script>(function(){var url="/projects/{{.Project.ID}}/planner/{{.Session.ID}}/activity";var stopped=false;function tick(){if(stopped){return;}fetch(url,{headers:{"Accept":"text/html"},cache:"no-store"}).then(function(response){if(!response.ok){throw new Error("activity unavailable");}return response.text();}).then(function(html){var current=document.getElementById("planner-generation-activity");var wrapper=document.createElement("div");wrapper.innerHTML=html.trim();var next=wrapper.firstElementChild;if(current&&next){current.replaceWith(next);if(next.getAttribute("data-generation-running")!=="true"){stopped=true;window.location.reload();return;}}window.setTimeout(tick,2000);}).catch(function(){var current=document.getElementById("planner-generation-activity");if(current&&!document.getElementById("planner-generation-retry")){var note=document.createElement("p");note.id="planner-generation-retry";note.className="hint";note.textContent="Progress updates paused; retrying…";current.appendChild(note);}window.setTimeout(tick,4000);});}window.setTimeout(tick,2000);})();</script>{{end}}
{{end}}
`

const newTaskTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / <a href="/projects/{{.Project.ID}}">{{.Project.Name}}</a> / New task</nav>
<div class="page-actions"><a class="back-link" href="/projects/{{.Project.ID}}">← Back to project</a></div>
<form method="post" action="/projects/{{.Project.ID}}/tasks/new" class="card form wide">
  <input type="hidden" name="csrf_token" value="{{.CSRFToken}}">
  <h1>Draft task plan</h1>
  {{if .Error}}<p class="error" role="alert">{{.Error}}</p>{{end}}
  <label>Title <input name="title" required></label>
  <p class="hint">This manual form stays available for precision. Planner sessions are now the preferred prototype flow.</p>
  <label>Objective <textarea name="objective" rows="5" required placeholder="Describe the outcome you want."></textarea></label>
  <label>Focus areas / useful context <textarea name="focus" rows="3" placeholder="Optional: files, folders, docs, or context that may be useful. Leave blank if unsure."></textarea></label>
  <label>Excluded paths / boundaries <textarea name="excluded_paths" rows="3" placeholder="Optional: files or folders agents must not touch, e.g. secrets, migrations, generated assets."></textarea></label>
  <label>Done when <textarea name="acceptance" rows="4" placeholder="What would make this task feel done?"></textarea></label>
  <button type="submit">Create draft plan</button>
</form>
{{end}}
`

const runTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / <a href="/projects/{{.Project.ID}}">{{.Project.Name}}</a> / Run</nav>
<div class="page-actions"><a class="back-link" href="/projects/{{.Project.ID}}">← Back to project</a></div>
<section class="hero">
  <div><h1>{{.Run.Title}}</h1><p>{{.Run.Goal}}</p></div>
  <span class="status large">{{statusLabel .Run.Status}}</span>
</section>
<section class="grid two">
  <div class="card"><h2>Tasks</h2><ul class="list">{{range .Tasks}}<li><a href="/tasks/{{.ID}}">{{.Title}}</a><span class="status">{{statusLabel .Status}}</span></li>{{end}}</ul></div>
  <div class="card"><h2>Activity</h2><ul class="events">{{range .Events}}<li><strong>{{eventLabel .Type}}</strong>{{with eventDetail .}}<span>{{.}}</span>{{end}}</li>{{end}}</ul></div>
</section>
{{end}}
`

const taskActivityTemplate = `{{define "task-activity"}}
<div id="task-activity" class="task-activity" data-task-active="{{if .TaskActivityActive}}true{{else}}false{{end}}" role="status" aria-live="polite" aria-atomic="false">
  <h2>Activity</h2>
  {{if .Events}}<ul class="events">{{range .Events}}<li><strong>{{eventLabel .Type}}</strong>{{with eventDetail .}}<span>{{.}}</span>{{end}}<small>{{with eventMeta .}}{{.}} · {{end}}{{since .Timestamp}}</small></li>{{end}}</ul>{{else}}<p class="hint">No task activity has been recorded yet.</p>{{end}}
</div>
{{end}}
`

const taskTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / <a href="/projects/{{.Project.ID}}">{{.Project.Name}}</a> / <a href="/runs/{{.Run.ID}}">Run</a> / Task</nav>
<div class="page-actions"><a class="back-link" href="/projects/{{.Project.ID}}">← Back to project</a><a class="back-link secondary" href="/runs/{{.Run.ID}}">← Back to run</a></div>
{{if .Error}}<section class="card"><p class="error">{{.Error}}</p></section>{{end}}
<section class="hero">
  <div><h1>{{.Task.Title}}</h1><p>{{.Task.Objective}}</p></div>
  <span class="status large">{{statusLabel .Task.Status}}</span>
</section>
{{if eq .Task.Status (index .K "TaskStatusQueued")}}<section class="actions card"><div><strong>Attempt queued</strong><p class="hint">This attempt is persisted and will run when the local dispatcher has capacity.</p></div></section>{{end}}
{{if or (eq .Task.Status (index .K "TaskStatusDraft")) (eq .Task.Status (index .K "TaskStatusAwaitingReview")) (eq .Task.Status (index .K "TaskStatusNeedsFix")) (eq .Task.Status (index .K "TaskStatusFailed"))}}
<section class="actions card">
  {{if and (eq .ExecutionMode (index .K "ExecutionModeLocalPi")) (or (eq .Task.Status (index .K "TaskStatusDraft")) (eq .Task.Status (index .K "TaskStatusNeedsFix")))}}<div class="callout"><strong>Experimental local-pi mode</strong><p>Approving or queueing a fix runs real local worker/reviewer agents on this machine before the final review gate.</p></div>{{end}}
  {{if eq .Task.Status (index .K "TaskStatusDraft")}}<div><strong>Draft ready</strong><p class="hint">{{if eq .Project.QueuePolicy (index .K "QueuePolicyAutoWhenReady")}}Auto-queue is enabled: approving this task queues the first attempt through the local dispatcher.{{else}}Approve to queue the first worker/reviewer loop.{{end}} Dry-run is default unless experimental local-pi is enabled.</p></div><form method="post" action="/tasks/{{.Task.ID}}/approve"><input type="hidden" name="csrf_token" value="{{.CSRFToken}}"><button type="submit">Approve and queue attempt</button></form>{{end}}
  {{if eq .Task.Status (index .K "TaskStatusAwaitingReview")}}<div><strong>Waiting for review</strong><p class="hint">The reviewer step finished. Choose the next gate action.</p></div><form method="post" action="/tasks/{{.Task.ID}}/accept"><input type="hidden" name="csrf_token" value="{{.CSRFToken}}"><button type="submit">Accept task</button></form><form method="post" action="/tasks/{{.Task.ID}}/request-fix"><input type="hidden" name="csrf_token" value="{{.CSRFToken}}"><button class="secondary" type="submit">Request fix</button></form><a class="button secondary-link" href="/tasks/{{.Task.ID}}/handoff">Preview mock runner handoff</a>{{end}}
  {{if eq .Task.Status (index .K "TaskStatusNeedsFix")}}<div><strong>Fix requested</strong><p class="hint">{{if eq .Project.QueuePolicy (index .K "QueuePolicyAutoWhenReady")}}Auto-queue is enabled; if the automatic queue attempt failed, retry locally with this button.{{else}}Queue the next attempt with the current plan and review notes.{{end}}</p></div><form method="post" action="/tasks/{{.Task.ID}}/resume-fix"><input type="hidden" name="csrf_token" value="{{.CSRFToken}}"><button type="submit">Queue fix attempt</button></form>{{end}}
  {{if eq .Task.Status (index .K "TaskStatusFailed")}}<div><strong>Attempt failed</strong><p class="hint">Diagnostics were saved. Request a fix to queue another attempt.</p></div><form method="post" action="/tasks/{{.Task.ID}}/request-fix"><input type="hidden" name="csrf_token" value="{{.CSRFToken}}"><button class="secondary" type="submit">Request fix</button></form>{{end}}
</section>
{{end}}
<section class="card workflow-card" data-workflow-views>
  <div class="section-head"><div><h2>Run chain</h2><p class="hint">Fixed workflow sequence. Dry-run is default; experimental local-pi changes the execution backend, not this planning template.</p></div><div class="workflow-view-control"><span class="workflow-view-label">View</span><div class="workflow-view-toggle" role="tablist" aria-label="Run chain views"><button type="button" role="tab" class="workflow-view-button active" aria-selected="true" aria-controls="workflow-view-flow" data-workflow-view="workflow-view-flow">Flow</button><button type="button" role="tab" class="workflow-view-button" aria-selected="false" aria-controls="workflow-view-map" data-workflow-view="workflow-view-map">Map</button><button type="button" role="tab" class="workflow-view-button" aria-selected="false" aria-controls="workflow-view-list" data-workflow-view="workflow-view-list">List</button></div></div></div>
  <div id="workflow-view-flow" class="workflow-view-panel" role="tabpanel"><div class="workflow-map" tabindex="0" aria-label="Scrollable workflow flow preview"><div class="map-canvas single-row">{{range $i, $step := .Chain}}<article class="map-node {{$step.State}}"><span class="node-kicker">{{$step.Name}}</span><strong>{{shortStep $step.Name}}</strong><small>{{$step.Description}}</small><em>{{$step.State}}</em></article>{{if lt $i 6}}<div class="map-arrow" aria-hidden="true"></div>{{end}}{{end}}</div></div><div class="loop-note"><strong>Fix loop:</strong> Final review can send work back to Worker attempt. Loop arrows wait until graph layout can calculate anchors precisely.</div></div>
  <div id="workflow-view-map" class="workflow-view-panel" role="tabpanel" hidden><div class="workflow-placeholder"><strong>Map view is still TBD.</strong><span>This will become the spatial workflow map when graph layout, zoom, and loop anchors are ready.</span></div></div>
  <div id="workflow-view-list" class="workflow-view-panel" role="tabpanel" hidden><ol class="workflow-simple" aria-label="Workflow steps">{{range .Chain}}<li><strong>{{shortStep .Name}}</strong><span>{{.Description}}</span><em>{{.State}}</em></li>{{end}}</ol></div>
  <script>(function(){var root=document.currentScript.closest('[data-workflow-views]');if(!root){return;}var buttons=Array.prototype.slice.call(root.querySelectorAll('[data-workflow-view]'));var panels=Array.prototype.slice.call(root.querySelectorAll('.workflow-view-panel'));function activate(button){buttons.forEach(function(item){var selected=item===button;item.setAttribute('aria-selected',selected?'true':'false');item.classList.toggle('active',selected);});panels.forEach(function(panel){panel.hidden=panel.id!==button.getAttribute('data-workflow-view');});}buttons.forEach(function(button){button.addEventListener('click',function(){activate(button);});});activate(buttons[0]);})();</script>
</section>
<section class="task-tabs card" data-task-tabs>
  <div class="tabbar" role="tablist" aria-label="Task detail sections">
    <button type="button" role="tab" id="tab-plan" aria-controls="panel-plan" aria-selected="true" data-tab-target="panel-plan">Plan</button>
    <button type="button" role="tab" id="tab-handoff" aria-controls="panel-handoff" aria-selected="false" data-tab-target="panel-handoff">Handoff</button>
    <button type="button" role="tab" id="tab-outputs" aria-controls="panel-outputs" aria-selected="false" data-tab-target="panel-outputs">Worker output</button>
    <button type="button" role="tab" id="tab-diff" aria-controls="panel-diff" aria-selected="false" data-tab-target="panel-diff">Diff</button>
    <button type="button" role="tab" id="tab-review" aria-controls="panel-review" aria-selected="false" data-tab-target="panel-review">Review</button>
    <button type="button" role="tab" id="tab-diagnostics" aria-controls="panel-diagnostics" aria-selected="false" data-tab-target="panel-diagnostics">Diagnostics{{if .Diagnostics}} ({{len .Diagnostics}}){{end}}</button>
    <button type="button" role="tab" id="tab-activity" aria-controls="panel-activity" aria-selected="false" data-tab-target="panel-activity">Activity</button>
  </div>
  <section id="panel-plan" class="tab-section" role="tabpanel" aria-labelledby="tab-plan"><h2>Plan</h2><dl><dt>Runner</dt><dd>{{index .RunnerNames .Task.AssignedExecutorID}} <span class="hint">(local execution target)</span></dd><dt>Agent profile</dt><dd>{{agentProfileLabel .Task.Adapter}}</dd><dt>Focus</dt><dd>{{.Task.Focus}}</dd><dt>Boundaries</dt><dd>{{.Task.ExcludedPaths}}</dd><dt>Done when</dt><dd>{{.Task.AcceptanceCriteria}}</dd><dt>Active run slot</dt><dd>{{if .Task.LeaseID}}Reserved{{else}}No active slot{{end}}</dd></dl></section>
  <section id="panel-handoff" class="tab-section" role="tabpanel" aria-labelledby="tab-handoff"><h2>Handoff</h2><p class="hint">Mock runner handoff previews what would need to move before work could continue on another runner. It does not copy files, contact a remote runner, or change task assignment.</p><div class="callout"><strong>Preview only:</strong> code movement is modeled as Git state, outputs are modeled through an explicit manifest, and mock approval records a prototype decision only.</div><div class="handoff-tab-actions"><a class="button secondary-link" href="/tasks/{{.Task.ID}}/handoff">Preview mock runner handoff</a></div>{{if .Handoffs}}<div class="attempt-list">{{range .Handoffs}}<article class="attempt-card"><div><strong>{{index $.RunnerNames .SourceExecutorID}} → {{index $.RunnerNames .DestinationExecutorID}}</strong><span class="status">{{.Status}}</span></div><p>{{.ResultSummary}}</p><small>{{.BranchName}}</small><a href="/tasks/{{$.Task.ID}}/handoff/{{.ID}}">Open handoff preview</a></article>{{end}}</div>{{else}}<p>No handoff previews yet.</p>{{end}}</section>
  <section id="panel-outputs" class="tab-section" role="tabpanel" aria-labelledby="tab-outputs"><h2>Worker output</h2>{{if .Attempts}}<div class="attempt-list">{{range .Attempts}}<article class="attempt-card"><div><strong>Attempt {{.Attempt.Number}}</strong><span class="status">{{.Attempt.Status}}</span></div><p>{{.Attempt.Summary}}</p>{{if .Artifacts}}<ul class="artifact-grid">{{range .Artifacts}}{{if and (or (eq .Sensitivity "") (eq .Sensitivity (index $.K "SensitivityNormal"))) (or (eq .Kind (index $.K "ArtifactKindWorkerOutput")) (eq .Kind (index $.K "ArtifactKindSummary")) (and (eq .Kind (index $.K "ArtifactKindChangedFiles")) (gt .SizeBytes 0)))}}<li><a href="/artifacts/{{.ID}}">{{artifactLabel .Kind}}</a><small>{{artifactName .Path}}</small></li>{{end}}{{end}}</ul>{{end}}</article>{{end}}</div>{{else}}<p>No attempts yet.</p>{{end}}</section>
  <section id="panel-diff" class="tab-section" role="tabpanel" aria-labelledby="tab-diff"><h2>Diff</h2>{{if .DiffArtifacts}}<ul class="artifact-grid">{{range .DiffArtifacts}}<li><a href="/artifacts/{{.ID}}">{{artifactLabel .Kind}}</a><small>{{artifactName .Path}}</small></li>{{end}}</ul>{{else}}<p>No diff was produced for this attempt.</p>{{end}}</section>
  <section id="panel-review" class="tab-section" role="tabpanel" aria-labelledby="tab-review"><h2>Review</h2><div class="review-grid">{{range .Attempts}}<article class="attempt-card"><strong>Attempt {{.Attempt.Number}} {{if eq .Attempt.Status (index $.K "AttemptStatusFailed")}}diagnostics{{else}}review{{end}}</strong><p>{{.Attempt.Summary}}</p>{{if .Artifacts}}<ul class="artifact-grid">{{range .Artifacts}}{{if and (or (eq .Sensitivity "") (eq .Sensitivity (index $.K "SensitivityNormal"))) (or (eq .Kind (index $.K "ArtifactKindReview")) (eq .Kind (index $.K "ArtifactKindFindings")))}}<li><a href="/artifacts/{{.ID}}">{{artifactLabel .Kind}}</a><small>{{artifactName .Path}}</small></li>{{end}}{{end}}</ul>{{end}}</article>{{end}}</div></section>
  <section id="panel-diagnostics" class="tab-section" role="tabpanel" aria-labelledby="tab-diagnostics"><h2>Diagnostics</h2><p class="hint">Internal diagnostics are local-only alpha previews for runner stdout/stderr, inputs, and checkpoints. They remain excluded from normal artifact preview and handoff manifests.</p>{{if .Diagnostics}}<ul class="artifact-grid diagnostics-grid">{{range .Diagnostics}}<li><a href="/tasks/{{$.Task.ID}}/diagnostics/{{.ID}}">{{artifactName .Path}}</a><small>{{artifactLabel .Kind}} · Attempt {{.AttemptNumber}} · {{.SizeBytes}} bytes</small><span class="diagnostic-sensitivity">{{.Sensitivity}}</span></li>{{end}}</ul>{{else}}<p>No internal diagnostics have been captured yet.</p>{{end}}</section>
  <section id="panel-activity" class="tab-section" role="tabpanel" aria-labelledby="tab-activity">{{template "task-activity" .}}</section>
  <script>(function(){var root=document.currentScript.closest('[data-task-tabs]');if(!root){return;}var tabs=Array.prototype.slice.call(root.querySelectorAll('[role="tab"]'));var panels=Array.prototype.slice.call(root.querySelectorAll('[role="tabpanel"]'));function activate(tab,scroll){tabs.forEach(function(t){var selected=t===tab;t.setAttribute('aria-selected',selected?'true':'false');t.classList.toggle('active',selected);});panels.forEach(function(panel){panel.hidden=panel.id!==tab.getAttribute('aria-controls');});if(scroll){root.scrollIntoView({behavior:'smooth',block:'start'});}}tabs.forEach(function(tab){tab.addEventListener('click',function(){activate(tab,true);});tab.addEventListener('keydown',function(event){var index=tabs.indexOf(tab);if(event.key==='ArrowRight'){event.preventDefault();tabs[(index+1)%tabs.length].focus();activate(tabs[(index+1)%tabs.length],true);}if(event.key==='ArrowLeft'){event.preventDefault();tabs[(index+tabs.length-1)%tabs.length].focus();activate(tabs[(index+tabs.length-1)%tabs.length],true);}});});activate(tabs[0],false);})();</script>
</section>
{{if .TaskActivityActive}}<script>(function(){var url="/tasks/{{.Task.ID}}/activity";var stopped=false;function tick(){if(stopped){return;}fetch(url,{headers:{"Accept":"text/html"},cache:"no-store"}).then(function(response){if(!response.ok){throw new Error("activity unavailable");}return response.text();}).then(function(html){var current=document.getElementById("task-activity");var wrapper=document.createElement("div");wrapper.innerHTML=html.trim();var next=wrapper.firstElementChild;if(current&&next){current.replaceWith(next);if(next.getAttribute("data-task-active")!=="true"){stopped=true;window.location.reload();return;}}window.setTimeout(tick,2000);}).catch(function(){var current=document.getElementById("task-activity");if(current&&!document.getElementById("task-activity-retry")){var note=document.createElement("p");note.id="task-activity-retry";note.className="hint";note.textContent="Activity updates paused; retrying…";current.appendChild(note);}window.setTimeout(tick,4000);});}window.setTimeout(tick,2000);})();</script>{{end}}
{{end}}
`
