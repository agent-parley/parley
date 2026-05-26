package app

const runnersTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / Runners</nav>
<div class="page-actions">{{if .Project}}<a class="back-link" href="/projects/{{.Project.ID}}">← Back to {{.Project.Name}}</a>{{else}}<a class="back-link" href="/projects">← Back to projects</a>{{end}}</div>
<section class="hero">
  <div>
    <h1>Runners</h1>
    <p>Prototype runner registry. Only the local runner executes today; remote runners are modeled for scheduling and handoff UX.</p>
  </div>
  <span class="status large">{{len .Runners}} runners</span>
</section>
<section class="runner-grid">
  {{range .Runners}}
  <a class="runner-card card" href="/runners/{{.Runner.ID}}{{$.ProjectQuery}}">
    <div class="runner-card-head"><strong>{{.Runner.Name}}</strong><span class="runner-status {{.Runner.Status}}">{{.Runner.Status}}</span></div>
    <p>{{.Runner.Description}}</p>
    <dl><dt>Class</dt><dd>{{.Runner.ResourceClass}}</dd><dt>Runtime</dt><dd>{{.Runner.ContainerRuntime}}</dd><dt>Run slots</dt><dd>{{.ActiveSlots}} / {{.Runner.MaxSlots}}</dd><dt>Repo</dt><dd>{{.Runner.RepoAvailability}}</dd></dl>
    <small>{{.Runner.Endpoint}}</small>
  </a>
  {{end}}
</section>
<section class="callout">Remote runner cards are UI mocks. Real runner auth, scheduling, and execution are still future work.</section>
{{end}}
`

const runnerDetailTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / <a href="/runners{{.ProjectQuery}}">Runners</a> / {{.Runner.Name}}</nav>
<div class="page-actions">{{if .Project}}<a class="back-link" href="/projects/{{.Project.ID}}">← Back to {{.Project.Name}}</a><a class="back-link secondary" href="/runners?project={{.Project.ID}}">← Back to runners</a>{{else}}<a class="back-link" href="/runners">← Back to runners</a>{{end}}</div>
<section class="hero">
  <div>
    <h1>{{.Runner.Name}}</h1>
    <p>{{.Runner.Description}}</p>
    <small>{{.Runner.Endpoint}}</small>
  </div>
  <span class="runner-status large {{.Runner.Status}}">{{.Runner.Status}}</span>
</section>
<section class="capability-grid">
  <div class="capability-card"><strong>Run slots</strong><span>{{.ActiveSlots}} / {{.Runner.MaxSlots}}</span><small>Prototype slot accounting</small></div>
  <div class="capability-card"><strong>Resource class</strong><span>{{.Runner.ResourceClass}}</span><small>Scheduling hint</small></div>
  <div class="capability-card"><strong>Container runtime</strong><span>{{.Runner.ContainerRuntime}}</span><small>Podman-first direction</small></div>
  <div class="capability-card"><strong>Repo availability</strong><span>{{.Runner.RepoAvailability}}</span><small>No remote mounts</small></div>
</section>
<section class="grid two">
  <div class="card">
    <h2>Agent profiles</h2>
    <div class="pill-list">{{range .Runner.AgentProfiles}}<span>{{agentProfileLabel .}}</span>{{end}}</div>
  </div>
  <div class="card">
    <h2>Capabilities</h2>
    <div class="pill-list">{{range .Runner.Capabilities}}<span>{{.}}</span>{{end}}</div>
  </div>
</section>
<section class="card">
  <h2>Notes</h2>
  <p>{{.Runner.Notes}}</p>
</section>
{{end}}
`

const handoffStartTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / <a href="/projects/{{.Project.ID}}">{{.Project.Name}}</a> / <a href="/tasks/{{.Task.ID}}">Task</a> / Handoff preview</nav>
<div class="page-actions"><a class="back-link" href="/tasks/{{.Task.ID}}">← Back to task</a><a class="back-link secondary" href="/projects/{{.Project.ID}}">← Back to project</a></div>
<section class="hero">
  <div>
    <h1>Preview mock runner handoff</h1>
    <p>Preview what a safe handoff would require. No files are copied, no remote runner is contacted, and task assignment does not change in this prototype.</p>
  </div>
  <span class="status large">mock flow</span>
</section>
{{if .Error}}<p class="error" role="alert">{{.Error}}</p>{{end}}
<form method="post" action="/tasks/{{.Task.ID}}/handoff/preview" class="handoff-shell">
  <input type="hidden" name="csrf_token" value="{{.CSRFToken}}">
  <section class="card">
    <h2>Choose runners</h2>
    <input type="hidden" name="source_runner" value="{{.Source}}">
    <dl><dt>Source</dt><dd>{{.SourceName}}</dd><dt>Task</dt><dd>{{.Task.Title}}</dd><dt>Status</dt><dd>{{statusLabel .Task.Status}}</dd></dl>
    <label>Destination runner
      <select name="destination_runner">
        {{range .Runners}}{{if ne .ID $.Source}}<option value="{{.ID}}" {{if ne .Status (index $.K "ExecutorStatusOnline")}}disabled{{end}}>{{.Name}} · {{.Status}} · {{.ResourceClass}}{{if ne .Status (index $.K "ExecutorStatusOnline")}} · unavailable for preview{{end}}</option>{{end}}{{end}}
      </select>
    </label>
    <button type="submit">Preview mock safe handoff</button>
    <p class="hint">Offline runners are disabled until real runner sync/auth exists. Previewing only builds a manifest and safety checklist.</p>
  </section>
  <section class="card check-panel">
    <h2>Safety model</h2>
    <ul class="list compact-list safety-list">
      <li><strong>Git for code</strong><span>Future handoff would use a branch/ref instead of copying a workspace.</span></li>
      <li><strong>Manifest for outputs</strong><span>The preview includes normal-sensitivity Parley outputs; sensitive/internal outputs are excluded.</span></li>
      <li><strong>No real sync today</strong><span>No files are copied, no runner is contacted, no task assignment changes, and no credentials are shared.</span></li>
    </ul>
  </section>
</form>
<section class="card">
  <h2>Current outputs</h2>
  {{if .Artifacts}}<ul class="artifact-grid">{{range .Artifacts}}<li><a href="/artifacts/{{.ID}}">{{artifactLabel .Kind}}</a><small>{{artifactName .Path}}</small></li>{{end}}</ul>{{else}}<p>No outputs yet. The preview will show planned output names.</p>{{end}}
</section>
{{end}}
`

const handoffApprovalTemplate = `{{define "content"}}
<nav class="crumb" aria-label="Breadcrumb"><a href="/projects">Projects</a> / <a href="/projects/{{.Project.ID}}">{{.Project.Name}}</a> / <a href="/tasks/{{.Task.ID}}">Task</a> / Handoff preview</nav>
<div class="page-actions"><a class="back-link" href="/tasks/{{.Task.ID}}">← Back to task</a><a class="back-link secondary" href="/projects/{{.Project.ID}}">← Back to project</a></div>
<section class="hero">
  <div>
    <h1>Mock safe handoff preview</h1>
    <p>{{.Source.Name}} → {{.Destination.Name}} · preview only, no files copied and no runner contacted.</p>
  </div>
  <span class="status large">{{.Handoff.Status}}</span>
</section>
{{if .Handoff.ResultSummary}}<section class="result-banner">{{.Handoff.ResultSummary}}</section>{{end}}
<section class="callout"><strong>Preview only:</strong> this page builds a safety manifest and can record mock approval. It does not copy files, contact a remote runner, or change task assignment.</section>
<section class="grid two">
  <div class="card check-panel">
    <h2>Planned Git checks</h2>
    <dl><dt>Branch/ref</dt><dd>{{.Handoff.BranchName}}</dd><dt>Commit check</dt><dd>{{.Handoff.CommitCheck}}</dd><dt>Remote check</dt><dd>{{.Handoff.RemoteCheck}}</dd></dl>
  </div>
  <div class="card check-panel">
    <h2>Runner path</h2>
    <dl><dt>Source</dt><dd>{{.Source.Name}}</dd><dt>Destination</dt><dd>{{.Destination.Name}}</dd><dt>Destination status</dt><dd>{{.Destination.Status}}</dd><dt>Destination runtime</dt><dd>{{.Destination.ContainerRuntime}}</dd></dl>
  </div>
</section>
<section class="include-exclude-grid">
  <div class="card">
    <h2>Included outputs</h2>
    <ul class="artifact-grid">{{range .Handoff.Included}}<li><strong>{{artifactLabel .Kind}}</strong><small>{{.RelativePath}}</small></li>{{end}}</ul>
  </div>
  <div class="card">
    <h2>Excluded boundaries</h2>
    <ul class="events">{{range .Handoff.Excluded}}<li><strong>{{.RelativePath}}</strong><span>{{.Reason}}</span></li>{{end}}</ul>
  </div>
</section>
<section class="grid two">
  <div class="card">
    <h2>.parleyignore preview</h2>
    <pre class="parleyignore-preview" tabindex="0" aria-label="Scrollable parley ignore preview">{{.Handoff.ParleyIgnorePreview}}</pre>
  </div>
  <div class="card">
    <h2>Manifest preview</h2>
    <pre class="manifest-preview" tabindex="0" aria-label="Scrollable handoff manifest preview">{{.Handoff.ManifestPreview}}</pre>
  </div>
</section>
{{if ne .Handoff.Status (index .K "HandoffStatusRecorded")}}
<section class="actions card">
  <div><strong>Record mock handoff approval</strong><p class="hint">This records a prototype decision only. It does not copy files, contact a remote runner, or change task assignment.</p></div>
  {{if eq .Destination.Status (index .K "ExecutorStatusOnline")}}<form method="post" action="/tasks/{{.Task.ID}}/handoff/{{.Handoff.ID}}/approve"><input type="hidden" name="csrf_token" value="{{.CSRFToken}}"><button type="submit">Record mock handoff approval</button></form>{{else}}<button class="secondary" type="button" disabled aria-disabled="true">Destination offline</button>{{end}}
</section>
{{end}}
{{end}}
`
