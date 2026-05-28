package app

import "net/http"

const cssContent = `
:root {
  color-scheme: light dark;
  --bg: #0f172a;
  --panel: #111827;
  --text: #e5e7eb;
  --muted: #9ca3af;
  --line: #374151;
  --accent: #8b5cf6;
  --accent-2: #22c55e;
  --danger: #f87171;
}
* { box-sizing: border-box; }
body { margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: var(--bg); color: var(--text); }
a { color: inherit; }
.topbar { display: flex; align-items: center; gap: .75rem; padding: 1rem; border-bottom: 1px solid var(--line); position: sticky; top: 0; background: rgba(15, 23, 42, .92); backdrop-filter: blur(10px); }
.brand { font-weight: 800; text-decoration: none; font-size: 1.2rem; }
.topnav { display: flex; gap: .35rem; flex-wrap: wrap; }
.topnav a { text-decoration: none; color: var(--muted); border: 1px solid transparent; border-radius: 999px; padding: .35rem .6rem; }
.topnav a:hover { border-color: var(--line); color: var(--text); }
.badge, .status { display: inline-flex; align-items: center; border: 1px solid var(--line); border-radius: 999px; padding: .2rem .55rem; color: var(--muted); font-size: .82rem; white-space: nowrap; }
.status.large { font-size: 1rem; padding: .45rem .75rem; color: var(--accent-2); flex-shrink: 0; }
.container { width: min(1120px, 100%); margin: 0 auto; padding: 1rem; }
.container > section + section, .container > form + section, .container > section + form, .container > form + form { margin-top: 1rem; }
.hero { display: flex; align-items: center; justify-content: space-between; gap: 1rem; margin: 1rem 0; }
.hero h1 { margin: 0; font-size: clamp(1.8rem, 5vw, 3rem); }
.hero p, .muted { color: var(--muted); }
.grid { display: grid; gap: 1rem; }
.grid.two { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.grid.three { grid-template-columns: repeat(3, minmax(0, 1fr)); }
.card { border: 1px solid var(--line); border-radius: 1rem; background: var(--panel); padding: 1rem; box-shadow: 0 10px 28px rgba(0,0,0,.18); }
.section-head { display: flex; align-items: center; justify-content: space-between; gap: 1rem; margin-bottom: 1rem; }
.section-head h2 { margin: 0 0 .25rem; }
.card.compact { min-width: 240px; display: grid; gap: .35rem; }
.form { display: grid; gap: .85rem; }
.form.wide { max-width: 820px; margin: 1rem auto; }
label { display: grid; gap: .35rem; color: var(--muted); }
input, textarea, select { width: 100%; border: 1px solid var(--line); border-radius: .65rem; background: #020617; color: var(--text); padding: .75rem; font: inherit; }
button, .button { border: 0; border-radius: .75rem; background: var(--accent); color: white; padding: .78rem 1rem; font-weight: 700; text-decoration: none; cursor: pointer; }
button.secondary, .button.secondary-link { background: #334155; }
button:disabled, button[aria-disabled="true"] { cursor: not-allowed; opacity: .58; }
.hero-actions { display: flex; gap: .75rem; flex-wrap: wrap; justify-content: flex-end; }
.page-actions { display: flex; gap: .65rem; flex-wrap: wrap; margin: .5rem 0 1rem; }
.back-link { display: inline-flex; align-items: center; border: 1px solid rgba(139, 92, 246, .5); border-radius: 999px; padding: .5rem .8rem; background: rgba(139, 92, 246, .10); color: #ddd6fe; text-decoration: none; font-weight: 700; }
.back-link.secondary { border-color: var(--line); background: rgba(2, 6, 23, .35); color: var(--muted); }
.hint { color: var(--muted); font-size: .94rem; line-height: 1.45; }
.callout { margin-top: 1rem; border: 1px solid rgba(139, 92, 246, .45); border-radius: .85rem; padding: .85rem; background: rgba(139, 92, 246, .09); color: #ddd6fe; }
.actions { display: flex; align-items: center; justify-content: space-between; gap: .75rem; flex-wrap: wrap; }
.actions p { margin: .25rem 0 0; }
.list, .events { list-style: none; padding: 0; margin: 0; display: grid; gap: .65rem; }
.list li, .events li { border-top: 1px solid var(--line); padding-top: .65rem; display: grid; gap: .2rem; }
.list small, .events span, .events small, dd { color: var(--muted); overflow-wrap: anywhere; }
.planner-shell { align-items: start; }
.planner-activity { margin-top: 1rem; border: 1px solid var(--line); border-radius: .9rem; padding: .85rem; background: rgba(2,6,23,.32); }
.planner-activity .section-head { margin-bottom: .65rem; }
.task-activity { display: grid; gap: .65rem; }
.task-activity h2 { margin: 0; }
.chat-panel { display: grid; gap: .85rem; }
.bubble { border-radius: 1rem; padding: .85rem; display: grid; gap: .3rem; max-width: 92%; }
.bubble.agent, .bubble.planner { background: rgba(139, 92, 246, .12); border: 1px solid rgba(139, 92, 246, .35); }
.bubble.critic { background: rgba(245, 158, 11, .12); border: 1px solid rgba(245, 158, 11, .35); }
.prompt-box { display: grid; gap: .75rem; border-top: 1px solid var(--line); padding-top: .85rem; }
.chain-list { display: grid; gap: .7rem; margin: 0; padding-left: 1.25rem; }
.chain-list li { padding-left: .25rem; }
.chain-list span { display: block; color: var(--muted); margin-top: .2rem; }
.template-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: .75rem; margin-top: 1rem; }
.template-card { border: 1px solid var(--line); border-radius: 1rem; padding: .9rem; display: grid; gap: .4rem; background: rgba(2, 6, 23, .35); text-decoration: none; }
.template-card span, .template-card small { color: var(--muted); }
.template-card.selected { border-color: rgba(139, 92, 246, .85); box-shadow: 0 0 0 1px rgba(139, 92, 246, .22); }
.toolbar { display: flex; gap: .45rem; flex-wrap: wrap; justify-content: flex-end; align-items: center; }
.toolbar button { padding: .55rem .72rem; font-size: .85rem; }
.toolbar-label { border: 1px solid rgba(139, 92, 246, .75); border-radius: 999px; padding: .45rem .7rem; color: #f5f3ff; background: rgba(139, 92, 246, .20); font-size: .85rem; font-weight: 800; }
.toolbar-note { color: var(--muted); font-size: .85rem; max-width: 360px; }
.workflow-card { overflow: hidden; }
.workflow-map { border: 1px solid rgba(148, 163, 184, .24); border-radius: 1rem; background: radial-gradient(circle at 16% 18%, rgba(139, 92, 246, .16), transparent 34%), radial-gradient(circle at 85% 72%, rgba(34, 197, 94, .10), transparent 30%), rgba(2, 6, 23, .38); overflow-x: auto; overflow-y: hidden; min-height: 230px; box-shadow: inset 0 0 0 1px rgba(255,255,255,.02); }
.workflow-map:focus, .manifest-preview:focus, .parleyignore-preview:focus { outline: 3px solid rgba(139, 92, 246, .85); outline-offset: 3px; }
.map-canvas.single-row { min-width: 1780px; min-height: 228px; padding: 2.2rem 2rem; display: flex; align-items: center; gap: 0; }
.map-node { flex: 0 0 190px; height: 150px; border: 1px solid rgba(148, 163, 184, .28); border-radius: 1.25rem; padding: .82rem; display: grid; grid-template-rows: auto auto 1fr auto; gap: .3rem; background: linear-gradient(145deg, rgba(15, 23, 42, .95), rgba(30, 41, 59, .72)); box-shadow: 0 18px 38px rgba(0,0,0,.22); overflow: hidden; }
.map-node strong { font-size: .98rem; line-height: 1.1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.map-node small { color: var(--muted); line-height: 1.25; display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; }
.map-node em, .node-kicker { justify-self: start; max-width: 100%; border-radius: 999px; padding: .14rem .48rem; font-style: normal; font-size: .72rem; border: 1px solid var(--line); color: var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.node-kicker { color: #c4b5fd; border-color: rgba(139, 92, 246, .45); }
.map-node.done { border-color: rgba(34, 197, 94, .65); background: linear-gradient(145deg, rgba(10, 54, 45, .95), rgba(15, 23, 42, .72)); }
.map-node.done em { color: #86efac; border-color: rgba(34, 197, 94, .6); }
.map-node.active { border-color: rgba(139, 92, 246, .95); box-shadow: 0 0 0 2px rgba(139, 92, 246, .18), 0 18px 38px rgba(0,0,0,.25); }
.map-node.active em { color: #ddd6fe; border-color: rgba(139, 92, 246, .95); }
.map-arrow { flex: 0 0 64px; height: 2px; margin: 0 .35rem; background: linear-gradient(90deg, rgba(139, 92, 246, .25), rgba(139, 92, 246, .95)); position: relative; filter: drop-shadow(0 0 6px rgba(139, 92, 246, .35)); }
.map-arrow::after { content: ''; position: absolute; right: -1px; top: -5px; border-left: 10px solid rgba(139, 92, 246, .95); border-top: 6px solid transparent; border-bottom: 6px solid transparent; }
.loop-note { margin-top: 1rem; border: 1px dashed rgba(139, 92, 246, .5); border-radius: .85rem; padding: .8rem; color: #ddd6fe; background: rgba(139, 92, 246, .08); }
.workflow-view-control { display: flex; align-items: center; gap: .6rem; flex-shrink: 0; }
.workflow-view-label { color: var(--muted); font-size: .85rem; font-weight: 800; }
.workflow-view-toggle { display: inline-flex; flex-wrap: nowrap; border: 1px solid var(--line); border-radius: 999px; padding: .2rem; background: rgba(2, 6, 23, .45); white-space: nowrap; }
.workflow-view-toggle button { width: auto; border: 0; border-radius: 999px; padding: .45rem .8rem; color: var(--muted); background: transparent; }
.workflow-view-toggle button:hover { color: var(--text); background: rgba(30, 41, 59, .88); }
.workflow-view-toggle button[aria-selected="true"], .workflow-view-toggle button.active { color: #fff; background: rgba(139, 92, 246, .42); }
.workflow-view-panel { margin-top: 1rem; }
.workflow-view-panel[hidden] { display: none; }
.workflow-placeholder { min-height: 230px; border: 1px dashed rgba(139, 92, 246, .55); border-radius: 1rem; background: rgba(139, 92, 246, .08); color: #ddd6fe; display: grid; place-content: center; gap: .35rem; text-align: center; padding: 1rem; }
.workflow-placeholder span { color: var(--muted); }
.workflow-simple { display: grid; gap: .55rem; margin: 0; padding-left: 1.25rem; }
.workflow-simple li { border: 1px solid var(--line); border-radius: .85rem; padding: .7rem; background: rgba(2, 6, 23, .35); }
.workflow-simple span { display: block; color: var(--muted); margin: .2rem 0; }
.workflow-simple em { color: #ddd6fe; font-style: normal; font-size: .82rem; }
.settings-grid { display: grid; gap: 1rem; align-items: end; }
.settings-grid:not(.settings-grid-primary):not(.settings-grid-compact) { grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }
.settings-grid-primary { grid-template-columns: 1fr; }
.settings-grid-compact { grid-template-columns: 1fr; }
.checkbox { display: flex; align-items: center; gap: .55rem; }
.checkbox input { width: auto; }
.step-editor { display: grid; gap: .85rem; }
.step-card { border: 1px solid var(--line); border-radius: 1rem; padding: .9rem; background: rgba(2, 6, 23, .35); display: grid; gap: .65rem; }
.draft-panel { display: grid; gap: .9rem; }
.compact-list { gap: .35rem; }
.compact-list li { padding-top: .35rem; }
.inline-actions { justify-content: start; }
.mini-chain { display: flex; flex-wrap: wrap; gap: .5rem; }
.mini-chain span { border: 1px solid rgba(139,92,246,.45); border-radius: 999px; padding: .35rem .65rem; color: #ddd6fe; background: rgba(139,92,246,.09); }
.task-tabs { display: grid; gap: 0; scroll-margin-top: 5.5rem; }
.tabbar { display: flex; gap: 0; flex-wrap: wrap; align-items: end; border-bottom: 1px solid var(--line); padding: 0 .25rem; margin: -.2rem -.25rem 0; }
.tabbar button { width: auto; border: 1px solid var(--line); border-bottom: 0; border-radius: .75rem .75rem 0 0; padding: .65rem 1rem; margin: 0 0 -1px -1px; color: var(--muted); background: rgba(15,23,42,.78); box-shadow: inset 0 -1px 0 rgba(255,255,255,.04); }
.tabbar button:first-child { margin-left: 0; }
.tabbar button:hover { color: var(--text); background: rgba(30,41,59,.92); }
.tabbar button[aria-selected="true"], .tabbar button.active { color: #fff; border-color: rgba(139,92,246,.85); border-bottom-color: rgba(2,6,23,.98); background: rgba(2,6,23,.98); box-shadow: 0 -1px 0 rgba(139,92,246,.22), 0 0 0 1px rgba(139,92,246,.12); position: relative; z-index: 1; }
.tab-section { border: 1px solid var(--line); border-top: 0; border-radius: 0 0 .9rem .9rem; padding: 1rem; background: rgba(2,6,23,.35); }
.tab-section[hidden] { display: none; }
.handoff-tab-actions { display: flex; margin: 1rem 0; }
.attempt-list, .review-grid { display: grid; gap: .8rem; }
.attempt-card { border: 1px solid var(--line); border-radius: 1rem; padding: .9rem; background: rgba(2,6,23,.35); display: grid; gap: .65rem; }
.attempt-card > div { display: flex; align-items: center; justify-content: space-between; gap: .75rem; }
.runner-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 1rem; }
.runner-card { text-decoration: none; display: grid; gap: .75rem; }
.runner-card-head { display: flex; align-items: center; justify-content: space-between; gap: .75rem; }
.runner-status { display: inline-flex; align-items: center; border: 1px solid var(--line); border-radius: 999px; padding: .25rem .6rem; color: var(--muted); font-size: .85rem; white-space: nowrap; }
.runner-status.online { color: #86efac; border-color: rgba(34,197,94,.65); background: rgba(34,197,94,.08); }
.runner-status.offline { color: #fca5a5; border-color: rgba(248,113,113,.5); background: rgba(248,113,113,.08); }
.runner-status.large { font-size: 1rem; padding: .45rem .75rem; }
.capability-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: .85rem; margin-bottom: 1rem; }
.capability-card { border: 1px solid var(--line); border-radius: 1rem; padding: 1rem; background: linear-gradient(145deg, rgba(15,23,42,.95), rgba(30,41,59,.65)); display: grid; gap: .35rem; }
.capability-card span { font-size: 1.15rem; font-weight: 800; }
.capability-card small { color: var(--muted); }
.pill-list { display: flex; gap: .5rem; flex-wrap: wrap; }
.pill-list span { border: 1px solid var(--line); border-radius: 999px; padding: .35rem .65rem; color: #ddd6fe; background: rgba(139,92,246,.08); }
.handoff-shell { display: grid; grid-template-columns: minmax(0, 1.05fr) minmax(300px, .95fr); gap: 1rem; align-items: start; }
.handoff-shell .card, .include-exclude-grid .card { min-width: 0; }
.handoff-shell button[type="submit"] { margin-top: .85rem; }
.check-panel { background: linear-gradient(145deg, rgba(15,23,42,.96), rgba(2,6,23,.62)); }
.safety-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: .65rem; margin: .8rem 0 0; }
.safety-list li { border: 1px solid rgba(139,92,246,.35); border-radius: .85rem; padding: .75rem; background: rgba(139,92,246,.08); }
.include-exclude-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1rem; margin: 1rem 0; }
.manifest-preview, .parleyignore-preview { max-height: 360px; overflow: auto; border: 1px solid var(--line); border-radius: .85rem; padding: .85rem; background: #020617; color: #c4b5fd; white-space: pre-wrap; font-size: .82rem; }
.result-banner { border: 1px solid rgba(34,197,94,.55); border-radius: 1rem; padding: 1rem; background: rgba(34,197,94,.08); color: #bbf7d0; margin-bottom: 1rem; }
.artifact-grid { list-style: none; padding: 0; margin: 0; display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: .65rem; }
.artifact-grid li { border: 1px solid var(--line); border-radius: .85rem; padding: .75rem; background: rgba(2, 6, 23, .45); display: grid; gap: .35rem; }
.artifact-grid a { font-weight: 700; text-decoration: none; }
.artifact-grid small { color: var(--muted); overflow-wrap: anywhere; }
.diagnostics-grid li { border-color: rgba(248,113,113,.36); background: rgba(127,29,29,.12); }
.diagnostic-sensitivity { justify-self: start; border: 1px solid rgba(248,113,113,.55); border-radius: 999px; padding: .18rem .5rem; color: #fecaca; font-size: .78rem; }
dl { display: grid; grid-template-columns: 120px 1fr; gap: .5rem; }
dt { color: var(--muted); }
.crumb { color: var(--muted); margin: .5rem 0 1rem; }
.error { border: 1px solid var(--danger); color: var(--danger); border-radius: .75rem; padding: .75rem; }
@media (max-width: 720px) {
  .container { padding: .75rem; }
  .hero { align-items: stretch; flex-direction: column; }
  .grid.two, .grid.three, .handoff-shell, .include-exclude-grid, .settings-grid-primary, .settings-grid-compact { grid-template-columns: 1fr; }
  .topbar { align-items: flex-start; flex-direction: column; }
  .section-head { align-items: stretch; flex-direction: column; }
  .workflow-map { min-height: 230px; }
  .map-canvas.single-row { min-width: 1680px; padding: 1.4rem; }
  dl { grid-template-columns: 1fr; }
  .hero-actions, .page-actions, .handoff-tab-actions { width: 100%; }
  button, .button, .back-link { width: 100%; text-align: center; justify-content: center; }
  .tabbar button { width: auto; }
}
`

func (s *Server) css(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	_, _ = w.Write([]byte(cssContent))
}
