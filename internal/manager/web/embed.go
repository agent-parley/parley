package web

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"strings"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/store"
)

//go:embed templates/*.html assets/*
var Embedded embed.FS

type Renderer interface {
	ExecutePage(name string, data any) (string, error)
	RenderRunFragments(store.RunBundle) (string, error)
	RenderProjectChat(ProjectChatData) (string, error)
}

type TemplateRenderer struct {
	templates *template.Template
}

type IndexData struct {
	Project         store.Project
	Runs            []store.Run
	Runners         []store.Runner
	RunnerEventPage store.SystemEventPage
	Queue           QueueView
	Chat            ProjectChatData
	Notice          *Notice
	CSRF            string
	Title           string
}

type ProjectChatData struct {
	Project      store.Project
	Conversation store.Conversation
	Messages     []store.Message
	Tasks        []store.Task
	CSRF         string
}

type Notice struct {
	Title   string
	Message string
}

type QueueView struct {
	Pending                int
	Running                int
	AutoWhenReady          bool
	MaxConcurrent          int
	BacklogCap             int
	RunnerSlots            int
	ReadyRunnerSlots       int
	EffectiveMaxConcurrent int
}

func NewQueueView(state orchestrator.QueueState) QueueView {
	return QueueView{
		Pending:                state.Pending,
		Running:                state.Running,
		AutoWhenReady:          state.Policy.AutoWhenReady,
		MaxConcurrent:          state.Policy.MaxConcurrent,
		BacklogCap:             state.Policy.BacklogCap,
		RunnerSlots:            state.RunnerSlots,
		ReadyRunnerSlots:       state.ReadyRunnerSlots,
		EffectiveMaxConcurrent: state.EffectiveMaxConcurrent,
	}
}

type RunData struct {
	View  RunView
	CSRF  string
	Title string
}

type RunView struct {
	store.RunBundle
	ArtifactViews        []ArtifactView
	DiffPatch            ArtifactView
	PRReady              PRReadyView
	PendingIdeaQuestions *IdeaQuestionView
}

type IdeaQuestionView struct {
	StageID   string
	Round     int
	MaxRounds int
	Questions []string
}

type ArtifactView struct {
	ID           string
	Kind         string
	MediaType    string
	Preview      string
	DownloadOnly bool
}

type PRReadyView struct {
	Ready          bool
	Branch         string
	CommitSHA      string
	DiffArtifactID string
}

func NewRunData(bundle store.RunBundle, csrf, title string) RunData {
	return RunData{View: NewRunView(bundle), CSRF: csrf, Title: title}
}

func NewRunView(bundle store.RunBundle) RunView {
	view := RunView{RunBundle: bundle, PRReady: prReadyFromEvents(bundle), PendingIdeaQuestions: pendingIdeaQuestions(bundle)}
	for _, artifact := range bundle.Artifacts {
		artifactView := newArtifactView(artifact)
		view.ArtifactViews = append(view.ArtifactViews, artifactView)
		if artifact.Kind == "diff_patch" {
			view.DiffPatch = artifactView
		}
	}
	return view
}

func NewRenderer() (*TemplateRenderer, error) {
	funcs := template.FuncMap{"short": short, "statusClass": statusClass, "statusLabel": statusLabel}
	tmpl, err := template.New("").Funcs(funcs).ParseFS(Embedded, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}
	return &TemplateRenderer{templates: tmpl}, nil
}

func (r *TemplateRenderer) ExecutePage(name string, data any) (string, error) {
	var buf bytes.Buffer
	if err := r.templates.ExecuteTemplate(&buf, name, data); err != nil {
		return "", fmt.Errorf("execute template %s: %w", name, err)
	}
	return buf.String(), nil
}

func (r *TemplateRenderer) RenderRunFragments(bundle store.RunBundle) (string, error) {
	view := NewRunView(bundle)
	var buf bytes.Buffer
	for _, name := range []string{"run_summary.html", "stage_statuses.html", "event_log.html", "diff_patch.html", "artifacts.html"} {
		if err := r.templates.ExecuteTemplate(&buf, name, view); err != nil {
			return "", fmt.Errorf("execute fragment %s: %w", name, err)
		}
	}
	return compactHTML(buf.String()), nil
}

func (r *TemplateRenderer) RenderProjectChat(data ProjectChatData) (string, error) {
	var buf bytes.Buffer
	if err := r.templates.ExecuteTemplate(&buf, "project_chat.html", data); err != nil {
		return "", fmt.Errorf("execute project chat fragment: %w", err)
	}
	return compactHTML(buf.String()), nil
}

func newArtifactView(artifact store.Artifact) ArtifactView {
	view := ArtifactView{ID: artifact.ID, Kind: artifact.Kind, MediaType: artifact.MediaType, DownloadOnly: isHTMLMediaType(artifact.MediaType)}
	if view.DownloadOnly {
		return view
	}
	if isPreviewableMediaType(artifact.MediaType) || artifact.Kind == "diff_patch" {
		view.Preview = readArtifactPreview(artifact, artifactPreviewLimit(artifact.Kind))
	}
	return view
}

func artifactPreviewLimit(kind string) int {
	if kind == "diff_patch" {
		return 0
	}
	return 4096
}

func readArtifactPreview(artifact store.Artifact, limit int) string {
	content, err := os.ReadFile(artifact.Path)
	if err != nil {
		return ""
	}
	if limit > 0 && len(content) > limit {
		return string(content[:limit]) + "\n… preview truncated …"
	}
	return string(content)
}

func isPreviewableMediaType(mediaType string) bool {
	mediaType = strings.ToLower(mediaType)
	return strings.HasPrefix(mediaType, "text/") || strings.Contains(mediaType, "json") || strings.Contains(mediaType, "xml")
}

func isHTMLMediaType(mediaType string) bool {
	mediaType = strings.ToLower(mediaType)
	return strings.HasPrefix(mediaType, "text/html") || strings.Contains(mediaType, "html")
}

func pendingIdeaQuestions(bundle store.RunBundle) *IdeaQuestionView {
	if bundle.Run.Status != store.RunStatusAwaitingHuman {
		return nil
	}
	type questionPacket struct {
		StageID   string   `json:"stage_id"`
		Round     int      `json:"round"`
		MaxRounds int      `json:"max_rounds"`
		Questions []string `json:"questions"`
	}
	type answerPacket struct {
		Round int `json:"round"`
	}
	answered := map[int]bool{}
	var latest questionPacket
	for _, artifact := range bundle.Artifacts {
		switch artifact.Kind {
		case "idea_refinement_answers":
			var packet answerPacket
			if readArtifactJSON(artifact, &packet) == nil && packet.Round > 0 {
				answered[packet.Round] = true
			}
		case "idea_refinement_questions":
			var packet questionPacket
			if readArtifactJSON(artifact, &packet) == nil && packet.Round > latest.Round {
				latest = packet
			}
		}
	}
	if latest.StageID == "" || latest.Round == 0 || answered[latest.Round] || len(latest.Questions) == 0 {
		return nil
	}
	return &IdeaQuestionView{StageID: latest.StageID, Round: latest.Round, MaxRounds: latest.MaxRounds, Questions: latest.Questions}
}

func readArtifactJSON(artifact store.Artifact, target any) error {
	content, err := os.ReadFile(artifact.Path)
	if err != nil {
		return err
	}
	return json.Unmarshal(content, target)
}

func prReadyFromEvents(bundle store.RunBundle) PRReadyView {
	var out PRReadyView
	for _, ev := range bundle.Events {
		stageType, _ := ev.Data["stage_type"].(string)
		if stageType != "pr_ready" && stageType != "pr_creation" && ev.Type != "run.completed" {
			continue
		}
		if branch, ok := ev.Data["branch"].(string); ok && branch != "" {
			out.Branch = branch
			out.Ready = true
		}
		if commitSHA, ok := ev.Data["commit_sha"].(string); ok && commitSHA != "" {
			out.CommitSHA = commitSHA
		}
		if diffID, ok := ev.Data["diff_artifact_id"].(string); ok && diffID != "" {
			out.DiffArtifactID = diffID
		}
	}
	return out
}

func short(s string) string {
	if len(s) <= 14 {
		return s
	}
	return s[:14]
}

func statusClass(status string) string {
	switch status {
	case "completed", "connected", "approved", "changes_requested":
		return "status-completed"
	case "failed", "invalid", "down", "cancelled":
		return "status-failed"
	case "running", "awaiting_human", "suspect":
		return "status-running"
	default:
		return "status-pending"
	}
}

func statusLabel(status string) string {
	if status == "pending" {
		return "queued"
	}
	return status
}

func compactHTML(in string) string {
	lines := strings.Split(in, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return strings.Join(out, "")
}
