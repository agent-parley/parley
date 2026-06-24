package web

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"sort"
	"strings"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
)

//go:embed templates/*.html assets/*
var Embedded embed.FS

type Renderer interface {
	ExecutePage(name string, data any) (string, error)
	RenderRunFragments(store.RunBundle) (string, error)
	RenderProjectChat(ProjectChatData) (string, error)
	RenderProjectHomeFragments(ProjectHomeFragmentsData) (string, error)
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
	Tasks           ProjectTasksData
	Chat            ProjectChatData
	Notice          *Notice
	CSRF            string
	Title           string
}

type ProjectHomeFragmentsData struct {
	Tasks ProjectTasksData
	Chat  ProjectChatData
}

type ProjectTasksData struct {
	Project store.Project
	Items   []TaskOverviewItem
	Queue   QueueView
	CSRF    string
}

type TaskOverviewItem struct {
	Task         store.Task
	Run          store.Run
	Idea         string
	Status       string
	Link         string
	DetailID     string
	NeedsYou     bool
	NeedsReason  string
	CurrentStage string
	LastUpdate   string
	Performer    string
	Runner       string
	StartQueued  bool
	UpdatedAt    string
}

type ProjectsIndexData struct {
	Projects      []ProjectNeedsYouView
	TotalNeedsYou int
	Title         string
}

type ProjectNeedsYouView struct {
	Project       store.Project
	NeedsYouCount int
}

type ProjectChatData struct {
	Project              store.Project
	Conversation         store.Conversation
	Messages             []store.Message
	TaskRuns             []TaskRunView
	IdeaQuestionMessages []ChatIdeaQuestionMessage
	CSRF                 string
}

type TaskRunView struct {
	Task        store.Task
	Run         store.Run
	HasRun      bool
	ReviewReady bool
}

type ChatIdeaQuestionMessage struct {
	Task      store.Task
	Run       store.Run
	StageID   string
	Round     int
	MaxRounds int
	Questions []string
	CreatedAt string

	AnswerText string
	Answers    []orchestrator.DeepIdeaAnswer
	AnsweredAt string
	Answered   bool
	Pending    bool
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
	Tab   string
}

type RunView struct {
	store.RunBundle
	ArtifactViews        []ArtifactView
	DiffPatch            ArtifactView
	PRReady              PRReadyView
	PendingIdeaQuestions *IdeaQuestionView
	PendingHumanReview   *HumanReviewView
	StageGroups          []StageGroupView
	TaskPlan             TaskPlanView
	Outcome              OutcomeView
	DiffLines            []DiffLineView
	DiffIsLong           bool
	CSRF                 string
}

type StageGroupView struct {
	Stage     store.Stage
	Label     string
	Performer string
	Summary   string
	Expanded  bool
	Events    []EventView
}

type EventView struct {
	Event      event.Event
	Family     string
	StageType  string
	StageLabel string
}

type DiffLineView struct {
	Text  string
	Class string
}

type TaskPlanView struct {
	Available  bool
	Artifact   ArtifactView
	StageID    string
	StageLabel string
	Fallback   string
}

type OutcomeView struct {
	Summary       string
	LastEvent     *EventView
	TerminalEvent *EventView
}

type IdeaQuestionView struct {
	StageID   string
	Round     int
	MaxRounds int
	Questions []string
}

type HumanReviewView struct {
	StageID          string
	WorkflowStageID  string
	PacketArtifactID string
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

func NewProjectTasksData(project store.Project, bundles []store.RunBundle, queue QueueView, csrf string) ProjectTasksData {
	items := make([]TaskOverviewItem, 0, len(bundles))
	for _, bundle := range bundles {
		items = append(items, newTaskOverviewItem(project.ID, bundle, queue.AutoWhenReady))
	}
	sort.SliceStable(items, func(i, j int) bool {
		leftRank := taskOverviewRank(items[i])
		rightRank := taskOverviewRank(items[j])
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		if items[i].UpdatedAt == items[j].UpdatedAt {
			return items[i].Run.ID < items[j].Run.ID
		}
		return items[i].UpdatedAt > items[j].UpdatedAt
	})
	return ProjectTasksData{Project: project, Items: items, Queue: queue, CSRF: csrf}
}

func newTaskOverviewItem(projectID string, bundle store.RunBundle, autoWhenReady bool) TaskOverviewItem {
	idea := strings.TrimSpace(bundle.Task.Idea)
	if idea == "" {
		idea = strings.TrimSpace(bundle.Run.Idea)
	}
	stage, performer := taskOverviewStage(bundle)
	lastUpdate := taskOverviewLastUpdate(bundle)
	link := "/projects/" + projectID + "/runs/" + bundle.Run.ID
	item := TaskOverviewItem{
		Task:         bundle.Task,
		Run:          bundle.Run,
		Idea:         idea,
		Status:       bundle.Run.Status,
		Link:         link,
		DetailID:     "task-" + bundle.Run.ID + "-modal",
		CurrentStage: stage,
		LastUpdate:   lastUpdate,
		Performer:    performer,
		Runner:       taskOverviewRunner(bundle.Events),
		StartQueued:  bundle.Run.Status == store.RunStatusPending && !autoWhenReady,
		UpdatedAt:    taskOverviewUpdatedAt(bundle),
	}
	if bundle.Run.Status == store.RunStatusAwaitingHuman {
		item.NeedsYou = true
		if pendingIdeaQuestions(bundle) != nil {
			item.NeedsReason = "question pending"
		} else {
			item.NeedsReason = "diff ready"
			item.Link += "?tab=review"
		}
	}
	return item
}

func taskOverviewRank(item TaskOverviewItem) int {
	if item.NeedsYou {
		return 0
	}
	if !store.RunStatusIsTerminal(item.Status) {
		return 1
	}
	return 2
}

func taskOverviewStage(bundle store.RunBundle) (string, string) {
	for _, stage := range bundle.Stages {
		if stage.Status == store.StageStatusRunning || stage.Status == store.RunStatusAwaitingHuman {
			return stageLabel(stage.StageType), performer(stage)
		}
	}
	for i := len(bundle.Stages) - 1; i >= 0; i-- {
		stage := bundle.Stages[i]
		if stage.Status != store.StageStatusPending {
			return stageLabel(stage.StageType), performer(stage)
		}
	}
	if len(bundle.Stages) > 0 {
		stage := bundle.Stages[0]
		return stageLabel(stage.StageType), performer(stage)
	}
	return "Run lifecycle", "Harness"
}

func taskOverviewLastUpdate(bundle store.RunBundle) string {
	if len(bundle.Events) == 0 {
		return timeLabel(taskOverviewUpdatedAt(bundle))
	}
	latest := bundle.Events[len(bundle.Events)-1]
	label := timeLabel(latest.Timestamp)
	if latest.Summary != "" {
		if label != "" {
			return label + " · " + latest.Summary
		}
		return latest.Summary
	}
	return label
}

func taskOverviewUpdatedAt(bundle store.RunBundle) string {
	if bundle.Run.UpdatedAt != "" {
		return bundle.Run.UpdatedAt
	}
	return bundle.Run.CreatedAt
}

func taskOverviewRunner(events []event.Event) string {
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if runnerID := eventString(ev, "runner_id"); runnerID != "" {
			return runnerID
		}
		if ev.Actor.ID != "" && ev.Actor.Kind != "" {
			return ev.Actor.Kind + "/" + ev.Actor.ID
		}
	}
	return "—"
}

func NewTaskRunViews(tasks []store.Task, runs []store.Run) []TaskRunView {
	latestRunByTask := map[string]store.Run{}
	for _, run := range runs {
		if _, exists := latestRunByTask[run.TaskID]; !exists {
			latestRunByTask[run.TaskID] = run
		}
	}
	views := make([]TaskRunView, 0, len(tasks))
	for _, task := range tasks {
		view := TaskRunView{Task: task}
		if run, ok := latestRunByTask[task.ID]; ok {
			view.Run = run
			view.HasRun = true
			view.ReviewReady = run.Status == store.RunStatusAwaitingHuman
		}
		views = append(views, view)
	}
	return views
}

func NewChatIdeaQuestionMessages(bundles []store.RunBundle) []ChatIdeaQuestionMessage {
	var messages []ChatIdeaQuestionMessage
	for _, bundle := range bundles {
		if contract.NormalizeRefinementLevel(bundle.Run.RefinementLevel) != contract.RefinementLevelDeep {
			continue
		}
		rounds := map[int]*ChatIdeaQuestionMessage{}
		for _, artifact := range bundle.Artifacts {
			switch artifact.Kind {
			case "idea_refinement_questions":
				var packet chatIdeaQuestionPacket
				if readArtifactJSON(artifact, &packet) != nil || packet.Round <= 0 || len(packet.Questions) == 0 {
					continue
				}
				round := chatIdeaQuestionRound(rounds, packet.Round, bundle)
				round.StageID = packet.StageID
				round.MaxRounds = packet.MaxRounds
				round.Questions = append([]string{}, packet.Questions...)
				round.CreatedAt = artifact.CreatedAt
			case "idea_refinement_answers":
				var packet chatIdeaAnswerPacket
				if readArtifactJSON(artifact, &packet) != nil || packet.Round <= 0 {
					continue
				}
				round := chatIdeaQuestionRound(rounds, packet.Round, bundle)
				round.AnswerText = strings.TrimSpace(packet.AnswerText)
				round.Answers = append([]orchestrator.DeepIdeaAnswer{}, packet.Answers...)
				round.AnsweredAt = artifact.CreatedAt
				round.Answered = true
			}
		}
		for _, round := range rounds {
			if len(round.Questions) == 0 {
				continue
			}
			round.Pending = bundle.Run.Status == store.RunStatusAwaitingHuman && !round.Answered && round.StageID != ""
			messages = append(messages, *round)
		}
	}
	sort.SliceStable(messages, func(i, j int) bool {
		left := chatIdeaQuestionMessageTime(messages[i])
		right := chatIdeaQuestionMessageTime(messages[j])
		if left == right {
			return messages[i].Round < messages[j].Round
		}
		return left < right
	})
	return messages
}

type chatIdeaQuestionPacket struct {
	StageID   string   `json:"stage_id"`
	Round     int      `json:"round"`
	MaxRounds int      `json:"max_rounds"`
	Questions []string `json:"questions"`
}

type chatIdeaAnswerPacket struct {
	Round      int                           `json:"round"`
	AnswerText string                        `json:"answer_text,omitempty"`
	Answers    []orchestrator.DeepIdeaAnswer `json:"answers,omitempty"`
}

func chatIdeaQuestionRound(rounds map[int]*ChatIdeaQuestionMessage, round int, bundle store.RunBundle) *ChatIdeaQuestionMessage {
	view := rounds[round]
	if view == nil {
		view = &ChatIdeaQuestionMessage{Task: bundle.Task, Run: bundle.Run, Round: round, CreatedAt: bundle.Run.CreatedAt}
		rounds[round] = view
	}
	return view
}

func chatIdeaQuestionMessageTime(message ChatIdeaQuestionMessage) string {
	if message.CreatedAt != "" {
		return message.CreatedAt
	}
	if message.AnsweredAt != "" {
		return message.AnsweredAt
	}
	return message.Run.CreatedAt
}

func NewRunData(bundle store.RunBundle, csrf, title, tab string) RunData {
	view := NewRunView(bundle)
	view.CSRF = csrf
	return RunData{View: view, CSRF: csrf, Title: title, Tab: normalizeRunTab(tab)}
}

func NewRunView(bundle store.RunBundle) RunView {
	view := RunView{RunBundle: bundle, PRReady: prReadyFromEvents(bundle), PendingIdeaQuestions: pendingIdeaQuestions(bundle), PendingHumanReview: pendingHumanReview(bundle)}
	for _, artifact := range bundle.Artifacts {
		artifactView := newArtifactView(artifact)
		view.ArtifactViews = append(view.ArtifactViews, artifactView)
		if artifact.Kind == "diff_patch" {
			view.DiffPatch = artifactView
		}
	}
	view.StageGroups = stageGroups(bundle.Stages, bundle.Events)
	view.TaskPlan = taskPlanView(bundle, view.ArtifactViews)
	view.Outcome = outcomeView(bundle)
	view.DiffLines = diffLines(view.DiffPatch.Preview)
	view.DiffIsLong = len(view.DiffLines) > 80
	return view
}

func NewRenderer() (*TemplateRenderer, error) {
	funcs := template.FuncMap{
		"short":         short,
		"statusClass":   statusClass,
		"statusLabel":   statusLabel,
		"stageLabel":    stageLabel,
		"artifactLabel": artifactLabel,
		"timeLabel":     timeLabel,
	}
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
	for _, name := range []string{"run_summary.html", "story_panel.html", "review_panel.html"} {
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

func (r *TemplateRenderer) RenderProjectHomeFragments(data ProjectHomeFragmentsData) (string, error) {
	var buf bytes.Buffer
	for _, name := range []string{"tasks_overview.html", "project_chat.html"} {
		var fragmentData any = data.Tasks
		if name == "project_chat.html" {
			fragmentData = data.Chat
		}
		if err := r.templates.ExecuteTemplate(&buf, name, fragmentData); err != nil {
			return "", fmt.Errorf("execute project home fragment %s: %w", name, err)
		}
	}
	return compactHTML(buf.String()), nil
}

func stageGroups(stages []store.Stage, events []event.Event) []StageGroupView {
	groups := make([]StageGroupView, 0, len(stages))
	for _, stage := range stages {
		group := StageGroupView{Stage: stage, Label: stageLabel(stage.StageType), Performer: performer(stage)}
		for _, ev := range events {
			if stageTypeFromEvent(ev) == stage.StageType {
				group.Events = append(group.Events, newEventView(ev))
			}
		}
		group.Summary = stageSummary(stage, group.Events)
		group.Expanded = stage.Status == store.StageStatusRunning || stage.Status == "awaiting_human" || len(group.Events) == 0
		groups = append(groups, group)
	}
	return groups
}

func newEventView(ev event.Event) EventView {
	stageType := stageTypeFromEvent(ev)
	return EventView{Event: ev, Family: eventFamily(ev.Type), StageType: stageType, StageLabel: stageLabel(stageType)}
}

func performer(stage store.Stage) string {
	if stage.Adapter != "" {
		return "Agent profile " + stage.Adapter
	}
	return "Harness"
}

func stageSummary(stage store.Stage, events []EventView) string {
	if len(events) > 0 {
		return events[len(events)-1].Event.Summary
	}
	switch stage.Status {
	case store.StageStatusPending:
		return "Waiting for the previous stage."
	case store.StageStatusRunning:
		return stageLabel(stage.StageType) + " is running."
	case "awaiting_human":
		return stageLabel(stage.StageType) + " is waiting for input."
	case "completed":
		return stageLabel(stage.StageType) + " completed."
	case "failed":
		return stageLabel(stage.StageType) + " failed."
	default:
		return stageLabel(stage.StageType) + " is " + stage.Status + "."
	}
}

func taskPlanView(bundle store.RunBundle, artifacts []ArtifactView) TaskPlanView {
	byID := map[string]ArtifactView{}
	for _, artifact := range artifacts {
		byID[artifact.ID] = artifact
	}
	for i := len(bundle.Stages) - 1; i >= 0; i-- {
		stage := bundle.Stages[i]
		if stage.TaskPlanArtifactID == "" {
			continue
		}
		view := TaskPlanView{Available: true, StageID: stage.ID, StageLabel: stageLabel(stage.StageType)}
		if artifact, ok := byID[stage.TaskPlanArtifactID]; ok {
			view.Artifact = artifact
		} else {
			view.Artifact = ArtifactView{ID: stage.TaskPlanArtifactID, Kind: "task_plan"}
		}
		return view
	}
	fallback := strings.TrimSpace(bundle.Task.Idea)
	if fallback == "" {
		fallback = strings.TrimSpace(bundle.Run.Idea)
	}
	return TaskPlanView{Fallback: fallback}
}

func outcomeView(bundle store.RunBundle) OutcomeView {
	out := OutcomeView{Summary: "Run is " + statusLabel(bundle.Run.Status) + "."}
	for _, ev := range bundle.Events {
		view := newEventView(ev)
		out.LastEvent = &view
		if strings.HasPrefix(ev.Type, "run.") && ev.Summary != "" {
			terminalStatus, _ := ev.Data["terminal_status"].(string)
			if terminalStatus != "" || ev.Type == "run.completed" || ev.Type == "run.failed" || ev.Type == "run.cancelled" {
				terminal := view
				out.TerminalEvent = &terminal
				out.Summary = ev.Summary
			}
		}
	}
	if out.TerminalEvent == nil && out.LastEvent != nil && out.LastEvent.Event.Summary != "" {
		out.Summary = out.LastEvent.Event.Summary
	}
	return out
}

func diffLines(preview string) []DiffLineView {
	if preview == "" {
		return nil
	}
	rawLines := strings.Split(strings.TrimRight(preview, "\n"), "\n")
	lines := make([]DiffLineView, 0, len(rawLines))
	for _, line := range rawLines {
		lines = append(lines, DiffLineView{Text: line, Class: diffLineClass(line)})
	}
	return lines
}

func diffLineClass(line string) string {
	switch {
	case strings.HasPrefix(line, "+") && !strings.HasPrefix(line, "+++"):
		return "diff-add"
	case strings.HasPrefix(line, "-") && !strings.HasPrefix(line, "---"):
		return "diff-del"
	case strings.HasPrefix(line, "diff "), strings.HasPrefix(line, "index "), strings.HasPrefix(line, "@@"), strings.HasPrefix(line, "---"), strings.HasPrefix(line, "+++"):
		return "diff-meta"
	default:
		return ""
	}
}

func stageTypeFromEvent(ev event.Event) string {
	if ev.Data == nil {
		return ""
	}
	stageType, _ := ev.Data["stage_type"].(string)
	return stageType
}

func eventFamily(eventType string) string {
	if i := strings.Index(eventType, "."); i > 0 {
		return eventType[:i]
	}
	return eventType
}

func stageLabel(stageType string) string {
	switch stageType {
	case contract.StageTypeIdeaIntake:
		return "Idea intake"
	case contract.StageTypeIdeaRefinement:
		return "Idea refinement"
	case contract.StageTypeReview:
		return "Review"
	case contract.StageTypeImplementation:
		return "Implementation"
	case contract.StageTypeValidation:
		return "Validation"
	case contract.StageTypeCommit:
		return "Commit"
	case contract.StageTypePRCreation:
		return "PR creation"
	case contract.StageTypePRReady:
		return "PR-ready"
	case contract.StageTypeMemoryUpdate:
		return "Memory update"
	case contract.StageTypeStopReport:
		return "Stop report"
	case "":
		return "Run lifecycle"
	default:
		return strings.ReplaceAll(stageType, "_", " ")
	}
}

func artifactLabel(kind string) string {
	switch kind {
	case "diff_patch":
		return "Diff patch"
	case "task_plan":
		return "Task plan"
	case "agent_output":
		return "Agent output"
	case "stage_brief":
		return "Stage brief"
	case "report":
		return "Run report"
	case "event_log":
		return "Event log"
	default:
		return strings.ReplaceAll(kind, "_", " ")
	}
}

func timeLabel(value string) string {
	if len(value) >= 16 && value[10] == 'T' {
		return value[11:16]
	}
	return value
}

func normalizeRunTab(tab string) string {
	if tab == "review" {
		return "review"
	}
	return "story"
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

func pendingHumanReview(bundle store.RunBundle) *HumanReviewView {
	if bundle.Run.Status != store.RunStatusAwaitingHuman {
		return nil
	}
	var view HumanReviewView
	for i := len(bundle.Events) - 1; i >= 0; i-- {
		ev := bundle.Events[i]
		if ev.Type != "stage.awaiting_human" {
			continue
		}
		packetID := eventString(ev, "human_review_packet_id")
		if packetID == "" {
			continue
		}
		stageID := eventString(ev, "pending_stage_id")
		if stageID == "" {
			stageID = eventString(ev, "stage_id")
		}
		if stageID == "" {
			continue
		}
		view = HumanReviewView{StageID: stageID, WorkflowStageID: eventString(ev, "workflow_stage_id"), PacketArtifactID: packetID}
		break
	}
	if view.StageID == "" {
		return nil
	}
	for _, stage := range bundle.Stages {
		if stage.ID == view.StageID && stage.Status == store.StageStatusRunning && stage.StageType == contract.StageTypeReview {
			return &view
		}
	}
	return nil
}

func eventString(ev event.Event, key string) string {
	value, _ := ev.Data[key].(string)
	return strings.TrimSpace(value)
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
