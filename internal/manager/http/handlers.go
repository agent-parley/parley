package managerhttp

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/agent-parley/parley/internal/manager/orchestrator"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/manager/web"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
)

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	s.handleProjectIndex(w, r, store.DefaultProjectID)
}

func (s *Server) handleProjectsIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/projects" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	data, err := s.projectsIndexData(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writePage(w, "projects.html", data)
}

func (s *Server) handleProjectPath(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/projects/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	projectID := parts[0]
	if len(parts) == 1 {
		s.handleProjectIndex(w, r, projectID)
		return
	}
	if parts[1] == "chat" {
		s.handleProjectChatPath(w, r, projectID, parts[2:])
		return
	}
	if parts[1] == "settings" {
		s.handleProjectSettingsPath(w, r, projectID, parts[2:])
		return
	}
	if parts[1] != "runs" {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 2 {
		s.handleProjectRuns(w, r, projectID)
		return
	}
	runID := parts[2]
	if runID == "" {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 4 && parts[3] == "events" {
		s.handleRunEvents(w, r, projectID, runID)
		return
	}
	if len(parts) == 4 && parts[3] == "cancel" {
		s.handleCancelRun(w, r, projectID, runID)
		return
	}
	if len(parts) == 4 && parts[3] == "start" {
		s.handleStartQueuedRun(w, r, projectID, runID)
		return
	}
	if len(parts) == 6 && parts[3] == "stages" && parts[5] == "rerun" {
		s.handleReRunStage(w, r, projectID, runID, parts[4])
		return
	}
	if len(parts) == 6 && parts[3] == "human-stages" && parts[5] == "verdict" {
		s.handleHumanReviewVerdict(w, r, projectID, runID, parts[4])
		return
	}
	if len(parts) == 3 {
		s.handleRunDetail(w, r, projectID, runID)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleProjectIndex(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	data, err := s.indexData(r, projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writePage(w, "index.html", data)
}

func (s *Server) indexData(r *http.Request, projectID string) (web.IndexData, error) {
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		return web.IndexData{}, err
	}
	runs, err := s.store.ListRunsForProject(r.Context(), projectID)
	if err != nil {
		return web.IndexData{}, err
	}
	runners, err := s.store.ListRunners(r.Context())
	if err != nil {
		return web.IndexData{}, err
	}
	runnerEventPage, err := s.store.ListSystemEventsPage(r.Context(), parseInt64Query(r, "runner_events_before"), 50)
	if err != nil {
		return web.IndexData{}, err
	}
	queueState, err := s.engine.QueueState(r.Context())
	if err != nil {
		return web.IndexData{}, err
	}
	queue := web.NewQueueView(queueState)
	csrf := csrfFromContext(r.Context())
	tasks, err := s.projectTasksDataFromRuns(r, project, runs, queue, csrf)
	if err != nil {
		return web.IndexData{}, err
	}
	chat, err := s.projectChatData(r, project)
	if err != nil {
		return web.IndexData{}, err
	}
	chat.CSRF = csrf
	notifications, err := s.notificationCenterData(r.Context(), csrf)
	if err != nil {
		return web.IndexData{}, err
	}
	return web.IndexData{Project: project, Runs: runs, Runners: runners, RunnerEventPage: runnerEventPage, Queue: queue, Tasks: tasks, Chat: chat, Notifications: notifications, CSRF: csrf, Title: "Parley · " + project.Name}, nil
}

func (s *Server) projectsIndexData(r *http.Request) (web.ProjectsIndexData, error) {
	projects, err := s.store.ListProjects(r.Context())
	if err != nil {
		return web.ProjectsIndexData{}, err
	}
	views := make([]web.ProjectNeedsYouView, 0, len(projects))
	total := 0
	for _, project := range projects {
		count, err := s.store.CountRunsByProjectStatus(r.Context(), project.ID, store.RunStatusAwaitingHuman)
		if err != nil {
			return web.ProjectsIndexData{}, err
		}
		total += count
		views = append(views, web.ProjectNeedsYouView{Project: project, NeedsYouCount: count})
	}
	csrf := csrfFromContext(r.Context())
	notifications, err := s.notificationCenterData(r.Context(), csrf)
	if err != nil {
		return web.ProjectsIndexData{}, err
	}
	return web.ProjectsIndexData{Projects: views, TotalNeedsYou: total, Notifications: notifications, CSRF: csrf, Title: "Parley · Projects"}, nil
}

func (s *Server) projectTasksData(r *http.Request, project store.Project, queue web.QueueView, csrf string) (web.ProjectTasksData, error) {
	runs, err := s.store.ListRunsForProject(r.Context(), project.ID)
	if err != nil {
		return web.ProjectTasksData{}, err
	}
	return s.projectTasksDataFromRuns(r, project, runs, queue, csrf)
}

func (s *Server) projectTasksDataFromRuns(r *http.Request, project store.Project, runs []store.Run, queue web.QueueView, csrf string) (web.ProjectTasksData, error) {
	bundles := make([]store.RunBundle, 0, len(runs))
	for _, run := range runs {
		bundle, err := s.store.RunBundle(r.Context(), run.ID)
		if err != nil {
			return web.ProjectTasksData{}, err
		}
		bundles = append(bundles, bundle)
	}
	return web.NewProjectTasksData(project, bundles, queue, csrf), nil
}

func (s *Server) projectHomeFragmentsData(r *http.Request, project store.Project) (web.ProjectHomeFragmentsData, error) {
	queueState, err := s.engine.QueueState(r.Context())
	if err != nil {
		return web.ProjectHomeFragmentsData{}, err
	}
	csrf := csrfFromContext(r.Context())
	queue := web.NewQueueView(queueState)
	tasks, err := s.projectTasksData(r, project, queue, csrf)
	if err != nil {
		return web.ProjectHomeFragmentsData{}, err
	}
	chat, err := s.projectChatData(r, project)
	if err != nil {
		return web.ProjectHomeFragmentsData{}, err
	}
	chat.CSRF = csrf
	return web.ProjectHomeFragmentsData{Tasks: tasks, Chat: chat}, nil
}

func (s *Server) handleProjectSettingsPath(w http.ResponseWriter, r *http.Request, projectID string, parts []string) {
	if len(parts) == 0 {
		s.handleProjectSettings(w, r, projectID)
		return
	}
	if len(parts) == 1 && parts[0] == "notifications" {
		s.handleProjectNotificationSettingsSave(w, r, projectID)
		return
	}
	if len(parts) == 2 && parts[0] == "memory" && parts[1] == "export" {
		s.handleProjectMemoryExport(w, r, projectID)
		return
	}
	if len(parts) == 1 && validProjectSettingsKind(parts[0]) {
		s.handleProjectSettingsSave(w, r, projectID, parts[0])
		return
	}
	if len(parts) == 2 && validProjectSettingsKind(parts[0]) && parts[1] == "candidate" {
		s.handleProjectSettingsCandidate(w, r, projectID, parts[0])
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleProjectSettings(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, err := s.projectSettingsData(r, project)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writePage(w, "settings.html", data)
}

func (s *Server) handleProjectSettingsSave(w http.ResponseWriter, r *http.Request, projectID, kind string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	content := r.Form.Get("content")
	var (
		project store.Project
		err     error
	)
	switch kind {
	case "rules":
		project, err = s.store.UpdateProjectRules(r.Context(), projectID, content)
	case "preferences":
		project, err = s.store.UpdateProjectPreferences(r.Context(), projectID, content)
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	section, err := s.projectSettingsSectionData(r, project, kind, content, "", "saved · updated "+project.UpdatedAt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeFragment(w, http.StatusAccepted, section)
}

func (s *Server) handleProjectNotificationSettingsSave(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	prefs := store.ProjectNotificationPreferences{
		OnlyWhenNeeded: r.Form.Get("only_when_needed") != "",
		WhenFinished:   r.Form.Get("when_finished") != "",
	}
	project, err := s.store.UpdateProjectNotificationPreferences(r.Context(), projectID, prefs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := s.projectNotificationSettingsData(r, project, "", "saved · updated "+project.UpdatedAt)
	fragment, err := s.renderer.ExecutePage("notification_settings.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(fragment))
}

func (s *Server) handleProjectSettingsCandidate(w http.ResponseWriter, r *http.Request, projectID, kind string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	repoPath, err := s.projectRepositoryPath(r, projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if repoPath == "" {
		section, err := s.projectSettingsSectionData(r, project, kind, projectSettingsSavedValue(project, kind), "No repository configured; repo candidate loading is unavailable.", "saved · updated "+project.UpdatedAt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.writeFragment(w, http.StatusOK, section)
		return
	}
	candidate, err := projectSettingsCandidate(repoPath, kind)
	if err != nil {
		notice := "Could not load `" + projectSettingsCandidatePath(kind) + "` from the repository."
		if errors.Is(err, os.ErrNotExist) {
			notice = "No `" + projectSettingsCandidatePath(kind) + "` in the repository."
		}
		section, sectionErr := s.projectSettingsSectionData(r, project, kind, projectSettingsSavedValue(project, kind), notice, "saved · updated "+project.UpdatedAt)
		if sectionErr != nil {
			http.Error(w, sectionErr.Error(), http.StatusInternalServerError)
			return
		}
		s.writeFragment(w, http.StatusOK, section)
		return
	}
	section, err := s.projectSettingsSectionData(r, project, kind, candidate, "", "repo candidate loaded as an unsaved draft · Save to commit")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeFragment(w, http.StatusOK, section)
}

func (s *Server) projectSettingsData(r *http.Request, project store.Project) (web.ProjectSettingsData, error) {
	csrf := csrfFromContext(r.Context())
	rules, err := s.projectSettingsSectionData(r, project, "rules", project.ProjectRules, "", "saved · updated "+project.UpdatedAt)
	if err != nil {
		return web.ProjectSettingsData{}, err
	}
	preferences, err := s.projectSettingsSectionData(r, project, "preferences", project.ProjectPreferences, "", "saved · updated "+project.UpdatedAt)
	if err != nil {
		return web.ProjectSettingsData{}, err
	}
	memoryExport, err := s.projectMemoryExportData(r, project, "", "", nil)
	if err != nil {
		return web.ProjectSettingsData{}, err
	}
	notificationSettings := s.projectNotificationSettingsData(r, project, "", "saved · updated "+project.UpdatedAt)
	center, err := s.notificationCenterData(r.Context(), csrf)
	if err != nil {
		return web.ProjectSettingsData{}, err
	}
	return web.ProjectSettingsData{Project: project, Rules: rules, Preferences: preferences, Memory: memoryExport, Notifications: notificationSettings, Center: center, CSRF: csrf, Title: "Parley · " + project.Name + " Settings"}, nil
}

func (s *Server) projectNotificationSettingsData(r *http.Request, project store.Project, notice, status string) web.NotificationSettingsData {
	return web.NotificationSettingsData{
		Project:        project,
		OnlyWhenNeeded: project.NotificationOnlyWhenNeeded,
		WhenFinished:   project.NotificationWhenFinished,
		SavePath:       "/projects/" + project.ID + "/settings/notifications",
		Notice:         notice,
		Status:         status,
		CSRF:           csrfFromContext(r.Context()),
	}
}

func (s *Server) projectSettingsSectionData(r *http.Request, project store.Project, kind, textareaValue, notice, status string) (web.ProjectSettingsSectionData, error) {
	repoPath, err := s.projectRepositoryPath(r, project.ID)
	if err != nil {
		return web.ProjectSettingsSectionData{}, err
	}
	candidateDiffers := false
	if repoPath != "" {
		if candidate, err := projectSettingsCandidate(repoPath, kind); err == nil {
			candidateDiffers = projectSettingsCandidateDiffers(candidate, projectSettingsSavedValue(project, kind))
		}
	}
	return web.ProjectSettingsSectionData{
		Project:              project,
		Kind:                 kind,
		Label:                projectSettingsLabel(kind),
		ShortLabel:           projectSettingsShortLabel(kind),
		Help:                 projectSettingsHelp(kind),
		TextareaID:           "project-settings-" + kind,
		TextareaValue:        textareaValue,
		Placeholder:          projectSettingsPlaceholder(kind),
		CandidatePath:        projectSettingsCandidatePath(kind),
		SavePath:             "/projects/" + project.ID + "/settings/" + kind,
		LoadCandidatePath:    "/projects/" + project.ID + "/settings/" + kind + "/candidate",
		RepositoryConfigured: repoPath != "",
		CandidateDiffers:     candidateDiffers,
		Notice:               notice,
		Status:               status,
		CSRF:                 csrfFromContext(r.Context()),
	}, nil
}

func (s *Server) projectRepositoryPath(r *http.Request, projectID string) (string, error) {
	repositoryID, err := s.store.DefaultRepositoryID(r.Context(), projectID)
	if err != nil {
		return "", err
	}
	if repositoryID == "" {
		return "", nil
	}
	repo, err := s.store.GetRepository(r.Context(), repositoryID)
	if err != nil {
		return "", err
	}
	if repo.ProjectID != projectID {
		return "", fmt.Errorf("repository %s does not belong to project %s", repo.ID, projectID)
	}
	return repo.Path, nil
}

func validProjectSettingsKind(kind string) bool {
	return kind == "rules" || kind == "preferences"
}

func projectSettingsSavedValue(project store.Project, kind string) string {
	if kind == "preferences" {
		return project.ProjectPreferences
	}
	return project.ProjectRules
}

func projectSettingsCandidate(repoPath, kind string) (string, error) {
	if kind == "preferences" {
		return store.ReadProjectPreferencesCandidate(repoPath)
	}
	return store.ReadProjectRulesCandidate(repoPath)
}

func projectSettingsCandidateDiffers(candidate, saved string) bool {
	return projectSettingsComparisonValue(candidate) != projectSettingsComparisonValue(saved)
}

func projectSettingsComparisonValue(value string) string {
	return strings.TrimRightFunc(value, unicode.IsSpace)
}

func projectSettingsCandidatePath(kind string) string {
	if kind == "preferences" {
		return store.ProjectPreferencesCandidatePath
	}
	return store.ProjectRulesCandidatePath
}

func projectSettingsLabel(kind string) string {
	if kind == "preferences" {
		return "Preferences"
	}
	return "Rules"
}

func projectSettingsShortLabel(kind string) string {
	if kind == "preferences" {
		return "preferences"
	}
	return "rules"
}

func projectSettingsHelp(kind string) string {
	if kind == "preferences" {
		return "Lower-precedence operator preferences that shape how Parley communicates and finishes work. Empty is valid."
	}
	return "Authoritative project rules Parley includes in stage briefs when present. Empty is valid."
}

func projectSettingsPlaceholder(kind string) string {
	if kind == "preferences" {
		return "No project preferences set"
	}
	return "No project rules set"
}

func (s *Server) handleProjectChatPath(w http.ResponseWriter, r *http.Request, projectID string, parts []string) {
	if len(parts) != 1 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	switch parts[0] {
	case "messages":
		s.handleProjectChatMessage(w, r, projectID)
	case "cancel":
		s.handleProjectChatCancel(w, r, projectID)
	case "events":
		s.handleProjectChatEvents(w, r, projectID)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleProjectChatCancel(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	canceller, ok := s.engine.(interface {
		CancelProjectConversationTurn(context.Context, string) error
	})
	if !ok {
		http.Error(w, "conversation cancellation unavailable", http.StatusNotImplemented)
		return
	}
	if err := canceller.CancelProjectConversationTurn(r.Context(), project.ID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/projects/"+project.ID, http.StatusSeeOther)
}

func (s *Server) handleProjectChatMessage(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	message := strings.TrimSpace(r.Form.Get("message"))
	if message == "" {
		http.Error(w, "message is required", http.StatusBadRequest)
		return
	}
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if _, err := s.engine.SubmitConversationMessage(r.Context(), project.ID, message); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/projects/"+project.ID, http.StatusSeeOther)
}

func (s *Server) handleProjectChatEvents(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.security.requireSession(r) {
		http.Error(w, "session required", http.StatusUnauthorized)
		return
	}
	project, err := s.store.GetProject(r.Context(), projectID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	data, err := s.projectHomeFragmentsData(r, project)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fragment, err := s.renderer.RenderProjectHomeFragments(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writer, ok := NewSSEWriter(w)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	writer.Patch(event.Event{ProjectID: project.ID, Sequence: parseLastEventID(r)}, fragment)

	type chatPatch struct {
		event    event.Event
		fragment string
		refresh  bool
	}
	updates := make(chan chatPatch, 16)
	var unsubscribes []func()
	subscribe := func(topic string, refresh bool) {
		ch, unsubscribe := s.hub.Subscribe(topic)
		unsubscribes = append(unsubscribes, unsubscribe)
		go func() {
			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						return
					}
					update := chatPatch{event: msg.Event, fragment: msg.Fragment, refresh: refresh}
					select {
					case updates <- update:
					case <-r.Context().Done():
						return
					}
				case <-r.Context().Done():
					return
				}
			}
		}()
	}
	subscribe(projectChatTopic(project.ID), true)
	defer func() {
		for _, unsubscribe := range unsubscribes {
			unsubscribe()
		}
	}()
	for {
		select {
		case update := <-updates:
			patch := update.fragment
			if update.refresh {
				data, err := s.projectHomeFragmentsData(r, project)
				if err != nil {
					continue
				}
				patch, err = s.renderer.RenderProjectHomeFragments(data)
				if err != nil {
					continue
				}
			}
			writer.Patch(update.event, patch)
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) projectChatData(r *http.Request, project store.Project) (web.ProjectChatData, error) {
	conversation, err := s.store.EnsureProjectConversation(r.Context(), project.ID)
	if err != nil {
		return web.ProjectChatData{}, err
	}
	messages, err := s.store.ListMessagesForConversation(r.Context(), conversation.ID)
	if err != nil {
		return web.ProjectChatData{}, err
	}
	tasks, err := s.store.ListTasksForConversation(r.Context(), conversation.ID)
	if err != nil {
		return web.ProjectChatData{}, err
	}
	runs, err := s.store.ListRunsForProject(r.Context(), project.ID)
	if err != nil {
		return web.ProjectChatData{}, err
	}
	taskRuns := web.NewTaskRunViews(tasks, runs)
	turnState := web.ProjectChatTurnState{Status: "idle"}
	if reader, ok := s.engine.(interface {
		ConversationTurnState(string) orchestrator.ConversationTurnState
	}); ok {
		state := reader.ConversationTurnState(conversation.ID)
		turnState = web.ProjectChatTurnState{Status: state.Status, InFlight: state.InFlight, Queued: state.Queued, Cancellable: state.Cancellable}
	}
	return web.ProjectChatData{
		Project:      project,
		Conversation: conversation,
		Messages:     messages,
		TaskRuns:     taskRuns,
		TurnState:    turnState,
		CSRF:         csrfFromContext(r.Context()),
	}, nil
}

func (s *Server) broadcastProjectChat(r *http.Request, project store.Project) {
	data, err := s.projectHomeFragmentsData(r, project)
	if err != nil {
		return
	}
	fragment, err := s.renderer.RenderProjectHomeFragments(data)
	if err != nil {
		return
	}
	s.hub.Broadcast(projectChatTopic(project.ID), event.Event{ProjectID: project.ID, Sequence: time.Now().UnixNano(), Type: "conversation.updated", Actor: event.Actor{Kind: event.ActorKindUser, ID: "local"}, Summary: "conversation updated"}, fragment)
}

func projectChatTopic(projectID string) string {
	return "project:" + projectID + ":chat"
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/runs" {
		http.NotFound(w, r)
		return
	}
	s.handleProjectRuns(w, r, store.DefaultProjectID)
}

func (s *Server) handleProjectRuns(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	idea := strings.TrimSpace(r.Form.Get("idea"))
	if idea == "" {
		http.Error(w, "idea is required", http.StatusBadRequest)
		return
	}
	input := contract.TaskInput{Idea: idea, RefinementLevel: r.Form.Get("refinement_level"), WorkflowTemplateID: r.Form.Get("workflow_template_id")}
	input.RefinementLevel = contract.NormalizeRefinementLevel(input.RefinementLevel)
	if err := contract.ValidateRefinementLevel(input.RefinementLevel); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	runID, err := s.engine.StartProjectRunInput(r.Context(), projectID, input)
	if err != nil {
		var backlogErr orchestrator.QueueBacklogFullError
		if errors.As(err, &backlogErr) {
			data, pageErr := s.indexData(r, projectID)
			if pageErr != nil {
				http.Error(w, pageErr.Error(), http.StatusInternalServerError)
				return
			}
			data.Notice = backlogFullNotice(backlogErr, data.Queue)
			s.writePageStatus(w, "index.html", data, http.StatusTooManyRequests)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, projectRunPath(projectID, runID), http.StatusSeeOther)
}

func (s *Server) handleRunPath(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/runs/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	runID := parts[0]
	run, err := s.store.GetRun(r.Context(), runID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	projectID := run.ProjectID
	if len(parts) == 2 && parts[1] == "events" {
		s.handleRunEvents(w, r, projectID, runID)
		return
	}
	if len(parts) == 2 && parts[1] == "cancel" {
		s.handleCancelRun(w, r, projectID, runID)
		return
	}
	if len(parts) == 2 && parts[1] == "start" {
		s.handleStartQueuedRun(w, r, projectID, runID)
		return
	}
	if len(parts) == 4 && parts[1] == "stages" && parts[3] == "rerun" {
		s.handleReRunStage(w, r, projectID, runID, parts[2])
		return
	}
	if len(parts) == 4 && parts[1] == "human-stages" && parts[3] == "verdict" {
		s.handleHumanReviewVerdict(w, r, projectID, runID, parts[2])
		return
	}
	if len(parts) == 1 {
		target := projectRunPath(projectID, runID)
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		http.Redirect(w, r, target, http.StatusSeeOther)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleRunDetail(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil || bundle.Run.ProjectID != projectID {
		http.NotFound(w, r)
		return
	}
	csrf := csrfFromContext(r.Context())
	data := web.NewRunData(bundle, csrf, "Run "+runID, r.URL.Query().Get("tab"))
	notifications, err := s.notificationCenterData(r.Context(), csrf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.Notifications = notifications
	s.writePage(w, "run.html", data)
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.runBelongsToProject(r, projectID, runID) {
		http.NotFound(w, r)
		return
	}
	if err := s.engine.CancelRun(r.Context(), runID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, projectRunPath(projectID, runID), http.StatusSeeOther)
}

func (s *Server) handleStartQueuedRun(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.runBelongsToProject(r, projectID, runID) {
		http.NotFound(w, r)
		return
	}
	if err := s.engine.StartQueuedRun(r.Context(), runID); err != nil {
		if errors.Is(err, orchestrator.ErrNoRunnerSlots) ||
			errors.Is(err, orchestrator.ErrRunNotPending) ||
			errors.Is(err, orchestrator.ErrRunHeld) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, projectRunPath(projectID, runID), http.StatusSeeOther)
}

func (s *Server) handleReRunStage(w http.ResponseWriter, r *http.Request, projectID, runID, stageID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.runBelongsToProject(r, projectID, runID) {
		http.NotFound(w, r)
		return
	}
	if _, err := s.engine.ReRunStage(r.Context(), runID, stageID, event.Actor{Kind: event.ActorKindOperator, ID: "operator"}); err != nil {
		if errors.Is(err, orchestrator.ErrStageReRunInvalidTarget) || errors.Is(err, orchestrator.ErrStageReRunPrerequisiteGap) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if errors.Is(err, orchestrator.ErrStageReRunRunNotTerminal) || errors.Is(err, orchestrator.ErrStageReRunFrozenSnapshot) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil || bundle.Run.ProjectID != projectID {
		http.Error(w, "run not found", http.StatusInternalServerError)
		return
	}
	fragment, err := s.renderer.RenderRunFragments(bundle)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(fragment)))
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(fragment))
}

func (s *Server) handleHumanReviewVerdict(w http.ResponseWriter, r *http.Request, projectID, runID, stageID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.runBelongsToProject(r, projectID, runID) {
		http.NotFound(w, r)
		return
	}
	submission, err := parseHumanReviewSubmission(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := s.engine.SubmitHumanReview(r.Context(), runID, stageID, submission); err != nil {
		if errors.Is(err, orchestrator.ErrHumanReviewNotAwaiting) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if errors.Is(err, orchestrator.ErrInvalidHumanReview) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil || bundle.Run.ProjectID != projectID {
		http.Error(w, "run not found", http.StatusInternalServerError)
		return
	}
	fragment, err := s.renderer.RenderRunFragments(bundle)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(fragment)))
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(fragment))
}

func parseHumanReviewSubmission(r *http.Request) (orchestrator.HumanReviewSubmission, error) {
	if err := r.ParseForm(); err != nil {
		return orchestrator.HumanReviewSubmission{}, err
	}
	submission := orchestrator.HumanReviewSubmission{
		ActorID: r.Form.Get("actor_id"),
		Verdict: r.Form.Get("verdict"),
		Summary: r.Form.Get("summary"),
	}
	for _, finding := range r.Form["finding"] {
		finding = strings.TrimSpace(finding)
		if finding != "" {
			submission.Findings = append(submission.Findings, orchestrator.HumanFinding{Summary: finding})
		}
	}
	return submission, nil
}

func (s *Server) handleRunEvents(w http.ResponseWriter, r *http.Request, projectID, runID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.security.requireSession(r) {
		http.Error(w, "session required", http.StatusUnauthorized)
		return
	}
	last := parseLastEventID(r)
	if _, err := s.store.ListEventsAfter(r.Context(), runID, last); err != nil {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	bundle, err := s.store.RunBundle(r.Context(), runID)
	if err != nil || bundle.Run.ProjectID != projectID {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	fragment, err := s.renderer.RenderRunFragments(bundle)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writer, ok := NewSSEWriter(w)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	seq := last
	if len(bundle.Events) > 0 {
		seq = bundle.Events[len(bundle.Events)-1].Sequence
	}
	writer.Patch(event.Event{ProjectID: projectID, Sequence: seq}, fragment)

	ch, unsubscribe := s.hub.Subscribe(runID)
	defer unsubscribe()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			writer.Patch(msg.Event, msg.Fragment)
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) handleArtifact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	artifactID := strings.TrimPrefix(r.URL.Path, "/artifacts/")
	if artifactID == "" || strings.Contains(artifactID, "/") {
		http.NotFound(w, r)
		return
	}
	artifact, content, err := s.store.GetArtifact(r.Context(), artifactID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if artifactIsHTML(artifact.MediaType) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", artifact.ID+".html"))
		_, _ = w.Write(content)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, "<!doctype html><title>%s</title><pre>%s</pre>", template.HTMLEscapeString(artifact.ID), template.HTMLEscapeString(string(content)))
}

func (s *Server) runBelongsToProject(r *http.Request, projectID, runID string) bool {
	run, err := s.store.GetRun(r.Context(), runID)
	return err == nil && run.ProjectID == projectID
}

func projectRunPath(projectID, runID string) string {
	return "/projects/" + projectID + "/runs/" + runID
}

func artifactIsHTML(mediaType string) bool {
	mediaType = strings.ToLower(mediaType)
	return strings.HasPrefix(mediaType, "text/html") || strings.Contains(mediaType, "html")
}

func parseInt64Query(r *http.Request, key string) int64 {
	value := r.URL.Query().Get(key)
	if value == "" {
		return 0
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func backlogFullNotice(backlogErr orchestrator.QueueBacklogFullError, queue web.QueueView) *web.Notice {
	return &web.Notice{
		Title: "Queue is full",
		Message: fmt.Sprintf(
			"%d pending runs are already waiting, which reaches backlog_cap %d. Effective policy: auto_when_ready=%t, max_concurrent=%d (effective %d), ready_slots=%d/%d.",
			backlogErr.Pending,
			backlogErr.Cap,
			queue.AutoWhenReady,
			queue.MaxConcurrent,
			queue.EffectiveMaxConcurrent,
			queue.ReadyRunnerSlots,
			queue.RunnerSlots,
		),
	}
}

func (s *Server) writeFragment(w http.ResponseWriter, status int, data web.ProjectSettingsSectionData) {
	fragment, err := s.renderer.RenderProjectSettingsSection(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(fragment)))
	w.WriteHeader(status)
	_, _ = w.Write([]byte(fragment))
}

func (s *Server) writePage(w http.ResponseWriter, name string, data any) {
	s.writePageStatus(w, name, data, http.StatusOK)
}

func (s *Server) writePageStatus(w http.ResponseWriter, name string, data any, status int) {
	html, err := s.renderer.ExecutePage(name, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(html)))
	w.WriteHeader(status)
	_, _ = w.Write([]byte(html))
}
