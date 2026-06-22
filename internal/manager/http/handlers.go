package managerhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	if len(parts) == 6 && parts[3] == "human-stages" && parts[5] == "verdict" {
		s.handleHumanReviewVerdict(w, r, projectID, runID, parts[4])
		return
	}
	if len(parts) == 6 && parts[3] == "idea-stages" && parts[5] == "answers" {
		s.handleDeepIdeaAnswers(w, r, projectID, runID, parts[4])
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
	chat, err := s.projectChatData(r, project)
	if err != nil {
		return web.IndexData{}, err
	}
	csrf := csrfFromContext(r.Context())
	chat.CSRF = csrf
	return web.IndexData{Project: project, Runs: runs, Runners: runners, RunnerEventPage: runnerEventPage, Queue: web.NewQueueView(queueState), Chat: chat, CSRF: csrf, Title: "Parley · " + project.Name}, nil
}

func (s *Server) handleProjectChatPath(w http.ResponseWriter, r *http.Request, projectID string, parts []string) {
	if len(parts) != 1 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	switch parts[0] {
	case "messages":
		s.handleProjectChatMessage(w, r, projectID)
	case "events":
		s.handleProjectChatEvents(w, r, projectID)
	default:
		http.NotFound(w, r)
	}
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
	conversation, err := s.store.EnsureProjectConversation(r.Context(), project.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.store.AddMessage(r.Context(), conversation.ID, store.MessageRoleUser, message); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	input := contract.TaskInput{Idea: message, RefinementLevel: contract.RefinementLevelDirect, ConversationID: conversation.ID}
	if _, err := s.engine.StartProjectRunInput(r.Context(), project.ID, input); err != nil {
		var backlogErr orchestrator.QueueBacklogFullError
		if errors.As(err, &backlogErr) {
			data, pageErr := s.indexData(r, project.ID)
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
	s.broadcastProjectChat(r, project)
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
	data, err := s.projectChatData(r, project)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fragment, err := s.renderer.RenderProjectChat(data)
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

	ch, unsubscribe := s.hub.Subscribe(projectChatTopic(project.ID))
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
	return web.ProjectChatData{Project: project, Conversation: conversation, Messages: messages, TaskRuns: web.NewTaskRunViews(tasks, runs), CSRF: csrfFromContext(r.Context())}, nil
}

func (s *Server) broadcastProjectChat(r *http.Request, project store.Project) {
	data, err := s.projectChatData(r, project)
	if err != nil {
		return
	}
	fragment, err := s.renderer.RenderProjectChat(data)
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
	if len(parts) == 4 && parts[1] == "human-stages" && parts[3] == "verdict" {
		s.handleHumanReviewVerdict(w, r, projectID, runID, parts[2])
		return
	}
	if len(parts) == 4 && parts[1] == "idea-stages" && parts[3] == "answers" {
		s.handleDeepIdeaAnswers(w, r, projectID, runID, parts[2])
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
	s.writePage(w, "run.html", web.NewRunData(bundle, csrfFromContext(r.Context()), "Run "+runID, r.URL.Query().Get("tab")))
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

func (s *Server) handleDeepIdeaAnswers(w http.ResponseWriter, r *http.Request, projectID, runID, stageID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !s.runBelongsToProject(r, projectID, runID) {
		http.NotFound(w, r)
		return
	}
	submission, err := parseDeepIdeaAnswersSubmission(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	receipt, err := s.engine.SubmitDeepIdeaAnswers(r.Context(), runID, stageID, submission)
	if err != nil {
		if errors.Is(err, orchestrator.ErrDeepIdeaNotAwaiting) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if errors.Is(err, orchestrator.ErrInvalidDeepIdeaAnswer) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !requestIsJSON(r) {
		http.Redirect(w, r, projectRunPath(projectID, runID), http.StatusSeeOther)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(receipt)
}

func parseDeepIdeaAnswersSubmission(r *http.Request) (orchestrator.DeepIdeaAnswersSubmission, error) {
	if requestIsJSON(r) {
		var submission orchestrator.DeepIdeaAnswersSubmission
		if err := json.NewDecoder(r.Body).Decode(&submission); err != nil {
			return orchestrator.DeepIdeaAnswersSubmission{}, err
		}
		return submission, nil
	}
	if err := r.ParseForm(); err != nil {
		return orchestrator.DeepIdeaAnswersSubmission{}, err
	}
	submission := orchestrator.DeepIdeaAnswersSubmission{
		ActorID:    r.Form.Get("actor_id"),
		AnswerText: firstNonEmptyFormValue(r, "answer_text", "answer", "answers"),
	}
	questions := r.Form["question"]
	answers := r.Form["answer"]
	for i, answer := range answers {
		answer = strings.TrimSpace(answer)
		if answer == "" {
			continue
		}
		item := orchestrator.DeepIdeaAnswer{Answer: answer}
		if i < len(questions) {
			item.Question = strings.TrimSpace(questions[i])
		}
		submission.Answers = append(submission.Answers, item)
	}
	return submission, nil
}

func requestIsJSON(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/json")
}

func firstNonEmptyFormValue(r *http.Request, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(r.Form.Get(key)); value != "" {
			return value
		}
	}
	return ""
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
