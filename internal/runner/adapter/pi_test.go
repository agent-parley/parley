package adapter

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/runner/provider"
	"github.com/agent-parley/parley/internal/runner/runnerio"
	"github.com/agent-parley/parley/internal/shared/contract"
	"github.com/agent-parley/parley/internal/shared/event"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestPiEventStreamParsing(t *testing.T) {
	disp := piTestDispatch()
	sink := &recordingSink{}
	stream := &piStreamSink{disp: disp, adapterID: piName, downstream: sink}
	lines := []string{
		`{"type":"agent_start"}`,
		`{"type":"tool_execution_start","toolCallId":"call_1","toolName":"bash","args":{"command":"echo hi"}}`,
		`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"done"}]}}`,
		`{"type":"error","message":"boom"}`,
		`{"type":"agent_end"}`,
	}
	for _, line := range lines {
		if err := stream.Emit(context.Background(), providerOutputEvent(line)); err != nil {
			t.Fatalf("Emit(%s) error = %v", line, err)
		}
	}
	if len(sink.events) != len(lines) {
		t.Fatalf("events len = %d, want %d", len(sink.events), len(lines))
	}
	assertEvent(t, sink.events[0], "adapter.progress", "pi agent started")
	assertEvent(t, sink.events[1], "adapter.progress", "pi tool started: bash")
	assertEvent(t, sink.events[2], "adapter.output", "done")
	assertEvent(t, sink.events[3], "adapter.progress", "pi error: boom")
	assertEvent(t, sink.events[4], "adapter.progress", "pi agent completed")
	if got := sink.events[1].Data["tool_name"]; got != "bash" {
		t.Fatalf("tool_name = %v, want bash", got)
	}
}

func TestPiPrepareBuildsPreflightSafeInvocationAndInputFiles(t *testing.T) {
	ctx := context.Background()
	source := initAdapterSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	reference := mkdirAdapterDir(t, t.TempDir(), "reference")
	agentState := mkdirAdapterDir(t, t.TempDir(), "agent-state")
	authPath := writeTestAuth(t, t.TempDir())
	adapter := NewPi(PiOptions{
		Provider:           &scriptedPiProvider{},
		CredentialStrategy: AuthJSONCredentialStrategy{SourcePath: authPath},
		DataRoot:           dataRoot,
		ProjectID:          "p1",
		SourceRepo:         source,
		ReferenceRoot:      reference,
		AgentStateRoot:     agentState,
		Image:              "localhost/test-pi:latest",
		Network:            provider.NetworkBridge,
		AppendSystemExtra:  "extra verification rule",
	})
	prepared, err := adapter.Prepare(ctx, piTestDispatch())
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if prepared.RepoSnapshotPath != "" || prepared.RepoSnapshotLock != nil {
		t.Fatalf("implementation prepare snapshot state = %q/%v, want none", prepared.RepoSnapshotPath, prepared.RepoSnapshotLock)
	}
	policy := provider.PreflightPolicy{
		WorktreeRoots:  []string{dataRoot},
		ArtifactRoots:  []string{dataRoot},
		ReferenceRoots: []string{reference},
		AgentStateRoot: agentState,
	}
	if err := provider.Preflight(prepared.Invocation, policy); err != nil {
		t.Fatalf("Preflight() error = %v", err)
	}
	if prepared.Invocation.Network != provider.NetworkBridge {
		t.Fatalf("network = %q, want controlled provider network bridge", prepared.Invocation.Network)
	}
	if got := prepared.Invocation.Env; len(got) != 2 || got["HARNESS_RUN_ID"] == "" || got["HARNESS_TASK_ID"] == "" {
		t.Fatalf("env = %#v, want only harness run/task IDs", got)
	}
	for key := range prepared.Invocation.Env {
		if strings.Contains(strings.ToLower(key), "token") || strings.Contains(strings.ToLower(key), "secret") || strings.Contains(strings.ToLower(key), "api_key") {
			t.Fatalf("secret-like env key present: %s", key)
		}
	}
	if got := mountHost(prepared.Invocation, containerAuthPath); got != prepared.AuthCopyPath {
		t.Fatalf("auth mount host = %q, want %q", got, prepared.AuthCopyPath)
	}
	if got := mountHost(prepared.Invocation, containerAgentDir); got != prepared.AgentDir {
		t.Fatalf("agent mount host = %q, want %q", got, prepared.AgentDir)
	}
	workerInput := readTestFile(t, prepared.WorkerInputPath)
	if !strings.Contains(workerInput, "Implement the following user request") || !strings.Contains(workerInput, "/project/workspace/report.json") {
		t.Fatalf("worker-input.md missing task/report contract:\n%s", workerInput)
	}
	appendSystem := readTestFile(t, prepared.AppendSystemPath)
	if !strings.Contains(appendSystem, "Parley Headless Worker Rules") || !strings.Contains(appendSystem, "Provider credentials") || !strings.Contains(appendSystem, "extra verification rule") {
		t.Fatalf("APPEND_SYSTEM.md missing standing rules or run-specific extra:\n%s", appendSystem)
	}
	if got := readTestFile(t, prepared.AuthCopyPath); got != readTestFile(t, authPath) {
		t.Fatalf("auth copy = %q, want source contents", got)
	}
}

func TestPiPrepareScopesStateByAdapterExecutionID(t *testing.T) {
	ctx := context.Background()
	adapter := newTestPiAdapter(t, ctx, &scriptedPiProvider{})
	disp := piTestDispatch()
	disp.Input["adapter_execution_id"] = "review_arbiter"
	prepared, err := adapter.Prepare(ctx, disp)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if !strings.Contains(prepared.WorktreePath, "attempt1-review_arbiter") || !strings.Contains(prepared.AgentDir, "attempt1-review_arbiter") {
		t.Fatalf("execution state not scoped by adapter_execution_id: worktree=%s agent=%s", prepared.WorktreePath, prepared.AgentDir)
	}
}

func TestPiPrepareReviewUsesSharedReadOnlyWorktreeAndIndependentState(t *testing.T) {
	ctx := context.Background()
	adapter := newTestPiAdapter(t, ctx, &scriptedPiProvider{})
	disp := piTestDispatch()
	disp.StageType = contract.StageTypeReview
	disp.Input["adapter_execution_id"] = "review_arbiter"
	prepared, err := adapter.Prepare(ctx, disp)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if strings.Contains(prepared.WorktreePath, "review_arbiter") || !strings.Contains(prepared.AgentDir, "attempt1-review_arbiter") {
		t.Fatalf("review state scoping mismatch: worktree=%s agent=%s", prepared.WorktreePath, prepared.AgentDir)
	}
	for _, mount := range prepared.Invocation.Mounts {
		if mount.Container == containerRepoPath && mount.Mode != "ro" {
			t.Fatalf("review repo mount mode = %s, want ro", mount.Mode)
		}
	}
}

func TestPiPreparePlanningUsesReadOnlyRepoAndPlanningPrompt(t *testing.T) {
	ctx := context.Background()
	adapter := newTestPiAdapter(t, ctx, &scriptedPiProvider{})
	disp := piTestDispatch()
	disp.StageType = contract.StageTypeIdeaRefinement
	disp.Input = map[string]any{
		"input_mode":        contract.AdapterInputModePlanning,
		"idea":              "add audit logging to login failures",
		"contract_markdown": "# Parley Task Contract\n\n## User idea (verbatim)\n\nadd audit logging to login failures\n",
	}
	prepared, err := adapter.Prepare(ctx, disp)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	for _, mount := range prepared.Invocation.Mounts {
		if mount.Container == containerRepoPath && mount.Mode != "ro" {
			t.Fatalf("planning repo mount mode = %s, want ro", mount.Mode)
		}
	}
	prompt := prepared.Invocation.Command[len(prepared.Invocation.Command)-1]
	if !strings.Contains(prompt, "single-shot task plan") || !strings.Contains(prompt, "payload.task_plan_markdown") {
		t.Fatalf("planning prompt missing task-plan instruction: %#v", prepared.Invocation.Command)
	}
	workerInput := readTestFile(t, prepared.WorkerInputPath)
	for _, want := range []string{"Standard idea-intake planner", "Do not ask the user questions", "payload.task_plan_markdown", "## Assumptions", "## Open Questions"} {
		if !strings.Contains(workerInput, want) {
			t.Fatalf("planning worker input missing %q:\n%s", want, workerInput)
		}
	}
	appendSystem := readTestFile(t, prepared.AppendSystemPath)
	if !strings.Contains(appendSystem, "Parley planning worker") || !strings.Contains(appendSystem, "Do not modify /project/repo during planning") {
		t.Fatalf("planning APPEND_SYSTEM.md missing planning rules:\n%s", appendSystem)
	}
}

func TestPiPrepareConversationUsesReadOnlyCommittedSnapshotAndWorkspace(t *testing.T) {
	ctx := context.Background()
	fake := &scriptedPiProvider{}
	source := initAdapterSourceRepo(t, ctx)
	if err := os.WriteFile(filepath.Join(source, "uncommitted.txt"), []byte("not canonical\n"), 0o600); err != nil {
		t.Fatalf("write uncommitted file: %v", err)
	}
	dataRoot := t.TempDir()
	workspaceRoot := mkdirAdapterDir(t, dataRoot, "projects", "p1", "workspace")
	agentState := mkdirAdapterDir(t, t.TempDir(), "agent-state")
	authPath := writeTestAuth(t, t.TempDir())
	adapter := NewPi(PiOptions{
		Provider:           fake,
		CredentialStrategy: AuthJSONCredentialStrategy{SourcePath: authPath},
		DataRoot:           dataRoot,
		ProjectID:          "p1",
		SourceRepo:         source,
		WorkspaceRoot:      workspaceRoot,
		AgentStateRoot:     agentState,
	})
	disp := piTestDispatch()
	disp.StageType = contract.StageTypeConversation
	disp.Input = map[string]any{
		"input_mode":                   contract.AdapterInputModeConversation,
		"conversation_id":              "conv_123",
		"trigger_message_id":           "msg_123",
		"orchestration_state_summary":  "- Most recent included run: run_123 is completed.\n- Latest report on that run: stage review status changes_requested verdict changes_requested — review rejected stale cache handling.",
		"orchestration_state_markdown": "# Parley Orchestration State Snapshot\n\nRun `run_123` review verdict `changes_requested`: review rejected stale cache handling.\n",
		"allowed_actions":              []string{"create-Task"},
		"messages": []map[string]any{{
			"role": "user",
			"body": "Where is auth handled?",
		}},
	}
	prepared, err := adapter.Prepare(ctx, disp)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if prepared.WorktreePath != "" {
		t.Fatalf("conversation worktree path = %q, want no worktree", prepared.WorktreePath)
	}
	repoMount := mountHost(prepared.Invocation, containerRepoPath)
	if repoMount == source || !strings.HasPrefix(repoMount, filepath.Join(dataRoot, "projects", "p1", "repo-snapshots")) {
		t.Fatalf("repo mount host = %q, want committed snapshot under data root", repoMount)
	}
	if _, err := os.Stat(filepath.Join(repoMount, ".git")); !os.IsNotExist(err) {
		t.Fatalf("snapshot .git stat err = %v, want no git worktree metadata", err)
	}
	if got := readTestFile(t, filepath.Join(repoMount, "README.md")); got != "hello\n" {
		t.Fatalf("snapshot README = %q, want committed content", got)
	}
	if _, err := os.Stat(filepath.Join(repoMount, "uncommitted.txt")); !os.IsNotExist(err) {
		t.Fatalf("snapshot uncommitted stat err = %v, want absent", err)
	}
	if mode := mountMode(prepared.Invocation, containerRepoPath); mode != "ro" {
		t.Fatalf("repo mount mode = %q, want ro", mode)
	}
	if got := mountHost(prepared.Invocation, containerWorkspacePath); got != workspaceRoot {
		t.Fatalf("workspace mount host = %q, want project workspace %q", got, workspaceRoot)
	}
	if prepared.Invocation.Role != "conversation" || prepared.Invocation.Profile != "conversation" {
		t.Fatalf("invocation role/profile = %q/%q, want conversation", prepared.Invocation.Role, prepared.Invocation.Profile)
	}
	if !strings.HasPrefix(prepared.WorkerInputPath, filepath.Join(workspaceRoot, ".parley", "conversation-turns")) {
		t.Fatalf("worker input path = %q, want under project workspace conversation turns", prepared.WorkerInputPath)
	}
	statePath := filepath.Join(filepath.Dir(prepared.WorkerInputPath), "orchestration-state.md")
	stateMarkdown := readTestFile(t, statePath)
	if !strings.Contains(stateMarkdown, "run_123") || !strings.Contains(stateMarkdown, "changes_requested") {
		t.Fatalf("orchestration-state.md missing run evidence:\n%s", stateMarkdown)
	}
	workerInput := readTestFile(t, prepared.WorkerInputPath)
	for _, want := range []string{"Conversational Planning Agent", "no resident session", "Repository tools: read, list, grep", "Orchestration state", "orchestration-state.md", "review rejected stale cache handling", "payload.reply_markdown", "Allowed actions", "create-Task", "plan-gated Balanced template", "Open assumptions"} {
		if !strings.Contains(workerInput, want) {
			t.Fatalf("conversation worker input missing %q:\n%s", want, workerInput)
		}
	}
	prompt := prepared.Invocation.Command[len(prepared.Invocation.Command)-1]
	if !strings.Contains(prompt, prepared.ContainerWorkerInputPath) || !strings.Contains(prompt, prepared.ContainerReportPath) || !strings.Contains(prompt, "orchestration-state") || !strings.Contains(prompt, "payload.reply_markdown") || !strings.Contains(prompt, "create-Task") {
		t.Fatalf("conversation prompt missing input/report paths or reply contract: %#v", prepared.Invocation.Command)
	}
}

func TestPiPrepareConversationReusesSnapshotByCommitAndCleansOldOnNewCommit(t *testing.T) {
	ctx := context.Background()
	counterPath := installCountingGitArchiveWrapper(t, "")

	fake := &scriptedPiProvider{}
	source := initAdapterSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	workspaceRoot := mkdirAdapterDir(t, dataRoot, "projects", "p1", "workspace")
	agentState := mkdirAdapterDir(t, t.TempDir(), "agent-state")
	authPath := writeTestAuth(t, t.TempDir())
	adapter := NewPi(PiOptions{
		Provider:           fake,
		CredentialStrategy: AuthJSONCredentialStrategy{SourcePath: authPath},
		DataRoot:           dataRoot,
		ProjectID:          "p1",
		SourceRepo:         source,
		WorkspaceRoot:      workspaceRoot,
		AgentStateRoot:     agentState,
	})
	disp := piTestDispatch()
	disp.StageType = contract.StageTypeConversation
	disp.Input = map[string]any{"input_mode": contract.AdapterInputModeConversation, "conversation_id": "conv_123", "trigger_message_id": "msg_123"}
	runTurn := func(turnID string) string {
		t.Helper()
		disp.Input["trigger_message_id"] = turnID
		before := len(fake.invocations)
		if _, err := adapter.Run(ctx, disp, &recordingSink{}); err != nil {
			t.Fatalf("Run(%s) error = %v", turnID, err)
		}
		if len(fake.invocations) == before {
			t.Fatalf("Run(%s) did not invoke provider", turnID)
		}
		return mountHost(fake.invocations[before], containerRepoPath)
	}
	snapshotRoot := filepath.Join(dataRoot, "projects", "p1", "repo-snapshots")

	firstMount := runTurn("msg_123")
	firstSnapshotDirs := conversationSnapshotDirs(t, snapshotRoot)
	if got := len(firstSnapshotDirs); got != 1 {
		t.Fatalf("first snapshot dirs = %d, want 1", got)
	}
	if got := strings.TrimSpace(readTestFile(t, counterPath)); got != "1" {
		t.Fatalf("git archive count after first run = %s, want 1", got)
	}

	if got := runTurn("msg_124"); got != firstMount {
		t.Fatalf("second same-commit repo mount = %q, want reused snapshot %q", got, firstMount)
	}
	if got := strings.TrimSpace(readTestFile(t, counterPath)); got != "1" {
		t.Fatalf("git archive count after same-commit run = %s, want 1", got)
	}
	if got := len(conversationSnapshotDirs(t, snapshotRoot)); got != 1 {
		t.Fatalf("same-commit snapshot dirs = %d, want 1", got)
	}

	commitAdapterFile(t, ctx, source, "new-commit.txt", "next\n", "second")
	newCommitMount := runTurn("msg_125")
	if got := strings.TrimSpace(readTestFile(t, counterPath)); got != "2" {
		t.Fatalf("git archive count after new commit = %s, want 2", got)
	}
	if newCommitMount == firstMount {
		t.Fatalf("new-commit repo mount reused old snapshot %q", newCommitMount)
	}
	if got := len(conversationSnapshotDirs(t, snapshotRoot)); got > maxConversationRepoSnapshots {
		t.Fatalf("snapshot dirs after second commit = %d, want bounded <= %d", got, maxConversationRepoSnapshots)
	}

	commitAdapterFile(t, ctx, source, "third-commit.txt", "third\n", "third")
	runTurn("msg_126")
	if got := strings.TrimSpace(readTestFile(t, counterPath)); got != "3" {
		t.Fatalf("git archive count after third commit = %s, want 3", got)
	}
	finalDirs := conversationSnapshotDirs(t, snapshotRoot)
	if got := len(finalDirs); got > maxConversationRepoSnapshots {
		t.Fatalf("snapshot dirs after third commit = %d, want bounded <= %d", got, maxConversationRepoSnapshots)
	}
	for _, dir := range finalDirs {
		if dir == firstSnapshotDirs[0] {
			t.Fatalf("oldest snapshot %q was not garbage-collected; final dirs=%v", dir, finalDirs)
		}
	}
}

func TestPiPrepareConversationFailureReleasesSnapshotLock(t *testing.T) {
	ctx := context.Background()
	source := initAdapterSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	adapter := NewPi(PiOptions{
		Provider:           &scriptedPiProvider{},
		CredentialStrategy: AuthJSONCredentialStrategy{SourcePath: filepath.Join(t.TempDir(), "missing-auth.json")},
		DataRoot:           dataRoot,
		ProjectID:          "p1",
		SourceRepo:         source,
		AgentStateRoot:     mkdirAdapterDir(t, t.TempDir(), "agent-state"),
	})
	disp := piTestDispatch()
	disp.StageType = contract.StageTypeConversation
	disp.Input = map[string]any{"input_mode": contract.AdapterInputModeConversation, "conversation_id": "conv_123", "trigger_message_id": "msg_123"}

	if _, err := adapter.Prepare(ctx, disp); err == nil {
		t.Fatalf("Prepare() error = nil, want credential failure")
	}
	snapshotRoot := filepath.Join(dataRoot, "projects", "p1", "repo-snapshots")
	lock, acquired, err := tryAcquireSnapshotRootLock(snapshotRoot, conversationSnapshotUseLockName, syscall.LOCK_EX)
	if err != nil {
		t.Fatalf("tryAcquireSnapshotRootLock() error = %v", err)
	}
	if !acquired {
		t.Fatalf("snapshot use lock still held after Prepare failure")
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("close snapshot lock: %v", err)
	}
}

func TestPiCreateConversationRepoSnapshotConcurrentSameSHAReusesPublishedSnapshot(t *testing.T) {
	ctx := context.Background()
	counterPath := installCountingGitArchiveWrapper(t, "1")
	source := initAdapterSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	adapter := NewPi(PiOptions{DataRoot: dataRoot, SourceRepo: source})

	const workers = 5
	start := make(chan struct{})
	var wg sync.WaitGroup
	paths := make([]string, workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			path, lock, err := adapter.createConversationRepoSnapshot(ctx, "p1")
			if lock != nil {
				defer lock.Close()
			}
			paths[i] = path
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d createConversationRepoSnapshot() error = %v", i, err)
		}
		if paths[i] != paths[0] {
			t.Fatalf("worker %d snapshot path = %q, want %q", i, paths[i], paths[0])
		}
	}
	if got := strings.TrimSpace(readTestFile(t, counterPath)); got != "1" {
		t.Fatalf("git archive count after concurrent same-SHA creates = %s, want 1", got)
	}
	if got := readTestFile(t, filepath.Join(paths[0], "README.md")); got != "hello\n" {
		t.Fatalf("snapshot README = %q, want committed content", got)
	}
	if got := len(conversationSnapshotDirs(t, filepath.Join(dataRoot, "projects", "p1", "repo-snapshots"))); got != 1 {
		t.Fatalf("snapshot dirs after concurrent creates = %d, want 1", got)
	}
}

func TestCleanupConversationRepoSnapshotsSkipsWhileSnapshotInUse(t *testing.T) {
	ctx := context.Background()
	source := initAdapterSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	adapter := NewPi(PiOptions{DataRoot: dataRoot, SourceRepo: source})

	oldPath, oldLock, err := adapter.createConversationRepoSnapshot(ctx, "p1")
	if err != nil {
		t.Fatalf("create old snapshot: %v", err)
	}
	defer oldLock.Close()
	commitAdapterFile(t, ctx, source, "new-commit.txt", "next\n", "second")
	newPath, newLock, err := adapter.createConversationRepoSnapshot(ctx, "p1")
	if err != nil {
		t.Fatalf("create new snapshot: %v", err)
	}
	if err := newLock.Close(); err != nil {
		t.Fatalf("close new snapshot lock: %v", err)
	}

	if err := cleanupConversationRepoSnapshots(newPath); err != nil {
		t.Fatalf("cleanup with old snapshot in use: %v", err)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("old in-use snapshot was removed: %v", err)
	}
}

func TestPiRunConversationReadsReplyWithoutDiffArtifact(t *testing.T) {
	ctx := context.Background()
	fake := &scriptedPiProvider{
		runs: []func(provider.PreparedInvocation) error{
			func(inv provider.PreparedInvocation) error {
				workspace, _ := piMountedDirs(inv)
				reportPath := filepath.Join(workspace, ".parley", "conversation-turns", "conv_123", "msg_123", "report.json")
				return os.WriteFile(reportPath, []byte(`{"status":"completed","summary":"answered","payload":{"reply_markdown":"Auth is in internal/auth."},"errors":[]}`), 0o600)
			},
		},
	}
	adapter := newTestPiAdapter(t, ctx, fake)
	disp := piTestDispatch()
	disp.StageType = contract.StageTypeConversation
	disp.Input = map[string]any{"input_mode": contract.AdapterInputModeConversation, "conversation_id": "conv_123", "trigger_message_id": "msg_123"}
	sink := &recordingSink{}
	rep, err := adapter.Run(ctx, disp, sink)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if rep.Status != report.StatusCompleted || rep.Payload["reply_markdown"] != "Auth is in internal/auth." {
		t.Fatalf("report = %#v, want conversation reply", rep)
	}
	if len(sink.artifacts) != 0 {
		t.Fatalf("artifacts = %#v, want no diff/artifact for conversation", sink.artifacts)
	}
	if len(sink.events) != 0 {
		t.Fatalf("events = %#v, want no run-scoped events for conversation", sink.events)
	}
}

func TestPiRunRepairsInvalidReportOnce(t *testing.T) {
	ctx := context.Background()
	fake := &scriptedPiProvider{
		runs: []func(provider.PreparedInvocation) error{
			func(inv provider.PreparedInvocation) error {
				workspace, repo := piMountedDirs(inv)
				if err := os.WriteFile(filepath.Join(repo, "pi-created.txt"), []byte("hello from pi\n"), 0o600); err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(workspace, "report.json"), []byte(`{"status":"surprised","summary":"bad","errors":[]}`), 0o600)
			},
			func(inv provider.PreparedInvocation) error {
				workspace, _ := piMountedDirs(inv)
				return os.WriteFile(filepath.Join(workspace, "report.json"), []byte(`{"status":"completed","summary":"fixed report","errors":[]}`), 0o600)
			},
		},
	}
	adapter := newTestPiAdapter(t, ctx, fake)
	sink := &recordingSink{}
	rep, err := adapter.Run(ctx, piTestDispatch(), sink)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if rep.Status != report.StatusCompleted || rep.Summary != "fixed report" {
		t.Fatalf("report = %#v, want completed fixed report", rep)
	}
	if fake.callCount != 2 {
		t.Fatalf("provider calls = %d, want one repair re-dispatch", fake.callCount)
	}
	artifacts := artifactsByName(sink.artifacts)
	if !strings.Contains(artifacts["diff.patch"], "pi-created.txt") {
		t.Fatalf("diff.patch missing pi-created.txt:\n%s", artifacts["diff.patch"])
	}
	if len(rep.EvidenceRefs) != 1 || rep.Payload["diff_artifact_id"] == "" {
		t.Fatalf("report evidence/payload missing diff artifact: %#v", rep)
	}
}

func TestPiRunPreservesReviewVerdictAndPayload(t *testing.T) {
	ctx := context.Background()
	fake := &scriptedPiProvider{
		runs: []func(provider.PreparedInvocation) error{
			func(inv provider.PreparedInvocation) error {
				workspace, _ := piMountedDirs(inv)
				return os.WriteFile(filepath.Join(workspace, "report.json"), []byte(`{
  "status":"completed",
  "verdict":"changes_requested",
  "summary":"accepted review finding",
  "evidence_refs":["critic-artifact"],
  "payload":{
    "raw_findings":[{"id":"finding-1"}],
    "arbitration_decisions":[{"finding_id":"finding-1","classification":"accepted","rationale":"real"}],
    "residual_risk":"medium",
    "confidence":"high"
  },
  "errors":[]
}`), 0o600)
			},
		},
	}
	adapter := newTestPiAdapter(t, ctx, fake)
	disp := piTestDispatch()
	disp.StageType = contract.StageTypeReview
	disp.Input = map[string]any{"review_role": contract.ReviewRoleArbiter, "review_profile": contract.ReviewProfileGeneralist, "review_intensity": contract.ReviewIntensityNormal}
	sink := &recordingSink{}
	rep, err := adapter.Run(ctx, disp, sink)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if rep.Verdict == nil || *rep.Verdict != report.ReviewVerdictChangesRequested {
		t.Fatalf("verdict = %v, want changes_requested", rep.Verdict)
	}
	if rep.Payload["residual_risk"] != "medium" || len(rep.EvidenceRefs) != 2 {
		t.Fatalf("review payload/evidence not preserved: %#v refs=%#v", rep.Payload, rep.EvidenceRefs)
	}
}

func TestPiRunTreatsContainerExecutionErrorAsFailure(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("podman run exited with code 1")
	fake := &scriptedPiProvider{
		runs: []func(provider.PreparedInvocation) error{
			func(inv provider.PreparedInvocation) error {
				workspace, repo := piMountedDirs(inv)
				if err := os.WriteFile(filepath.Join(repo, "pi-created.txt"), []byte("hello from pi\n"), 0o600); err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(workspace, "report.json"), []byte(`{"status":"completed","summary":"claimed success","errors":[]}`), 0o600); err != nil {
					return err
				}
				return boom
			},
		},
	}
	adapter := newTestPiAdapter(t, ctx, fake)
	sink := &recordingSink{}
	rep, err := adapter.Run(ctx, piTestDispatch(), sink)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if rep.Status != report.StatusFailed {
		t.Fatalf("report status = %s, want failed; report=%#v", rep.Status, rep)
	}
	if len(rep.Errors) != 1 || !strings.Contains(rep.Errors[0], boom.Error()) {
		t.Fatalf("errors = %#v, want provider run error", rep.Errors)
	}
	if fake.callCount != 1 {
		t.Fatalf("provider calls = %d, want no repair for schema-valid execution failure", fake.callCount)
	}
}

func TestPiRunReturnsInvalidAfterOneFailedRepair(t *testing.T) {
	ctx := context.Background()
	fake := &scriptedPiProvider{
		runs: []func(provider.PreparedInvocation) error{
			func(inv provider.PreparedInvocation) error {
				workspace, repo := piMountedDirs(inv)
				if err := os.WriteFile(filepath.Join(repo, "pi-created.txt"), []byte("hello from pi\n"), 0o600); err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(workspace, "report.json"), []byte(`{"status":"failed","summary":"missing errors","errors":[]}`), 0o600)
			},
			func(inv provider.PreparedInvocation) error {
				workspace, _ := piMountedDirs(inv)
				return os.WriteFile(filepath.Join(workspace, "report.json"), []byte(`not json`), 0o600)
			},
		},
	}
	adapter := newTestPiAdapter(t, ctx, fake)
	sink := &recordingSink{}
	rep, err := adapter.Run(ctx, piTestDispatch(), sink)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if rep.Status != report.StatusInvalid || rep.Actor.Kind != report.ActorKindHarness {
		t.Fatalf("report = %#v, want harness invalid", rep)
	}
	if fake.callCount != 2 {
		t.Fatalf("provider calls = %d, want exactly two", fake.callCount)
	}
	if len(rep.Errors) == 0 || !strings.Contains(rep.Errors[0], "parse report.json") {
		t.Fatalf("invalid errors = %#v, want parse report error", rep.Errors)
	}
	if len(rep.EvidenceRefs) != 1 {
		t.Fatalf("evidence refs = %#v, want diff artifact", rep.EvidenceRefs)
	}
}

func providerOutputEvent(line string) event.Event {
	return event.Event{
		SchemaVersion: event.SchemaVersion,
		Type:          "adapter.output",
		Actor:         event.Actor{Kind: event.ActorKindAdapter, ID: piName},
		Summary:       line,
		Data:          map[string]any{"provider": "podman", "stream": "stdout", "line": line},
	}
}

func assertEvent(t *testing.T, ev event.Event, typ, summary string) {
	t.Helper()
	if ev.Type != typ || ev.Summary != summary {
		t.Fatalf("event = (%s, %q), want (%s, %q); full=%#v", ev.Type, ev.Summary, typ, summary, ev)
	}
}

func piTestDispatch() contract.Dispatch {
	return contract.Dispatch{
		RunID:     "run1",
		TaskID:    "task1",
		AttemptID: "attempt1",
		StageID:   "stage1",
		StageType: contract.StageTypeImplementation,
		Adapter:   piName,
		Input:     map[string]any{"idea": "Add a file named pi-created.txt."},
	}
}

type scriptedPiProvider struct {
	invocations []provider.PreparedInvocation
	runs        []func(provider.PreparedInvocation) error
	callCount   int
}

func (p *scriptedPiProvider) Name() string { return "fake-podman" }

func (p *scriptedPiProvider) Run(ctx context.Context, inv provider.PreparedInvocation, sink runnerio.Sink) (provider.Result, error) {
	p.invocations = append(p.invocations, inv)
	p.callCount++
	if err := sink.Emit(ctx, providerOutputEvent(`{"type":"agent_start"}`)); err != nil {
		return provider.Result{}, err
	}
	if err := sink.Emit(ctx, providerOutputEvent(`{"type":"agent_end"}`)); err != nil {
		return provider.Result{}, err
	}
	var err error
	if idx := p.callCount - 1; idx < len(p.runs) && p.runs[idx] != nil {
		err = p.runs[idx](inv)
	}
	now := time.Now().UTC()
	return provider.Result{ExitCode: 0, StartedAt: now, EndedAt: now}, err
}

func newTestPiAdapter(t *testing.T, ctx context.Context, fake *scriptedPiProvider) Pi {
	t.Helper()
	source := initAdapterSourceRepo(t, ctx)
	dataRoot := t.TempDir()
	reference := mkdirAdapterDir(t, t.TempDir(), "reference")
	agentState := mkdirAdapterDir(t, t.TempDir(), "agent-state")
	authPath := writeTestAuth(t, t.TempDir())
	return NewPi(PiOptions{
		Provider:           fake,
		CredentialStrategy: AuthJSONCredentialStrategy{SourcePath: authPath},
		DataRoot:           dataRoot,
		ProjectID:          "p1",
		SourceRepo:         source,
		ReferenceRoot:      reference,
		AgentStateRoot:     agentState,
		Image:              "localhost/test-pi:latest",
	})
}

func writeTestAuth(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "auth.json")
	if err := os.WriteFile(path, []byte("{\"openai-codex\":{\"type\":\"oauth\"}}\n"), 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	return path
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}

func installCountingGitArchiveWrapper(t *testing.T, delay string) string {
	t.Helper()
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("lookpath git: %v", err)
	}
	binDir := t.TempDir()
	counterPath := filepath.Join(t.TempDir(), "git-archive-count")
	delayLine := ""
	if delay != "" {
		delayLine = "  sleep " + delay + "\n"
	}
	wrapper := "#!/bin/sh\nset -eu\nif [ \"${1:-}\" = \"-C\" ] && [ \"${3:-}\" = \"archive\" ]; then\n  n=0\n  if [ -f \"" + counterPath + "\" ]; then n=$(cat \"" + counterPath + "\"); fi\n  n=$((n+1))\n  printf '%s' \"$n\" > \"" + counterPath + "\"\n" + delayLine + "fi\nexec \"" + realGit + "\" \"$@\"\n"
	if err := os.WriteFile(filepath.Join(binDir, "git"), []byte(wrapper), 0o700); err != nil {
		t.Fatalf("write git wrapper: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return counterPath
}

func commitAdapterFile(t *testing.T, ctx context.Context, source, name, content, message string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(source, name), []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	runAdapterGit(t, ctx, source, "add", name)
	runAdapterGit(t, ctx, source, "commit", "-m", message)
}

func conversationSnapshotDirs(t *testing.T, snapshotRoot string) []string {
	t.Helper()
	entries, err := os.ReadDir(snapshotRoot)
	if err != nil {
		t.Fatalf("read snapshot root: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && !strings.Contains(entry.Name(), ".tmp-") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}

func mountMode(inv provider.PreparedInvocation, containerPath string) string {
	for _, mount := range inv.Mounts {
		if mount.Container == containerPath {
			return mount.Mode
		}
	}
	return ""
}

func piMountedDirs(inv provider.PreparedInvocation) (workspace, repo string) {
	for _, mount := range inv.Mounts {
		switch mount.Container {
		case containerWorkspacePath:
			workspace = mount.Host
		case containerRepoPath:
			repo = mount.Host
		}
	}
	return workspace, repo
}

func artifactsByName(artifacts []runnerio.Artifact) map[string]string {
	out := map[string]string{}
	for _, artifact := range artifacts {
		out[artifact.Name] = string(artifact.Content)
	}
	return out
}
