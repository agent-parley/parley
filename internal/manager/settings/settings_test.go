package settings

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/agentregistry"
)

func TestLoadAbsentFilesUsesDefaults(t *testing.T) {
	loaded, err := Load(LoadOptions{GlobalPath: filepath.Join(t.TempDir(), "global.toml"), ProjectPath: filepath.Join(t.TempDir(), "project.toml")})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Settings.Queue.AutoWhenReady != true || loaded.Settings.Queue.MaxConcurrent != 1 || loaded.Settings.Queue.BacklogCap != 100 {
		t.Fatalf("settings = %+v, want queue defaults", loaded.Settings)
	}
	if loaded.Settings.Conversation.Budget != 1 || loaded.Settings.Conversation.IdleWarmHoldTTL != 15*time.Minute {
		t.Fatalf("conversation = %+v, want conversation defaults", loaded.Settings.Conversation)
	}
	if len(loaded.Settings.AgentRegistry.Families) != 1 || loaded.Settings.AgentRegistry.Families[0].ID != agentregistry.FamilyPi {
		t.Fatalf("agent families = %+v, want Pi-only defaults", loaded.Settings.AgentRegistry.Families)
	}
}

func TestResolveDefaultsBackfillsProgrammaticPartialSettings(t *testing.T) {
	settings := ResolveDefaults(Settings{Queue: QueueSettings{AutoWhenReady: false, MaxConcurrent: 2, BacklogCap: 10}})
	if settings.Queue.AutoWhenReady != false || settings.Queue.MaxConcurrent != 2 || settings.Queue.BacklogCap != 10 {
		t.Fatalf("queue = %+v, want caller-provided queue preserved", settings.Queue)
	}
	if settings.Conversation.Budget != 1 || settings.Conversation.IdleWarmHoldTTL != 15*time.Minute {
		t.Fatalf("conversation = %+v, want conversation defaults", settings.Conversation)
	}
	if len(settings.AgentRegistry.Families) != 1 || settings.AgentRegistry.Families[0].ID != agentregistry.FamilyPi {
		t.Fatalf("agent families = %+v, want Pi defaults", settings.AgentRegistry.Families)
	}

	settings = ResolveDefaults(Settings{AgentRegistry: agentregistry.Defaults()})
	if settings.Queue.AutoWhenReady != true || settings.Queue.MaxConcurrent != 1 || settings.Queue.BacklogCap != 100 {
		t.Fatalf("queue = %+v, want queue defaults when only registry is provided", settings.Queue)
	}
	if settings.Conversation.Budget != 1 || settings.Conversation.IdleWarmHoldTTL != 15*time.Minute {
		t.Fatalf("conversation = %+v, want conversation defaults when only registry is provided", settings.Conversation)
	}
}

func TestLoadProjectOverridesGlobal(t *testing.T) {
	dir := t.TempDir()
	globalPath := filepath.Join(dir, "global.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeFile(t, globalPath, "[queue]\nauto_when_ready = false\nmax_concurrent = 2\nbacklog_cap = 10\n\n[conversation]\nbudget = 2\nidle_warm_hold_ttl = \"10m\"\n")
	writeFile(t, projectPath, "[queue]\nauto_when_ready = true\nbacklog_cap = 25\n\n[conversation]\nidle_warm_hold_ttl = \"30m\"\n")
	loaded, err := Load(LoadOptions{GlobalPath: globalPath, ProjectPath: projectPath})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	queue := loaded.Settings.Queue
	if queue.AutoWhenReady != true || queue.MaxConcurrent != 2 || queue.BacklogCap != 25 {
		t.Fatalf("queue = %+v, want project overrides layered on global", queue)
	}
	conversation := loaded.Settings.Conversation
	if conversation.Budget != 2 || conversation.IdleWarmHoldTTL != 30*time.Minute {
		t.Fatalf("conversation = %+v, want project overrides layered on global", conversation)
	}
}

func TestLoadAgentRegistryProjectOverridesGlobal(t *testing.T) {
	dir := t.TempDir()
	globalPath := filepath.Join(dir, "global.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeFile(t, globalPath, `[agents]
default_profile_id = "global_worker"

[agents.profiles.global_worker]
family_id = "pi"
name = "Global worker"
role = "implementation"
headless = true
model = "global-model"
context_policy = "task_contract_only"
output_style = "structured_report"
suggested_stage_types = ["implementation"]

[agents.profiles.pi_headless_worker]
model = "global-default-model"
default_instructions = "Prefer repository conventions."
suggested_stage_types = ["implementation", "pr_creation"]
`)
	writeFile(t, projectPath, `[agents]
default_profile_id = "pi_headless_worker"

[agents.profiles.pi_headless_worker]
name = "Project worker"
model = "project-model"
`)
	loaded, err := Load(LoadOptions{GlobalPath: globalPath, ProjectPath: projectPath})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	registry := loaded.Settings.AgentRegistry
	if registry.DefaultProfileID != agentregistry.ProfilePiHeadlessWorker {
		t.Fatalf("default profile = %q, want project override", registry.DefaultProfileID)
	}
	worker, ok := agentregistry.ProfileByID(registry, agentregistry.ProfilePiHeadlessWorker)
	if !ok {
		t.Fatalf("profile %q missing", agentregistry.ProfilePiHeadlessWorker)
	}
	if worker.Name != "Project worker" || worker.Model != "project-model" || worker.DefaultInstructions != "Prefer repository conventions." {
		t.Fatalf("worker profile = %+v, want project overrides with inherited default instructions", worker)
	}
	if strings.Join(worker.SuggestedStageTypes, ",") != "implementation,pr_creation" {
		t.Fatalf("worker suggested stages = %#v, want inherited global stages", worker.SuggestedStageTypes)
	}
	globalWorker, ok := agentregistry.ProfileByID(registry, "global_worker")
	if !ok || globalWorker.FamilyID != agentregistry.FamilyPi {
		t.Fatalf("global worker = %+v/%v, want retained Pi profile", globalWorker, ok)
	}
}

func TestLoadRejectsUnsupportedAgentFamilies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	writeFile(t, path, "[agents.families.docker]\nname = \"Docker\"\n")
	_, err := Load(LoadOptions{ProjectPath: path})
	if err == nil {
		t.Fatal("Load() error = nil, want unsupported family failure")
	}
	if !strings.Contains(err.Error(), "only \"pi\" is supported") {
		t.Fatalf("error = %q, want Pi-only failure", err.Error())
	}
}

func TestLoadRejectsSecretLikeValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	writeFile(t, path, "[queue]\nbacklog_cap = 10\napi_token = \"shh\"\n")
	_, err := Load(LoadOptions{ProjectPath: path})
	if err == nil {
		t.Fatal("Load() error = nil, want secret-safety failure")
	}
	if !strings.Contains(err.Error(), "secret-like values") || !strings.Contains(err.Error(), "queue.api_token") {
		t.Fatalf("error = %q, want secret-like path", err.Error())
	}
}

func TestLoadRejectsInvalidQueuePolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	writeFile(t, path, "[queue]\nmax_concurrent = 0\nbacklog_cap = 10\n")
	_, err := Load(LoadOptions{ProjectPath: path})
	if err == nil {
		t.Fatal("Load() error = nil, want validation failure")
	}
	if !strings.Contains(err.Error(), "queue.max_concurrent") {
		t.Fatalf("error = %q, want max_concurrent validation", err.Error())
	}
}

func TestLoadRejectsInvalidConversationPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	writeFile(t, path, "[conversation]\nbudget = 0\nidle_warm_hold_ttl = \"15m\"\n")
	_, err := Load(LoadOptions{ProjectPath: path})
	if err == nil {
		t.Fatal("Load() error = nil, want validation failure")
	}
	if !strings.Contains(err.Error(), "conversation.budget") {
		t.Fatalf("error = %q, want conversation budget validation", err.Error())
	}

	writeFile(t, path, "[conversation]\nbudget = 1\nidle_warm_hold_ttl = \"0s\"\n")
	_, err = Load(LoadOptions{ProjectPath: path})
	if err == nil {
		t.Fatal("Load() error = nil, want ttl validation failure")
	}
	if !strings.Contains(err.Error(), "conversation.idle_warm_hold_ttl") {
		t.Fatalf("error = %q, want ttl validation", err.Error())
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	t.Run("queue-level typo", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.toml")
		writeFile(t, path, "[queue]\nbacklog_caps = 10\n")
		_, err := Load(LoadOptions{ProjectPath: path})
		if err == nil {
			t.Fatal("Load() error = nil, want typo to fail loudly instead of falling back to default")
		}
		if !strings.Contains(err.Error(), "parse project settings") {
			t.Fatalf("error = %q, want parse error for unknown key", err.Error())
		}
	})
	t.Run("unknown table", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.toml")
		writeFile(t, path, "[queue]\nbacklog_cap = 10\n\n[unknown]\nfoo = 1\n")
		if _, err := Load(LoadOptions{ProjectPath: path}); err == nil {
			t.Fatal("Load() error = nil, want unknown-table failure")
		}
	})
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
}
