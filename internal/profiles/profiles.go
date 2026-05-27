package profiles

import "strings"

const (
	RolePlanner  = "planner"
	RoleCritic   = "critic"
	RoleWorker   = "worker"
	RoleReviewer = "reviewer"
)

const (
	FamilyPi           = "pi"
	FamilyCodex        = "codex"
	FamilyClaudeCode   = "claude-code"
	FamilyGemini       = "gemini"
	FamilyCopilotLater = "copilot-later"
)

const (
	ExecutionStatusActive  = "active"
	ExecutionStatusPlanned = "planned"
	ExecutionStatusDisabled = "disabled"
)

const (
	ProfilePlanner = "pi-planner"
	ProfileCritic  = "pi-critic"
)

type Profile struct {
	ID                       string
	Label                    string
	Family                   string
	Role                     string
	ExecutionStatus          string
	Image                    string
	Command                  []string
	EnvPrefixes              []string
	SelectableProjectDefault bool
	ReviewerOnly             bool
	Notes                    string
}

const DefaultImage = "ghcr.io/agent-parley/pi-runner:prototype"

func All() []Profile {
	return []Profile{
		{ID: ProfilePlanner, Label: "Pi planner", Family: FamilyPi, Role: RolePlanner, ExecutionStatus: ExecutionStatusActive, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}},
		{ID: ProfileCritic, Label: "Pi critic", Family: FamilyPi, Role: RoleCritic, ExecutionStatus: ExecutionStatusActive, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}},
		{ID: "pi-standard", Label: "Pi standard worker", Family: FamilyPi, Role: RoleWorker, ExecutionStatus: ExecutionStatusActive, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}, SelectableProjectDefault: true},
		{ID: "pi-high-context", Label: "Pi high-context", Family: FamilyPi, Role: RoleWorker, ExecutionStatus: ExecutionStatusActive, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}, SelectableProjectDefault: true},
		{ID: "pi-readonly-scout", Label: "Read-only scout", Family: FamilyPi, Role: RoleWorker, ExecutionStatus: ExecutionStatusActive, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}, SelectableProjectDefault: true},
		{ID: "pi-reviewer", Label: "Pi reviewer", Family: FamilyPi, Role: RoleReviewer, ExecutionStatus: ExecutionStatusActive, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}, ReviewerOnly: true},
		{ID: "codex-worker", Label: "Codex worker (planned)", Family: FamilyCodex, Role: RoleWorker, ExecutionStatus: ExecutionStatusPlanned, Notes: "Metadata-only profile; adapter and runtime execution are disabled."},
		{ID: "codex-reviewer", Label: "Codex reviewer (planned)", Family: FamilyCodex, Role: RoleReviewer, ExecutionStatus: ExecutionStatusPlanned, Notes: "Metadata-only profile; adapter and runtime execution are disabled."},
		{ID: "claude-code-worker", Label: "Claude Code worker (planned)", Family: FamilyClaudeCode, Role: RoleWorker, ExecutionStatus: ExecutionStatusPlanned, Notes: "Metadata-only profile; adapter and runtime execution are disabled."},
		{ID: "claude-code-reviewer", Label: "Claude Code reviewer (planned)", Family: FamilyClaudeCode, Role: RoleReviewer, ExecutionStatus: ExecutionStatusPlanned, Notes: "Metadata-only profile; adapter and runtime execution are disabled."},
		{ID: "gemini-worker", Label: "Gemini worker (planned)", Family: FamilyGemini, Role: RoleWorker, ExecutionStatus: ExecutionStatusPlanned, Notes: "Metadata-only profile; adapter and runtime execution are disabled."},
		{ID: "gemini-reviewer", Label: "Gemini reviewer (planned)", Family: FamilyGemini, Role: RoleReviewer, ExecutionStatus: ExecutionStatusPlanned, Notes: "Metadata-only profile; adapter and runtime execution are disabled."},
		{ID: "copilot-later-worker", Label: "Copilot worker (later)", Family: FamilyCopilotLater, Role: RoleWorker, ExecutionStatus: ExecutionStatusDisabled, Notes: "Roadmap placeholder only; no adapter or execution path exists."},
	}
}

func Lookup(id string) (Profile, bool) {
	id = strings.TrimSpace(id)
	for _, profile := range All() {
		if profile.ID == id {
			return profile, true
		}
	}
	return Profile{}, false
}

func WorkerDefaultIDs() []string {
	var ids []string
	for _, profile := range All() {
		if profile.SelectableProjectDefault {
			ids = append(ids, profile.ID)
		}
	}
	return ids
}

func ReviewerIDs() []string {
	var ids []string
	for _, profile := range All() {
		if profile.ReviewerOnly {
			ids = append(ids, profile.ID)
		}
	}
	return ids
}

func PlannerIDs() []string { return executableRoleIDs(RolePlanner) }

func CriticIDs() []string { return executableRoleIDs(RoleCritic) }

func executableRoleIDs(role string) []string {
	var ids []string
	for _, profile := range All() {
		if profile.Role == role && IsExecutableProfile(profile) {
			ids = append(ids, profile.ID)
		}
	}
	return ids
}

func PlannedIDs() []string {
	return idsByExecutionStatus(ExecutionStatusPlanned)
}

func DisabledIDs() []string {
	return idsByExecutionStatus(ExecutionStatusDisabled)
}

func MetadataOnlyIDs() []string {
	var ids []string
	for _, profile := range All() {
		if !IsExecutableProfile(profile) {
			ids = append(ids, profile.ID)
		}
	}
	return ids
}

func idsByExecutionStatus(status string) []string {
	var ids []string
	for _, profile := range All() {
		if profile.ExecutionStatus == status {
			ids = append(ids, profile.ID)
		}
	}
	return ids
}

func IsExecutable(id string) bool {
	profile, ok := Lookup(id)
	return ok && IsExecutableProfile(profile)
}

func IsExecutableProfile(profile Profile) bool {
	return profile.ExecutionStatus == ExecutionStatusActive && profile.Family == FamilyPi && len(profile.Command) > 0
}

func IsWorkerDefault(id string) bool {
	profile, ok := Lookup(id)
	return ok && profile.SelectableProjectDefault
}

func IsReviewer(id string) bool {
	profile, ok := Lookup(id)
	return ok && profile.ReviewerOnly
}

func CommandForInput(profile Profile, inputPath string) []string {
	command := append([]string(nil), profile.Command...)
	for i, part := range command {
		if part == "{input}" {
			command[i] = inputPath
		}
	}
	return command
}
