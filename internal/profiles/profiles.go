package profiles

import "strings"

const (
	RolePlanner  = "planner"
	RoleCritic   = "critic"
	RoleWorker   = "worker"
	RoleReviewer = "reviewer"
)

const (
	ProfilePlanner = "pi-planner"
	ProfileCritic  = "pi-critic"
)

type Profile struct {
	ID                       string
	Label                    string
	Role                     string
	Image                    string
	Command                  []string
	EnvPrefixes              []string
	SelectableProjectDefault bool
	ReviewerOnly             bool
}

const DefaultImage = "ghcr.io/agent-parley/pi-runner:prototype"

func All() []Profile {
	return []Profile{
		{ID: ProfilePlanner, Label: "Pi planner", Role: RolePlanner, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}},
		{ID: ProfileCritic, Label: "Pi critic", Role: RoleCritic, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}},
		{ID: "pi-standard", Label: "Pi standard worker", Role: RoleWorker, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}, SelectableProjectDefault: true},
		{ID: "pi-high-context", Label: "Pi high-context", Role: RoleWorker, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}, SelectableProjectDefault: true},
		{ID: "pi-readonly-scout", Label: "Read-only scout", Role: RoleWorker, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}, SelectableProjectDefault: true},
		{ID: "pi-reviewer", Label: "Pi reviewer", Role: RoleReviewer, Image: DefaultImage, Command: []string{"pi", "--headless", "--input", "{input}"}, EnvPrefixes: []string{"PARLEY_", "PI_"}, ReviewerOnly: true},
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

func PlannerIDs() []string { return roleIDs(RolePlanner) }

func CriticIDs() []string { return roleIDs(RoleCritic) }

func roleIDs(role string) []string {
	var ids []string
	for _, profile := range All() {
		if profile.Role == role {
			ids = append(ids, profile.ID)
		}
	}
	return ids
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
