package store

import "github.com/agent-parley/parley/internal/profiles"

func isWorkerAgentProfile(profile string) bool {
	return profiles.IsWorkerDefault(profile)
}
