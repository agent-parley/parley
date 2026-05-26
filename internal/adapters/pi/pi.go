// Package pi defines the Pi adapter boundary.
package pi

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/agent-parley/parley/internal/profiles"
)

type PreparedInvocation struct {
	Role      string
	Profile   string
	InputPath string
	Image     string
	Command   []string
	Env       map[string]string
}

type Adapter struct{}

func (Adapter) Name() string { return "pi" }

func (Adapter) Prepare(ctx context.Context, role, profile, inputPath string) (PreparedInvocation, error) {
	select {
	case <-ctx.Done():
		return PreparedInvocation{}, ctx.Err()
	default:
	}
	role = strings.TrimSpace(role)
	if role == "" {
		role = "worker"
	}
	profile = strings.TrimSpace(profile)
	if profile == "" {
		profile = role
	}
	contract, ok := profiles.Lookup(profile)
	if !ok {
		return PreparedInvocation{}, fmt.Errorf("unknown Pi profile %q", profile)
	}
	image := strings.TrimSpace(os.Getenv("PARLEY_PI_IMAGE"))
	if image == "" {
		image = contract.Image
	}
	return PreparedInvocation{
		Role:      role,
		Profile:   contract.ID,
		InputPath: inputPath,
		Image:     image,
		Command:   profiles.CommandForInput(contract, inputPath),
		Env:       map[string]string{"PARLEY_AGENT_ROLE": role, "PARLEY_AGENT_PROFILE": contract.ID},
	}, nil
}
