package executor_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-parley/parley/internal/containers"
	"github.com/agent-parley/parley/internal/executor"
)

func validInvocation(root string) (containers.Invocation, executor.InvocationValidation) {
	mountPath := filepath.Join(root, "mount")
	return containers.Invocation{
		Image: "image",
		Command: []string{"pi", "--headless"},
		Profile: "pi-standard",
		Network: "none",
		Env: map[string]string{"PARLEY_AGENT_ROLE": "worker", "PI_TOKEN_PATH": "/tmp/token"},
		Mounts: []containers.Mount{{HostPath: mountPath, ContainerPath: "/workspace", Mode: "rw"}},
	}, executor.InvocationValidation{AllowedExecutables: []string{"pi"}, AllowedProfiles: []string{"pi-standard"}, AllowedHostRoots: []string{root}, AllowedEnvPrefixes: []string{"PARLEY_", "PI_"}}
}

func TestValidateInvocationAcceptsLocalPiWorkerShape(t *testing.T) {
	root := t.TempDir()
	invocation, rules := validInvocation(root)
	if err := ensureDir(invocation.Mounts[0].HostPath); err != nil { t.Fatal(err) }
	if err := executor.ValidateInvocation(invocation, rules); err != nil {
		t.Fatalf("expected invocation to validate: %v", err)
	}
}

func TestValidateInvocationRejectsPrivilegedNetworkExecutableProfileEnvAndMountEscape(t *testing.T) {
	root := t.TempDir()
	invocation, rules := validInvocation(root)
	if err := ensureDir(invocation.Mounts[0].HostPath); err != nil { t.Fatal(err) }
	outside := t.TempDir()
	cases := map[string]func(containers.Invocation) containers.Invocation{
		"privileged": func(inv containers.Invocation) containers.Invocation { inv.Privileged = true; return inv },
		"network": func(inv containers.Invocation) containers.Invocation { inv.Network = "host"; return inv },
		"executable": func(inv containers.Invocation) containers.Invocation { inv.Command[0] = "bash"; return inv },
		"profile": func(inv containers.Invocation) containers.Invocation { inv.Profile = "pi-reviewer"; return inv },
		"env": func(inv containers.Invocation) containers.Invocation { inv.Env["SECRET_TOKEN"] = "x"; return inv },
		"mount_escape": func(inv containers.Invocation) containers.Invocation { inv.Mounts[0].HostPath = outside; return inv },
		"relative_container_path": func(inv containers.Invocation) containers.Invocation { inv.Mounts[0].ContainerPath = "workspace"; return inv },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			inv := mutate(cloneInvocation(invocation))
			if err := executor.ValidateInvocation(inv, rules); err == nil {
				t.Fatalf("expected validation failure")
			}
		})
	}
}

func cloneInvocation(inv containers.Invocation) containers.Invocation {
	inv.Command = append([]string(nil), inv.Command...)
	inv.Mounts = append([]containers.Mount(nil), inv.Mounts...)
	env := map[string]string{}
	for k, v := range inv.Env { env[k] = v }
	inv.Env = env
	return inv
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0o700)
}
