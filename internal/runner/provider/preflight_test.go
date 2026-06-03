package provider

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreflightRejectsUnsafeInvocation(t *testing.T) {
	fixture := newPreflightFixture(t)

	cases := []struct {
		name   string
		mutate func(*PreparedInvocation, *PreflightPolicy)
		want   error
	}{
		{
			name: "happy path",
			want: nil,
		},
		{
			name: "host home mount",
			mutate: func(inv *PreparedInvocation, policy *PreflightPolicy) {
				inv.Mounts[0].Host = fixture.homeRepo
				policy.WorktreeRoots = append(policy.WorktreeRoots, fixture.home)
			},
			want: ErrHostHomeMount,
		},
		{
			name: "arbitrary path outside registered roots",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.Mounts[0].Host = fixture.outside
			},
			want: ErrPathOutsideRegisteredRoots,
		},
		{
			name: "raw token env passthrough",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.Env["GH_TOKEN"] = "raw-token"
			},
			want: ErrRawSecretEnv,
		},
		{
			name: "privileged container",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.Privileged = true
			},
			want: ErrPrivilegedContainer,
		},
		{
			name: "adapter-selected uid gid rejected",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.User = "0:0"
			},
			want: ErrInvalidUserMapping,
		},
		{
			name: "adapter-selected user namespace rejected",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.UserNS = "host"
			},
			want: ErrInvalidUserNamespace,
		},
		{
			name: "container runtime socket mount",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.Mounts[0].Host = fixture.runtimeSocket
			},
			want: ErrRuntimeSocketMount,
		},
		{
			name: "docker socket inside worker",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.Mounts[0].Container = "/var/run/docker.sock"
			},
			want: ErrRuntimeSocketMount,
		},
		{
			name: "host network rejected",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.Network = NetworkHost
			},
			want: ErrHostNetwork,
		},
		{
			name: "host network explicitly allowed",
			mutate: func(inv *PreparedInvocation, policy *PreflightPolicy) {
				inv.Network = NetworkHost
				policy.AllowHostNetwork = true
			},
			want: nil,
		},
		{
			name: "custom network rejected by default",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.Network = Network("parley-provider-egress")
			},
			want: ErrInvalidNetworkPolicy,
		},
		{
			name: "control-plane allowlisted custom network accepted",
			mutate: func(inv *PreparedInvocation, policy *PreflightPolicy) {
				inv.Network = Network("parley-provider-egress")
				policy.AllowedNetworks = []Network{Network("parley-provider-egress")}
			},
			want: nil,
		},
		{
			name: "host network not accepted through custom allowlist",
			mutate: func(inv *PreparedInvocation, policy *PreflightPolicy) {
				inv.Network = NetworkHost
				policy.AllowedNetworks = []Network{NetworkHost}
			},
			want: ErrHostNetwork,
		},
		{
			name: "credential mount outside agent-state root",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.Mounts[3].Host = fixture.credentialOutside
			},
			want: ErrCredentialMountOutsideAgentState,
		},
		{
			name: "bad mount mode",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.Mounts[0].Mode = "write"
			},
			want: ErrInvalidMountMode,
		},
		{
			name: "symlink escape",
			mutate: func(inv *PreparedInvocation, _ *PreflightPolicy) {
				inv.Mounts[0].Host = fixture.symlinkEscape
			},
			want: ErrSymlinkEscape,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inv := fixture.invocation()
			policy := fixture.policy()
			if tc.mutate != nil {
				tc.mutate(&inv, &policy)
			}
			err := Preflight(inv, policy)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Preflight() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Preflight() error = %v, want errors.Is(..., %v)", err, tc.want)
			}
		})
	}
}

func TestPreflightReportsOutsideRootOnce(t *testing.T) {
	fixture := newPreflightFixture(t)
	inv := fixture.invocation()
	inv.Mounts[0].Host = fixture.outside

	err := Preflight(inv, fixture.policy())
	if !errors.Is(err, ErrPathOutsideRegisteredRoots) {
		t.Fatalf("Preflight() error = %v, want outside-root sentinel", err)
	}
	if got := strings.Count(err.Error(), ErrPathOutsideRegisteredRoots.Error()); got != 1 {
		t.Fatalf("outside-root error count = %d, want 1; error = %v", got, err)
	}
}

func TestPreflightJoinsRejectReasons(t *testing.T) {
	fixture := newPreflightFixture(t)
	inv := fixture.invocation()
	inv.Privileged = true
	inv.Env["PARLEY_SECRET"] = "raw-secret"

	err := Preflight(inv, fixture.policy())
	if !errors.Is(err, ErrPrivilegedContainer) {
		t.Fatalf("Preflight() error = %v, want privileged sentinel", err)
	}
	if !errors.Is(err, ErrRawSecretEnv) {
		t.Fatalf("Preflight() error = %v, want secret env sentinel", err)
	}
}

type preflightFixture struct {
	worktree          string
	artifact          string
	reference         string
	agentState        string
	home              string
	homeRepo          string
	outside           string
	credentialOutside string
	runtimeSocket     string
	symlinkEscape     string
}

func newPreflightFixture(t *testing.T) preflightFixture {
	t.Helper()
	root := t.TempDir()
	fixture := preflightFixture{
		worktree:          mkdir(t, root, "registered", "worktree"),
		artifact:          mkdir(t, root, "registered", "artifact"),
		reference:         mkdir(t, root, "registered", "reference"),
		agentState:        mkdir(t, root, "agent-state"),
		home:              mkdir(t, root, "home"),
		homeRepo:          mkdir(t, root, "home", "repo"),
		outside:           mkdir(t, root, "outside"),
		credentialOutside: mkdir(t, root, "outside-creds"),
	}
	fixture.runtimeSocket = filepath.Join(fixture.worktree, "docker.sock")
	if err := os.WriteFile(fixture.runtimeSocket, []byte{}, 0o600); err != nil {
		t.Fatalf("write socket fixture: %v", err)
	}
	fixture.symlinkEscape = filepath.Join(fixture.worktree, "escape")
	if err := os.Symlink(fixture.outside, fixture.symlinkEscape); err != nil {
		t.Fatalf("create symlink escape fixture: %v", err)
	}
	return fixture
}

func (f preflightFixture) invocation() PreparedInvocation {
	return PreparedInvocation{
		Adapter:        "container_sample",
		Profile:        "worker",
		Role:           "worker",
		ContainerImage: "docker.io/library/alpine:3.20",
		Mounts: []Mount{
			{Host: f.worktree, Container: "/project/repo", Mode: "rw", Relabel: "Z"},
			{Host: f.artifact, Container: "/project/workspace", Mode: "rw", Relabel: "Z"},
			{Host: f.reference, Container: "/project/reference", Mode: "ro"},
			{Host: f.agentState, Container: "/project/creds", Mode: "ro", Credential: true},
		},
		Env: map[string]string{
			"HARNESS_RUN_ID":  "run_1",
			"HARNESS_TASK_ID": "task_1",
		},
		Command: []string{"sh", "-c", "echo hi"},
		WorkDir: "/project/repo",
		Network: NetworkNone,
	}
}

func (f preflightFixture) policy() PreflightPolicy {
	return PreflightPolicy{
		WorktreeRoots:  []string{f.worktree},
		ArtifactRoots:  []string{f.artifact},
		ReferenceRoots: []string{f.reference},
		AgentStateRoot: f.agentState,
		HomeDir:        f.home,
	}
}

func mkdir(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(parts...)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	return path
}
