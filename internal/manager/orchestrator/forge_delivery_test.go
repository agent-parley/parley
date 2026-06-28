package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-parley/parley/internal/manager/secrets"
	"github.com/agent-parley/parley/internal/manager/store"
	"github.com/agent-parley/parley/internal/shared/report"
)

func TestCLIForgeDeliveryUsesStoredCredentialForGHAndGitPush(t *testing.T) {
	ctx := context.Background()
	st, svc := openForgeDeliveryStoreAndSecrets(t, ctx)
	credential := insertForgeCredentialForTest(t, ctx, st, svc, "fcr_cli", "sealed-token")
	git, gh, logPath, statePath := fakeForgeCommands(t)
	if err := os.WriteFile(statePath, []byte("success"), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}
	client := cliForgeDeliveryClient{git: git, gh: gh, store: st, secrets: svc, checkPollInterval: time.Millisecond}

	result, err := client.CompletePR(ctx, ForgeDeliveryRequest{
		WorktreePath:     t.TempDir(),
		Branch:           "agent/test",
		TargetBranch:     "main",
		CommitSHA:        "abc123",
		Title:            "Test PR",
		Body:             "body",
		CredentialRef:    credential.ID,
		RequiredChecks:   []string{"ci/test"},
		MergeWaitTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("CompletePR() error = %v", err)
	}
	if !result.PushPerformed || !result.PRCreated || !result.MergeAttempted || !result.Merged || result.PRURL == "" {
		t.Fatalf("result = %+v, want pushed, created, checked, merged", result)
	}
	logContent := readTestFile(t, logPath)
	for _, want := range []string{
		"gh auth status GH_TOKEN=sealed-token",
		"git push --set-upstream https://github.com/acme/repo.git agent/test:agent/test",
		"GIT_CONFIG_VALUE_0=AUTHORIZATION: bearer sealed-token",
		"gh pr merge https://github.com/acme/repo/pull/42 --merge GH_TOKEN=sealed-token",
	} {
		if !strings.Contains(logContent, want) {
			t.Fatalf("fake forge log missing %q:\n%s", want, logContent)
		}
	}
}

func TestCLIForgeDeliveryFailsClosedWhenCredentialUnavailableOrMissing(t *testing.T) {
	ctx := context.Background()
	t.Run("secrets unavailable", func(t *testing.T) {
		st, err := store.Open(ctx, t.TempDir())
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		t.Cleanup(func() { _ = st.Close() })
		svc, err := secrets.New(ctx, st, secrets.Config{KeyProvider: noForgeSecretsProvider{}})
		if err != nil {
			t.Fatalf("new secrets: %v", err)
		}
		if svc.Available() {
			t.Fatal("test secrets service unexpectedly available")
		}
		client := cliForgeDeliveryClient{git: "git", gh: "gh", store: st, secrets: svc}
		_, err = client.CompletePR(ctx, minimalForgeDeliveryRequest("fcr_missing"))
		gateErr, ok := asDeliveryGateError(err)
		if !ok || gateErr.status != report.StatusNeedsInput || !strings.Contains(gateErr.reason, "secrets facility") {
			t.Fatalf("error = %v, want needs_input secrets reason", err)
		}
	})

	t.Run("credential missing", func(t *testing.T) {
		st, svc := openForgeDeliveryStoreAndSecrets(t, ctx)
		client := cliForgeDeliveryClient{git: "git", gh: "gh", store: st, secrets: svc}
		_, err := client.CompletePR(ctx, minimalForgeDeliveryRequest("fcr_missing"))
		gateErr, ok := asDeliveryGateError(err)
		if !ok || gateErr.status != report.StatusNeedsInput || !strings.Contains(gateErr.reason, "was not found") {
			t.Fatalf("error = %v, want needs_input missing credential reason", err)
		}
	})
}

func TestCLIForgeDeliveryPollsPendingChecksAndStopsOnFailedChecks(t *testing.T) {
	ctx := context.Background()
	t.Run("pending then green", func(t *testing.T) {
		st, svc := openForgeDeliveryStoreAndSecrets(t, ctx)
		credential := insertForgeCredentialForTest(t, ctx, st, svc, "fcr_pending", "poll-token")
		git, gh, logPath, statePath := fakeForgeCommands(t)
		if err := os.WriteFile(statePath, []byte("pending-once"), 0o600); err != nil {
			t.Fatalf("write state: %v", err)
		}
		client := cliForgeDeliveryClient{git: git, gh: gh, store: st, secrets: svc, checkPollInterval: time.Millisecond}
		result, err := client.CompletePR(ctx, minimalForgeDeliveryRequest(credential.ID))
		if err != nil {
			t.Fatalf("CompletePR() error = %v", err)
		}
		if !result.Merged || len(result.ChecksPassed) != 1 || result.ChecksPassed[0] != "ci/test" {
			t.Fatalf("result = %+v, want merged after check pass", result)
		}
		logContent := readTestFile(t, logPath)
		if got := strings.Count(logContent, "gh pr checks"); got < 2 {
			t.Fatalf("check polls = %d, want at least 2; log:\n%s", got, logContent)
		}
	})

	t.Run("failed check stops", func(t *testing.T) {
		st, svc := openForgeDeliveryStoreAndSecrets(t, ctx)
		credential := insertForgeCredentialForTest(t, ctx, st, svc, "fcr_failed", "poll-token")
		git, gh, logPath, statePath := fakeForgeCommands(t)
		if err := os.WriteFile(statePath, []byte("fail"), 0o600); err != nil {
			t.Fatalf("write state: %v", err)
		}
		client := cliForgeDeliveryClient{git: git, gh: gh, store: st, secrets: svc, checkPollInterval: time.Millisecond}
		_, err := client.CompletePR(ctx, minimalForgeDeliveryRequest(credential.ID))
		gateErr, ok := asDeliveryGateError(err)
		if !ok || gateErr.status != report.StatusFailed || !strings.Contains(gateErr.reason, "not passing") {
			t.Fatalf("error = %v, want failed check gate error", err)
		}
		logContent := readTestFile(t, logPath)
		if strings.Contains(logContent, "gh pr merge") {
			t.Fatalf("merge ran despite failed check:\n%s", logContent)
		}
	})
}

func openForgeDeliveryStoreAndSecrets(t *testing.T, ctx context.Context) (*store.Store, *secrets.Service) {
	t.Helper()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc, err := secrets.New(ctx, st, secrets.Config{})
	if err != nil {
		t.Fatalf("new secrets: %v", err)
	}
	if !svc.Available() {
		t.Fatalf("secrets state = %s, err = %v", svc.State(), svc.Err())
	}
	return st, svc
}

func insertForgeCredentialForTest(t *testing.T, ctx context.Context, st *store.Store, svc *secrets.Service, id, token string) store.ForgeCredential {
	t.Helper()
	table, column, rowID := store.ForgeCredentialSecretAD(id)
	ciphertext, err := svc.Seal(ctx, []byte(token), secrets.AssociatedData{Table: table, Column: column, RowID: rowID})
	if err != nil {
		t.Fatalf("seal forge credential: %v", err)
	}
	credential, err := st.InsertForgeCredential(ctx, store.ForgeCredentialInput{ID: id, Host: "github.com", SecretCiphertext: ciphertext})
	if err != nil {
		t.Fatalf("insert forge credential: %v", err)
	}
	return credential
}

func fakeForgeCommands(t *testing.T) (git, gh, logPath, statePath string) {
	t.Helper()
	dir := t.TempDir()
	logPath = filepath.Join(dir, "forge.log")
	statePath = filepath.Join(dir, "checks.state")
	t.Setenv("FORGE_TEST_LOG", logPath)
	t.Setenv("FORGE_TEST_CHECK_STATE", statePath)
	git = filepath.Join(dir, "git")
	gh = filepath.Join(dir, "gh")
	gitScript := `#!/bin/sh
printf 'git %s GIT_CONFIG_VALUE_0=%s\n' "$*" "$GIT_CONFIG_VALUE_0" >> "$FORGE_TEST_LOG"
if [ "$1" = "remote" ] && [ "$2" = "get-url" ]; then
  echo 'git@github.com:acme/repo.git'
  exit 0
fi
if [ "$1" = "remote" ]; then
  echo 'origin'
  exit 0
fi
exit 0
`
	ghScript := `#!/bin/sh
printf 'gh %s GH_TOKEN=%s\n' "$*" "$GH_TOKEN" >> "$FORGE_TEST_LOG"
if [ "$1" = "auth" ] && [ "$2" = "status" ]; then
  exit 0
fi
if [ "$1" = "pr" ] && [ "$2" = "view" ]; then
  exit 1
fi
if [ "$1" = "pr" ] && [ "$2" = "create" ]; then
  echo 'https://github.com/acme/repo/pull/42'
  exit 0
fi
if [ "$1" = "pr" ] && [ "$2" = "checks" ]; then
  state="$(cat "$FORGE_TEST_CHECK_STATE" 2>/dev/null || echo success)"
  case "$state" in
    pending-once)
      echo success > "$FORGE_TEST_CHECK_STATE"
      echo '[{"name":"ci/test","state":"PENDING","bucket":"pending"}]'
      ;;
    fail)
      echo '[{"name":"ci/test","state":"FAILURE","bucket":"fail"}]'
      ;;
    *)
      echo '[{"name":"ci/test","state":"SUCCESS","bucket":"pass"}]'
      ;;
  esac
  exit 0
fi
if [ "$1" = "pr" ] && [ "$2" = "merge" ]; then
  exit 0
fi
exit 0
`
	if err := os.WriteFile(git, []byte(gitScript), 0o700); err != nil {
		t.Fatalf("write fake git: %v", err)
	}
	if err := os.WriteFile(gh, []byte(ghScript), 0o700); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	return git, gh, logPath, statePath
}

func minimalForgeDeliveryRequest(credentialID string) ForgeDeliveryRequest {
	return ForgeDeliveryRequest{
		WorktreePath:     os.TempDir(),
		Branch:           "agent/test",
		TargetBranch:     "main",
		CommitSHA:        "abc123",
		Title:            "Test PR",
		Body:             "body",
		CredentialRef:    credentialID,
		RequiredChecks:   []string{"ci/test"},
		MergeWaitTimeout: time.Second,
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}

type noForgeSecretsProvider struct{}

func (noForgeSecretsProvider) ResolveKey(context.Context, secrets.KeyRequest) (secrets.KeyMaterial, error) {
	return secrets.KeyMaterial{}, secrets.ErrNoKEK
}
