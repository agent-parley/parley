package secretpolicy_test

import (
	"strings"
	"testing"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/secretpolicy"
)

func TestValidateReferenceForProfileAllowsProfileEnvPrefixes(t *testing.T) {
	for _, ref := range []string{"env:PARLEY_AGENT_TOKEN", "env:PI_AGENT_TOKEN"} {
		if err := secretpolicy.ValidateReferenceForProfile(ref, "pi-standard"); err != nil {
			t.Fatalf("expected %s to validate: %v", ref, err)
		}
	}
}

func TestValidateReferenceForProfileRejectsNonAllowlistedEnv(t *testing.T) {
	err := secretpolicy.ValidateReferenceForProfile("env:OPENAI_API_KEY", "pi-standard")
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected allowlist rejection, got %v", err)
	}
}

func TestValidateReferenceForProfileRejectsProcessConfigEnv(t *testing.T) {
	for _, ref := range []string{"env:PARLEY_BIND", "env:PARLEY_DATA_ROOT", "env:PARLEY_EXECUTION_MODE", "env:PARLEY_RUNTIME_PROVIDER"} {
		err := secretpolicy.ValidateReferenceForProfile(ref, "pi-standard")
		if err == nil || !strings.Contains(err.Error(), "process configuration") {
			t.Fatalf("expected process config rejection for %s, got %v", ref, err)
		}
	}
}

func TestValidateReferenceForProfileRejectsNonCredentialPrefixedEnv(t *testing.T) {
	err := secretpolicy.ValidateReferenceForProfile("env:PARLEY_WORKSPACE", "pi-standard")
	if err == nil || !strings.Contains(err.Error(), "credential-intended") {
		t.Fatalf("expected non-credential env rejection, got %v", err)
	}
}

func TestParseReferenceRejectsUnsafeSourcesAndShapes(t *testing.T) {
	cases := []string{
		"env:*",
		"env:",
		"env:BAD-NAME",
		"file:.parley/secrets/token",
		"Bearer abcdefghijklmnopqrstuvwxyz",
		"OPENAI_API_KEY=sk-secretsecretsecretsecret",
		"keychain:agent-token",
	}
	for _, ref := range cases {
		if _, err := secretpolicy.ParseReference(ref); err == nil {
			t.Fatalf("expected %q to be rejected", ref)
		}
	}
}

func TestRedactRemovesCommonSecretPatterns(t *testing.T) {
	body := "Authorization: Bearer abcdefghijklmnopqrstuvwxyz\nOPENAI_API_KEY=sk-secretsecretsecretsecret\n-----BEGIN TOKEN-----\nsecret\n-----END TOKEN-----"
	redacted := secretpolicy.Redact(body)
	for _, leaked := range []string{"abcdefghijklmnopqrstuvwxyz", "sk-secretsecret", "BEGIN TOKEN"} {
		if strings.Contains(redacted, leaked) {
			t.Fatalf("redaction leaked %q in %q", leaked, redacted)
		}
	}
	if !strings.Contains(redacted, "[REDACTED]") {
		t.Fatalf("expected redaction marker in %q", redacted)
	}
}

func TestClassifyArtifactUpgradesSecretLikeNormalContent(t *testing.T) {
	got := secretpolicy.ClassifyArtifact("summary.md", models.ArtifactKindSummary, models.SensitivityNormal, "Authorization: Bearer abcdefghijklmnopqrstuvwxyz")
	if got != models.SensitivitySecret {
		t.Fatalf("expected secret sensitivity, got %q", got)
	}
	internal := secretpolicy.ClassifyArtifact("runtime/stderr.txt", models.ArtifactKindWorkerOutput, models.SensitivityInternal, "Authorization: Bearer abcdefghijklmnopqrstuvwxyz")
	if internal != models.SensitivityInternal {
		t.Fatalf("explicit internal sensitivity should be preserved, got %q", internal)
	}
}

func TestPublicAndHandoffSafetyOnlyAllowNormalSensitivity(t *testing.T) {
	for _, sensitivity := range []string{"", models.SensitivityNormal} {
		if !secretpolicy.IsPublicPreviewSensitivity(sensitivity) || !secretpolicy.IsHandoffSafeSensitivity(sensitivity) {
			t.Fatalf("expected %q to be public and handoff safe", sensitivity)
		}
	}
	for _, sensitivity := range []string{models.SensitivityInternal, models.SensitivitySecret} {
		if secretpolicy.IsPublicPreviewSensitivity(sensitivity) || secretpolicy.IsHandoffSafeSensitivity(sensitivity) {
			t.Fatalf("expected %q to be restricted", sensitivity)
		}
	}
}
