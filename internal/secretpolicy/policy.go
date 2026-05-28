package secretpolicy

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/profiles"
)

const (
	ReferenceSourceEnv      = "env"
	ReferenceSourceKeychain = "keychain"
)

type SecretReference struct {
	Source string
	Name   string
}

var redactors = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{regexp.MustCompile(`(?is)-----BEGIN [^-]+-----.*?-----END [^-]+-----`), `[REDACTED]`},
	{regexp.MustCompile(`(?i)\b(authorization)(\s*:\s*)(Bearer\s+)[A-Za-z0-9._~+/-]+=*`), `$1$2$3[REDACTED]`},
	{regexp.MustCompile(`(?i)\b([a-z0-9_]*(?:api[_-]?key|token|secret|password|credential)[a-z0-9_]*)(\s*[:=]\s*["']?)[^"'\s,;}\]]+`), `$1$2[REDACTED]`},
	{regexp.MustCompile(`(?i)\b(Bearer\s+)[A-Za-z0-9._~+/-]+=*`), `${1}[REDACTED]`},
	{regexp.MustCompile(`\b(sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{20,})\b`), `[REDACTED]`},
}

func ParseReference(ref string) (SecretReference, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return SecretReference{}, fmt.Errorf("secret reference is required")
	}
	source, name, ok := strings.Cut(ref, ":")
	if !ok {
		if ContainsSecretLike(ref) {
			return SecretReference{}, fmt.Errorf("inline secret-like values are not valid secret references")
		}
		return SecretReference{}, fmt.Errorf("secret references must use env:VAR_NAME")
	}
	source = strings.ToLower(strings.TrimSpace(source))
	name = strings.TrimSpace(name)
	switch source {
	case ReferenceSourceEnv:
		if name == "" {
			return SecretReference{}, fmt.Errorf("env secret reference requires a variable name")
		}
		if name == "*" || strings.HasSuffix(name, "*") || strings.Contains(name, "*") {
			return SecretReference{}, fmt.Errorf("wildcard env secret references are not allowed")
		}
		if !ValidEnvName(name) {
			return SecretReference{}, fmt.Errorf("invalid env secret reference %q", name)
		}
		return SecretReference{Source: source, Name: name}, nil
	case ReferenceSourceKeychain:
		return SecretReference{}, fmt.Errorf("keychain secret references are planned but unsupported")
	case "file", "path":
		return SecretReference{}, fmt.Errorf("file secret references are not allowed")
	default:
		return SecretReference{}, fmt.Errorf("unsupported secret reference source %q", source)
	}
}

func ValidateReferenceForProfile(ref, profileID string) error {
	parsed, err := ParseReference(ref)
	if err != nil {
		return err
	}
	profile, ok := profiles.Lookup(profileID)
	if !ok {
		return fmt.Errorf("agent profile %q is not known", profileID)
	}
	if parsed.Source != ReferenceSourceEnv {
		return fmt.Errorf("secret reference source %q is unsupported", parsed.Source)
	}
	if !HasAllowedEnvPrefix(parsed.Name, profile.EnvPrefixes) {
		return fmt.Errorf("env secret reference %q is not allowed for profile %q", parsed.Name, profileID)
	}
	if isKnownProcessConfigEnv(parsed.Name) {
		return fmt.Errorf("env secret reference %q is process configuration, not a credential reference", parsed.Name)
	}
	if !LooksCredentialEnvName(parsed.Name) {
		return fmt.Errorf("env secret reference %q does not look credential-intended", parsed.Name)
	}
	return nil
}

func LooksCredentialEnvName(name string) bool {
	upper := strings.ToUpper(strings.TrimSpace(name))
	fragments := []string{"TOKEN", "API_KEY", "SECRET", "CREDENTIAL", "PASSWORD", "PASSKEY", "PRIVATE_KEY", "ACCESS_KEY"}
	for _, fragment := range fragments {
		if strings.Contains(upper, fragment) {
			return true
		}
	}
	return false
}

func isKnownProcessConfigEnv(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "PARLEY_BIND", "PARLEY_DATA_ROOT", "PARLEY_EXECUTION_MODE", "PARLEY_RUNTIME_PROVIDER", "PARLEY_APP_CONTAINER", "PARLEY_TRUSTED_LAN", "PARLEY_MODE", "PARLEY_AGENT_IDLE_RETENTION_MINUTES", "PARLEY_MAX_IDLE_AGENTS":
		return true
	default:
		return false
	}
}

func ValidEnvName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if i == 0 && !(unicode.IsLetter(r) || r == '_') {
			return false
		}
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			return false
		}
	}
	return true
}

func HasAllowedEnvPrefix(name string, prefixes []string) bool {
	if len(prefixes) == 0 {
		return false
	}
	for _, prefix := range prefixes {
		if strings.TrimSpace(prefix) != "" && strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func Redact(text string) string {
	for _, redactor := range redactors {
		text = redactor.pattern.ReplaceAllString(text, redactor.replacement)
	}
	return text
}

func ContainsSecretLike(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false
	}
	if Redact(trimmed) != trimmed {
		return true
	}
	lower := strings.ToLower(trimmed)
	fragments := []string{
		"bearer ", "token", "password", "credential", "api_key", "secret", "sk-",
		"-----begin", "-----end",
	}
	for _, fragment := range fragments {
		if strings.Contains(lower, fragment) {
			return true
		}
	}
	return false
}

func ClassifyArtifact(name, kind, explicitSensitivity, body string) string {
	sensitivity := strings.TrimSpace(explicitSensitivity)
	switch sensitivity {
	case models.SensitivityInternal, models.SensitivitySecret:
		return sensitivity
	case "":
		sensitivity = models.SensitivityNormal
	}
	if sensitivity == models.SensitivityNormal && (ContainsSecretLike(name) || ContainsSecretLike(kind) || ContainsSecretLike(body)) {
		return models.SensitivitySecret
	}
	return sensitivity
}

func IsPublicPreviewSensitivity(sensitivity string) bool {
	sensitivity = strings.TrimSpace(sensitivity)
	return sensitivity == "" || sensitivity == models.SensitivityNormal
}

func IsHandoffSafeSensitivity(sensitivity string) bool {
	sensitivity = strings.TrimSpace(sensitivity)
	return sensitivity == "" || sensitivity == models.SensitivityNormal
}
