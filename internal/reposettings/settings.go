package reposettings

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/agent-parley/parley/internal/config"
	"github.com/agent-parley/parley/internal/models"
	"github.com/agent-parley/parley/internal/profiles"
	"github.com/agent-parley/parley/internal/secretpolicy"
)

const RelativePath = ".parley/settings.toml"

type Settings struct {
	Path                string
	DefaultAgentProfile string
	WorkflowTemplate    string
	QueuePolicy         string
	ReviewProfiles      []string
	RuntimeProvider     string
	ContainerBackend    string
}

type Error struct {
	Path string
	Err  error
}

func (e *Error) Error() string {
	if e == nil || e.Err == nil {
		return "repo settings invalid"
	}
	if e.Path != "" {
		return fmt.Sprintf("repo settings invalid %s: %v", e.Path, e.Err)
	}
	return "repo settings invalid: " + e.Err.Error()
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsError(err error) bool {
	var settingsErr *Error
	return errors.As(err, &settingsErr)
}

func Load(repoPath string) (Settings, bool, error) {
	settingsDir := filepath.Join(repoPath, ".parley")
	settingsDirInfo, err := os.Lstat(settingsDir)
	if errors.Is(err, os.ErrNotExist) {
		return Settings{}, false, nil
	}
	if err != nil {
		return Settings{}, false, &Error{Path: settingsDir, Err: err}
	}
	if settingsDirInfo.Mode()&os.ModeSymlink != 0 {
		return Settings{}, true, &Error{Path: settingsDir, Err: fmt.Errorf("repo settings directory must not be a symlink")}
	}
	if !settingsDirInfo.IsDir() {
		return Settings{}, true, &Error{Path: settingsDir, Err: fmt.Errorf("repo settings path must be a directory")}
	}

	path := filepath.Join(settingsDir, "settings.toml")
	settingsInfo, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return Settings{}, false, nil
	}
	if err != nil {
		return Settings{}, false, &Error{Path: path, Err: err}
	}
	if settingsInfo.Mode()&os.ModeSymlink != 0 {
		return Settings{}, true, &Error{Path: path, Err: fmt.Errorf("repo settings file must not be a symlink")}
	}
	if !settingsInfo.Mode().IsRegular() {
		return Settings{}, true, &Error{Path: path, Err: fmt.Errorf("repo settings file must be a regular file")}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Settings{}, false, &Error{Path: path, Err: err}
	}
	settings, err := Parse(path, string(data))
	if err != nil {
		return Settings{}, true, &Error{Path: path, Err: err}
	}
	if err := Validate(settings); err != nil {
		return Settings{}, true, &Error{Path: path, Err: err}
	}
	return settings, true, nil
}

func Parse(path, body string) (Settings, error) {
	settings := Settings{Path: path}
	seen := map[string]bool{}
	scanner := bufio.NewScanner(strings.NewReader(body))
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(stripComment(scanner.Text()))
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") {
			return Settings{}, fmt.Errorf("line %d: tables are not supported in repo settings", lineNumber)
		}
		key, rawValue, ok := strings.Cut(line, "=")
		if !ok {
			return Settings{}, fmt.Errorf("line %d: expected key = value", lineNumber)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return Settings{}, fmt.Errorf("line %d: key is required", lineNumber)
		}
		if seen[key] {
			return Settings{}, fmt.Errorf("line %d: duplicate field %q", lineNumber, key)
		}
		seen[key] = true
		value := strings.TrimSpace(rawValue)

		switch key {
		case "default_agent_profile":
			parsed, err := parseString(value)
			if err != nil {
				return Settings{}, fmt.Errorf("line %d: %w", lineNumber, err)
			}
			settings.DefaultAgentProfile = parsed
		case "workflow_template":
			parsed, err := parseString(value)
			if err != nil {
				return Settings{}, fmt.Errorf("line %d: %w", lineNumber, err)
			}
			settings.WorkflowTemplate = parsed
		case "queue_policy":
			parsed, err := parseString(value)
			if err != nil {
				return Settings{}, fmt.Errorf("line %d: %w", lineNumber, err)
			}
			settings.QueuePolicy = parsed
		case "runtime_provider":
			parsed, err := parseString(value)
			if err != nil {
				return Settings{}, fmt.Errorf("line %d: %w", lineNumber, err)
			}
			settings.RuntimeProvider = parsed
		case "container_backend":
			parsed, err := parseString(value)
			if err != nil {
				return Settings{}, fmt.Errorf("line %d: %w", lineNumber, err)
			}
			settings.ContainerBackend = parsed
		case "review_profiles":
			parsed, err := parseStringArray(value)
			if err != nil {
				return Settings{}, fmt.Errorf("line %d: %w", lineNumber, err)
			}
			settings.ReviewProfiles = parsed
		default:
			return Settings{}, unsupportedFieldError(key)
		}
	}
	if err := scanner.Err(); err != nil {
		return Settings{}, err
	}
	return settings, nil
}

func Validate(settings Settings) error {
	provider := firstNonEmpty(settings.RuntimeProvider, settings.ContainerBackend)
	if settings.RuntimeProvider != "" && settings.ContainerBackend != "" && settings.RuntimeProvider != settings.ContainerBackend {
		return fmt.Errorf("runtime_provider and container_backend disagree")
	}
	for _, value := range []string{settings.DefaultAgentProfile, settings.WorkflowTemplate, settings.QueuePolicy, provider} {
		if containsUnsafeValue(value) {
			return fmt.Errorf("repo settings contain secret-like or unsafe value")
		}
	}
	for _, value := range settings.ReviewProfiles {
		if containsUnsafeValue(value) {
			return fmt.Errorf("repo settings contain secret-like or unsafe value")
		}
	}
	if settings.DefaultAgentProfile != "" && !profiles.IsWorkerDefault(settings.DefaultAgentProfile) {
		return fmt.Errorf("default_agent_profile %q is not a selectable worker profile", settings.DefaultAgentProfile)
	}
	for _, profile := range settings.ReviewProfiles {
		if !profiles.IsReviewer(profile) {
			return fmt.Errorf("review_profiles contains non-reviewer profile %q", profile)
		}
	}
	switch settings.QueuePolicy {
	case "", models.QueuePolicyManual, models.QueuePolicyAutoWhenReady:
	default:
		return fmt.Errorf("queue_policy %q is unsupported", settings.QueuePolicy)
	}
	switch provider {
	case "", config.RuntimeProviderPodman:
	case config.RuntimeProviderDocker:
		return fmt.Errorf("runtime provider %q is planned but unsupported; use %q", provider, config.RuntimeProviderPodman)
	default:
		return fmt.Errorf("runtime provider %q is unsupported", provider)
	}
	return nil
}

func Snapshot(settings Settings) *models.RepoSettingsSnapshot {
	provider := firstNonEmpty(settings.RuntimeProvider, settings.ContainerBackend)
	snapshot := &models.RepoSettingsSnapshot{
		Path:                settings.Path,
		DefaultAgentProfile: settings.DefaultAgentProfile,
		WorkflowTemplate:    settings.WorkflowTemplate,
		QueuePolicy:         settings.QueuePolicy,
		ReviewProfiles:      append([]string(nil), settings.ReviewProfiles...),
		RuntimeProvider:     provider,
	}
	if settings.QueuePolicy == models.QueuePolicyAutoWhenReady {
		snapshot.Warnings = append(snapshot.Warnings, "queue_policy queues only after explicit task approval or fix-ready gates; approval gates still control execution")
	}
	return snapshot
}

func parseString(value string) (string, error) {
	if !strings.HasPrefix(value, "\"") || !strings.HasSuffix(value, "\"") {
		return "", fmt.Errorf("expected quoted string")
	}
	parsed, err := strconv.Unquote(value)
	if err != nil {
		return "", fmt.Errorf("invalid quoted string: %w", err)
	}
	return strings.TrimSpace(parsed), nil
}

func parseStringArray(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "[") || !strings.HasSuffix(value, "]") {
		return nil, fmt.Errorf("expected string array")
	}
	inner := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(value, "["), "]"))
	if inner == "" {
		return nil, nil
	}
	var values []string
	for inner != "" {
		inner = strings.TrimSpace(inner)
		if !strings.HasPrefix(inner, "\"") {
			return nil, fmt.Errorf("expected quoted string array value")
		}
		end, err := quotedStringEnd(inner)
		if err != nil {
			return nil, err
		}
		parsed, err := parseString(inner[:end])
		if err != nil {
			return nil, err
		}
		values = append(values, parsed)
		inner = strings.TrimSpace(inner[end:])
		if inner == "" {
			break
		}
		if !strings.HasPrefix(inner, ",") {
			return nil, fmt.Errorf("expected comma between array values")
		}
		inner = strings.TrimSpace(strings.TrimPrefix(inner, ","))
	}
	return values, nil
}

func quotedStringEnd(value string) (int, error) {
	escaped := false
	for i := 1; i < len(value); i++ {
		if escaped {
			escaped = false
			continue
		}
		switch value[i] {
		case '\\':
			escaped = true
		case '"':
			return i + 1, nil
		}
	}
	return 0, fmt.Errorf("unterminated quoted string")
}

func stripComment(line string) string {
	inQuote := false
	escaped := false
	for i, r := range line {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' && inQuote {
			escaped = true
			continue
		}
		if r == '"' {
			inQuote = !inQuote
			continue
		}
		if r == '#' && !inQuote {
			return line[:i]
		}
	}
	return line
}

func unsupportedFieldError(key string) error {
	if isSensitiveOrUnsafeKey(key) {
		return fmt.Errorf("unsupported sensitive or unsafe repo settings field %q", key)
	}
	return fmt.Errorf("unsupported repo settings field %q; supported fields are default_agent_profile, workflow_template, queue_policy, review_profiles, runtime_provider, and container_backend", key)
}

func isSensitiveOrUnsafeKey(key string) bool {
	lower := strings.ToLower(key)
	fragments := []string{
		"auth", "credential", "secret", "token", "password", "api_key", "key_file",
		"secret_file", "path", "socket", "sock", "container", "network", "privileged",
		"volume", "mount", "env", "environment", "ssh", "remote", "sync", "handoff",
		"memory", "docker_host",
	}
	for _, fragment := range fragments {
		if strings.Contains(lower, fragment) {
			return true
		}
	}
	return false
}

func containsUnsafeValue(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	if secretpolicy.ContainsSecretLike(trimmed) {
		return true
	}
	lower := strings.ToLower(trimmed)
	fragments := []string{".sock", "unix://", "tcp://", "/var/run", "docker.sock"}
	for _, fragment := range fragments {
		if strings.Contains(lower, fragment) {
			return true
		}
	}
	if filepath.IsAbs(trimmed) {
		return true
	}
	for _, part := range strings.FieldsFunc(trimmed, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == ".." {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
