package settings

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

const DefaultProjectConfigPath = ".parley/config.toml"

type Settings struct {
	Queue QueueSettings `toml:"queue"`
}

type QueueSettings struct {
	AutoWhenReady bool `toml:"auto_when_ready"`
	MaxConcurrent int  `toml:"max_concurrent"`
	BacklogCap    int  `toml:"backlog_cap"`
}

type LoadOptions struct {
	GlobalPath  string
	ProjectPath string
}

type Loaded struct {
	Settings    Settings
	GlobalPath  string
	ProjectPath string
}

type fileSettings struct {
	Queue *fileQueueSettings `toml:"queue"`
}

type fileQueueSettings struct {
	AutoWhenReady *bool `toml:"auto_when_ready"`
	MaxConcurrent *int  `toml:"max_concurrent"`
	BacklogCap    *int  `toml:"backlog_cap"`
}

func Defaults() Settings {
	return Settings{Queue: QueueSettings{AutoWhenReady: true, MaxConcurrent: 1, BacklogCap: 100}}
}

func Load(opts LoadOptions) (Loaded, error) {
	if opts.ProjectPath == "" {
		opts.ProjectPath = DefaultProjectConfigPath
	}
	loaded := Loaded{Settings: Defaults(), GlobalPath: opts.GlobalPath, ProjectPath: opts.ProjectPath}
	for _, source := range []struct {
		name string
		path string
	}{
		{name: "global", path: opts.GlobalPath},
		{name: "project", path: opts.ProjectPath},
	} {
		if source.path == "" {
			continue
		}
		if err := mergeFile(&loaded.Settings, source.name, source.path); err != nil {
			return Loaded{}, err
		}
	}
	if err := Validate(loaded.Settings); err != nil {
		return Loaded{}, err
	}
	return loaded, nil
}

func Validate(s Settings) error {
	if s.Queue.MaxConcurrent < 1 {
		return fmt.Errorf("queue.max_concurrent must be at least 1")
	}
	if s.Queue.BacklogCap < 1 {
		return fmt.Errorf("queue.backlog_cap must be at least 1")
	}
	return nil
}

func mergeFile(dst *Settings, sourceName, path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read %s settings %s: %w", sourceName, path, err)
	}
	if err := rejectSecretMaterial(content, path); err != nil {
		return err
	}
	var file fileSettings
	if err := toml.Unmarshal(content, &file); err != nil {
		return fmt.Errorf("parse %s settings %s: %w", sourceName, path, err)
	}
	if err := validateFileSettings(file, sourceName, path); err != nil {
		return err
	}
	merge(dst, file)
	return nil
}

func validateFileSettings(file fileSettings, sourceName, path string) error {
	if file.Queue == nil {
		return nil
	}
	if file.Queue.MaxConcurrent != nil && *file.Queue.MaxConcurrent < 1 {
		return fmt.Errorf("%s settings %s: queue.max_concurrent must be at least 1", sourceName, path)
	}
	if file.Queue.BacklogCap != nil && *file.Queue.BacklogCap < 1 {
		return fmt.Errorf("%s settings %s: queue.backlog_cap must be at least 1", sourceName, path)
	}
	return nil
}

func merge(dst *Settings, file fileSettings) {
	if file.Queue == nil {
		return
	}
	if file.Queue.AutoWhenReady != nil {
		dst.Queue.AutoWhenReady = *file.Queue.AutoWhenReady
	}
	if file.Queue.MaxConcurrent != nil {
		dst.Queue.MaxConcurrent = *file.Queue.MaxConcurrent
	}
	if file.Queue.BacklogCap != nil {
		dst.Queue.BacklogCap = *file.Queue.BacklogCap
	}
}

func rejectSecretMaterial(content []byte, path string) error {
	var decoded map[string]any
	if err := toml.Unmarshal(content, &decoded); err != nil {
		return fmt.Errorf("parse settings %s for secret scan: %w", path, err)
	}
	var hits []string
	scanSecretMap(decoded, nil, false, &hits)
	if len(hits) > 0 {
		return fmt.Errorf("settings file %s contains secret-like values at %s; store credentials outside settings", path, strings.Join(hits, ", "))
	}
	return nil
}

func scanSecretMap(values map[string]any, path []string, underSecretKey bool, hits *[]string) {
	for key, value := range values {
		nextPath := appendPath(path, key)
		secretKey := underSecretKey || secretLikeKey(key)
		if secretKey && containsMaterialValue(value) {
			*hits = append(*hits, strings.Join(nextPath, "."))
			continue
		}
		switch typed := value.(type) {
		case map[string]any:
			scanSecretMap(typed, nextPath, secretKey, hits)
		case []any:
			scanSecretList(typed, nextPath, secretKey, hits)
		}
	}
}

func scanSecretList(values []any, path []string, underSecretKey bool, hits *[]string) {
	for i, value := range values {
		nextPath := appendPath(path, fmt.Sprintf("[%d]", i))
		if underSecretKey && containsMaterialValue(value) {
			*hits = append(*hits, strings.Join(nextPath, "."))
			continue
		}
		switch typed := value.(type) {
		case map[string]any:
			scanSecretMap(typed, nextPath, underSecretKey, hits)
		case []any:
			scanSecretList(typed, nextPath, underSecretKey, hits)
		}
	}
}

func containsMaterialValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return typed != ""
	case map[string]any:
		for _, nested := range typed {
			if containsMaterialValue(nested) {
				return true
			}
		}
		return false
	case []any:
		for _, nested := range typed {
			if containsMaterialValue(nested) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func secretLikeKey(key string) bool {
	lower := strings.ToLower(key)
	for _, marker := range []string{"token", "secret", "password", "auth", "key"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func appendPath(path []string, elem string) []string {
	out := make([]string, 0, len(path)+1)
	out = append(out, path...)
	out = append(out, elem)
	return out
}

func DefaultGlobalConfigPath() string {
	dir, err := os.UserConfigDir()
	if err != nil || dir == "" {
		return ""
	}
	return filepath.Join(dir, "parley", "config.toml")
}
