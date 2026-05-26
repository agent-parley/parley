package pathsafe

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func Within(root, candidate string) bool {
	root, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	candidate, err = filepath.Abs(candidate)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(root, candidate)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func ResolvedExistingPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("path is required")
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("path %q must not be a symlink", path)
	} else if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(resolved)
}

func MkdirAllNoSymlink(path string, perm os.FileMode) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("path must not be a symlink: %s", path)
		}
		if !info.IsDir() {
			return fmt.Errorf("path is not a directory: %s", path)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(path)
	if parent == path {
		return nil
	}
	if err := MkdirAllNoSymlink(parent, perm); err != nil {
		return err
	}
	if err := os.Mkdir(path, perm); err != nil && !os.IsExist(err) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("path must not be a symlink: %s", path)
	}
	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", path)
	}
	return nil
}
