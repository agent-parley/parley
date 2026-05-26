//go:build windows

package pathsafe

import (
	"fmt"
	"os"
)

func WriteFileNoFollow(path string, data []byte, perm os.FileMode) error {
	file, err := CreateFileNoFollow(path, perm)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(data)
	return err
}

func ReadFileNoFollow(path string) ([]byte, error) {
	return nil, fmt.Errorf("no-follow artifact reads are disabled on Windows in this prototype")
}

func CreateFileNoFollow(path string, perm os.FileMode) (*os.File, error) {
	if err := rejectSymlinkTarget(path); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
}

func rejectSymlinkTarget(path string) error {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("file must not be a symlink: %s", path)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
