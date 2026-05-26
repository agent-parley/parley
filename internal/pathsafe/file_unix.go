//go:build !windows

package pathsafe

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
	fd, err := openFileAtParent(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("failed to open file")
	}
	defer file.Close()
	return io.ReadAll(file)
}

func CreateFileNoFollow(path string, perm os.FileMode) (*os.File, error) {
	fd, err := openFileAtParent(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_TRUNC|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, uint32(perm))
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("failed to open file")
	}
	return file, nil
}

func openFileAtParent(path string, flags int, perm uint32) (int, error) {
	if strings.TrimSpace(path) == "" {
		return -1, fmt.Errorf("invalid file path")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return -1, err
	}
	name := filepath.Base(absPath)
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, os.PathSeparator) {
		return -1, fmt.Errorf("invalid file path")
	}
	parentFD, err := openDirNoFollow(filepath.Dir(absPath))
	if err != nil {
		return -1, err
	}
	defer syscall.Close(parentFD)
	fd, err := syscall.Openat(parentFD, name, flags, perm)
	if err != nil {
		return -1, err
	}
	return fd, nil
}

func openDirNoFollow(absDir string) (int, error) {
	absDir = filepath.Clean(absDir)
	if !filepath.IsAbs(absDir) {
		return -1, fmt.Errorf("directory path must be absolute")
	}
	current, err := syscall.Open(string(os.PathSeparator), syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	if absDir == string(os.PathSeparator) {
		return current, nil
	}
	parts := strings.Split(strings.TrimPrefix(absDir, string(os.PathSeparator)), string(os.PathSeparator))
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			syscall.Close(current)
			return -1, fmt.Errorf("invalid directory component")
		}
		next, err := syscall.Openat(current, part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		syscall.Close(current)
		if err != nil {
			return -1, err
		}
		current = next
	}
	return current, nil
}
