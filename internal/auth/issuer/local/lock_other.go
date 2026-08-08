//go:build !darwin && !linux

package local

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type exclusiveGenerationLock struct {
	file *os.File
	path string
}

func acquireGenerationLock(parentDir, outputBase string) (io.Closer, error) {
	path := filepath.Join(parentDir, generationLockName(outputBase))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		return nil, errors.New("another credential generation is already active; remove the stale lock only when no generator is running")
	}
	if err != nil {
		return nil, fmt.Errorf("create generation lock: %w", err)
	}
	return &exclusiveGenerationLock{file: file, path: path}, nil
}

func (lock *exclusiveGenerationLock) Close() error {
	return errors.Join(lock.file.Close(), os.Remove(lock.path))
}
