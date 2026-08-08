//go:build darwin || linux

package local

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type advisoryGenerationLock struct {
	file *os.File
}

func acquireGenerationLock(parentDir, outputBase string) (io.Closer, error) {
	path := filepath.Join(parentDir, generationLockName(outputBase))
	fileDescriptor, err := unix.Open(
		path,
		unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open generation lock: %w", err)
	}
	file := os.NewFile(uintptr(fileDescriptor), path)
	if file == nil {
		unix.Close(fileDescriptor)
		return nil, errors.New("open generation lock: invalid file descriptor")
	}
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return nil, fmt.Errorf("protect generation lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.New("another credential generation is already active")
		}
		return nil, fmt.Errorf("lock credential generation: %w", err)
	}
	return &advisoryGenerationLock{file: file}, nil
}

func (lock *advisoryGenerationLock) Close() error {
	unlockErr := unix.Flock(int(lock.file.Fd()), unix.LOCK_UN)
	return errors.Join(unlockErr, lock.file.Close())
}
