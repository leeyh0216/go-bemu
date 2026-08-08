//go:build linux

package local

import "golang.org/x/sys/unix"

func atomicSwapDirectories(stagingDir, outputDir string) error {
	return unix.Renameat2(
		unix.AT_FDCWD,
		stagingDir,
		unix.AT_FDCWD,
		outputDir,
		uint(unix.RENAME_EXCHANGE),
	)
}
