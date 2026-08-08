//go:build darwin

package local

import "golang.org/x/sys/unix"

func atomicSwapDirectories(stagingDir, outputDir string) error {
	return unix.RenameatxNp(
		unix.AT_FDCWD,
		stagingDir,
		unix.AT_FDCWD,
		outputDir,
		uint32(unix.RENAME_SWAP),
	)
}
