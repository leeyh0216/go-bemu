//go:build !darwin && !linux

package local

import "errors"

func atomicSwapDirectories(_, _ string) error {
	return errors.New("atomic directory replacement is unsupported on this operating system")
}
