//go:build linux

package spool

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// allocateFile has the file system allocate the blocks of the file at path
// from off to end (fallocate, mode 0: a shorter file grows to end), so that
// bbolt's writes there cannot fail for lack of space. It returns an error
// wrapping errors.ErrUnsupported if the file system cannot do this.
func allocateFile(path string, off, end int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var errno error
	if err := rc.Control(func(fd uintptr) {
		for {
			errno = syscall.Fallocate(int(fd), 0, off, end-off) //nolint:gosec // a file descriptor
			if !errors.Is(errno, syscall.EINTR) {
				return
			}
		}
	}); err != nil {
		return err
	}
	switch {
	case errno == nil:
		return nil
	case errors.Is(errno, syscall.EOPNOTSUPP), errors.Is(errno, syscall.ENOSYS):
		return fmt.Errorf("fallocate %s: %w (%w)", path, errors.ErrUnsupported, errno)
	}
	return &os.PathError{Op: "fallocate", Path: path, Err: errno}
}
