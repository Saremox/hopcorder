//go:build linux

package spool

import (
	"errors"
	"fmt"
	"math"
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

// freeSpace returns the bytes the file system of path has free for an
// unprivileged process, or an error if it does not say (statfs fails, or it
// reports no size, as some FUSE file systems do).
func freeSpace(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, &os.PathError{Op: "statfs", Path: path, Err: err}
	}
	bs := uint64(st.Frsize) //nolint:gosec // a block size
	if bs == 0 {
		bs = uint64(st.Bsize) //nolint:gosec // a block size
	}
	if st.Blocks == 0 || bs == 0 {
		return 0, fmt.Errorf("statfs %s: no size: %w", path, errors.ErrUnsupported)
	}
	return int64(min(st.Bavail, math.MaxInt64/bs) * bs), nil //nolint:gosec // at most MaxInt64
}
