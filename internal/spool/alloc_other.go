//go:build !linux

package spool

import "errors"

// allocateFile is not available here: the spool allocates space ahead of
// its pages by writing zeros (fillFile).
func allocateFile(string, int64, int64) error { return errors.ErrUnsupported }

// freeSpace is not available here: fillFile writes its zeros without
// checking the free space first.
func freeSpace(string) (int64, error) { return 0, errors.ErrUnsupported }
