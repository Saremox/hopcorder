//go:build !linux

package spool

import "errors"

// allocateFile is not available here: the spool keeps no space allocated
// ahead of its pages, and a full file system is noticed when a write fails.
func allocateFile(string, int64, int64) error { return errors.ErrUnsupported }
