package spool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

// The size bound and a full file system (review, round six).
//
// Options.MaxBytes bounds the database file, every page of it: the records
// and the B+tree around them. Counting only the stored record bytes, as
// the spool did before, let the file grow to about 1.5 times the bound; on
// a volume as large as the bound the file system filled
// first, and from then on every Append failed: the newest records were
// lost, the oldest kept, the reverse of §5.2, and none counted as dropped.
// An Ack deleting many records failed too, since bbolt writes every change,
// a deletion included, to newly allocated pages.
//
// Now:
//
//   - bbolt's MaxSize keeps the file from growing past MaxBytes; an
//     allocation that would grow it fails with ErrMaxSizeReached.
//   - The pages in use (the file's high water mark less the free and
//     pending pages bbolt counted after the last write transaction) are kept
//     a reserve below what the file may hold. An Append that finds them
//     beyond that first drops the oldest records, in transactions of their
//     own, so every transaction, those that delete included, finds the
//     pages it needs among the free ones or the room left.
//   - The reserve's worth of disk is allocated ahead of the high water mark
//     (fallocate, or where the file system cannot, zeros written there: see
//     fillFile). When the file system cannot give it, the spool keeps to
//     the file it has: what is allocated becomes the bound, the reserve,
//     allocated while there was room, carries the transactions that drop
//     records, and every minute the spool checks whether there is room
//     again.
//   - An operation that fails for room all the same (bbolt's MaxSize, or
//     ENOSPC, EDQUOT or EFBIG from the file system) drops the oldest
//     records in a transaction of its own and runs again, a few times.
const (
	// The reserve is a 64th of MaxBytes, at least minReservePages pages and
	// at most maxReserve (and a quarter of MaxBytes); it is also bbolt's
	// AllocSize, the step in which it grows the file.
	maxReserve      = 1 << 20
	minReservePages = 16
	// txDeletes bounds the records one transaction deletes, so that the
	// pages it needs (rewritten branches) stay within the reserve.
	txDeletes = 1024
	// roomRounds bounds how often an operation that found no room drops
	// records and runs again.
	roomRounds = 16
	// diskRetry is how often a spool held back by a full file system checks
	// whether there is room again.
	diskRetry = time.Minute
)

// MinMaxBytes is the least size bound (Options.MaxBytes) a spool takes.
const MinMaxBytes = 1 << 20

// errNoRoom is an Append's when the pages in use are at their bound: the
// oldest records are dropped first.
var errNoRoom = errors.New("spool at its size bound")

// allocate is allocateFile and fill is fillFile; tests replace them.
var (
	allocate = allocateFile
	fill     = fillFile
)

// room is what the spool knows of the space for its file. Guarded by
// roomMu; reset when a file is opened.
type room struct {
	ps       int64     // page size
	maxSize  int64     // Options.MaxBytes in whole pages: the bound on the file
	reserve  int64     // bytes kept allocatable beyond the pages in use
	hwm      int64     // the file's high water mark (bytes) as the latest write transaction began
	reserved int64     // the file is allocated on disk up to here
	diskFull bool      // the file system has no room for more: the file keeps to reserved
	since    time.Time // when diskFull was set or last checked
	zeros    bool      // the file system cannot fallocate: the spool allocates by writing zeros (fill)
	noAlloc  bool      // nothing allocates ahead (writing zeros failed too): a full disk is noticed when a write fails
}

func newRoom(ps int, maxBytes int64) room {
	p := int64(ps)
	r := room{ps: p, maxSize: maxBytes / p * p}
	r.reserve = min(max(min(r.maxSize/64, maxReserve), minReservePages*p), r.maxSize/4) / p * p
	return r
}

// limit is the most bytes of pages the spool keeps in use: what bbolt lets
// the file reach (MaxSize less one AllocSize) or, while the file system is
// full, what the file has allocated, less the reserve (half of a file
// smaller than twice the reserve).
func (r *room) limit() int64 {
	c := r.maxSize - r.reserve
	if r.diskFull {
		c = min(c, r.reserved)
	}
	return c - min(r.reserve, c/2)
}

// setRoom sets up the room for db, just opened: bbolt's MaxSize and
// AllocSize, set before any write transaction. A file larger than MaxBytes
// (an earlier version's, or a lower max_bytes) does not shrink; bbolt may
// grow it by one reserve, which the transactions that bring its pages in
// use under the bound may need.
func (s *Spool) setRoom(db *bolt.DB) error {
	var hwm int64
	if err := guard(func() error {
		return db.View(func(tx *bolt.Tx) error { hwm = tx.Size(); return nil })
	}); err != nil {
		return err
	}
	r := newRoom(db.Info().PageSize, s.opts.MaxBytes)
	db.AllocSize = int(r.reserve)
	db.MaxSize = int(max(r.maxSize, hwm+2*r.reserve))
	s.roomMu.Lock()
	s.room = r
	s.roomMu.Unlock()
	return nil
}

// prepare runs as every write transaction begins (bbolt runs them one at a
// time): it notes the high water mark and allocates the reserve ahead of it
// or, while the file system is full, checks now and then whether there is
// room again.
func (s *Spool) prepare(tx *bolt.Tx) {
	s.roomMu.Lock()
	defer s.roomMu.Unlock()
	r := &s.room
	r.hwm = tx.Size()
	var end int64
	switch {
	case r.diskFull:
		if now := s.opts.Now(); now.Sub(r.since) >= diskRetry {
			r.since = now
			if r.noAlloc {
				// Nothing to check with: let the file grow, and see.
				r.diskFull = false
				s.opts.Logger.Info("spool: letting the spool file grow again")
				return
			}
			end = min(r.maxSize, max(r.reserved, r.hwm)+r.reserve)
		}
	case r.noAlloc:
	case r.hwm+r.reserve > r.reserved:
		end = min(r.maxSize, r.hwm+2*r.reserve)
	}
	if end <= r.reserved {
		return
	}
	switch err := s.allocLocked(tx.DB().Path(), end); {
	case err == nil:
		r.reserved = end
		if r.diskFull {
			r.diskFull = false
			s.opts.Logger.Info("spool: the file system has room again; the spool may grow to max_bytes")
		}
	case r.diskFull:
		// Still full.
	case diskRefused(err):
		s.fullLocked(err)
	default:
		r.noAlloc = true
		s.opts.Logger.Warn("spool: cannot allocate space ahead; a full disk is noticed when a write fails", "err", err)
	}
}

// allocLocked allocates the file's blocks up to end: with fallocate or,
// once the file system has refused that as unsupported, by writing zeros
// from where they are allocated or the high water mark, whichever is
// further. The caller holds roomMu.
func (s *Spool) allocLocked(path string, end int64) error {
	r := &s.room
	if !r.zeros {
		err := allocate(path, r.reserved, end)
		if !errors.Is(err, errors.ErrUnsupported) {
			return err
		}
		r.zeros = true
		s.opts.Logger.Info("spool: the file system cannot allocate space ahead (fallocate); writing zeros ahead instead", "err", err)
	}
	return fill(path, max(r.reserved, r.hwm), end, r.reserve)
}

// zeroBlock is what fillFile writes.
var zeroBlock [64 << 10]byte

// fillFile allocates the blocks of the file at path from off to end on a
// file system without fallocate, as glibc's posix_fallocate does there: it
// writes zeros, then syncs the file, since some file systems (NFS) report a
// full disk only then. Nothing from off on may be in use: the caller passes
// the high water mark or more. A copy-on-write file system (ZFS) may store
// zeros without allocating anything, and needs room for every overwrite, so
// before the file grows the file system must have room for the growth and
// margin more (statfs, where it says); else the error wraps ENOSPC.
func fillFile(path string, off, end, margin int64) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if grow := end - max(off, fi.Size()); grow > 0 {
		if free, err := freeSpace(path); err == nil && free < grow+margin {
			return fmt.Errorf("%s: %d bytes free on its file system, %d to grow by and %d to keep: %w", path, free, grow, margin, syscall.ENOSPC)
		}
	}
	for ; off < end; off += int64(len(zeroBlock)) {
		if _, err := f.WriteAt(zeroBlock[:min(end-off, int64(len(zeroBlock)))], off); err != nil {
			return err
		}
	}
	return f.Sync()
}

// fullLocked records that the file system has no room for the file to
// grow: from now on the file keeps to what it has, every page below the
// high water mark having been written once. The caller holds roomMu.
func (s *Spool) fullLocked(err error) {
	r := &s.room
	r.reserved = max(r.reserved, r.hwm)
	if !r.diskFull {
		s.opts.Logger.Warn("spool: the file system has no room for the spool file to grow; keeping to its size, oldest records dropped first",
			"file_bytes", r.reserved, "max_bytes", r.maxSize, "err", err)
	}
	r.diskFull, r.since = true, s.opts.Now()
}

// overBound reports whether the pages in use exceed the bound, in write
// transaction tx: bbolt counts the free and pending pages as each write
// transaction ends, and only one runs at a time.
func (s *Spool) overBound(tx *bolt.Tx) bool {
	st := tx.DB().Stats()
	s.roomMu.Lock()
	defer s.roomMu.Unlock()
	return tx.Size()-int64(st.FreePageN+st.PendingPageN)*s.room.ps > s.room.limit()
}

// diskRefused reports whether err is the file system refusing to store
// more: no space, a quota, a file size limit. bbolt reports a failed
// truncate as text ("file resize error: ..."), so that is matched by its
// message.
func diskRefused(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, e := range []syscall.Errno{syscall.ENOSPC, syscall.EDQUOT, syscall.EFBIG} {
		if errors.Is(err, e) || strings.Contains(msg, "file resize error") && strings.Contains(msg, e.Error()) {
			return true
		}
	}
	return false
}

// noRoom reports whether an operation failed for lack of room: the spool's
// bound, the file's (bbolt's MaxSize), or the file system's.
func noRoom(err error) bool {
	return errors.Is(err, errNoRoom) || errors.Is(err, berrors.ErrMaxSizeReached) || diskRefused(err)
}

// txFunc is the work of a write transaction.
type txFunc func(tx *bolt.Tx, st *txState) error

// change runs fn in a write transaction by run (update, or updateDB while
// opening). When it finds no room it drops the oldest records, in a
// transaction of their own, and runs fn again, up to roomRounds times: the
// newest record is kept and the oldest go, counted, as §5.2 has it. If
// there was nothing to drop while at the spool's own bound, fn runs with
// st.force set, and may find the room it needs all the same.
func (s *Spool) change(run func(txFunc) error, fn txFunc) error {
	force := false
	for round := 1; ; round++ {
		err := run(func(tx *bolt.Tx, st *txState) error {
			st.force = force
			return fn(tx, st)
		})
		if err == nil || !noRoom(err) || round > roomRounds {
			return err
		}
		if diskRefused(err) {
			s.roomMu.Lock()
			s.fullLocked(err)
			s.roomMu.Unlock()
		}
		bound := errors.Is(err, errNoRoom)
		n, derr := s.dropOldest(run, !bound)
		switch {
		case derr != nil:
			return fmt.Errorf("%w (and making room: %w)", err, derr)
		case n == 0 && !bound:
			return err // nothing left to drop
		case n == 0:
			force = true // made room meanwhile, or nothing to drop
		}
	}
}

// errNothing rolls back a transaction that has nothing to do: even an
// empty commit writes and syncs a meta page.
var errNothing = errors.New("nothing to do")

// dropOldest deletes the oldest records in a transaction of its own (by
// run): records worth half the reserve in stored bytes, at least one, at
// most txDeletes; if always is false, only while the pages in use are over
// the bound. It returns how many it deleted; those some consumer had not
// delivered count as dropped.
func (s *Spool) dropOldest(run func(txFunc) error, always bool) (int, error) {
	var n int
	err := run(func(tx *bolt.Tx, st *txState) error {
		n = 0
		meta, recs := tx.Bucket(bucketMeta), tx.Bucket(bucketRecords)
		if meta == nil || recs == nil || tx.Bucket(bucketCursors) == nil || !always && !s.overBound(tx) {
			return errNothing
		}
		s.roomMu.Lock()
		chunk := s.room.reserve / 2
		s.roomMu.Unlock()
		delivered := s.minCursorLocked(tx)
		c := recs.Cursor()
		var bytes int64
		var dropped uint64
		for k, v := c.First(); k != nil && n < txDeletes && (n == 0 || bytes < chunk); k, v = c.First() {
			bytes += int64(len(v))
			n++
			if binary.BigEndian.Uint64(k) > delivered {
				dropped++
			}
			if err := c.Delete(); err != nil {
				return err
			}
		}
		if n == 0 {
			return errNothing
		}
		st.dropped += dropped
		return s.adjustLocked(meta, -bytes, -int64(n))
	})
	if errors.Is(err, errNothing) {
		return 0, nil
	}
	return n, err
}
