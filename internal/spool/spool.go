// Package spool is the agent's durable queue. Every record is committed here
// before any output tries to ship it, because the agent's link to the server
// fails in exactly the windows it is measuring. See docs/HANDOFF.md §5.2.
//
// The spool is a bbolt database. Appending a record, advancing the sequence
// number and enforcing the age bound happen in one transaction, as does
// advancing a consumer's cursor and deleting what every consumer has
// delivered. bbolt commits are atomic and fsynced, so a crash at any point
// leaves either the old or the new state and no recovery code is needed.
// The size bound is on the database file; the records dropped to keep
// within it go in transactions of their own (see room.go).
package spool

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/saremox/hopcorder/internal/record"
)

const fileName = "spool.db"

var (
	bucketMeta    = []byte("meta")
	bucketRecords = []byte("records")
	bucketCursors = []byte("cursors")

	keyNextSeq = []byte("next_seq")
	keyBytes   = []byte("bytes")
	keyCount   = []byte("count")
)

// Options configures a spool.
type Options struct {
	Dir    string
	MaxAge time.Duration
	// MaxBytes bounds the database file, every page of it (the records, the
	// B+tree around them, bbolt's freelist), not only the stored records.
	// At least MinMaxBytes. A file already larger keeps its size, and the
	// spool keeps no more than MaxBytes of it in use.
	MaxBytes int64
	// Consumers are the names of the outputs reading from the spool, each
	// with its own delivery cursor. A record is deleted once every consumer
	// has acknowledged it. Cursors of consumers no longer listed are
	// removed; new consumers start at the newest record.
	Consumers []string
	Logger    *slog.Logger
	// Now returns the current time, for the age bound; default time.Now.
	Now func() time.Time
}

// Entry is one spooled record.
type Entry struct {
	Seq  uint64
	TS   time.Time
	Data []byte // the record's JSON
}

// Stats describe the spool's contents.
type Stats struct {
	Bytes   int64  // stored (compressed) record bytes; the file holding them and their B+tree is bounded by MaxBytes
	Records int64  // records stored
	Dropped uint64 // undelivered records dropped by the age or size bound (or a full disk) since Open
	LastSeq uint64 // highest seq assigned so far
}

// Spool is a durable, bounded, multi-consumer queue of records.
type Spool struct {
	opts     Options
	path     string
	enc      *zstd.Encoder
	dec      *zstd.Decoder
	firstSeq uint64 // seq of a new spool's first record

	// dbMu guards db: transactions hold it for reading; it is held for
	// writing to close the database, or to replace a file that bbolt
	// panicked or faulted on (see replace).
	dbMu    sync.RWMutex
	db      *bolt.DB // nil once closed, or while a damaged file could not be replaced
	damaged error    // why the file at path is to be moved aside; nil: it is not
	closed  bool

	roomMu sync.Mutex
	room   room // the space for the file; see room.go

	mu         sync.Mutex
	notify     map[string]chan struct{}
	cursors    map[string]uint64
	txGen      uint64 // incremented inside write transactions (serialized by bbolt)
	appliedGen uint64 // generation whose counters the mirrors below hold; guarded by mu

	bytes   atomic.Int64
	records atomic.Int64
	dropped atomic.Uint64
	lastSeq atomic.Uint64
}

// maxRecordSize bounds what one stored record may decode to; a record that
// would decode to more is undecodable, and skipped. The largest record the
// agent can write is about 220 KB of JSON (255 hops, each with an IPv6
// address with zone and 16 MPLS labels, and their loops), so this leaves
// room more than 35 times over for the format to grow. It is also the
// largest window any zstd encoder level declares (the spool's SpeedFastest
// declares at most 4 MiB), so the decoder refuses no frame a spool wrote
// for its window, which it may not exceed either. Without a bound the
// decoder's default of 64 GiB applied: a 48 KiB value decoding to 1.5 GiB
// took every start out of memory, and was no damage the checks could see.
const maxRecordSize = 8 << 20

// ErrLocked is returned by Open when another process holds the spool.
var ErrLocked = errors.New("spool is in use by another hopcorder process")

// Open opens or creates the spool in o.Dir.
func Open(o Options) (*Spool, error) {
	if o.MaxAge <= 0 || o.MaxBytes < MinMaxBytes {
		return nil, errors.New("spool: max age must be positive, max bytes at least 1MiB")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if err := os.MkdirAll(o.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("spool: %w", err)
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxRecordSize))
	s := &Spool{
		opts: o, path: filepath.Join(o.Dir, fileName), enc: enc, dec: dec,
		notify:  map[string]chan struct{}{},
		cursors: map[string]uint64{},
	}
	for _, c := range o.Consumers {
		s.notify[c] = make(chan struct{}, 1)
	}
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	if err := s.openLocked(); err != nil {
		enc.Close()
		dec.Close()
		return nil, err
	}
	return s, nil
}

// openLocked opens the spool file and loads its state. A corrupt file must
// not stop the agent for good (bbolt can even fault on one): it is kept for
// inspection and the spool starts over, with seqs from the clock like any
// new spool. The caller holds dbMu for writing.
func (s *Spool) openLocked() error {
	for retried := false; ; retried = true {
		if s.damaged != nil {
			bad := fmt.Sprintf("%s.corrupt-%d", s.path, s.opts.Now().Unix())
			if err := os.Rename(s.path, bad); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("%w (and cannot move it aside: %w)", s.damaged, err)
			}
			s.opts.Logger.Error("spool: database unusable; moved aside, starting an empty spool",
				"err", s.damaged, "kept_as", bad)
			s.damaged = nil
		}
		s.firstSeq = clockSeq(s.opts.Now())
		db, err := openDB(s.path)
		if err == nil {
			if err = s.initDB(db); err != nil {
				closeWithin(db, s.opts.Logger)
				err = fmt.Errorf("spool %s: %w", s.path, err)
			}
		}
		if err == nil {
			s.db = db
			return nil
		}
		if !errors.Is(err, errCorrupt) || retried {
			return err
		}
		s.damaged = err
	}
}

// closeWithin closes db, giving up after a few seconds: after a panic
// inside bbolt its locks might be held for good, and the spool must go on.
func closeWithin(db *bolt.DB, log *slog.Logger) {
	done := make(chan error, 1)
	go func() { done <- guard(db.Close) }()
	select {
	case err := <-done:
		if err != nil {
			log.Warn("spool: closing the damaged database", "err", err)
		}
	case <-time.After(5 * time.Second):
		log.Error("spool: the damaged database does not close; abandoning it")
	}
}

// clockSeq is the first seq of a new spool: the current time in
// microseconds. A spool counts up by one per record, far slower than the
// clock, so a new spool (the old one deleted, lost or quarantined) starts
// above any seq an earlier one under the same agent_id reached. Starting at
// 1 instead, its records would be dropped by the server as duplicates until
// the seq passed the old high-water mark. An existing spool keeps its
// counter; only a clock far behind (no RTC, no NTP yet) defeats this.
func clockSeq(now time.Time) uint64 {
	return uint64(max(now.UnixMicro(), 1)) //nolint:gosec // positive
}

// errCorrupt marks a spool file that cannot be used: bbolt found it
// invalid, its page structure or consistency check failed, or reading it
// faulted or made bbolt panic. Other errors (locked, permissions, a full
// disk) leave the file alone.
var errCorrupt = errors.New("corrupt")

// guard runs fn, turning a panic (bbolt's, or a fault on its memory-mapped
// file, which SetPanicOnFault makes one in this goroutine) into an error
// wrapping errCorrupt.
func guard(fn func() error) (err error) {
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", errCorrupt, r)
		}
	}()
	return fn()
}

// openDB opens the spool file and checks it: first its page structure
// (checkPages, under a shared lock), then every key and value and bbolt's
// own consistency check (checkDB). An error wraps errCorrupt if the file is
// unusable, ErrLocked if another process has it.
func openDB(path string) (*bolt.DB, error) {
	var db *bolt.DB
	err := guard(func() error {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			if err := checkFile(path); err != nil {
				return err
			}
		}
		var err error
		db, err = bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
		if err := openError(err); err != nil {
			return err
		}
		if err := checkDB(db); err != nil {
			return fmt.Errorf("%w: %w", errCorrupt, err)
		}
		return nil
	})
	if err != nil {
		if db != nil {
			_ = guard(db.Close)
		}
		return nil, fmt.Errorf("spool %s: %w", path, err)
	}
	return db, nil
}

// checkFile checks the page structure of an existing spool file while a
// read-only bbolt holds a shared lock on it: that open reads only the meta
// pages (a writable one would load the freelist), and it waits for, then
// gives up on, a process that has the file open for writing.
func checkFile(path string) error {
	ro, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err := openError(err); err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()
	if err := checkPages(path, ro.Info().PageSize); err != nil {
		return fmt.Errorf("%w: %w", errCorrupt, err)
	}
	return nil
}

// openError classifies an error of bolt.Open.
func openError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, berrors.ErrTimeout):
		return ErrLocked
	case errors.Is(err, berrors.ErrInvalid), errors.Is(err, berrors.ErrVersionMismatch), errors.Is(err, berrors.ErrChecksum):
		return fmt.Errorf("%w: %w", errCorrupt, err)
	}
	return err
}

// checkDB reads every key and value, then runs bbolt's consistency check
// (page structure and freelist).
//
// checkPages has made sure before that no element points outside its page,
// no overflow count beyond the file, and that the pages form a tree without
// nested buckets, so neither of these faults, loops or recurses without
// end. The walk still runs first, in the caller's goroutine, where guard
// turns faults into errors (a read error on the mapped file is one): tx.Check
// runs in a goroutine of its own, where a fault is fatal.
func checkDB(db *bolt.DB) error {
	if err := db.View(walkAll); err != nil {
		return err
	}
	var first error
	err := db.View(func(tx *bolt.Tx) error {
		for err := range tx.Check() { // drain: the checker runs until done
			if first == nil {
				first = err
			}
		}
		return nil
	})
	return errors.Join(first, err)
}

// walkAll visits every key and value of every bucket, checking that keys
// are in order and that no bucket holds a bucket (the spool has none).
func walkAll(tx *bolt.Tx) error { return tx.ForEach(walkBucket) }

// valueSink keeps the compiler from eliding the reads of the values.
var valueSink atomic.Uint32

func walkBucket(name []byte, b *bolt.Bucket) error {
	if b == nil {
		return fmt.Errorf("%q in the root is not a bucket", name)
	}
	var prev []byte
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			return fmt.Errorf("bucket %q: keys out of order", name)
		}
		prev = k
		if v == nil {
			return fmt.Errorf("bucket %q: nested bucket %q", name, k)
		}
		valueSink.Add(crc32.ChecksumIEEE(v)) // read the value itself, not just its header
	}
	return nil
}

// txState collects side effects of a write transaction that must only be
// published once it has committed.
type txState struct {
	dropped uint64
	force   bool // there was nothing to drop for the size bound: try anyway (see change)
}

// errClosed is returned for operations on a closed spool.
var errClosed = errors.New("spool: closed")

// acquire returns the database with dbMu held for reading, which release
// gives back. If an earlier operation found the file damaged and it could
// not be replaced then, that is tried again first.
func (s *Spool) acquire() (*bolt.DB, error) {
	s.dbMu.RLock()
	if s.db != nil || s.closed {
		if s.db == nil {
			s.dbMu.RUnlock()
			return nil, errClosed
		}
		return s.db, nil
	}
	s.dbMu.RUnlock()
	s.dbMu.Lock()
	if s.db == nil && !s.closed {
		if err := s.openLocked(); err != nil {
			s.dbMu.Unlock()
			return nil, fmt.Errorf("spool unavailable: %w", err)
		}
	}
	s.dbMu.Unlock()
	return s.acquire()
}

func (s *Spool) release() { s.dbMu.RUnlock() }

// replace handles an operation on db that bbolt panicked or faulted on
// (err, wrapping errCorrupt): the file is damaged in a way the checks at
// start did not catch, and bbolt's state in memory may be inconsistent
// after the panic. Like Open with a file that fails its checks, it moves
// the file aside and continues with an empty spool, at once, so the agent
// keeps recording. If that fails, every operation tries again.
func (s *Spool) replace(db *bolt.DB, err error) {
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	if s.db != db {
		return // closed, or already replaced after another operation failed
	}
	s.opts.Logger.Error("spool: database damaged while in use", "err", err)
	s.db, s.damaged = nil, err
	closeWithin(db, s.opts.Logger)
	if err := s.openLocked(); err != nil {
		s.opts.Logger.Error("spool: cannot start an empty spool; retrying with the next operation", "err", err)
	}
}

// update runs fn in a write transaction and, after a successful commit,
// refreshes the in-memory mirrors of the stored counters. Commits of
// concurrent appends can return in any order, so mirrors are only replaced
// by a newer transaction's values. A panic (or fault) inside replaces the
// damaged file; see replace.
func (s *Spool) update(fn txFunc) error {
	db, err := s.acquire()
	if err != nil {
		return err
	}
	err = s.updateDB(db, fn)
	s.release()
	if errors.Is(err, errCorrupt) {
		s.replace(db, err)
	}
	return err
}

// updateDB is update on db, without dbMu (which the caller holds).
func (s *Spool) updateDB(db *bolt.DB, fn txFunc) error {
	var (
		st             txState
		bytes, n, last uint64
		gen            uint64
	)
	err := guard(func() error {
		return db.Update(func(tx *bolt.Tx) error {
			s.prepare(tx)
			if err := fn(tx, &st); err != nil {
				return err
			}
			meta := tx.Bucket(bucketMeta)
			bytes, n = getU64(meta, keyBytes), getU64(meta, keyCount)
			last = getU64(meta, keyNextSeq) - 1
			s.txGen++ // only one write transaction runs at a time
			gen = s.txGen
			return nil
		})
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	if gen > s.appliedGen {
		s.appliedGen = gen
		s.bytes.Store(int64(bytes)) //nolint:gosec // bounded by MaxBytes
		s.records.Store(int64(n))   //nolint:gosec // record count
		s.lastSeq.Store(last)
	}
	s.mu.Unlock()
	if st.dropped > 0 {
		s.dropped.Add(st.dropped)
		s.opts.Logger.Warn("spool: bound reached, dropped oldest undelivered records", "dropped", st.dropped)
	}
	return nil
}

// initDB creates the buckets of a new spool in db and loads the state of
// an existing one, then settles the bounds.
func (s *Spool) initDB(db *bolt.DB) error {
	if err := s.setRoom(db); err != nil {
		return err
	}
	run := func(fn txFunc) error { return s.updateDB(db, fn) }
	cursors := map[string]uint64{}
	var more bool
	err := s.change(run, func(tx *bolt.Tx, st *txState) error {
		for _, b := range [][]byte{bucketMeta, bucketRecords, bucketCursors} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		meta, curs := tx.Bucket(bucketMeta), tx.Bucket(bucketCursors)
		next := getU64(meta, keyNextSeq)
		if next == 0 {
			next = s.firstSeq
			if err := putU64(meta, keyNextSeq, next); err != nil {
				return err
			}
		}
		// Drop cursors of consumers that are no longer configured.
		var stale [][]byte
		if err := curs.ForEach(func(k, _ []byte) error {
			if !slices.Contains(s.opts.Consumers, string(k)) {
				stale = append(stale, slices.Clone(k))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, k := range stale {
			s.opts.Logger.Info("spool: removing cursor of unconfigured output", "output", string(k))
			if err := curs.Delete(k); err != nil {
				return err
			}
		}
		for _, c := range s.opts.Consumers {
			v := curs.Get([]byte(c))
			if v == nil {
				if err := putU64(curs, []byte(c), next-1); err != nil {
					return err
				}
				cursors[c] = next - 1
				continue
			}
			cursors[c] = binary.BigEndian.Uint64(v)
		}
		var err error
		more, err = s.trimLocked(tx, st)
		return err
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.cursors = cursors
	s.mu.Unlock()
	return s.settle(run, more)
}

// trimLocked deletes, in tx, records every consumer has delivered and those
// older than the age bound, at most txDeletes of each, and reports whether
// more remain.
func (s *Spool) trimLocked(tx *bolt.Tx, st *txState) (bool, error) {
	more, err := s.collectLocked(tx)
	if err != nil {
		return false, err
	}
	old, err := s.enforceBoundsLocked(tx, st, 0)
	return more || old, err
}

// settle does at open what initDB's transaction left, in transactions of
// their own: delivered and expired records beyond its share, then the size
// bound (a file from an earlier version, or a lower MaxBytes, may have more
// pages in use). It fails only for damage; anything else is logged and left
// to later operations.
func (s *Spool) settle(run func(txFunc) error, more bool) error {
	var err error
	for err == nil && more {
		err = s.change(run, func(tx *bolt.Tx, st *txState) error {
			var err error
			more, err = s.trimLocked(tx, st)
			return err
		})
	}
	for n := 1; err == nil && n > 0; {
		n, err = s.dropOldest(run, false)
	}
	if err != nil && !errors.Is(err, errCorrupt) {
		s.opts.Logger.Warn("spool: cannot apply the bounds at open; later operations will", "err", err)
		return nil
	}
	return err
}

// Close closes the database.
func (s *Spool) Close() error {
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.enc.Close()
	s.dec.Close()
	if s.db == nil {
		return nil
	}
	db := s.db
	s.db = nil
	return guard(db.Close)
}

// Append assigns the next seq, calls encode with it and stores the result.
// The record is durable when Append returns. If encode fails nothing is
// stored and the seq is not consumed. At the size bound, or on a full file
// system, the oldest records are dropped to make room (see change); encode
// may then be called more than once, with the same seq.
func (s *Spool) Append(ts time.Time, encode func(seq uint64) ([]byte, error)) (uint64, error) {
	var seq uint64
	err := s.change(s.update, func(tx *bolt.Tx, st *txState) error {
		if len(s.opts.Consumers) > 0 && !st.force && s.overBound(tx) {
			return errNoRoom
		}
		meta, recs := tx.Bucket(bucketMeta), tx.Bucket(bucketRecords)
		recs.FillPercent = 0.95 // keys only ever increase
		seq = getU64(meta, keyNextSeq)
		data, err := encode(seq)
		if err != nil {
			return err
		}
		if err := putU64(meta, keyNextSeq, seq+1); err != nil {
			return err
		}
		if len(s.opts.Consumers) == 0 {
			return nil // nobody would ever read it; only the seq matters
		}
		val := make([]byte, 8, 8+len(data)/2)
		binary.BigEndian.PutUint64(val, uint64(ts.UnixNano())) //nolint:gosec // timestamps after 1970
		val = s.enc.EncodeAll(data, val)
		if err := recs.Put(seqKey(seq), val); err != nil {
			return err
		}
		if err := s.adjustLocked(meta, int64(len(val)), 1); err != nil {
			return err
		}
		_, err = s.enforceBoundsLocked(tx, st, seq)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("spool append: %w", err)
	}
	s.mu.Lock()
	for _, ch := range s.notify {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	s.mu.Unlock()
	return seq, nil
}

// Publish appends rec, setting rec.Seq. It makes *Spool an agent.Sink.
func (s *Spool) Publish(_ context.Context, rec *record.Record) error {
	_, err := s.Append(rec.TS, func(seq uint64) ([]byte, error) {
		rec.Seq = seq
		return json.Marshal(rec)
	})
	return err
}

// Read returns up to maxRecords entries after consumer's cursor, stopping
// before maxBytes of record data (but always returning at least one entry
// if any is pending). A panic (or fault) inside replaces the damaged file;
// see replace.
func (s *Spool) Read(consumer string, maxRecords, maxBytes int) ([]Entry, error) {
	if _, ok := s.notify[consumer]; !ok {
		return nil, fmt.Errorf("spool: unknown consumer %q", consumer)
	}
	db, err := s.acquire()
	if err != nil {
		return nil, fmt.Errorf("spool read: %w", err)
	}
	out, corrupt, err := s.read(db, consumer, maxRecords, maxBytes)
	s.release()
	if err != nil {
		if errors.Is(err, errCorrupt) {
			s.replace(db, err)
		}
		return nil, fmt.Errorf("spool read: %w", err)
	}
	for _, seq := range corrupt {
		// A record that cannot be decoded would block the consumer forever.
		s.opts.Logger.Error("spool: skipping undecodable record", "seq", seq)
	}
	if len(corrupt) > 0 && len(out) == 0 {
		// Everything in this window was corrupt: move the cursor past it.
		if err := s.Ack(consumer, slices.Max(corrupt)); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// read is Read's transaction on db: the entries, and the seqs of records
// that cannot be decoded.
func (s *Spool) read(db *bolt.DB, consumer string, maxRecords, maxBytes int) (out []Entry, corrupt []uint64, err error) {
	err = guard(func() error {
		return db.View(func(tx *bolt.Tx) error {
			from := getU64(tx.Bucket(bucketCursors), []byte(consumer)) + 1
			c := tx.Bucket(bucketRecords).Cursor()
			size := 0
			for k, v := c.Seek(seqKey(from)); k != nil && len(out) < maxRecords; k, v = c.Next() {
				if len(k) != 8 {
					continue // not a record key; cannot be acknowledged either
				}
				seq := binary.BigEndian.Uint64(k)
				if len(v) < 8 {
					corrupt = append(corrupt, seq)
					continue
				}
				data, err := s.dec.DecodeAll(v[8:], nil) // at most maxRecordSize
				if err != nil {
					corrupt = append(corrupt, seq) // ErrDecoderSizeExceeded included
					continue
				}
				if len(out) > 0 && size+len(data) > maxBytes {
					break
				}
				size += len(data)
				ts := time.Unix(0, int64(binary.BigEndian.Uint64(v[:8]))) //nolint:gosec // written from UnixNano
				out = append(out, Entry{Seq: seq, TS: ts, Data: data})
			}
			return nil
		})
	})
	return out, corrupt, err
}

// Ack records that consumer has delivered every record up to and including
// seq, and deletes records that all consumers have delivered: up to
// txDeletes with the cursor, the rest in transactions of their own, so that
// an Ack frees room even on a full disk.
func (s *Spool) Ack(consumer string, seq uint64) error {
	if _, ok := s.notify[consumer]; !ok {
		return fmt.Errorf("spool: unknown consumer %q", consumer)
	}
	var more bool
	err := s.change(s.update, func(tx *bolt.Tx, _ *txState) error {
		more = false
		curs := tx.Bucket(bucketCursors)
		if getU64(curs, []byte(consumer)) >= seq {
			return nil
		}
		if err := putU64(curs, []byte(consumer), seq); err != nil {
			return err
		}
		var err error
		more, err = s.collectLocked(tx)
		return err
	})
	if err != nil {
		return fmt.Errorf("spool ack: %w", err)
	}
	s.mu.Lock()
	s.cursors[consumer] = max(s.cursors[consumer], seq)
	s.mu.Unlock()
	for more {
		if err := s.change(s.update, func(tx *bolt.Tx, _ *txState) error {
			var err error
			more, err = s.collectLocked(tx)
			return err
		}); err != nil {
			// The cursor moved; the next Ack, or the next start, deletes them.
			s.opts.Logger.Warn("spool: cannot delete delivered records", "err", err)
			break
		}
	}
	return nil
}

// Notify returns a channel that receives a value after records are
// appended. It is buffered with capacity 1, so a slow reader sees one
// wake-up for many appends.
func (s *Spool) Notify(consumer string) <-chan struct{} { return s.notify[consumer] }

// Pending returns how many records consumer has not yet acknowledged
// (including any that the bounds have since dropped).
func (s *Spool) Pending(consumer string) uint64 {
	s.mu.Lock()
	c := s.cursors[consumer]
	s.mu.Unlock()
	last := s.lastSeq.Load()
	if last <= c {
		return 0
	}
	return last - c
}

// Stats returns current counters.
func (s *Spool) Stats() Stats {
	return Stats{
		Bytes:   s.bytes.Load(),
		Records: s.records.Load(),
		Dropped: s.dropped.Load(),
		LastSeq: s.lastSeq.Load(),
	}
}

// minCursorLocked returns the lowest cursor over all consumers, or MaxUint64
// when there are none.
func (s *Spool) minCursorLocked(tx *bolt.Tx) uint64 {
	m := uint64(math.MaxUint64)
	curs := tx.Bucket(bucketCursors)
	for _, c := range s.opts.Consumers {
		m = min(m, getU64(curs, []byte(c)))
	}
	return m
}

// collectLocked deletes records every consumer has delivered, at most
// txDeletes, and reports whether more remain.
func (s *Spool) collectLocked(tx *bolt.Tx) (bool, error) {
	upto := s.minCursorLocked(tx)
	meta, c := tx.Bucket(bucketMeta), tx.Bucket(bucketRecords).Cursor()
	var bytes, n int64
	k, v := c.First()
	for ; k != nil && binary.BigEndian.Uint64(k) <= upto && n < txDeletes; k, v = c.First() {
		bytes += int64(len(v))
		n++
		if err := c.Delete(); err != nil {
			return false, err
		}
	}
	more := k != nil && binary.BigEndian.Uint64(k) <= upto
	if n == 0 {
		return more, nil
	}
	return more, s.adjustLocked(meta, -bytes, -n)
}

// enforceBoundsLocked drops the oldest records while they are older than
// the age bound, at most txDeletes, and reports whether more remain. keep
// is the seq just appended, which is never dropped. The size bound is kept
// by dropOldest, in transactions of their own.
func (s *Spool) enforceBoundsLocked(tx *bolt.Tx, st *txState, keep uint64) (bool, error) {
	meta := tx.Bucket(bucketMeta)
	cutoff := s.opts.Now().Add(-s.opts.MaxAge).UnixNano()
	delivered := s.minCursorLocked(tx)
	c := tx.Bucket(bucketRecords).Cursor()
	var bytes, n int64
	var dropped uint64
	more := false
	for k, v := c.First(); k != nil; k, v = c.First() {
		seq := binary.BigEndian.Uint64(k)
		tooOld := len(v) >= 8 && int64(binary.BigEndian.Uint64(v[:8])) < cutoff //nolint:gosec // UnixNano
		if seq == keep || !tooOld {
			break
		}
		if n == txDeletes {
			more = true
			break
		}
		bytes += int64(len(v))
		n++
		if seq > delivered {
			dropped++
		}
		if err := c.Delete(); err != nil {
			return false, err
		}
	}
	if n == 0 {
		return false, nil
	}
	st.dropped += dropped
	return more, s.adjustLocked(meta, -bytes, -n)
}

func (s *Spool) adjustLocked(meta *bolt.Bucket, dBytes, dCount int64) error {
	b := int64(getU64(meta, keyBytes)) + dBytes                       //nolint:gosec // non-negative
	n := int64(getU64(meta, keyCount)) + dCount                       //nolint:gosec // non-negative
	if err := putU64(meta, keyBytes, uint64(max(b, 0))); err != nil { //nolint:gosec // clamped
		return err
	}
	return putU64(meta, keyCount, uint64(max(n, 0))) //nolint:gosec // clamped
}

func seqKey(seq uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, seq)
}

func getU64(b *bolt.Bucket, k []byte) uint64 {
	v := b.Get(k)
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

func putU64(b *bolt.Bucket, k []byte, v uint64) error {
	return b.Put(k, binary.BigEndian.AppendUint64(nil, v))
}
