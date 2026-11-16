// Package spool is the agent's durable queue. Every record is committed here
// before any output tries to ship it, because the agent's link to the server
// fails in exactly the windows it is measuring. See docs/HANDOFF.md §5.2.
//
// The spool is a bbolt database. Appending a record, advancing the sequence
// number and enforcing the bounds happen in one transaction, as does
// advancing a consumer's cursor and deleting what every consumer has
// delivered. bbolt commits are atomic and fsynced, so a crash at any point
// leaves either the old or the new state and no recovery code is needed.
package spool

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
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
	Dir      string
	MaxAge   time.Duration
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
	Bytes   int64  // stored (compressed) record bytes, the quantity bounded by MaxBytes
	Records int64  // records stored
	Dropped uint64 // undelivered records dropped by the age or size bound since Open
	LastSeq uint64 // highest seq assigned so far
}

// Spool is a durable, bounded, multi-consumer queue of records.
type Spool struct {
	db   *bolt.DB
	opts Options
	enc  *zstd.Encoder
	dec  *zstd.Decoder

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

// ErrLocked is returned by Open when another process holds the spool.
var ErrLocked = errors.New("spool is in use by another hopcorder process")

// Open opens or creates the spool in o.Dir.
func Open(o Options) (*Spool, error) {
	if o.MaxAge <= 0 || o.MaxBytes <= 0 {
		return nil, errors.New("spool: max age and max bytes must be positive")
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
	path := filepath.Join(o.Dir, fileName)
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		if errors.Is(err, berrors.ErrTimeout) {
			return nil, fmt.Errorf("spool %s: %w", path, ErrLocked)
		}
		return nil, fmt.Errorf("spool %s: %w", path, err)
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	s := &Spool{
		db: db, opts: o, enc: enc, dec: dec,
		notify:  map[string]chan struct{}{},
		cursors: map[string]uint64{},
	}
	for _, c := range o.Consumers {
		s.notify[c] = make(chan struct{}, 1)
	}
	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("spool %s: %w", path, err)
	}
	return s, nil
}

// txState collects side effects of a write transaction that must only be
// published once it has committed.
type txState struct {
	dropped uint64
}

// update runs fn in a write transaction and, after a successful commit,
// refreshes the in-memory mirrors of the stored counters. Commits of
// concurrent appends can return in any order, so mirrors are only replaced
// by a newer transaction's values.
func (s *Spool) update(fn func(tx *bolt.Tx, st *txState) error) error {
	var (
		st             txState
		bytes, n, last uint64
		gen            uint64
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
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

func (s *Spool) init() error {
	return s.update(func(tx *bolt.Tx, st *txState) error {
		for _, b := range [][]byte{bucketMeta, bucketRecords, bucketCursors} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		meta, curs := tx.Bucket(bucketMeta), tx.Bucket(bucketCursors)
		next := getU64(meta, keyNextSeq)
		if next == 0 {
			next = 1
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
				s.cursors[c] = next - 1
				continue
			}
			s.cursors[c] = binary.BigEndian.Uint64(v)
		}
		if err := s.collectLocked(tx); err != nil {
			return err
		}
		return s.enforceBoundsLocked(tx, st, 0)
	})
}

// Close closes the database.
func (s *Spool) Close() error {
	s.enc.Close()
	s.dec.Close()
	return s.db.Close()
}

// Append assigns the next seq, calls encode with it and stores the result.
// The record is durable when Append returns. If encode fails nothing is
// stored and the seq is not consumed.
func (s *Spool) Append(ts time.Time, encode func(seq uint64) ([]byte, error)) (uint64, error) {
	var seq uint64
	err := s.update(func(tx *bolt.Tx, st *txState) error {
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
		return s.enforceBoundsLocked(tx, st, seq)
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
// if any is pending).
func (s *Spool) Read(consumer string, maxRecords, maxBytes int) ([]Entry, error) {
	if _, ok := s.notify[consumer]; !ok {
		return nil, fmt.Errorf("spool: unknown consumer %q", consumer)
	}
	var out []Entry
	var corrupt []uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		from := getU64(tx.Bucket(bucketCursors), []byte(consumer)) + 1
		c := tx.Bucket(bucketRecords).Cursor()
		size := 0
		for k, v := c.Seek(seqKey(from)); k != nil && len(out) < maxRecords; k, v = c.Next() {
			seq := binary.BigEndian.Uint64(k)
			if len(v) < 8 {
				corrupt = append(corrupt, seq)
				continue
			}
			data, err := s.dec.DecodeAll(v[8:], nil)
			if err != nil {
				corrupt = append(corrupt, seq)
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
	if err != nil {
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

// Ack records that consumer has delivered every record up to and including
// seq, and deletes records that all consumers have delivered.
func (s *Spool) Ack(consumer string, seq uint64) error {
	if _, ok := s.notify[consumer]; !ok {
		return fmt.Errorf("spool: unknown consumer %q", consumer)
	}
	err := s.update(func(tx *bolt.Tx, _ *txState) error {
		curs := tx.Bucket(bucketCursors)
		if getU64(curs, []byte(consumer)) >= seq {
			return nil
		}
		if err := putU64(curs, []byte(consumer), seq); err != nil {
			return err
		}
		return s.collectLocked(tx)
	})
	if err != nil {
		return fmt.Errorf("spool ack: %w", err)
	}
	s.mu.Lock()
	s.cursors[consumer] = max(s.cursors[consumer], seq)
	s.mu.Unlock()
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

// collectLocked deletes records every consumer has delivered.
func (s *Spool) collectLocked(tx *bolt.Tx) error {
	upto := s.minCursorLocked(tx)
	meta, c := tx.Bucket(bucketMeta), tx.Bucket(bucketRecords).Cursor()
	var bytes, n int64
	for k, v := c.First(); k != nil && binary.BigEndian.Uint64(k) <= upto; k, v = c.First() {
		bytes += int64(len(v))
		n++
		if err := c.Delete(); err != nil {
			return err
		}
	}
	if n == 0 {
		return nil
	}
	return s.adjustLocked(meta, -bytes, -n)
}

// enforceBoundsLocked drops the oldest records while the spool is over its
// size bound or they are older than the age bound. keep is the seq just
// appended, which is never dropped.
func (s *Spool) enforceBoundsLocked(tx *bolt.Tx, st *txState, keep uint64) error {
	meta := tx.Bucket(bucketMeta)
	total := int64(getU64(meta, keyBytes)) //nolint:gosec // non-negative
	cutoff := s.opts.Now().Add(-s.opts.MaxAge).UnixNano()
	delivered := s.minCursorLocked(tx)
	c := tx.Bucket(bucketRecords).Cursor()
	var bytes, n int64
	var dropped uint64
	for k, v := c.First(); k != nil; k, v = c.First() {
		seq := binary.BigEndian.Uint64(k)
		tooBig := total-bytes > s.opts.MaxBytes
		tooOld := len(v) >= 8 && int64(binary.BigEndian.Uint64(v[:8])) < cutoff //nolint:gosec // UnixNano
		if seq == keep || (!tooBig && !tooOld) {
			break
		}
		bytes += int64(len(v))
		n++
		if seq > delivered {
			dropped++
		}
		if err := c.Delete(); err != nil {
			return err
		}
	}
	if n == 0 {
		return nil
	}
	st.dropped += dropped
	return s.adjustLocked(meta, -bytes, -n)
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
