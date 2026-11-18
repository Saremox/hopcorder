package spool

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func open(t *testing.T, dir string, consumers ...string) *Spool {
	t.Helper()
	s, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: consumers})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// behind is a clock behind every seq the tests' spools assign: opened with
// it, a spool goes on from its own counter, not from the clock (see raise).
func behind() time.Time { return time.Unix(0, 0) }

// reopen opens the spool in dir as open does, but with the clock behind,
// so that its seqs go on from where they were: for tests of what an open
// keeps, seq by seq.
func reopen(t *testing.T, dir string, consumers ...string) *Spool {
	t.Helper()
	s, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: consumers, Now: behind,
		Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// ahead opens the spool in dir as open does, but with the clock an hour
// ahead: it goes on from the clock's seq, far above the ones it assigned.
func ahead(t *testing.T, dir string, consumers ...string) *Spool {
	t.Helper()
	now := time.Now().Add(time.Hour)
	s, err := Open(Options{Dir: dir, MaxAge: 48 * time.Hour, MaxBytes: 1 << 30, Consumers: consumers,
		Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func appendN(t *testing.T, s *Spool, n int) []uint64 {
	t.Helper()
	var seqs []uint64
	for range n {
		seq, err := s.Append(time.Now(), func(seq uint64) ([]byte, error) {
			return []byte(fmt.Sprintf(`{"seq":%d,"pad":%q}`, seq, strings.Repeat("x", 100))), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	return seqs
}

func seqsOf(es []Entry) []uint64 {
	out := []uint64{}
	for _, e := range es {
		out = append(out, e.Seq)
	}
	return out
}

// rel numbers seqs from a spool's start: base is its Stats().LastSeq when
// it was new (its first seq minus one), so its first record is 1.
func rel(base uint64, seqs []uint64) []uint64 {
	out := make([]uint64, len(seqs))
	for i, s := range seqs {
		out[i] = s - base
	}
	return out
}

func TestAppendReadAck(t *testing.T) {
	s := open(t, t.TempDir(), "jsonl", "http")
	defer s.Close()
	base := s.Stats().LastSeq
	seqs := appendN(t, s, 5)
	if fmt.Sprint(rel(base, seqs)) != "[1 2 3 4 5]" {
		t.Fatalf("seqs %v", seqs)
	}
	es, err := s.Read("http", 3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(rel(base, seqsOf(es))) != "[1 2 3]" || !bytes.HasPrefix(es[0].Data, fmt.Appendf(nil, `{"seq":%d,`, base+1)) {
		t.Fatalf("read %v %q", seqsOf(es), es[0].Data)
	}
	if err := s.Ack("http", base+3); err != nil {
		t.Fatal(err)
	}
	es, _ = s.Read("http", 10, 1<<20)
	if fmt.Sprint(rel(base, seqsOf(es))) != "[4 5]" {
		t.Errorf("after ack: %v", seqsOf(es))
	}
	// jsonl has acknowledged nothing, so nothing may be deleted yet.
	if st := s.Stats(); st.Records != 5 || st.LastSeq != base+5 {
		t.Errorf("stats %+v", st)
	}
	if p := s.Pending("http"); p != 2 {
		t.Errorf("pending http = %d", p)
	}
	if err := s.Ack("jsonl", base+4); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.Records != 2 {
		t.Errorf("records after both acked through 3: %+v", st)
	}
	if err := s.Ack("http", base+2); err != nil { // going backwards is a no-op
		t.Fatal(err)
	}
	es, _ = s.Read("http", 10, 1<<20)
	if fmt.Sprint(rel(base, seqsOf(es))) != "[4 5]" {
		t.Errorf("cursor moved backwards: %v", seqsOf(es))
	}
	if _, err := s.Read("nope", 1, 1); err == nil {
		t.Error("unknown consumer accepted")
	}
}

func TestReadMaxBytes(t *testing.T) {
	s := open(t, t.TempDir(), "c")
	defer s.Close()
	appendN(t, s, 10)
	es, _ := s.Read("c", 100, 300) // each record is ~120 bytes
	if len(es) != 2 {
		t.Errorf("got %d entries for 300 bytes", len(es))
	}
	es, _ = s.Read("c", 100, 1) // always at least one
	if len(es) != 1 {
		t.Errorf("got %d entries for 1 byte", len(es))
	}
}

func TestSeqAndCursorsPersist(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "a")
	base := s.Stats().LastSeq
	appendN(t, s, 3)
	if err := s.Ack("a", base+2); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// With the clock behind its seqs, the spool goes on from its counter.
	s = reopen(t, dir, "a")
	if seqs := appendN(t, s, 1); seqs[0] != base+4 {
		t.Errorf("seq after reopen = %d, want %d", seqs[0], base+4)
	}
	es, _ := s.Read("a", 10, 1<<20)
	if fmt.Sprint(rel(base, seqsOf(es))) != "[3 4]" {
		t.Errorf("read after reopen: %v", seqsOf(es))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// With the clock ahead, from the clock; the cursor is kept all the same.
	before := clockSeq(time.Now())
	s = open(t, dir, "a")
	defer s.Close()
	seq := appendN(t, s, 1)[0]
	if seq < before {
		t.Errorf("seq after a reopen with the clock at %d: %d", before, seq)
	}
	es, _ = s.Read("a", 10, 1<<20)
	if fmt.Sprint(rel(base, seqsOf(es))) != fmt.Sprint([]uint64{3, 4, seq - base}) {
		t.Errorf("read after the second reopen: %v, want %d, %d and %d", seqsOf(es), base+3, base+4, seq)
	}
}

func TestConsumerChanges(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "old")
	base := s.Stats().LastSeq
	appendN(t, s, 3)
	_ = s.Close()

	// "old" is gone (its backlog is released), "new" starts at the head.
	s = open(t, dir, "new")
	if st := s.Stats(); st.Records != 0 {
		t.Errorf("records of a removed consumer kept: %+v", st)
	}
	es, _ := s.Read("new", 10, 1<<20)
	if len(es) != 0 {
		t.Errorf("new consumer saw old records %v", seqsOf(es))
	}
	if p := s.Pending("new"); p != 0 {
		t.Errorf("new consumer: %d pending", p)
	}
	seqs := appendN(t, s, 1)
	es, _ = s.Read("new", 10, 1<<20)
	if fmt.Sprint(seqsOf(es)) != fmt.Sprint(seqs) || seqs[0] <= base+3 {
		t.Errorf("new consumer: %v, want %v, above %d", seqsOf(es), seqs, base+3)
	}
	_ = s.Close()

	// No consumers: records are not kept, but seq still advances.
	s = open(t, dir)
	defer s.Close()
	if next := appendN(t, s, 2); next[0] <= seqs[0] || next[1] != next[0]+1 {
		t.Errorf("seqs %v after %d", next, seqs[0])
	}
	if st := s.Stats(); st.Records != 0 || st.Bytes != 0 {
		t.Errorf("stats without consumers %+v", st)
	}
}

// noisePool holds random bytes for noise.
var noisePool = sync.OnceValue(func() []byte {
	r := rand.New(rand.NewPCG(1, 11)) //nolint:gosec // test data
	b := make([]byte, 1<<20)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
})

// noise is record i's data, size bytes that do not compress: stored, a
// record takes about size bytes, as a real one of that compressed size.
func noise(i, size int) []byte {
	pool := noisePool()
	off := i * 4099 % (len(pool) - size)
	return pool[off : off+size]
}

// appendNoise appends n records of noise of size bytes; every append must
// succeed.
func appendNoise(t testing.TB, s *Spool, n, size int) {
	t.Helper()
	for i := range n {
		if _, err := s.Append(time.Now(), func(uint64) ([]byte, error) { return noise(i, size), nil }); err != nil {
			t.Fatalf("append %d of %d: %v (stats %+v)", i+1, n, err, s.Stats())
		}
	}
}

// checkNewestKept checks that s holds its newest records, all unread by
// consumer, and if first is not 0, that every record since seq first that
// it does not hold counts as dropped. skips are where an open went on from
// the clock (see raise), each as Stats().LastSeq before the spool was
// closed and after it was opened again: no seq between them was assigned.
func checkNewestKept(t testing.TB, s *Spool, consumer string, first uint64, skips ...[2]uint64) Stats {
	t.Helper()
	st := s.Stats()
	appended := st.LastSeq - first
	for _, k := range skips {
		appended -= k[1] - k[0]
	}
	if st.Records == 0 || first != 0 && st.Dropped != appended-uint64(st.Records) {
		t.Fatalf("stats %+v: want every record not kept counted as dropped (%d appended)", st, appended)
	}
	es, err := s.Read(consumer, int(st.Records)+1, 1<<30)
	// The newest seqs assigned, counting back from the last past the skips.
	want, ok := st.LastSeq, err == nil && len(es) == int(st.Records)
	for i := len(es) - 1; ok && i >= 0; i-- {
		ok = es[i].Seq == want
		want--
		for _, k := range skips {
			if want == k[1] {
				want = k[0]
			}
		}
	}
	if !ok {
		var from, to uint64
		if len(es) > 0 {
			from, to = es[0].Seq, es[len(es)-1].Seq
		}
		t.Fatalf("kept %d records, seqs %d..%d (%v); want the newest %d, up to %d", len(es), from, to, err, st.Records, st.LastSeq)
	}
	return st
}

// usedBytes is what the bound counts: the file's pages in use.
func usedBytes(t testing.TB, s *Spool) int64 {
	t.Helper()
	var used int64
	if err := s.db.View(func(tx *bolt.Tx) error {
		st := s.db.Stats()
		used = tx.Size() - int64(st.FreePageN+st.PendingPageN)*int64(s.db.Info().PageSize)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return used
}

// The size bound is on the database file (review, round six). With
// MaxBytes 1 MiB and nothing acknowledged, 2 MB of records are appended:
// every append succeeds, the file stays within 1 MiB, the newest records
// are kept and every other one counts as dropped. Before, the bound counted
// the stored record bytes only, and the file grew to about 1.5 times it
// (a volume as large as the bound filled first): bbolt fills its leaves
// to about two thirds with records of this size, and to half with records
// of a quarter page (it splits no leaf of up to 4 elements).
func TestSizeBoundIsTheFile(t *testing.T) {
	dir := t.TempDir()
	const maxBytes = MinMaxBytes
	s, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: maxBytes, Consumers: []string{"c"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := s.Stats().LastSeq
	appendNoise(t, s, 1500, 1300)
	st := checkNewestKept(t, s, "c", first)
	fi, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("file %d bytes, %d in use, %d records of %d bytes stored, %d dropped", fi.Size(), usedBytes(t, s), st.Records, st.Bytes, st.Dropped)
	if fi.Size() > maxBytes {
		t.Errorf("file %d bytes, over the bound of %d", fi.Size(), maxBytes)
	}
	if st.Bytes < maxBytes*2/5 {
		t.Errorf("%d bytes of records stored: the bound is not used", st.Bytes)
	}
}

// A file larger than MaxBytes (an earlier version's, or a lower
// max_bytes) keeps its size, and the spool keeps no more than MaxBytes of
// it in use: dropped at open, oldest first, counted.
func TestLowerMaxBytes(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 4 << 20, Consumers: []string{"c"}})
	if err != nil {
		t.Fatal(err)
	}
	first := s.Stats().LastSeq
	appendNoise(t, s, 1700, 1300)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	if s.Stats().Dropped != 0 {
		t.Fatalf("dropped under the larger bound: %+v", s.Stats())
	}
	const maxBytes = MinMaxBytes
	// With the clock behind, the seqs go on from the counter (see raise),
	// so the records kept can be checked by seq.
	s, err = Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: maxBytes, Consumers: []string{"c"}, Now: behind,
		Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if used := usedBytes(t, s); used > maxBytes {
		t.Errorf("%d bytes in use after opening with a bound of %d", used, maxBytes)
	}
	if s.Stats().Dropped == 0 {
		t.Error("nothing counted as dropped")
	}
	appendNoise(t, s, 300, 1300)
	if used := usedBytes(t, s); used > maxBytes {
		t.Errorf("%d bytes in use, bound %d", used, maxBytes)
	}
	st := s.Stats()
	if es, err := s.Read("c", 1, 1<<20); err != nil || es[0].Seq != st.LastSeq-uint64(st.Records)+1 || st.LastSeq != first+2000 {
		t.Errorf("oldest kept %v (%v), stats %+v", seqsOf(es), err, st)
	}
	after, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() > before.Size() {
		t.Errorf("the file grew from %d to %d bytes", before.Size(), after.Size())
	}
}

func TestAgeBoundDropsOldest(t *testing.T) {
	now := time.Now()
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	s, err := Open(Options{Dir: t.TempDir(), MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"c"}, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := s.Stats().LastSeq
	for i := range 3 {
		ts := now.Add(time.Duration(i) * 40 * time.Minute)
		if _, err := s.Append(ts, func(uint64) ([]byte, error) { return []byte("{}"), nil }); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	now = now.Add(110 * time.Minute) // records at +0 and +40m are now older than 1h
	mu.Unlock()
	if _, err := s.Append(clock(), func(uint64) ([]byte, error) { return []byte("{}"), nil }); err != nil {
		t.Fatal(err)
	}
	es, _ := s.Read("c", 10, 1<<20)
	if fmt.Sprint(rel(base, seqsOf(es))) != "[3 4]" {
		t.Errorf("after age bound: %v", seqsOf(es))
	}
	if d := s.Stats().Dropped; d != 2 {
		t.Errorf("dropped %d, want 2", d)
	}
}

func TestDeliveredRecordsAreNotCountedAsDropped(t *testing.T) {
	s, err := Open(Options{Dir: t.TempDir(), MaxAge: time.Hour, MaxBytes: MinMaxBytes, Consumers: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := s.Stats().LastSeq
	appendNoise(t, s, 100, 1300)
	if err := s.Ack("a", first+50); err != nil { // b lags; records stay but are delivered to a
		t.Fatal(err)
	}
	appendNoise(t, s, 1000, 1300)
	// Every dropped record was undelivered by b, so all count.
	if st := s.Stats(); st.Dropped == 0 || st.Dropped != 1100-uint64(st.Records) {
		t.Errorf("stats %+v", st)
	}
	// Records every consumer delivered are deleted, not dropped.
	st := s.Stats()
	for _, c := range []string{"a", "b"} {
		if err := s.Ack(c, st.LastSeq-10); err != nil {
			t.Fatal(err)
		}
	}
	appendNoise(t, s, 5, 1300)
	if got := s.Stats(); got.Dropped != st.Dropped || got.Records != 15 {
		t.Errorf("after delivery: stats %+v, before %+v", got, st)
	}
}

func TestEncodeErrorConsumesNothing(t *testing.T) {
	s := open(t, t.TempDir(), "c")
	defer s.Close()
	base := s.Stats().LastSeq
	if _, err := s.Append(time.Now(), func(uint64) ([]byte, error) { return nil, errors.New("boom") }); err == nil {
		t.Fatal("encode error swallowed")
	}
	if seqs := appendN(t, s, 1); seqs[0] != base+1 {
		t.Errorf("seq %d after failed append, want %d", seqs[0], base+1)
	}
}

func TestCorruptRecordIsSkipped(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "c")
	base := s.Stats().LastSeq
	appendN(t, s, 3)
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRecords).Put(seqKey(base+2), []byte("\x00\x00\x00\x00\x00\x00\x00\x01garbage"))
	})
	if err != nil {
		t.Fatal(err)
	}
	es, err := s.Read("c", 10, 1<<20)
	if err != nil || fmt.Sprint(rel(base, seqsOf(es))) != "[1 3]" {
		t.Errorf("read %v, %v", seqsOf(es), err)
	}
	_ = s.Close()
}

// A record decodes to at most maxRecordSize; one that would decode to more
// (a stored value cannot get there honestly) is skipped like any
// undecodable record. Records far larger than the agent writes still
// decode.
func TestRecordSizeIsBounded(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "c")
	base := s.Stats().LastSeq
	appendN(t, s, 3)
	big := bytes.Repeat([]byte("x"), 2<<20) // about ten times the largest record
	if _, err := s.Append(time.Now(), func(uint64) ([]byte, error) { return big, nil }); err != nil {
		t.Fatal(err)
	}
	// Just over the bound: 65 RLE blocks of 128 KiB.
	val := binary.BigEndian.AppendUint64(nil, uint64(time.Now().UnixNano())) //nolint:gosec // now
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRecords).Put(seqKey(base+2), append(val, rleBomb(maxRecordSize/(128<<10)+1)...))
	}); err != nil {
		t.Fatal(err)
	}
	es, err := s.Read("c", 10, 16<<20)
	if err != nil || fmt.Sprint(rel(base, seqsOf(es))) != "[1 3 4]" {
		t.Fatalf("read %v, %v", rel(base, seqsOf(es)), err)
	}
	if !bytes.Equal(es[2].Data, big) {
		t.Errorf("the 2 MiB record came back as %d bytes", len(es[2].Data))
	}
	_ = s.Close()
}

func TestNotify(t *testing.T) {
	s := open(t, t.TempDir(), "c")
	defer s.Close()
	ch := s.Notify("c")
	appendN(t, s, 3)
	select {
	case <-ch:
	default:
		t.Fatal("no notification after append")
	}
	select {
	case <-ch:
		t.Fatal("notifications must coalesce")
	default:
	}
}

func TestLocked(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "c")
	defer s.Close()
	if _, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 20}); !errors.Is(err, ErrLocked) {
		t.Errorf("second open: %v, want ErrLocked", err)
	}
}

func TestConcurrentAppends(t *testing.T) {
	s := open(t, t.TempDir(), "c")
	defer s.Close()
	base := s.Stats().LastSeq
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { appendN(t, s, 25) })
	}
	wg.Wait()
	es, _ := s.Read("c", 1000, 1<<30)
	if len(es) != 200 {
		t.Fatalf("%d records", len(es))
	}
	for i, e := range es {
		if e.Seq != base+uint64(i+1) || !bytes.HasPrefix(e.Data, []byte(fmt.Sprintf(`{"seq":%d,`, e.Seq))) {
			t.Fatalf("entry %d: seq %d data %q", i, e.Seq, e.Data)
		}
	}
}

// TestKill9 appends from a child process, SIGKILLs it at an arbitrary moment,
// and checks that every append the child reported as done survived, intact,
// and that seq goes on after the highest committed one: right after it
// with the clock behind (the counter is committed with the record), from
// the clock otherwise. Mode "noalloc" runs the child without fallocate: it
// writes zeros ahead of its pages. Mode "reopen" has the child close and
// reopen its spool every ten appends, each open going on from the clock
// (see raise), so that the kill may land in an open: the seqs it reported
// must only go up, and those after it lie above them.
func TestKill9(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	for _, mode := range []string{"alloc", "noalloc", "reopen"} {
		t.Run(mode, func(t *testing.T) { kill9(t, mode) })
	}
}

func kill9(t *testing.T, mode string) {
	for round := range 5 {
		dir := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$")
		cmd.Env = append(os.Environ(), "HOPCORDER_SPOOL_CRASH_DIR="+dir, "HOPCORDER_SPOOL_CRASH_MODE="+mode)
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(out)
		var acked []uint64
		killAt := 20 + round*37
		killed := false
		// Keep reading after the kill: lines the child printed before it
		// died may still sit in the pipe, and each of them reports a
		// committed append.
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "appended ") {
				continue
			}
			seq, _ := strconv.ParseUint(strings.TrimPrefix(line, "appended "), 10, 64)
			acked = append(acked, seq)
			if !killed && len(acked) == killAt {
				_ = cmd.Process.Signal(syscall.SIGKILL)
				killed = true
			}
		}
		_ = cmd.Wait()
		if len(acked) < killAt {
			t.Fatalf("child produced only %d appends", len(acked))
		}
		for i := 1; i < len(acked); i++ {
			if acked[i] <= acked[i-1] {
				t.Fatalf("round %d: the child assigned %d after %d", round, acked[i], acked[i-1])
			}
		}

		s := reopen(t, dir, "reader", "acker") // the clock behind: the spool goes on from its counter
		es, err := s.Read("reader", 100000, 1<<30)
		if err != nil {
			t.Fatal(err)
		}
		have := map[uint64]bool{}
		for _, e := range es {
			if !bytes.HasPrefix(e.Data, []byte(fmt.Sprintf(`{"seq":%d,`, e.Seq))) {
				t.Fatalf("round %d: record %d corrupted: %q", round, e.Seq, e.Data)
			}
			have[e.Seq] = true
		}
		// The child acks for "acker" as it goes, so records may have been
		// collected only once both cursors passed them; "reader" never
		// acked, so every committed record must still be here.
		for _, seq := range acked {
			if !have[seq] {
				t.Fatalf("round %d: acknowledged append %d lost after kill -9", round, seq)
			}
		}
		last := acked[len(acked)-1]
		next := appendN(t, s, 1)[0]
		// At most one append can have committed without being reported
		// (in mode reopen, after an open that went on from the clock).
		if next <= last || mode != "reopen" && next > last+2 {
			t.Errorf("round %d: seq after crash %d, last reported %d", round, next, last)
		}
		_ = s.Close()
		// With the clock ahead of the counter, from the clock.
		before := clockSeq(time.Now())
		s = open(t, dir, "reader", "acker")
		if seq := appendN(t, s, 1)[0]; seq < before || seq <= next {
			t.Errorf("round %d: seq after a reopen with the clock at %d: %d, after %d", round, before, seq, next)
		}
		_ = s.Close()
	}
}

// TestCrashChild is the child half of TestKill9.
func TestCrashChild(t *testing.T) {
	dir := os.Getenv("HOPCORDER_SPOOL_CRASH_DIR")
	if dir == "" {
		t.Skip("helper process for TestKill9")
	}
	mode := os.Getenv("HOPCORDER_SPOOL_CRASH_MODE")
	if mode == "noalloc" {
		allocate = func(string, int64, int64) error { return errors.ErrUnsupported }
	}
	opts := Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"reader", "acker"}}
	s, err := Open(opts)
	if err != nil {
		fmt.Println("open failed:", err)
		os.Exit(1)
	}
	for n := 1; ; n++ {
		seq, err := s.Append(time.Now(), func(seq uint64) ([]byte, error) {
			return []byte(fmt.Sprintf(`{"seq":%d,"pad":%q}`, seq, strings.Repeat("y", 500))), nil
		})
		if err != nil {
			fmt.Println("append failed:", err)
			os.Exit(1)
		}
		fmt.Printf("appended %d\n", seq)
		if seq%7 == 0 {
			if err := s.Ack("acker", seq-3); err != nil {
				fmt.Println("ack failed:", err)
				os.Exit(1)
			}
		}
		if mode == "reopen" && n%10 == 0 {
			if err := s.Close(); err != nil {
				fmt.Println("close failed:", err)
				os.Exit(1)
			}
			if s, err = Open(opts); err != nil {
				fmt.Println("open failed:", err)
				os.Exit(1)
			}
		}
	}
}

// A damaged spool file (bit rot, a torn page) must not stop the agent for
// good: it is moved aside and a new spool starts, with seqs above any the old
// one used.
func TestCorruptFileIsQuarantined(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "a")
	appendN(t, s, 200)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fileName)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Garbage over everything after the two meta pages and page 2 (the
	// freelist bbolt starts a file with).
	for i := 3 * 4096; i < len(b); i++ {
		b[i] = byte(i*31 + 7)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}

	before := time.Now()
	s = open(t, dir, "a")
	defer s.Close()
	if st := s.Stats(); st.Records != 0 {
		t.Errorf("new spool holds %d records", st.Records)
	}
	seqs := appendN(t, s, 1)
	if seqs[0] < uint64(before.UnixMicro()) { //nolint:gosec // positive
		t.Errorf("first seq %d, want one above %d", seqs[0], before.UnixMicro())
	}
	moved, _ := filepath.Glob(path + ".corrupt-*")
	if len(moved) != 1 {
		t.Errorf("corrupt file kept as %v", moved)
	}
}

// A healthy file is never moved aside, whatever else fails.
func TestHealthyFileIsKept(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "a")
	base := s.Stats().LastSeq
	appendN(t, s, 3)
	// A second open while the first holds the lock fails without touching it.
	if _, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 20, Consumers: []string{"a"}}); !errors.Is(err, ErrLocked) {
		t.Errorf("second open: %v", err)
	}
	_ = s.Close()
	if moved, _ := filepath.Glob(filepath.Join(dir, fileName) + ".corrupt-*"); len(moved) != 0 {
		t.Errorf("healthy file moved: %v", moved)
	}
	s = reopen(t, dir, "a") // the clock behind: the seqs go on from the counter
	defer s.Close()
	if seqs := appendN(t, s, 1); seqs[0] != base+4 || s.Stats().Records != 4 {
		t.Errorf("seq after reopen %d, want %d; %+v", seqs[0], base+4, s.Stats())
	}
}

// Damage inside a leaf page used to crash the agent instead of being
// quarantined: bbolt's consistency check runs in a goroutine of its own,
// where a fault on the mapped file is fatal. The check never reads values,
// so a value pointing outside the file went unnoticed until a drain read it
// (faulting or not, depending on the bytes). Each case opens the damaged
// file in a child process, so a regression fails this test instead of
// killing the test binary.
func TestDamagedLeafIsQuarantined(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	cases := []struct {
		name  string
		field int // byte offset in a leaf element: flags 0, pos 4, ksize 8, vsize 12
	}{
		{"key outside the mapping", 4},    // faults in tx.Check's key order check
		{"value outside the mapping", 12}, // never read by tx.Check
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			s := open(t, dir, "a")
			appendN(t, s, 400)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, fileName)
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			const pageSize = 4096
			damaged := false
			for pg := 2; (pg+1)*pageSize <= len(b) && !damaged; pg++ {
				p := b[pg*pageSize:]
				// A leaf page (flags 0x02) of the records bucket: many elements.
				if binary.LittleEndian.Uint16(p[8:]) == 0x02 && binary.LittleEndian.Uint16(p[10:]) > 10 {
					binary.LittleEndian.PutUint32(p[16+c.field:], 0x7ff00000)
					damaged = true
				}
			}
			if !damaged {
				t.Fatal("no records leaf page found")
			}
			if err := os.WriteFile(path, b, 0o600); err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command(os.Args[0], "-test.run=^TestOpenDamagedChild$")
			cmd.Env = append(os.Environ(), "HOPCORDER_SPOOL_DAMAGED_DIR="+dir)
			out, err := cmd.CombinedOutput()
			if err != nil || !strings.Contains(string(out), "opened, records 0") {
				t.Fatalf("child: %v\n%s", err, out)
			}
			if moved, _ := filepath.Glob(path + ".corrupt-*"); len(moved) != 1 {
				t.Errorf("damaged file kept as %v", moved)
			}
		})
	}
}

// TestOpenDamagedChild is the child half of TestDamagedLeafIsQuarantined:
// it opens the spool and reads it the way a drain does.
func TestOpenDamagedChild(t *testing.T) {
	dir := os.Getenv("HOPCORDER_SPOOL_DAMAGED_DIR")
	if dir == "" {
		t.Skip("helper process for TestDamagedLeafIsQuarantined")
	}
	s, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"a"}})
	if err != nil {
		fmt.Println("open failed:", err)
		os.Exit(1)
	}
	done := make(chan error, 1)
	go func() { // drains read in goroutines of their own
		_, err := s.Read("a", 1000, 1<<30)
		done <- err
	}()
	err = <-done
	_ = s.Close()
	if err != nil {
		fmt.Println("read failed:", err)
		os.Exit(1)
	}
	fmt.Printf("opened, records %d\n", s.Stats().Records)
}

// A new spool starts its seqs at the clock in microseconds, so records of a
// spool that was deleted and recreated under the same agent_id lie above
// everything the old one sent (the server drops seqs at or below its
// high-water mark as duplicates). Since round eight an existing spool goes
// on from the clock too when that is ahead of its counter, and from its
// counter when the clock is behind: its seqs never go back.
func TestNewSpoolStartsAtTheClock(t *testing.T) {
	dir := t.TempDir()
	before := time.Now()
	s := open(t, dir, "a")
	first := appendN(t, s, 1)[0]
	if first < uint64(before.UnixMicro()) || first > uint64(time.Now().UnixMicro()) { //nolint:gosec // positive
		t.Errorf("first seq %d, want the time in microseconds (%d)", first, before.UnixMicro())
	}
	appendN(t, s, 2)
	_ = s.Close()

	// Reopened much later, it goes on from the clock.
	later := time.Now().Add(24 * time.Hour)
	s, err := Open(Options{Dir: dir, MaxAge: 48 * time.Hour, MaxBytes: 1 << 30, Consumers: []string{"a"},
		Now: func() time.Time { return later }})
	if err != nil {
		t.Fatal(err)
	}
	if seq := appendN(t, s, 1)[0]; seq != clockSeq(later) {
		t.Errorf("seq after a reopen a day later %d, want %d", seq, clockSeq(later))
	}
	last := s.Stats().LastSeq
	_ = s.Close()

	// Reopened with the clock behind (the day before), it goes on from its
	// counter, and says so.
	var logs bytes.Buffer
	s, err = Open(Options{Dir: dir, MaxAge: 48 * time.Hour, MaxBytes: 1 << 30, Consumers: []string{"a"},
		Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	if seq := appendN(t, s, 1)[0]; seq != last+1 {
		t.Errorf("seq after a reopen with the clock behind %d, want %d", seq, last+1)
	}
	if !strings.Contains(logs.String(), "the clock is behind") {
		t.Errorf("no warning that the clock is behind: %q", logs.String())
	}
	last++
	_ = s.Close()

	// Deleted and recreated, it starts above the old one (as long as the
	// clock is right).
	if err := os.Remove(filepath.Join(dir, fileName)); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Options{Dir: dir, MaxAge: 48 * time.Hour, MaxBytes: 1 << 30, Consumers: []string{"a"},
		Now: func() time.Time { return later.Add(time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	if seq := appendN(t, s, 1)[0]; seq <= last {
		t.Errorf("seq of a recreated spool %d, not above the old %d", seq, last)
	}
	_ = s.Close()

	// A spool created before seqs came from the clock (counting from 1)
	// goes on from the clock too.
	old := t.TempDir()
	epoch := func() time.Time { return time.Unix(0, 0) } // the clock's seq is 1
	s, err = Open(Options{Dir: old, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"a"}, Logger: slog.Default(), Now: epoch})
	if err != nil {
		t.Fatal(err)
	}
	if seqs := appendN(t, s, 3); fmt.Sprint(seqs) != "[1 2 3]" {
		t.Errorf("seqs of a spool created at the epoch %v", seqs)
	}
	_ = s.Close()
	before = time.Now()
	s = open(t, old, "a")
	defer s.Close()
	if seq := appendN(t, s, 1)[0]; seq < clockSeq(before) {
		t.Errorf("seq of a spool counting from 1 after reopen %d, want the clock's (%d)", seq, clockSeq(before))
	}
}

// A spool that went back to an earlier state (a VM snapshot reverted, a
// volume restored from a backup, a disk that lost writes it had
// confirmed) went on from the seq it held then, and so reused seqs the
// server already had: the server dropped the new cycles as duplicates
// (review, round eight, agent M1). Opened, a spool goes on from the clock
// when that is ahead, above every seq assigned before the rollback; the
// records the earlier state held are delivered as they were.
func TestRolledBackSpoolGoesOnAboveItsSeqs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, fileName)
	now := time.Now()
	opts := Options{Dir: dir, MaxAge: 7 * 24 * time.Hour, MaxBytes: 1 << 30, Consumers: []string{"http"},
		Now: func() time.Time { return now }}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	early := appendN(t, s, 3)
	if err := s.Ack("http", early[1]); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// The agent runs on (and restarts, and delivers) for a while...
	var issued []uint64
	for range 3 {
		now = now.Add(time.Minute)
		if s, err = Open(opts); err != nil {
			t.Fatal(err)
		}
		issued = append(issued, appendN(t, s, 5)...)
		if err := s.Ack("http", issued[len(issued)-1]); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// ...then the spool goes back to the snapshot.
	if err := os.WriteFile(path, snapshot, 0o600); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if s, err = Open(opts); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after := appendN(t, s, 5)
	if top := slices.Max(issued); after[0] <= top {
		t.Errorf("after the rollback the spool assigned %v; before it, up to %d", after, top)
	}
	if es, err := s.Read("http", 100, 1<<20); err != nil || fmt.Sprint(seqsOf(es)) != fmt.Sprint(append(early[2:], after...)) {
		t.Errorf("read %v (%v), want %d then %v", seqsOf(es), err, early[2], after)
	}
}

// Pending counts records, not seqs: an open that goes on from the clock
// skips seqs that no record holds (review, round eight), and the outputs'
// batching, the shutdown's wait and the pending metric read it. After the
// open, an output that had delivered everything has nothing pending, as
// has one added then; one that had not has the records it had not
// delivered, and nothing once it delivered them, though nothing was
// appended since.
func TestPendingAcrossTheClock(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "jsonl", "http")
	seqs := appendN(t, s, 5)
	if err := errors.Join(s.Ack("jsonl", seqs[4]), s.Ack("http", seqs[1]), s.Close()); err != nil {
		t.Fatal(err)
	}
	consumers := []string{"jsonl", "http", "new"}
	s = ahead(t, dir, consumers...)
	defer s.Close()
	pending := func(want ...uint64) {
		t.Helper()
		for i, c := range consumers {
			if p := s.Pending(c); p != want[i] {
				t.Errorf("%s: %d pending, want %d (%+v)", c, p, want[i], s.Stats())
			}
		}
	}
	if st := s.Stats(); st.LastSeq < seqs[4]+uint64(time.Hour/time.Microsecond) || st.Records != 3 {
		t.Errorf("after the open: %+v, want the last seq an hour of microseconds past %d, 3 records", st, seqs[4])
	}
	pending(0, 3, 0)
	got, _ := readAll(t, s, "http", 0)
	if fmt.Sprint(got) != fmt.Sprint(seqs[2:]) {
		t.Errorf("http read %v, want %v", got, seqs[2:])
	}
	pending(0, 0, 0)
	added := appendN(t, s, 2)
	pending(2, 2, 2)
	for _, c := range consumers {
		if got, _ := readAll(t, s, c, 0); fmt.Sprint(got) != fmt.Sprint(added) {
			t.Errorf("%s read %v, want %v", c, got, added)
		}
	}
	pending(0, 0, 0)
	if st := s.Stats(); st.Records != 0 {
		t.Errorf("delivered records kept: %+v", st)
	}
}

// With the clock behind a spool's counter (no RTC, no NTP yet, set back),
// a spool goes on from its counter: its seqs never go back, and nothing
// else changes. A rolled-back spool then reuses seqs, as before round
// eight; that the clock is behind is logged (see TestNewSpoolStartsAtTheClock).
func TestClockBehindKeepsTheCounter(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	opts := Options{Dir: dir, MaxAge: 7 * 24 * time.Hour, MaxBytes: 1 << 30, Consumers: []string{"jsonl", "http"},
		Now: func() time.Time { return now }, Logger: slog.New(slog.DiscardHandler)}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	seqs := appendN(t, s, 4)
	if err := errors.Join(s.Ack("jsonl", seqs[3]), s.Ack("http", seqs[1])); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	pending := uint64(2)
	for _, back := range []time.Duration{time.Microsecond, time.Hour, 50 * 365 * 24 * time.Hour} {
		now = now.Add(-back)
		if s, err = Open(opts); err != nil {
			t.Fatal(err)
		}
		last := seqs[len(seqs)-1]
		if st := s.Stats(); st.LastSeq != last || st.Records != int64(pending) || s.Pending("jsonl") != 0 || s.Pending("http") != pending {
			t.Errorf("clock %v back: %+v, pending %d and %d; want last seq %d, %d records, 0 and %d pending",
				back, st, s.Pending("jsonl"), s.Pending("http"), last, pending, pending)
		}
		next := appendN(t, s, 1)[0]
		if next != last+1 {
			t.Errorf("clock %v back: seq %d, want %d", back, next, last+1)
		}
		seqs = append(seqs, next)
		pending++
		if err := errors.Join(s.Ack("jsonl", next), s.Close()); err != nil {
			t.Fatal(err)
		}
	}
}

// rawUpdate runs fn in a write transaction on the closed spool in dir, with
// bbolt alone: the damage of a value or a key that the page checks cannot
// see (pages carry no checksums).
func rawUpdate(t *testing.T, dir string, fn func(tx *bolt.Tx) error) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(dir, fileName), 0o600, boltOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(fn); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// readAll reads and acknowledges every record consumer has pending and
// returns their seqs, numbered from base (see rel), and data.
func readAll(t *testing.T, s *Spool, consumer string, base uint64) ([]uint64, [][]byte) {
	t.Helper()
	es, err := s.Read(consumer, 1000, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	var data [][]byte
	for _, e := range es {
		data = append(data, e.Data)
	}
	if len(es) > 0 {
		if err := s.Ack(consumer, es[len(es)-1].Seq); err != nil {
			t.Fatal(err)
		}
	}
	return rel(base, seqsOf(es)), data
}

// Damage that raised a delivery cursor above the last seq (a flipped bit:
// the corpus's case 1025, whenever the bit was clear) stalled that output
// for good: it read nothing, every record appended later lay below its
// cursor, and acknowledgements of the other outputs, or with none the
// open itself, deleted the records it had not delivered (review, round
// seven). At open the cursor goes back to before the oldest record stored:
// the output delivers every stored record, some of them again, then those
// appended later.
func TestCursorAboveLastSeqIsRewound(t *testing.T) {
	for _, tc := range []struct {
		name      string
		consumers []string
		acked     []uint64 // each consumer's cursor before the damage, from base
		pending   uint64   // "a"'s after the open
		want      string   // the stored records "a" reads first
	}{
		{"one output", []string{"a"}, []uint64{3}, 7, "[4 5 6 7 8 9 10"},
		{"behind the other output", []string{"a", "b"}, []uint64{2, 6}, 8, "[3 4 5 6 7 8 9 10"},
		// The records between the two cursors are delivered again.
		{"ahead of the other output", []string{"a", "b"}, []uint64{6, 2}, 8, "[3 4 5 6 7 8 9 10"},
	} {
		// The cursor far above the last seq (a high bit set), or just above
		// it, below the seq the spool goes on from (the clock's, see raise):
		// it is found all the same.
		for _, damage := range []struct {
			name   string
			cursor func(base, acked uint64) uint64
		}{
			{"far above", func(base, acked uint64) uint64 { return base + acked | 1<<52 }},
			{"just above", func(base, _ uint64) uint64 { return base + 15 }},
		} {
			t.Run(tc.name+"/"+damage.name, func(t *testing.T) {
				dir := t.TempDir()
				s := open(t, dir, tc.consumers...)
				base := s.Stats().LastSeq
				appendN(t, s, 10)
				for i, c := range tc.consumers {
					if err := s.Ack(c, base+tc.acked[i]); err != nil {
						t.Fatal(err)
					}
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				rawUpdate(t, dir, func(tx *bolt.Tx) error {
					return putU64(tx.Bucket(bucketCursors), []byte("a"), damage.cursor(base, tc.acked[0]))
				})
				s = ahead(t, dir, tc.consumers...)
				defer s.Close()
				if p := s.Pending("a"); p != tc.pending {
					t.Errorf("pending %d, want %d", p, tc.pending)
				}
				added := rel(base, appendN(t, s, 2))
				if want := tc.want + " " + strings.Trim(fmt.Sprint(added), "[]") + "]"; added[0] <= 15 {
					t.Errorf("appended as %v", added)
				} else if got, _ := readAll(t, s, "a", base); fmt.Sprint(got) != want {
					t.Errorf("read %v, want %s", got, want)
				}
				more := rel(base, appendN(t, s, 1))
				if got, _ := readAll(t, s, "a", base); fmt.Sprint(got) != fmt.Sprint(more) {
					t.Errorf("read after the next append %v, want %v", got, more)
				}
				if len(tc.consumers) > 1 {
					want := append(append(seqRange(tc.acked[1]+1, 10), added...), more...)
					if got, _ := readAll(t, s, "b", base); fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("b read %v, want %v", got, want)
					}
				}
			})
		}
	}
}

// seqRange is from..to.
func seqRange(from, to uint64) []uint64 {
	var out []uint64
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

// Damage that raised the newest record's key above next_seq - 1 (a damaged
// word: the review's at-bound corpus, case 834) stalled every output: each
// delivered that record, acknowledged its key, and every record appended
// later lay below its cursor (review, round seven). At open such a record
// gets the next seq, keeping its data and the seq in it (which the server
// deduplicates on), so nothing is lost. A next_seq lowered by damage, with
// records above it, comes back above them; before, appends overwrote
// those records. Each case opens with the clock behind, where the spool
// goes on right after the records, and with the clock ahead, where it
// goes on from the clock (see raise).
func TestKeyAboveNextSeqIsRenumbered(t *testing.T) {
	for _, tc := range []struct {
		name   string
		damage func(tx *bolt.Tx, base uint64) error
		want   string // what "a" reads, numbered from base: the five records, in order
	}{
		{"newest key raised", func(tx *bolt.Tx, base uint64) error {
			return rekey(tx, base+5, base+5|1<<62)
		}, "[1 2 3 4 6]"},
		// Below the seq the spool goes on from (the clock's): renumbered
		// all the same.
		{"newest key raised a little", func(tx *bolt.Tx, base uint64) error {
			return rekey(tx, base+5, base+105)
		}, "[1 2 3 4 6]"},
		{"two newest keys raised", func(tx *bolt.Tx, base uint64) error {
			return errors.Join(rekey(tx, base+4, base+4+1<<40), rekey(tx, base+5, base+5|1<<62))
		}, "[1 2 3 6 7]"},
		// The stall as a restart found it: the raised key acknowledged.
		{"newest key raised and acknowledged", func(tx *bolt.Tx, base uint64) error {
			return errors.Join(rekey(tx, base+5, base+5|1<<62), putU64(tx.Bucket(bucketCursors), []byte("a"), base+5|1<<62))
		}, "[1 2 3 4 6]"},
		{"next_seq lowered", func(tx *bolt.Tx, base uint64) error {
			return putU64(tx.Bucket(bucketMeta), keyNextSeq, base+3)
		}, "[1 2 3 4 5]"},
	} {
		for _, clock := range []string{"behind", "ahead"} {
			t.Run(tc.name+"/clock "+clock, func(t *testing.T) {
				dir := t.TempDir()
				s := open(t, dir, "a")
				base := s.Stats().LastSeq
				appendN(t, s, 5)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				rawUpdate(t, dir, func(tx *bolt.Tx) error { return tc.damage(tx, base) })
				if clock == "behind" {
					s = reopen(t, dir, "a")
				} else {
					s = ahead(t, dir, "a")
				}
				defer s.Close()
				got, data := readAll(t, s, "a", base)
				if fmt.Sprint(got) != tc.want {
					t.Errorf("read %v, want %s", got, tc.want)
				}
				if len(got) != 5 {
					t.FailNow()
				}
				for i, d := range data {
					if !bytes.HasPrefix(d, fmt.Appendf(nil, `{"seq":%d,`, base+uint64(i)+1)) {
						t.Errorf("record %d: %.30s, want the one appended as %d", got[i], d, i+1)
					}
				}
				last := got[4]
				added := rel(base, appendN(t, s, 2))
				if clock == "behind" && added[0] != last+1 || added[0] <= last+100 && clock == "ahead" || added[1] != added[0]+1 {
					t.Errorf("appended as %v after %d", added, last)
				}
				if got, _ := readAll(t, s, "a", base); fmt.Sprint(got) != fmt.Sprint(added) {
					t.Errorf("read after appending %v, want %v", got, added)
				}
			})
		}
	}
}

// rekey moves the record stored under seq from to seq to.
func rekey(tx *bolt.Tx, from, to uint64) error {
	recs := tx.Bucket(bucketRecords)
	v := append([]byte(nil), recs.Get(seqKey(from))...)
	if v == nil {
		return fmt.Errorf("no record %d", from)
	}
	return errors.Join(recs.Delete(seqKey(from)), recs.Put(seqKey(to), v))
}
