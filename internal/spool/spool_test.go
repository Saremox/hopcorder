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
	s = open(t, dir, "a")
	defer s.Close()
	if seqs := appendN(t, s, 1); seqs[0] != base+4 {
		t.Errorf("seq after reopen = %d, want %d", seqs[0], base+4)
	}
	es, _ := s.Read("a", 10, 1<<20)
	if fmt.Sprint(rel(base, seqsOf(es))) != "[3 4]" {
		t.Errorf("read after reopen: %v", seqsOf(es))
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
	appendN(t, s, 1)
	es, _ = s.Read("new", 10, 1<<20)
	if fmt.Sprint(rel(base, seqsOf(es))) != "[4]" {
		t.Errorf("new consumer: %v", seqsOf(es))
	}
	_ = s.Close()

	// No consumers: records are not kept, but seq still advances.
	s = open(t, dir)
	defer s.Close()
	if seqs := appendN(t, s, 2); fmt.Sprint(rel(base, seqs)) != "[5 6]" {
		t.Errorf("seqs %v", seqs)
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
// it does not hold counts as dropped.
func checkNewestKept(t testing.TB, s *Spool, consumer string, first uint64) Stats {
	t.Helper()
	st := s.Stats()
	if st.Records == 0 || first != 0 && st.Dropped != st.LastSeq-first-uint64(st.Records) {
		t.Fatalf("stats %+v: want every record not kept counted as dropped (%d appended)", st, st.LastSeq-first)
	}
	es, err := s.Read(consumer, int(st.Records)+1, 1<<30)
	if err != nil || len(es) != int(st.Records) || es[0].Seq != st.LastSeq-uint64(st.Records)+1 || es[len(es)-1].Seq != st.LastSeq {
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
	s, err = Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: maxBytes, Consumers: []string{"c"}})
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
// and that seq continues after the highest committed one.
func TestKill9(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	for round := range 5 {
		dir := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$")
		cmd.Env = append(os.Environ(), "HOPCORDER_SPOOL_CRASH_DIR="+dir)
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

		s := open(t, dir, "reader", "acker")
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
		// At most one append can have committed without being reported.
		if next != last+1 && next != last+2 {
			t.Errorf("round %d: seq after crash %d, last reported %d", round, next, last)
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
	s, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"reader", "acker"}})
	if err != nil {
		fmt.Println("open failed:", err)
		os.Exit(1)
	}
	for {
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
	// Garbage over everything after the two meta pages and the freelist.
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
	s = open(t, dir, "a")
	defer s.Close()
	if seqs := appendN(t, s, 1); seqs[0] != base+4 {
		t.Errorf("seq after reopen %d, want %d", seqs[0], base+4)
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
// high-water mark as duplicates). An existing spool keeps counting.
func TestNewSpoolStartsAtTheClock(t *testing.T) {
	dir := t.TempDir()
	before := time.Now()
	s := open(t, dir, "a")
	first := appendN(t, s, 1)[0]
	if first < uint64(before.UnixMicro()) || first > uint64(time.Now().UnixMicro()) { //nolint:gosec // positive
		t.Errorf("first seq %d, want the time in microseconds (%d)", first, before.UnixMicro())
	}
	last := appendN(t, s, 2)[1]
	_ = s.Close()

	// Reopened much later, it continues where it was.
	later := func() time.Time { return time.Now().Add(24 * time.Hour) }
	s, err := Open(Options{Dir: dir, MaxAge: 48 * time.Hour, MaxBytes: 1 << 30, Consumers: []string{"a"}, Now: later})
	if err != nil {
		t.Fatal(err)
	}
	if seq := appendN(t, s, 1)[0]; seq != last+1 {
		t.Errorf("seq after reopen %d, want %d", seq, last+1)
	}
	_ = s.Close()

	// Deleted and recreated, it starts above the old one.
	if err := os.Remove(filepath.Join(dir, fileName)); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir, "a")
	if seq := appendN(t, s, 1)[0]; seq <= last+1 {
		t.Errorf("seq of a recreated spool %d, not above the old %d", seq, last+1)
	}
	_ = s.Close()

	// A spool created before seqs came from the clock (counting from 1)
	// keeps its counter.
	old := t.TempDir()
	epoch := func() time.Time { return time.Unix(0, 0) } // the clock's seq is 1
	s, err = Open(Options{Dir: old, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"a"}, Logger: slog.Default(), Now: epoch})
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, s, 3)
	_ = s.Close()
	s = open(t, old, "a")
	defer s.Close()
	if seq := appendN(t, s, 1)[0]; seq != 4 {
		t.Errorf("seq of a pre-existing spool after reopen %d, want 4", seq)
	}
}
