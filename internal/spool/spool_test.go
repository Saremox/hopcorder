package spool

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
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

func TestAppendReadAck(t *testing.T) {
	s := open(t, t.TempDir(), "jsonl", "http")
	defer s.Close()
	seqs := appendN(t, s, 5)
	if fmt.Sprint(seqs) != "[1 2 3 4 5]" {
		t.Fatalf("seqs %v", seqs)
	}
	es, err := s.Read("http", 3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(seqsOf(es)) != "[1 2 3]" || !bytes.HasPrefix(es[0].Data, []byte(`{"seq":1,`)) {
		t.Fatalf("read %v %q", seqsOf(es), es[0].Data)
	}
	if err := s.Ack("http", 3); err != nil {
		t.Fatal(err)
	}
	es, _ = s.Read("http", 10, 1<<20)
	if fmt.Sprint(seqsOf(es)) != "[4 5]" {
		t.Errorf("after ack: %v", seqsOf(es))
	}
	// jsonl has acknowledged nothing, so nothing may be deleted yet.
	if st := s.Stats(); st.Records != 5 || st.LastSeq != 5 {
		t.Errorf("stats %+v", st)
	}
	if p := s.Pending("http"); p != 2 {
		t.Errorf("pending http = %d", p)
	}
	if err := s.Ack("jsonl", 4); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.Records != 2 {
		t.Errorf("records after both acked through 3: %+v", st)
	}
	if err := s.Ack("http", 2); err != nil { // going backwards is a no-op
		t.Fatal(err)
	}
	es, _ = s.Read("http", 10, 1<<20)
	if fmt.Sprint(seqsOf(es)) != "[4 5]" {
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
	appendN(t, s, 3)
	if err := s.Ack("a", 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir, "a")
	defer s.Close()
	if seqs := appendN(t, s, 1); seqs[0] != 4 {
		t.Errorf("seq after reopen = %d, want 4", seqs[0])
	}
	es, _ := s.Read("a", 10, 1<<20)
	if fmt.Sprint(seqsOf(es)) != "[3 4]" {
		t.Errorf("read after reopen: %v", seqsOf(es))
	}
}

func TestConsumerChanges(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "old")
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
	if fmt.Sprint(seqsOf(es)) != "[4]" {
		t.Errorf("new consumer: %v", seqsOf(es))
	}
	_ = s.Close()

	// No consumers: records are not kept, but seq still advances.
	s = open(t, dir)
	defer s.Close()
	if seqs := appendN(t, s, 2); fmt.Sprint(seqs) != "[5 6]" {
		t.Errorf("seqs %v", seqs)
	}
	if st := s.Stats(); st.Records != 0 || st.Bytes != 0 {
		t.Errorf("stats without consumers %+v", st)
	}
}

func TestSizeBoundDropsOldest(t *testing.T) {
	s, err := Open(Options{Dir: t.TempDir(), MaxAge: time.Hour, MaxBytes: 1000, Consumers: []string{"c"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	appendN(t, s, 50)
	st := s.Stats()
	if st.Bytes > 1000 || st.Records == 0 || st.Records >= 50 {
		t.Fatalf("stats %+v", st)
	}
	if st.Dropped != uint64(50-st.Records) {
		t.Errorf("dropped %d, want %d", st.Dropped, 50-st.Records)
	}
	es, _ := s.Read("c", 100, 1<<20)
	if es[len(es)-1].Seq != 50 || es[0].Seq != uint64(51-st.Records) {
		t.Errorf("kept %v; the newest must survive", seqsOf(es))
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
	if fmt.Sprint(seqsOf(es)) != "[3 4]" {
		t.Errorf("after age bound: %v", seqsOf(es))
	}
	if d := s.Stats().Dropped; d != 2 {
		t.Errorf("dropped %d, want 2", d)
	}
}

func TestDeliveredRecordsAreNotCountedAsDropped(t *testing.T) {
	s, err := Open(Options{Dir: t.TempDir(), MaxAge: time.Hour, MaxBytes: 1000, Consumers: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	appendN(t, s, 5)
	_ = s.Ack("a", 5) // b lags; records stay but are delivered to a
	appendN(t, s, 50)
	// Every dropped record was undelivered by b, so all count.
	if st := s.Stats(); st.Dropped != uint64(55-st.Records) {
		t.Errorf("stats %+v", st)
	}
}

func TestEncodeErrorConsumesNothing(t *testing.T) {
	s := open(t, t.TempDir(), "c")
	defer s.Close()
	if _, err := s.Append(time.Now(), func(uint64) ([]byte, error) { return nil, errors.New("boom") }); err == nil {
		t.Fatal("encode error swallowed")
	}
	if seqs := appendN(t, s, 1); seqs[0] != 1 {
		t.Errorf("seq %d after failed append, want 1", seqs[0])
	}
}

func TestCorruptRecordIsSkipped(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "c")
	appendN(t, s, 3)
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRecords).Put(seqKey(2), []byte("\x00\x00\x00\x00\x00\x00\x00\x01garbage"))
	})
	if err != nil {
		t.Fatal(err)
	}
	es, err := s.Read("c", 10, 1<<20)
	if err != nil || fmt.Sprint(seqsOf(es)) != "[1 3]" {
		t.Errorf("read %v, %v", seqsOf(es), err)
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
		if e.Seq != uint64(i+1) || !bytes.HasPrefix(e.Data, []byte(fmt.Sprintf(`{"seq":%d,`, e.Seq))) {
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
