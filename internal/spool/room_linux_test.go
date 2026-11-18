//go:build linux

package spool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"
)

// A full file system (review, round six). The file system here is
// RLIMIT_FSIZE, set in a child process: past it, writes, fallocate and
// truncate fail with EFBIG, as they fail with ENOSPC on a full disk; no
// root needed (TestFullTmpfs has a real one). The spool's own bound, 16
// MiB, is far above the 2 MiB the file may have. During an outage (nothing
// acknowledged) every append succeeds by dropping the oldest records,
// counted; delivering everything in one acknowledgement works; so do
// appends after it and after a restart; and once there is room again the
// spool grows. Mode "noalloc" runs it as on a file system without
// fallocate (the spool writes zeros ahead instead). Before, every append
// past 2 MiB failed (the newest records lost, none counted as dropped), and
// so did the large acknowledgement.
func TestFullFileSystem(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	for _, mode := range []string{"alloc", "noalloc"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestFullFileSystemChild$", "-test.v")
			cmd.Env = append(os.Environ(), "HOPCORDER_SPOOL_FULL="+mode+" "+t.TempDir())
			out, err := cmd.CombinedOutput()
			for line := range bytes.Lines(out) {
				if bytes.HasPrefix(line, []byte("CHILD")) || bytes.Contains(line, []byte(".go:")) {
					t.Logf("%s", bytes.TrimSpace(line))
				}
			}
			if err != nil || !bytes.Contains(out, []byte("CHILD OK")) {
				t.Fatalf("child: %v\n%s", err, out)
			}
		})
	}
}

// TestFullFileSystemChild is TestFullFileSystem's child process.
func TestFullFileSystemChild(t *testing.T) {
	spec := strings.Fields(os.Getenv("HOPCORDER_SPOOL_FULL"))
	if len(spec) != 2 {
		t.Skip("helper process for TestFullFileSystem")
	}
	mode, dir := spec[0], spec[1]
	if mode == "noalloc" {
		allocate = func(string, int64, int64) error { return errors.ErrUnsupported }
	}
	const disk = 2 << 20
	setFileLimit(t, disk)
	clock := newClock()
	opts := Options{Dir: dir, MaxAge: 24 * time.Hour, MaxBytes: 16 << 20, Consumers: []string{"http"}, Now: clock.now}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	first := s.Stats().LastSeq
	appendNoise(t, s, 2000, 1300) // 2.6 MB: the outage
	st := checkNewestKept(t, s, "http", first)
	size := fileSize(t, dir)
	fmt.Printf("CHILD outage: %d appended, %d kept (%d bytes), %d dropped, file %d bytes\n",
		st.LastSeq-first, st.Records, st.Bytes, st.Dropped, size)
	if size > disk {
		t.Fatalf("file %d bytes past the limit", size)
	}
	if err := s.Ack("http", st.LastSeq); err != nil { // ingest is back: one large acknowledgement
		t.Fatalf("ack of everything: %v", err)
	}
	if got := s.Stats(); got.Records != 0 || got.Dropped != st.Dropped {
		t.Fatalf("after the acknowledgement: %+v", got)
	}
	appendNoise(t, s, 500, 1300)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = Open(opts); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	appendNoise(t, s, 500, 1300)
	checkNewestKept(t, s, "http", 0)
	if err := s.Ack("http", s.Stats().LastSeq); err != nil {
		t.Fatal(err)
	}

	// Room again: a minute later the spool grows past the old limit, and
	// drops nothing.
	setFileLimit(t, 32<<20)
	clock.add(2 * diskRetry)
	appendNoise(t, s, 20, 1300) // notices
	dropped := s.Stats().Dropped
	appendNoise(t, s, 1500, 1300)
	st = s.Stats()
	fmt.Printf("CHILD room again: %d kept (%d bytes), file %d bytes\n", st.Records, st.Bytes, fileSize(t, dir))
	if st.Dropped != dropped || fileSize(t, dir) <= disk {
		t.Fatalf("with room again: %+v (%d dropped before), file %d bytes", st, dropped, fileSize(t, dir))
	}
	fmt.Println("CHILD OK")
}

// setFileLimit sets RLIMIT_FSIZE: no file of this process grows past n
// bytes. Go ignores the SIGXFSZ that comes with it.
func setFileLimit(t *testing.T, n uint64) {
	t.Helper()
	var r syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &r); err != nil {
		t.Fatal(err)
	}
	r.Cur = min(n, r.Max)
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &r); err != nil {
		t.Fatal(err)
	}
}

func fileSize(t testing.TB, dir string) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Now()} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// Without fallocate the spool writes zeros ahead of its high water mark, so
// that the reserve is allocated on disk as with fallocate (review, round
// seven). Before, it allocated nothing ahead: bbolt grows the file with
// ftruncate, which allocates nothing, so the file was allocated only up to
// its high water mark, and on a file system that filled (the spool itself,
// with max_bytes larger than the disk) no transaction found the pages to
// drop records with. Every record reads back intact, also after a restart,
// which allocates from the high water mark again: no zeros over a page in
// use.
func TestNoFallocateAllocatesAhead(t *testing.T) {
	allocate = func(string, int64, int64) error { return errors.ErrUnsupported }
	t.Cleanup(func() { allocate = allocateFile })
	dir := t.TempDir()
	opts := Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 64 << 20, Consumers: []string{"c"}}
	want := map[uint64]int{} // seq: noise index
	for start := range 2 {
		s, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 2000 {
			k := start*2000 + i
			seq, err := s.Append(time.Now(), func(uint64) ([]byte, error) { return noise(k, 1300), nil })
			if err != nil {
				t.Fatalf("start %d, append %d: %v", start, i, err)
			}
			want[seq] = k
			if i%100 != 99 {
				continue
			}
			var hwm int64
			if err := s.db.View(func(tx *bolt.Tx) error { hwm = tx.Size(); return nil }); err != nil {
				t.Fatal(err)
			}
			var st syscall.Stat_t
			if err := syscall.Stat(filepath.Join(dir, fileName), &st); err != nil {
				t.Fatal(err)
			}
			s.roomMu.Lock()
			reserve := s.room.reserve
			s.roomMu.Unlock()
			if allocated := st.Blocks * 512; allocated < hwm+reserve/2 {
				t.Fatalf("start %d, append %d: %d bytes allocated, high water mark %d: want a reserve (%d) ahead", start, i, allocated, hwm, reserve)
			}
		}
		es, err := s.Read("c", len(want)+1, 1<<30)
		if err != nil || len(es) != len(want) {
			t.Fatalf("start %d: read %d of %d records: %v", start, len(es), len(want), err)
		}
		for _, e := range es {
			if k, ok := want[e.Seq]; !ok || !bytes.Equal(e.Data, noise(k, 1300)) {
				t.Fatalf("start %d: record %d damaged", start, e.Seq)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// A real full file system, a 4 MiB tmpfs (root only; CI's rawsock job runs
// it). Mode "alloc" has fallocate; "noalloc" has not, and the spool writes
// zeros ahead instead; "holes" stores those zeros as holes, as a
// copy-on-write file system with compression does (ZFS), so that only the
// free space the spool checks first keeps room for it. Two ways it fills:
//
//   - "others": while the spool grows, with no free pages in its file,
//     another file takes all the space left, as on a node's disk shared with
//     others; then the spool fills its file. (Not with "holes": a
//     copy-on-write file system that others fill is round six's accepted
//     risk.)
//   - "itself": max_bytes is larger than the file system, and nothing else
//     uses it (review, round seven). The spool fills it during an outage,
//     restarts while it is full, delivers its backlog in batches while
//     appending, and a minute later checks for room again.
//
// Every append succeeds, the oldest records dropped and counted, and the
// acknowledgements work. Before, without fallocate the spool filled the
// file system itself to its last page and from then on lost every append
// (and the drop it tried), after a restart too.
func TestFullTmpfs(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("mounts a tmpfs: root only")
	}
	for _, mode := range []string{"alloc", "noalloc", "holes"} {
		for _, by := range []string{"others", "itself"} {
			if mode == "holes" && by == "others" {
				continue
			}
			t.Run(mode+"/"+by, func(t *testing.T) {
				if mode != "alloc" {
					allocate = func(string, int64, int64) error { return errors.ErrUnsupported }
					t.Cleanup(func() { allocate = allocateFile })
				}
				if mode == "holes" {
					fill = func(path string, off, end, margin int64) error {
						err := fillFile(path, off, end, margin)
						return errors.Join(err, punchHole(path, off, end))
					}
					t.Cleanup(func() { fill = fillFile })
				}
				dir := t.TempDir()
				if err := syscall.Mount("tmpfs", dir, "tmpfs", 0, "size=4m"); err != nil {
					t.Skipf("mount: %v", err)
				}
				t.Cleanup(func() {
					if err := syscall.Unmount(dir, 0); err != nil {
						t.Error(err)
					}
				})
				if by == "others" {
					othersFill(t, dir)
				} else {
					spoolFills(t, dir)
				}
			})
		}
	}
}

// othersFill is TestFullTmpfs's "others" case on the file system at dir.
func othersFill(t *testing.T, dir string) {
	s, err := Open(Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 16 << 20, Consumers: []string{"http"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := s.Stats().LastSeq
	appendNoise(t, s, 600, 1300)
	f, err := os.Create(filepath.Join(dir, "other"))
	if err != nil {
		t.Fatal(err)
	}
	for _, err = f.Write(make([]byte, 64<<10)); err == nil; _, err = f.Write(make([]byte, 4<<10)) {
	}
	_ = f.Close()
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("filling the file system: %v", err)
	}
	size := fileSize(t, dir)
	appendNoise(t, s, 3000, 1300)
	st := checkNewestKept(t, s, "http", first)
	t.Logf("%d appended, %d kept (%d bytes), %d dropped, file %d bytes (%d when the disk filled)",
		st.LastSeq-first, st.Records, st.Bytes, st.Dropped, fileSize(t, dir), size)
	if err := s.Ack("http", st.LastSeq); err != nil {
		t.Fatal(err)
	}
	appendNoise(t, s, 300, 1300)
}

// spoolFills is TestFullTmpfs's "itself" case on the file system at dir:
// an outage of both outputs, 13 MB of records (three times the file
// system), with the spool checking for room every minute; a restart and as
// much again; then the backlog delivered, 500 records at a time, with
// appends in between.
func spoolFills(t *testing.T, dir string) {
	clock := newClock()
	opts := Options{Dir: dir, MaxAge: 24 * time.Hour, MaxBytes: 64 << 20, Consumers: []string{"jsonl", "http"}, Now: clock.now}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	outage := func() Stats {
		t.Helper()
		was := s.Stats()
		for range 10 {
			appendNoise(t, s, 1000, 1300)
			clock.add(diskRetry)
		}
		st := checkNewestKept(t, s, "http", 0)
		dropped := st.Dropped - was.Dropped
		if gone := st.LastSeq - was.LastSeq + uint64(was.Records) - uint64(st.Records); dropped != gone {
			t.Fatalf("%d records gone, %d counted as dropped", gone, dropped)
		}
		t.Logf("%d appended, %d kept (%d bytes), %d dropped, file %d bytes",
			st.LastSeq-was.LastSeq, st.Records, st.Bytes, dropped, fileSize(t, dir))
		return st
	}
	outage()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = Open(opts); err != nil {
		t.Fatalf("restart on the full file system: %v", err)
	}
	st := outage()
	for _, c := range opts.Consumers {
		es, err := s.Read(c, int(st.Records)+1, 1<<30)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < len(es); i += 500 {
			if err := s.Ack(c, es[min(i+500, len(es))-1].Seq); err != nil {
				t.Fatalf("delivering the backlog: %v", err)
			}
			appendNoise(t, s, 20, 1300)
		}
	}
	appendNoise(t, s, 1000, 1300)
	checkNewestKept(t, s, "http", 0)
}

// After a backlog is delivered, a commit writes the pages it changes and
// no more (review, round five). The spool wrote bbolt's freelist, the id of
// every free page, on every commit: after a delivered backlog that was most
// of the file, so every append and acknowledgement wrote 8 bytes for every
// 4 KiB of the file, for good: 109 KiB an append here, 1.4 MiB with the
// chart's 768 MiB. Now bbolt keeps the freelist in memory and finds it
// again at open.
func TestCommitsAfterBacklogWriteLittle(t *testing.T) {
	dir := t.TempDir()
	const maxBytes = 64 << 20
	first, next := buildBacklog(t, filepath.Join(dir, fileName), maxBytes*9/10)
	built := fileSize(t, dir)
	opts := Options{Dir: dir, MaxAge: 30 * 24 * time.Hour, MaxBytes: maxBytes, Consumers: []string{"http"}}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if st := s.Stats(); st.Records != int64(next-first) {
		t.Fatalf("backlog of %d records, %+v", next-first, st)
	}
	for c := first + 499; c < next+499; c += 500 { // delivered as the HTTP output does
		if err := s.Ack("http", min(c, next-1)); err != nil {
			t.Fatal(err)
		}
	}
	perAppend := func(when string) {
		t.Helper()
		w := writtenBytes(t)
		appendNoise(t, s, 100, 480)
		n := (writtenBytes(t) - w) / 100
		t.Logf("%s: %d bytes written an append, file %d bytes", when, n, fileSize(t, dir))
		if n > 32<<10 {
			t.Errorf("%s: %d bytes written an append, want at most 32 KiB", when, n)
		}
	}
	perAppend("backlog delivered")
	skip := [2]uint64{s.Stats().LastSeq}
	if err := s.Close(); err != nil { // the freelist found again
		t.Fatal(err)
	}
	if s, err = Open(opts); err != nil {
		t.Fatal(err)
	}
	skip[1] = s.Stats().LastSeq // went on from the clock
	perAppend("after a restart")
	appendNoise(t, s, 5000, 480) // into the free pages
	if size := fileSize(t, dir); size > built {
		t.Errorf("file grew from %d to %d bytes", built, size)
	}
	checkNewestKept(t, s, "http", 0, skip)
}

// buildBacklog writes, in a few large transactions, a spool file at path
// of about size bytes whose records (noise of 480 bytes, as stored records
// of real cycles are about) consumer "http" has none of: an outage. It
// returns the first seq and the next.
func buildBacklog(t *testing.T, path string, size int64) (first, next uint64) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, boltOptions())
	if err != nil {
		t.Fatal(err)
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	defer enc.Close()
	first = clockSeq(time.Now())
	next = first
	var stored int64
	ts := binary.BigEndian.AppendUint64(nil, uint64(time.Now().UnixNano())) //nolint:gosec // now
	for fileSize(t, filepath.Dir(path)) < size {
		if err := db.Update(func(tx *bolt.Tx) error {
			recs, err := tx.CreateBucketIfNotExists(bucketRecords)
			if err != nil {
				return err
			}
			recs.FillPercent = 0.95
			for range 10000 {
				val := enc.EncodeAll(noise(int(next%100000), 480), slices.Clone(ts)) //nolint:gosec // small
				if err := recs.Put(seqKey(next), val); err != nil {
					return err
				}
				stored += int64(len(val))
				next++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		curs, err := tx.CreateBucketIfNotExists(bucketCursors)
		if err != nil {
			return err
		}
		return errors.Join(putU64(meta, keyNextSeq, next), putU64(meta, keyBytes, uint64(stored)), //nolint:gosec // positive
			putU64(meta, keyCount, next-first), putU64(curs, []byte("http"), first-1))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return first, next
}

// writtenBytes is what this process has written so far (wchar: write
// calls, not what reached the disk).
func writtenBytes(t *testing.T) int64 {
	t.Helper()
	b, err := os.ReadFile("/proc/self/io")
	if err != nil {
		t.Fatal(err)
	}
	for l := range strings.Lines(string(b)) {
		if v, ok := strings.CutPrefix(strings.TrimSpace(l), "wchar: "); ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatal("no wchar in /proc/self/io")
	return 0
}

// punchHole deallocates the blocks of the file at path from off to end.
func punchHole(path string, off, end int64) error {
	const mode = 0x01 | 0x02 // FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return syscall.Fallocate(int(f.Fd()), mode, off, end-off) //nolint:gosec // a file descriptor
}
