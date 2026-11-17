//go:build linux

package spool

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// A full file system (review, round six). The file system here is
// RLIMIT_FSIZE, set in a child process: past it, writes, fallocate and
// truncate fail with EFBIG, as they fail with ENOSPC on a full disk; no
// root needed (TestFullTmpfs has a real one). The spool's own bound, 16
// MiB, is far above the 2 MiB the file may have. During an outage (nothing
// acknowledged) every append succeeds by dropping the oldest records,
// counted; delivering everything in one acknowledgement works; so do
// appends after it and after a restart; and once there is room again the
// spool grows. Mode "noalloc" runs it without space allocated ahead, as on
// a file system without fallocate. Before, every append past 2 MiB failed
// (the newest records lost, none counted as dropped), and so did the large
// acknowledgement.
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

// A real full file system, a 4 MiB tmpfs (root only; CI's rawsock job runs
// it). While the spool grows, with no free pages in its file, another file
// takes all the space left, as on a node's disk shared with others; then
// the spool fills its file. Every append succeeds, the oldest records
// dropped and counted, and the acknowledgements work.
func TestFullTmpfs(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("mounts a tmpfs: root only")
	}
	for _, mode := range []string{"alloc", "noalloc"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "noalloc" {
				allocate = func(string, int64, int64) error { return errors.ErrUnsupported }
				t.Cleanup(func() { allocate = allocateFile })
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
		})
	}
}
