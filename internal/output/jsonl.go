package output

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// JSONL appends records, one per line, to a file or to stdout.
//
// A failed write must not leave half a line for the retry to append to:
// for files, a short write is truncated away; where that is impossible
// (stdout, a pipe, or a torn tail found at startup after a power loss) the
// next batch starts on a fresh line so every later record stays parseable.
type JSONL struct {
	path    string
	mu      sync.Mutex
	w       io.Writer
	f       *os.File // the file opened; nil for stdout and NewJSONLWriter
	out     *os.File // the file written to (f or stdout), if it takes write deadlines
	sync    bool     // f is a regular file: sync each batch to disk
	buf     []byte
	newline bool // the output may end mid-line; start the next batch with '\n'
}

// NewJSONL opens path for appending ("-" means stdout).
func NewJSONL(path string) (*JSONL, error) {
	j := &JSONL{path: path}
	if path == "-" {
		j.w = os.Stdout
		j.out = deadlines(os.Stdout)
		return j, nil
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o640)
	if err != nil {
		return nil, fmt.Errorf("jsonl output: %w", err)
	}
	j.f, j.w, j.out = f, f, deadlines(f)
	// A FIFO, a pipe or a terminal cannot be synced (fsync fails with
	// EINVAL, which failed every delivery, to be repeated for ever).
	j.sync = true
	if st, err := f.Stat(); err == nil {
		j.sync = st.Mode().IsRegular()
	}
	j.newline = !endsWithNewline(f)
	return j, nil
}

// deadlines returns f if it takes write deadlines (a pipe or a FIFO opened
// here, which Go makes non-blocking, or a non-blocking stdout), else nil:
// regular files never do, and neither does a blocking stdout.
func deadlines(f *os.File) *os.File {
	if f.SetWriteDeadline(time.Time{}) != nil {
		return nil
	}
	return f
}

// endsWithNewline reports whether f is empty or ends in '\n'.
func endsWithNewline(f *os.File) bool {
	st, err := f.Stat()
	if err != nil || st.Size() == 0 {
		return true
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], st.Size()-1); err != nil {
		return true
	}
	return b[0] == '\n'
}

// NewJSONLWriter writes to w (for tests and --once).
func NewJSONLWriter(w io.Writer) *JSONL { return &JSONL{path: "writer", w: w} }

// Name implements Output.
func (j *JSONL) Name() string { return "jsonl" }

// Deliver writes the batch with a single write call and, for regular files,
// syncs it to disk before reporting success, so the spool never advances
// past lines that a power loss could still take back.
//
// Where the output takes write deadlines (a pipe or FIFO), the write gives
// up once ctx ends: one that nobody reads would otherwise block the drain,
// and with it the agent's reload or shutdown, for good. Elsewhere such a
// write is left behind by Drains.Stop.
func (j *JSONL) Deliver(ctx context.Context, lines [][]byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.buf = j.buf[:0]
	if j.newline {
		j.buf = append(j.buf, '\n')
	}
	for _, l := range lines {
		j.buf = append(j.buf, l...)
		j.buf = append(j.buf, '\n')
	}
	var size int64 = -1
	if j.f != nil {
		if st, err := j.f.Stat(); err == nil {
			size = st.Size()
		}
	}
	if j.out != nil {
		// Clear what an earlier, ended context may have left set.
		_ = j.out.SetWriteDeadline(time.Time{})
		stop := context.AfterFunc(ctx, func() { _ = j.out.SetWriteDeadline(time.Unix(1, 0)) })
		defer stop()
	}
	if n, err := j.w.Write(j.buf); err != nil {
		if n > 0 {
			// Remove the partial batch; failing that, terminate it.
			if size < 0 || j.f.Truncate(size) != nil {
				j.newline = true
			}
		}
		return fmt.Errorf("write %s: %w", j.path, err)
	}
	j.newline = false
	if j.sync {
		if err := j.f.Sync(); err != nil {
			return fmt.Errorf("sync %s: %w", j.path, err)
		}
	}
	return nil
}

// Close closes the file (stdout is left open).
func (j *JSONL) Close() error {
	if j.f == nil {
		return nil
	}
	return j.f.Close()
}
