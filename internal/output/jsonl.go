package output

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

// JSONL appends records, one per line, to a file or to stdout.
type JSONL struct {
	path string
	mu   sync.Mutex
	w    io.Writer
	f    *os.File // nil for stdout
	buf  []byte
}

// NewJSONL opens path for appending ("-" means stdout).
func NewJSONL(path string) (*JSONL, error) {
	j := &JSONL{path: path}
	if path == "-" {
		j.w = os.Stdout
		return j, nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o640)
	if err != nil {
		return nil, fmt.Errorf("jsonl output: %w", err)
	}
	j.f, j.w = f, f
	return j, nil
}

// NewJSONLWriter writes to w (for tests and --once).
func NewJSONLWriter(w io.Writer) *JSONL { return &JSONL{path: "writer", w: w} }

// Name implements Output.
func (j *JSONL) Name() string { return "jsonl" }

// Deliver writes the batch with a single write call and, for files, syncs
// it to disk before reporting success, so the spool never advances past
// lines that a power loss could still take back.
func (j *JSONL) Deliver(_ context.Context, lines [][]byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.buf = j.buf[:0]
	for _, l := range lines {
		j.buf = append(j.buf, l...)
		j.buf = append(j.buf, '\n')
	}
	if _, err := j.w.Write(j.buf); err != nil {
		return fmt.Errorf("write %s: %w", j.path, err)
	}
	if j.f != nil {
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
