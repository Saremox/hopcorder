//go:build linux

package output

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// A FIFO cannot be synced (fsync fails with EINVAL): every delivery to one
// failed after its write and was written again on each retry, for ever.
func TestJSONLToAFIFO(t *testing.T) {
	p := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(p, 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := os.OpenFile(p, os.O_RDONLY|syscall.O_NONBLOCK, 0) // the reader, before any writer
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	j, err := NewJSONL(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range []string{`{"a":1}`, `{"b":2}`} {
		if err := j.Deliver(t.Context(), [][]byte{[]byte(l)}); err != nil {
			t.Fatalf("delivery to a FIFO: %v", err)
		}
	}
	_ = j.Close() // the last writer: the reader gets what is in the pipe, then EOF
	var got []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		got = append(got, sc.Text())
	}
	if len(got) != 2 || got[0] != `{"a":1}` || got[1] != `{"b":2}` {
		t.Errorf("the reader got %q", got)
	}
}

// A write to a FIFO nobody reads gives up when its context ends, so the
// drain, and with it a reload or the shutdown, is not held up by it.
func TestJSONLWriteHeedsItsContext(t *testing.T) {
	p := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(p, 0o600); err != nil {
		t.Fatal(err)
	}
	fd, err := syscall.Open(p, syscall.O_RDWR|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	j, err := NewJSONL(p)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- j.Deliver(ctx, [][]byte{make([]byte, 1<<20)}) }() // 1 MiB: more than the pipe holds
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("err = %v, want a deadline error", err)
		}
		t.Logf("gave up after %v", time.Since(start).Round(time.Millisecond))
	case <-time.After(5 * time.Second):
		t.Fatal("the write ignored its context")
	}
	// The next batch starts on a fresh line after the partial one.
	if !j.newline {
		t.Error("a partial write to a FIFO did not mark the line as torn")
	}
}
