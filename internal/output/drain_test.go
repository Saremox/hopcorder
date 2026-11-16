package output

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/spool"
)

type memOutput struct {
	name    string
	mu      sync.Mutex
	batches [][]string
	fail    atomic.Bool
	calls   atomic.Int64
}

func (m *memOutput) Name() string { return m.name }
func (m *memOutput) Close() error { return nil }
func (m *memOutput) Deliver(_ context.Context, lines [][]byte) error {
	m.calls.Add(1)
	if m.fail.Load() {
		return errors.New("down")
	}
	b := make([]string, len(lines))
	for i, l := range lines {
		b[i] = string(l)
	}
	m.mu.Lock()
	m.batches = append(m.batches, b)
	m.mu.Unlock()
	return nil
}

func (m *memOutput) all() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, b := range m.batches {
		out = append(out, b...)
	}
	return out
}

func openSpool(t *testing.T, consumers ...string) *spool.Spool {
	t.Helper()
	s, err := spool.Open(spool.Options{Dir: t.TempDir(), MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: consumers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func push(t *testing.T, s *spool.Spool, n int) {
	t.Helper()
	for range n {
		if _, err := s.Append(time.Now(), func(seq uint64) ([]byte, error) {
			return fmt.Appendf(nil, `{"seq":%d}`, seq), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func eventually(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func startDrain(t *testing.T, s *spool.Spool, out Output, o DrainOptions) {
	t.Helper()
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { Drain(ctx, s, out, o); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func TestDrainImmediate(t *testing.T) {
	s := openSpool(t, "m")
	out := &memOutput{name: "m"}
	startDrain(t, s, out, DrainOptions{})
	push(t, s, 3)
	eventually(t, time.Second, func() bool { return len(out.all()) == 3 }, "records not delivered")
	push(t, s, 2)
	eventually(t, time.Second, func() bool { return len(out.all()) == 5 }, "later records not delivered")
	eventually(t, time.Second, func() bool { return s.Stats().Records == 0 }, "delivered records not collected")
}

func TestDrainBatchesUntilFlushInterval(t *testing.T) {
	s := openSpool(t, "m")
	out := &memOutput{name: "m"}
	startDrain(t, s, out, DrainOptions{MaxRecords: 10, FlushInterval: 300 * time.Millisecond})
	start := time.Now()
	push(t, s, 3)
	time.Sleep(100 * time.Millisecond)
	if n := out.calls.Load(); n != 0 {
		t.Fatalf("partial batch delivered early (%d calls)", n)
	}
	eventually(t, 2*time.Second, func() bool { return len(out.all()) == 3 }, "partial batch never flushed")
	if d := time.Since(start); d < 250*time.Millisecond {
		t.Errorf("flushed after %v, before the flush interval", d)
	}
	if len(out.batches) != 1 {
		t.Errorf("%d batches, want 1", len(out.batches))
	}
}

func TestDrainFullBatchGoesImmediately(t *testing.T) {
	s := openSpool(t, "m")
	out := &memOutput{name: "m"}
	startDrain(t, s, out, DrainOptions{MaxRecords: 5, FlushInterval: time.Hour})
	push(t, s, 12)
	eventually(t, 2*time.Second, func() bool { return len(out.all()) == 10 }, "full batches not sent")
	time.Sleep(50 * time.Millisecond)
	if n := len(out.all()); n != 10 {
		t.Errorf("%d delivered; the partial third batch must wait", n)
	}
	for _, b := range out.batches {
		if len(b) != 5 {
			t.Errorf("batch of %d", len(b))
		}
	}
}

func TestDrainRetriesWithBackoffAndKeepsOrder(t *testing.T) {
	s := openSpool(t, "m")
	out := &memOutput{name: "m"}
	out.fail.Store(true)
	var errs atomic.Int64
	startDrain(t, s, out, DrainOptions{MaxRecords: 4, RetryMin: 10 * time.Millisecond, RetryMax: 40 * time.Millisecond,
		OnError: func(error) { errs.Add(1) }})
	push(t, s, 10)
	time.Sleep(300 * time.Millisecond)
	calls := out.calls.Load()
	// Backoff 10, 20, 40, 40... (jittered to half..full): far fewer than a
	// tight loop, but more than one attempt.
	if calls < 3 || calls > 40 {
		t.Errorf("%d attempts in 300ms", calls)
	}
	if errs.Load() != calls {
		t.Errorf("OnError %d times for %d failures", errs.Load(), calls)
	}
	if s.Stats().Records != 10 {
		t.Errorf("records lost while output was down: %+v", s.Stats())
	}
	out.fail.Store(false)
	eventually(t, 2*time.Second, func() bool { return len(out.all()) == 10 }, "not delivered after recovery")
	got := strings.Join(out.all(), ",")
	want := `{"seq":1},{"seq":2},{"seq":3},{"seq":4},{"seq":5},{"seq":6},{"seq":7},{"seq":8},{"seq":9},{"seq":10}`
	if got != want {
		t.Errorf("delivered %s", got)
	}
}

func TestDrainResumesFromCursorAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s, _ := spool.Open(spool.Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"m"}})
	push(t, s, 3)
	_ = s.Ack("m", 2)
	_ = s.Close()

	s, _ = spool.Open(spool.Options{Dir: dir, MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"m"}})
	defer s.Close()
	out := &memOutput{name: "m"}
	startDrain(t, s, out, DrainOptions{})
	eventually(t, time.Second, func() bool { return len(out.all()) == 1 }, "backlog not delivered")
	if got := out.all()[0]; got != `{"seq":3}` {
		t.Errorf("delivered %s", got)
	}
}

func TestDrainFlushNowShipsPartialBatch(t *testing.T) {
	s := openSpool(t, "m")
	out := &memOutput{name: "m"}
	flush := make(chan struct{})
	startDrain(t, s, out, DrainOptions{MaxRecords: 500, FlushInterval: time.Hour, FlushNow: flush})
	push(t, s, 3)
	time.Sleep(50 * time.Millisecond)
	if n := out.calls.Load(); n != 0 {
		t.Fatalf("delivered before flush (%d calls)", n)
	}
	close(flush)
	eventually(t, time.Second, func() bool { return len(out.all()) == 3 }, "FlushNow did not ship the partial batch")
	push(t, s, 1) // after FlushNow, nothing waits any more
	eventually(t, time.Second, func() bool { return len(out.all()) == 4 }, "record after FlushNow waited")
}

// A wall-clock step backwards leaves records stamped in the future; the
// flush wait must still be bounded by FlushInterval.
func TestDrainFutureTimestampsDoNotStall(t *testing.T) {
	s := openSpool(t, "m")
	out := &memOutput{name: "m"}
	startDrain(t, s, out, DrainOptions{MaxRecords: 500, FlushInterval: 100 * time.Millisecond})
	for range 3 {
		if _, err := s.Append(time.Now().Add(time.Hour), func(seq uint64) ([]byte, error) {
			return fmt.Appendf(nil, `{"seq":%d}`, seq), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 2*time.Second, func() bool { return len(out.all()) == 3 }, "future-stamped records stalled")
}
