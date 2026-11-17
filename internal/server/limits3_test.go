package server

// Regression tests for the third security review of ingest: each failed
// before its fix.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// nullStore stores nothing and finds nothing, after an insert delay like a
// real ClickHouse's.
type nullStore struct{ delay time.Duration }

func (n nullStore) InsertJSON(context.Context, string, [][]byte) error {
	time.Sleep(n.delay)
	return nil
}

func (nullStore) Query(context.Context, string, clickhouse.Params, func(json.RawMessage) error) error {
	return nil
}

// lockedAgent keeps agent "x" locked, as the review's attack did: two
// requests always in flight for it, each with 4096 streams it has not sent
// before (from a set of 40,000, so that a test that fails holds bounded
// memory). It stops when stop is closed.
func lockedAgent(t *testing.T, in *Ingester, stop <-chan struct{}) *sync.WaitGroup {
	t.Helper()
	var seq atomic.Uint64
	var next atomic.Int64
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				recs := make([]*record.Record, 0, maxStreamsPerRequest)
				for range maxStreamsPerRequest {
					r := mk("x", seq.Add(1), pathA...)
					r.Target = fmt.Sprintf("t%d.example", next.Add(1)%40000)
					recs = append(recs, r)
				}
				if _, err := in.Ingest(context.Background(), recs); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	return &wg
}

// streamsHeld counts the stream states the ingester holds, agent by agent.
func streamsHeld(in *Ingester) map[string]int {
	in.mu.Lock()
	agents := make(map[string]*agentState, len(in.agents))
	for id, a := range in.agents {
		agents[id] = a
	}
	in.mu.Unlock()
	out := map[string]int{}
	for id, a := range agents {
		a.mu.Lock()
		out[id] = len(a.streams)
		a.mu.Unlock()
	}
	return out
}

// An agent kept locked by two requests in flight escaped max_streams:
// sweeps skip locked agents, so its streams were never dropped, and nothing
// else stopped them (256k streams and 503 MiB after 41 s in the review).
// Now no stream gets state beyond max_streams, and no agent beyond its
// share.
func TestLockedAgentStreamsBounded(t *testing.T) {
	in := NewIngester(nullStore{delay: time.Millisecond}, nil, quietLogger(), nil)
	in.maxStreams = 2000
	stop := make(chan struct{})
	wg := lockedAgent(t, in, stop)
	var peak int64
	for end := time.Now().Add(1500 * time.Millisecond); time.Now().Before(end) && peak <= 4*2000; {
		peak = max(peak, in.nstreams.Load())
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	held := streamsHeld(in)
	if peak > 2000 || held["x"] > in.streamsPerAgent() || int64(held["x"]) != in.nstreams.Load() {
		t.Errorf("streams counted at most %d (max_streams 2000), held by the agent %d (its share %d), counted now %d",
			peak, held["x"], in.streamsPerAgent(), in.nstreams.Load())
	}
}

// While an agent was kept locked over max_streams, every sweep (every
// second) dropped every other agent's streams: an agent with 300 streams
// restored 256 per batch and derived no events for the rest, batch after
// batch. Now the locked agent cannot crowd the others out.
func TestLockedAgentDoesNotEvictOthers(t *testing.T) {
	lost := &lostCycles{n: map[string]int{}}
	in := NewIngester(nullStore{delay: time.Millisecond}, nil, slog.New(lost), nil)
	in.maxStreams = 2000
	start := time.Now()
	in.now = func() time.Time { return t0.Add(10 * time.Since(start)) } // sweeps every 100 ms
	stop := make(chan struct{})
	wg := lockedAgent(t, in, stop)
	time.Sleep(200 * time.Millisecond)
	var unrestored []int
	for b := range 6 {
		var recs []*record.Record
		for i := range 300 {
			r := mk("legit", uint64(b*1000+i+1), pathA...)
			r.Target = fmt.Sprintf("target-%d.example", i)
			recs = append(recs, r)
		}
		before := lost.of("legit")
		if _, err := in.Ingest(t.Context(), recs); err != nil {
			t.Fatal(err)
		}
		unrestored = append(unrestored, lost.of("legit")-before)
		time.Sleep(150 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	// The first batch restores 256 of its 300 new streams (the per-request
	// budget); all later ones derive every event.
	if unrestored[0] != 300-maxRestores || fmt.Sprint(unrestored[1:]) != "[0 0 0 0 0]" {
		t.Errorf("legitimate agent's cycles without events, batch by batch: %v", unrestored)
	}
}

// lostCycles is a log handler that adds up, per agent_id, the cycles that
// derived no events (the "cycles" of the ingester's warnings about them).
type lostCycles struct {
	mu sync.Mutex
	n  map[string]int
}

func (*lostCycles) Enabled(context.Context, slog.Level) bool { return true }
func (l *lostCycles) WithAttrs([]slog.Attr) slog.Handler     { return l }
func (l *lostCycles) WithGroup(string) slog.Handler          { return l }

func (l *lostCycles) Handle(_ context.Context, r slog.Record) error {
	var agent string
	var n int64
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "agent_id":
			agent = a.Value.String()
		case "cycles":
			n = a.Value.Int64()
		}
		return true
	})
	l.mu.Lock()
	l.n[agent] += int(n)
	l.mu.Unlock()
	return nil
}

func (l *lostCycles) of(agent string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.n[agent]
}

// When the streams reach max_streams, the agents with the most give theirs
// up first, and an agent that is locked while a sweep runs drops its share
// itself before it unlocks; agents with fewer keep theirs. Before, the
// excess was taken from every agent that could be swept, by age alone.
func TestHeavyAgentPaysAtMaxStreams(t *testing.T) {
	reg := prometheus.NewRegistry()
	lost := &lostCycles{n: map[string]int{}}
	in := NewIngester(nullStore{delay: time.Millisecond}, nil, slog.New(lost), newServerMetrics(reg))
	in.maxStreams = 2000
	start := time.Now()
	in.now = func() time.Time { return t0.Add(10 * time.Since(start)) }
	batch := func(agent int, b int) []*record.Record {
		var recs []*record.Record
		for i := range 240 {
			r := mk(fmt.Sprintf("legit%d", agent), uint64(b*1000+i+1), pathA...)
			r.Target = fmt.Sprintf("target-%d.example", i)
			recs = append(recs, r)
		}
		return recs
	}
	for a := range 5 { // 1200 streams
		if _, err := in.Ingest(t.Context(), batch(a, 0)); err != nil {
			t.Fatal(err)
		}
	}
	stop := make(chan struct{})
	wg := lockedAgent(t, in, stop) // up to its 1000, where 800 fit
	time.Sleep(200 * time.Millisecond)
	for b := 1; b <= 5; b++ {
		for a := range 5 {
			if _, err := in.Ingest(t.Context(), batch(a, b)); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	held := streamsHeld(in)
	for a := range 5 {
		id := fmt.Sprintf("legit%d", a)
		if n := lost.of(id); n != 0 || held[id] != 240 {
			t.Errorf("%s: %d cycles derived no events, %d streams held (want 240)", id, n, held[id])
		}
	}
	var evicted float64
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "hopcorder_server_state_evictions_total" {
			for _, m := range mf.GetMetric() {
				if m.GetLabel()[0].GetValue() == "stream" {
					evicted = m.GetCounter().GetValue()
				}
			}
		}
	}
	if evicted == 0 || held["x"] > 800 {
		t.Errorf("the locked agent dropped %v streams and holds %d", evicted, held["x"])
	}
}

func TestWaterLevel(t *testing.T) {
	for _, tc := range []struct {
		counts []int
		excess int64
		want   int
	}{
		{[]int{1700, 300}, 200, 1500},
		{[]int{300, 800, 300}, 200, 600},
		{[]int{300, 300, 300}, 100, 266}, // removes 102
		{[]int{10, 5}, 15, 0},
		{[]int{10, 5}, 20, 0},
		{[]int{7}, 1, 6},
	} {
		got := waterLevel(tc.counts, tc.excess)
		var removed int64
		for _, c := range tc.counts {
			removed += int64(max(c-got, 0))
		}
		if got != tc.want || (removed < tc.excess && got > 0) {
			t.Errorf("waterLevel(%v, %d) = %d (removes %d), want %d", tc.counts, tc.excess, got, removed, tc.want)
		}
	}
}

// An agent with more streams than max_streams_per_agent: the others' cycles
// are stored, derive no events, and are counted like unrestored ones.
func TestStreamsPerAgentBound(t *testing.T) {
	st := newFakeStore()
	reg := prometheus.NewRegistry()
	in := NewIngester(st, nil, quietLogger(), newServerMetrics(reg))
	in.maxStreamsPerAgent = 3
	var recs []*record.Record
	for i := range 5 {
		r := mk("lab", uint64(i+1), brokenA...) // each opens an outage
		r.Target = fmt.Sprintf("t%d.example", i)
		recs = append(recs, r)
	}
	res, err := in.Ingest(t.Context(), recs)
	if err != nil || res.Accepted != 5 {
		t.Fatalf("%+v %v", res, err)
	}
	if n := len(st.table("cycles")); n != 5 {
		t.Errorf("%d cycles stored, want 5", n)
	}
	if n := len(st.table("outages")); n != 3 {
		t.Errorf("%d outages derived, want 3 (the streams with state)", n)
	}
	if got := counter(t, reg, "hopcorder_server_unrestored_cycles_total"); got != 2 {
		t.Errorf("%v cycles counted without events, want 2", got)
	}
}
