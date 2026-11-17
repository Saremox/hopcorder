package server

// Regression tests for the third security review of ingest: each failed
// before its fix.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"runtime"
	"slices"
	"strconv"
	"strings"
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

func (nullStore) Exec(context.Context, string, clickhouse.Params) error { return nil }

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

// What hops_mv makes of an insert counts towards its size: each hop row,
// and the loop TTLs it writes on it as text. With 255 hops at one address
// and a loop over all of them, an 8 MiB insert wrote 155 MiB of loop_ttls
// in ClickHouse (and the view copied the cycle's loops onto every hop row
// to find them). The estimate here is read from the rows ClickHouse gets.
func TestInsertPartsCountHopRows(t *testing.T) {
	st := &rowStore{}
	in := NewIngester(st, nil, quietLogger(), nil)
	ttls := make([]int, record.MaxHops)
	spec := make([]string, record.MaxHops)
	for i := range ttls {
		ttls[i], spec[i] = i+1, "198.18.120.1"
	}
	var recs []*record.Record
	for seq := uint64(1); seq <= 40; seq++ {
		r := mk("lab", seq, spec...)
		r.Loops = []record.Loop{{IP: netip.MustParseAddr("198.18.120.1"), TTLs: ttls}}
		recs = append(recs, r)
	}
	if _, err := in.Ingest(t.Context(), recs); err != nil {
		t.Fatal(err)
	}
	for i, rows := range st.inserts {
		size, largest := 0, 0
		for _, b := range rows {
			n := viewCost(t, b)
			size, largest = size+n, max(largest, n)
		}
		if size > maxInsertBytes+largest {
			t.Errorf("insert %d: %d rows, %d bytes counting the hop rows (budget %d)", i, len(rows), size, maxInsertBytes)
		}
	}
	if len(st.inserts) < 2 {
		t.Errorf("%d inserts", len(st.inserts))
	}
}

// rowStore keeps the rows of every cycles insert.
type rowStore struct {
	nullStore
	mu      sync.Mutex
	inserts [][][]byte
}

func (r *rowStore) InsertJSON(_ context.Context, table string, rows [][]byte) error {
	if table == "cycles" {
		r.mu.Lock()
		r.inserts = append(r.inserts, rows)
		r.mu.Unlock()
	}
	return nil
}

// viewCost is a cycles row's JSON plus what hops_mv writes for it: per hop
// hopRowBytes and the loop_ttls of the first loop with the hop's address,
// as text (the view of migration 0002 looked it up like this).
func viewCost(t *testing.T, b []byte) int {
	t.Helper()
	var row struct {
		HopIP    []string `json:"hops.ip"`
		LoopIP   []string `json:"loops.ip"`
		LoopTTLs [][]int  `json:"loops.ttls"`
	}
	if err := json.Unmarshal(b, &row); err != nil {
		t.Fatal(err)
	}
	n := len(b)
	for _, ip := range row.HopIP {
		n += hopRowBytes
		for j, lip := range row.LoopIP {
			if lip == ip {
				var s []string
				for _, ttl := range row.LoopTTLs[j] {
					s = append(s, strconv.Itoa(ttl))
				}
				n += len(strings.Join(s, ","))
				break
			}
		}
	}
	return n
}

// The cycles row carries each hop's loop TTLs for hops_mv: those of the
// first loop with the hop's address, none for the others.
func TestCycleRowHopLoopTTLs(t *testing.T) {
	r := mk("lab", 1, "10.0.0.1", "198.18.120.1", "198.18.120.2", "198.18.120.1", "*", "198.18.120.2")
	r.Loops = append(r.Loops, record.Loop{IP: netip.MustParseAddr("198.18.120.1"), TTLs: []int{1, 2}})
	b, cost, err := buildCycleRow(r, noEnrich{})
	if err != nil {
		t.Fatal(err)
	}
	var row map[string]json.RawMessage
	if err := json.Unmarshal(b, &row); err != nil {
		t.Fatal(err)
	}
	if got := string(row["hops.loop_ttls"]); got != "[[],[2,4],[3,6],[2,4],[],[3,6]]" {
		t.Errorf("hops.loop_ttls = %s", got)
	}
	if want := len(b) + 6*hopRowBytes + 4*len("2,4"); cost != want {
		t.Errorf("cost %d, want %d", cost, want)
	}
}

// postRaw posts body as NDJSON from any goroutine.
func postRaw(url string, body []byte) (int, error) {
	req, err := http.NewRequest(http.MethodPost, url+"/api/v1/ingest", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// Readers took read-buffer chunks one at a time (plus one to see the end of
// the body) and held them until their request ended, so readers could hold
// every chunk between them, each waiting for another: two bodies of four
// chunks each, sent in halves, with six chunks in the pool. Both stalled
// until the read timeout. Now a body reserves its four at once, so the
// second waits until the first is done.
func TestReadBufferNoDeadlock(t *testing.T) {
	s := New(Options{Store: newFakeStore(), Logger: quietLogger(), MaxBodyBytes: 1 << 20})
	s.ingester.now = func() time.Time { return t0.Add(time.Hour) }
	s.bodyTimeout = 3 * time.Second
	s.readBuf = newChunkPool(6)
	s.SetReady()
	srv := httptestServer(t, s.Handler())
	half := bytes.Repeat([]byte("\n"), 2*readChunk) // valid NDJSON: empty lines
	var conns []net.Conn
	for range 2 {
		c, err := net.Dial("tcp", strings.TrimPrefix(srv, "http://"))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		fmt.Fprintf(c, "POST /api/v1/ingest HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-ndjson\r\n"+
			"Content-Length: %d\r\n\r\n", 2*len(half))
		_, _ = c.Write(half)
		conns = append(conns, c)
		time.Sleep(100 * time.Millisecond)
	}
	start := time.Now()
	for _, c := range conns {
		_, _ = c.Write(half)
	}
	for i, c := range conns {
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		buf := make([]byte, 64)
		n, _ := c.Read(buf)
		if !strings.HasPrefix(string(buf[:n]), "HTTP/1.1 200") {
			t.Errorf("body %d: %q after %v", i, buf[:n], time.Since(start))
		}
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("both bodies took %v", d)
	}
	if n := s.readBuf.inUse(); n != 0 {
		t.Errorf("%d chunks still held", n)
	}
}

// The review's version: ten bodies of max_body_bytes at full speed (with
// the default limits, 128 chunks each of a pool of 512) and an agent's
// batch. Most runs stalled every one of them until the read timeout.
func TestReadBufferManyMaxBodies(t *testing.T) {
	s := New(Options{Store: newFakeStore(), Logger: quietLogger()})
	s.ingester.now = func() time.Time { return t0.Add(time.Hour) }
	s.bodyTimeout = 4 * time.Second
	s.SetReady()
	srv := httptestServer(t, s.Handler())
	// Valid NDJSON: blank lines (long ones, so they decode quickly).
	body := bytes.Repeat(append(bytes.Repeat([]byte(" "), 4095), '\n'), DefaultMaxBodyBytes/4096)
	var wg sync.WaitGroup
	codes := make([]int, 10)
	start := time.Now()
	for i := range codes {
		wg.Go(func() {
			codes[i], _ = postRaw(srv, body)
		})
	}
	time.Sleep(100 * time.Millisecond)
	code, res := post(t, srv, "", ndjson(mk("lab", 1, pathA...)))
	wg.Wait()
	if d := time.Since(start); code != 200 || res.Accepted != 1 || fmt.Sprint(codes) != "[200 200 200 200 200 200 200 200 200 200]" ||
		d > 3*time.Second {
		t.Errorf("agent %d %+v, max-size bodies %v, after %v", code, res, codes, d)
	}
	if n := s.readBuf.inUse(); n != 0 {
		t.Errorf("%d chunks still held", n)
	}
}

// Reservations are granted in order: a large one is not passed over by
// smaller ones behind it, and one given up lets those behind it through.
func TestChunkPoolOrder(t *testing.T) {
	p := newChunkPool(4)
	if err := p.acquire(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 3)
	queued := 0
	wait := func(name string, ctx context.Context, n int) {
		go func() {
			if err := p.acquire(ctx, n); err != nil {
				got <- name + " gave up"
				return
			}
			got <- name
		}()
		// Queued in this order: wait until this one is in line.
		queued++
		for deadline := time.Now().Add(5 * time.Second); p.waiting() < queued; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%s never queued", name)
			}
		}
	}
	cancelled, cancel := context.WithCancel(t.Context())
	wait("big", cancelled, 4) // first in line, does not fit
	wait("small", t.Context(), 1)
	select {
	case g := <-got:
		t.Fatalf("%s got through ahead of its turn", g)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if g := []string{<-got, <-got}; !slices.Contains(g, "big gave up") || !slices.Contains(g, "small") {
		t.Fatalf("got %q", g)
	}
	p.release(3)
	if err := p.acquire(t.Context(), 3); err != nil || p.inUse() != 4 {
		t.Errorf("%v, %d in use", err, p.inUse())
	}
	if err := p.acquire(t.Context(), 5); err == nil {
		t.Error("a reservation larger than the pool was granted")
	}
}

// waiting returns how many reservations are queued.
func (p *chunkPool) waiting() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waiters.Len()
}

// The records of a request take about three times its decoded size, not
// twice as documented (88 bytes of memory per hop for as little as 30 of
// JSON), and slices grown by doubling made it 3.6 times. The documented
// factor is three.
func TestDecodedRecordsMemory(t *testing.T) {
	s := New(Options{Store: newFakeStore(), Logger: quietLogger()})
	s.ingester.now = func() time.Time { return t0.Add(time.Hour) }
	for _, hops := range []int{9, 255} {
		r := &record.Record{Schema: record.SchemaVersion, AgentID: "lab", TS: t0, Target: "dc1.example",
			TargetIP: netip.MustParseAddr("192.0.2.9"), AF: 4, Protocol: "icmp", PathLen: hops, PathHash: "x",
			Loops: []record.Loop{}}
		for i := range hops {
			r.Hops = append(r.Hops, record.Hop{TTL: i + 1, Status: record.StatusNoReply})
		}
		r.PathHash = record.PathHash(r.Hops)
		var b bytes.Buffer
		for seq := uint64(1); b.Len() < 4<<20; seq++ {
			r.Seq = seq
			line, _ := json.Marshal(r)
			b.Write(line)
			b.WriteByte('\n')
		}
		raw := b.Bytes()
		var m0, m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m0)
		recs, res, code, _ := s.decode("", bytes.NewReader(raw))
		runtime.GC()
		runtime.ReadMemStats(&m1)
		live := float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc)) / float64(len(raw))
		if code != 0 || res.Rejected != 0 || live > 3.2 {
			t.Errorf("%d silent hops a record: code %d, %d rejected, records hold %.2f times their decoded size",
				hops, code, res.Rejected, live)
		}
		runtime.KeepAlive(recs)
		runtime.KeepAlive(raw)
	}
}
