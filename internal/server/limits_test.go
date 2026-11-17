package server

// Regression tests for the second security review of ingest: every bound
// here was missing or could be got around before.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// hookStore is a fakeStore whose queries and inserts can be delayed or
// failed.
type hookStore struct {
	*fakeStore
	query  func(q string) error            // before each query; an error fails it
	insert func(table string, n int) error // before each insert
	mu     sync.Mutex
	sizes  map[string][]int // bytes per insert, by table
}

func newHookStore() *hookStore {
	return &hookStore{fakeStore: newFakeStore(), sizes: map[string][]int{}}
}

func (h *hookStore) Query(ctx context.Context, q string, p clickhouse.Params, row func(json.RawMessage) error) error {
	if h.query != nil {
		if err := h.query(q); err != nil {
			return err
		}
	}
	return h.fakeStore.Query(ctx, q, p, row)
}

func (h *hookStore) InsertJSON(ctx context.Context, table string, rows [][]byte) error {
	n := 0
	for _, r := range rows {
		n += len(r) + 1
	}
	h.mu.Lock()
	h.sizes[table] = append(h.sizes[table], n)
	h.mu.Unlock()
	if h.insert != nil {
		if err := h.insert(table, len(rows)); err != nil {
			return err
		}
	}
	return h.fakeStore.InsertJSON(ctx, table, rows)
}

func isRestore(q string) bool { return strings.Contains(q, "seq < {before:UInt64}") }

// recordLine is a wire record whose target_ip and first hop are given as
// raw strings, so tests can send what the agent never would.
func recordLine(seq uint64, ts time.Time, targetIP, hopIP string) string {
	tip, _ := json.Marshal(targetIP)
	hip, _ := json.Marshal(hopIP)
	ip, _ := netip.ParseAddr(hopIP)
	hash := record.PathHash([]record.Hop{{TTL: 1, Status: record.StatusTTLExpired, IP: ip}})
	return fmt.Sprintf(`{"schema":1,"agent_id":"lab","seq":%d,"ts":%q,"target":"dc1.example","target_ip":%s,`+
		`"af":6,"protocol":"icmp","flow_id":1,"reached":false,"path_len":1,"path_hash":%q,"loops":[],`+
		`"hops":[{"ttl":1,"status":"ttl-expired","ip":%s,"rtt_us":5}]}`, seq, ts.Format(time.RFC3339Nano), tip, hash, hip)
}

// IPv6 zones are any text to netip: refused on hops and loops, and on
// target_ip beyond an interface name. One request with 1 MB zones made the
// server hold that megabyte per stream.
func TestIngestRefusesZones(t *testing.T) {
	st := newFakeStore()
	_, ts := newTestServer(t, st)
	huge := strings.Repeat("z", 400_000)
	body := strings.Join([]string{
		recordLine(1, t0, "fe80::1%eth0", "fe80::2"),           // a link-local target keeps its zone
		recordLine(2, t0, "fe80::1%"+huge, "fe80::2"),          // a zone no interface has
		recordLine(3, t0, "fe80::1%<svg onload=x>", "fe80::2"), // markup
		recordLine(4, t0, "2001:db8::1", "fe80::2%"+huge),      // hops never have zones
		recordLine(5, t0, "2001:db8::1", "198.51.100.1"),       // IPv4 hop in an IPv6 record
	}, "\n") + "\n"
	code, res := post(t, ts.URL, "", []byte(body))
	if code != 200 || res.Accepted != 1 || res.Rejected != 4 {
		t.Fatalf("%d %+v", code, res)
	}
	for _, e := range res.Errors {
		if len(e) > maxErrorLen || strings.Contains(e, "zzz") || strings.Contains(e, "svg") {
			t.Errorf("error quotes the record: %q", e)
		}
	}
	if ip := st.table("cycles")[0]["target_ip"]; ip != "fe80::1%eth0" {
		t.Errorf("stored target_ip %v", ip)
	}
}

// ts must lie within [now - max_record_age, now + 1 day]: it bounds what a
// batch costs ClickHouse (monthly partitions; ts from 1970 to 2262 made
// thousands) and how far back a restore reads.
func TestIngestRecordAge(t *testing.T) {
	st := newFakeStore()
	now := t0.Add(time.Hour)
	_, ts := newTestServerAt(t, st, now)
	at := func(seq uint64, ts time.Time) *record.Record {
		r := mk("lab", seq, pathA...)
		r.TS = ts
		return r
	}
	code, res := post(t, ts.URL, "", ndjson(
		at(1, now.Add(-DefaultMaxRecordAge+time.Minute)), // just within
		at(2, now.Add(-DefaultMaxRecordAge-time.Minute)), // too old
		at(3, now.Add(23*time.Hour)),                     // a skewed clock
		at(4, now.Add(25*time.Hour)),                     // too far ahead
		at(5, time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)),
	))
	if code != 200 || res.Accepted != 2 || res.Rejected != 3 {
		t.Fatalf("%d %+v", code, res)
	}
	if !strings.Contains(res.Errors[0], "max_record_age") || !strings.Contains(res.Errors[1], "ahead") {
		t.Errorf("errors %q", res.Errors)
	}
}

// Validation and decoding errors are cut to maxErrorLen and do not quote
// values: a 233-byte zstd request used to write a 3 MiB log line.
func TestIngestErrorsBounded(t *testing.T) {
	var logged bytes.Buffer
	var mu sync.Mutex
	st := newFakeStore()
	s := New(Options{Store: st, Logger: slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return logged.Write(p)
	}), nil))})
	s.ingester.now = func() time.Time { return t0.Add(time.Hour) }
	s.SetReady()
	junk := strings.Repeat("\xff<junk>", 100_000)
	good := string(ndjson(mk("lab", 1, pathA...)))
	lines := []string{
		strings.Replace(good, `"status":"ttl-expired"`, `"status":"`+junk+`"`, 1),
		strings.Replace(good, `"target_ip":"192.0.2.9"`, `"target_ip":"`+junk+`"`, 1),
		strings.Replace(good, `"ts":"`, `"ts":"`+junk, 1),
		strings.Replace(good, `"seq":1`, `"seq":"`+junk+`"`, 1),
		`{"schema":1,` + junk,
	}
	var zb bytes.Buffer
	zw, _ := zstd.NewWriter(&zb)
	_, _ = io.WriteString(zw, strings.Join(lines, ""))
	_ = zw.Close()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/ingest", &zb)
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "zstd")
	w := newRecorder()
	s.Handler().ServeHTTP(w, req)
	var res IngestResult
	_ = json.Unmarshal(w.body.Bytes(), &res)
	if w.code != 200 || res.Rejected != len(lines) || len(res.Errors) != len(lines) {
		t.Fatalf("%d %s", w.code, w.body.String())
	}
	for _, e := range res.Errors {
		if len(e) > maxErrorLen || strings.Contains(e, "junk") {
			t.Errorf("error quotes the record: %q", e)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if logged.Len() > 1000 || strings.Contains(logged.String(), "junk") {
		t.Errorf("%d bytes logged: %.300s", logged.Len(), logged.String())
	}
}

func httptestServer(t *testing.T, h http.Handler) string {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts.URL
}

func counter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("no metric %s", name)
	return 0
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// recorder is a minimal ResponseWriter (httptest's would do, but this one
// makes SetReadDeadline unsupported on purpose, as some writers are).
type recorder struct {
	h    http.Header
	code int
	body bytes.Buffer
}

func newRecorder() *recorder                    { return &recorder{h: http.Header{}, code: 200} }
func (r *recorder) Header() http.Header         { return r.h }
func (r *recorder) WriteHeader(c int)           { r.code = c }
func (r *recorder) Write(b []byte) (int, error) { return r.body.Write(b) }

var _ http.ResponseWriter = (*recorder)(nil)

// Request bodies are read before a request takes an ingest slot: clients
// that send their bodies slowly (or never) hold none, and a stalled body is
// cut off after the read timeout. Four such connections used to block all
// ingest for the server's two-minute ReadTimeout.
func TestSlowBodiesHoldNoIngestSlot(t *testing.T) {
	st := newFakeStore()
	s := New(Options{Store: st, Logger: quietLogger(), MaxBodyBytes: 1 << 20})
	s.ingester.now = func() time.Time { return t0.Add(time.Hour) }
	s.bodyTimeout = 3 * time.Second
	s.SetReady()
	srv := httptestServer(t, s.Handler())
	addr := strings.TrimPrefix(srv, "http://")
	var conns []net.Conn
	for range 2 * DefaultMaxConcurrentIngest {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		fmt.Fprintf(c, "POST /api/v1/ingest HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-ndjson\r\nContent-Length: 100000\r\n\r\n{")
	}
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	code, res := post(t, srv, "", ndjson(mk("lab", 1, pathA...)))
	if code != 200 || res.Accepted != 1 || time.Since(start) > 1500*time.Millisecond {
		t.Errorf("an agent behind slow clients: %d %+v after %v", code, res, time.Since(start))
	}
	// The stalled bodies are cut off at the read timeout.
	buf := make([]byte, 512)
	_ = conns[0].SetReadDeadline(time.Now().Add(10 * time.Second))
	n, _ := conns[0].Read(buf)
	if !strings.HasPrefix(string(buf[:n]), "HTTP/1.1 408") {
		t.Errorf("stalled body answered %q", buf[:n])
	}
	for _, c := range conns {
		_ = c.Close()
	}
	// Every buffered chunk is returned.
	deadline := time.Now().Add(5 * time.Second)
	for s.readBuf.inUse() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := s.readBuf.inUse(); n != 0 {
		t.Errorf("%d read-buffer chunks still held", n)
	}
}

// One request restores at most maxRestores streams, within restoreTimeout:
// 4096 new streams used to hold an ingest slot for 20 s of serial queries.
// The streams left over derive no events for this batch and are restored
// with their next one.
func TestRestoresPerRequestBounded(t *testing.T) {
	st := newHookStore()
	var restores atomic.Int64
	st.query = func(q string) error {
		if isRestore(q) {
			restores.Add(1)
			time.Sleep(2 * time.Millisecond)
		}
		return nil
	}
	reg := prometheus.NewRegistry()
	in := NewIngester(st, nil, quietLogger(), newServerMetrics(reg))
	in.maxRestores = 10
	batch := func(seq0 uint64) []*record.Record {
		var recs []*record.Record
		for i := range 50 {
			r := mk("lab", seq0+uint64(i), pathA...)
			r.FlowID = i
			recs = append(recs, r)
		}
		return recs
	}
	if _, err := in.Ingest(t.Context(), batch(1)); err != nil {
		t.Fatal(err)
	}
	// A new stream's restore is two cycles queries and one outages query.
	if n := restores.Load(); n > 3*10 {
		t.Errorf("%d restore queries for at most 10 restores", n)
	}
	if got := counter(t, reg, "hopcorder_server_unrestored_cycles_total"); got != 40 {
		t.Errorf("%v cycles without restored state, want 40", got)
	}
	loaded := func() (n int) {
		a := in.lockAgent("lab")
		defer a.mu.Unlock()
		for _, s := range a.streams {
			if s.loaded {
				n++
			}
		}
		return n
	}
	if n := loaded(); n != 10 {
		t.Errorf("%d streams loaded, want 10", n)
	}
	if _, err := in.Ingest(t.Context(), batch(51)); err != nil {
		t.Fatal(err)
	}
	if n := loaded(); n != 20 {
		t.Errorf("after the next batch %d streams loaded, want 20", n)
	}

	// And within the time budget, however slow ClickHouse is.
	in2 := NewIngester(st, nil, quietLogger(), nil)
	in2.restoreTimeout = 100 * time.Millisecond
	st.query = func(q string) error {
		if isRestore(q) {
			time.Sleep(30 * time.Millisecond)
		}
		return nil
	}
	recs := batch(1000)
	for _, r := range recs {
		r.AgentID = "other"
	}
	start := time.Now()
	if _, err := in2.Ingest(t.Context(), recs); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("restores took %v with a 100ms budget", d)
	}
}

// A stream whose restore fails stays unloaded and is restored with its next
// batch. It used to start from scratch: an outage open in storage was never
// closed, and a second one opened.
func TestRestoreFailureKeepsStreamUnloaded(t *testing.T) {
	st := newHookStore()
	in := NewIngester(st, nil, quietLogger(), nil)
	for i, spec := range [][]string{pathA, brokenA} { // the outage opens at seq 2
		if _, err := in.Ingest(t.Context(), []*record.Record{mk("victim", uint64(i+1), spec...)}); err != nil {
			t.Fatal(err)
		}
	}
	// The stream's state is lost (dropped as least recently used, or a
	// restart), and ClickHouse is too slow to restore it.
	in = NewIngester(st, nil, quietLogger(), nil)
	var failing atomic.Bool
	failing.Store(true)
	st.query = func(q string) error {
		if failing.Load() && isRestore(q) {
			return context.DeadlineExceeded
		}
		return nil
	}
	if _, err := in.Ingest(t.Context(), []*record.Record{mk("victim", 3, brokenA...)}); err != nil {
		t.Fatal(err)
	}
	failing.Store(false)
	if _, err := in.Ingest(t.Context(), []*record.Record{mk("victim", 4, brokenA...), mk("victim", 5, pathA...)}); err != nil {
		t.Fatal(err)
	}
	latest := map[uint64]map[string]any{}
	for _, o := range st.table("outages") {
		if cur, ok := latest[num(o["start_seq"])]; !ok || num(o["version"]) >= num(cur["version"]) {
			latest[num(o["start_seq"])] = o
		}
	}
	o, ok := latest[2]
	if len(latest) != 1 || !ok || num(o["end_seq"]) != 5 || num(o["cycles"]) != 3 {
		t.Errorf("outages %v, want one from 2 to 5 over 3 cycles", latest)
	}
}

// If the cycle that opened an outage derived no events (its batch's restore
// did not happen in time), the restore opens the outage from the replayed
// cycles instead of losing it.
func TestRestoreOpensUnrecordedOutage(t *testing.T) {
	st := newHookStore()
	in := NewIngester(st, nil, quietLogger(), nil)
	if _, err := in.Ingest(t.Context(), []*record.Record{mk("lab", 1, pathA...)}); err != nil {
		t.Fatal(err)
	}
	in = NewIngester(st, nil, quietLogger(), nil) // state lost
	in.maxRestores = 0                            // and no restore this time
	if _, err := in.Ingest(t.Context(), []*record.Record{mk("lab", 2, brokenA...), mk("lab", 3, brokenA...)}); err != nil {
		t.Fatal(err)
	}
	in.maxRestores = maxRestores
	if _, err := in.Ingest(t.Context(), []*record.Record{mk("lab", 4, brokenA...), mk("lab", 5, pathA...)}); err != nil {
		t.Fatal(err)
	}
	out := st.table("outages")
	if len(out) != 2 || num(out[1]["start_seq"]) != 2 || num(out[1]["end_seq"]) != 5 || num(out[1]["cycles"]) != 3 ||
		out[1]["last_hop_ip"] != "198.18.120.1" {
		t.Errorf("outages %v, want one opened at 2 and closed at 5", out)
	}
}

type bigEnrich struct{}

var (
	bigAS   = strings.Repeat("N", 128)
	bigName = strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
)

func (bigEnrich) ASN(netip.Addr) (uint32, string) { return 64500, bigAS }
func (bigEnrich) Name(netip.Addr) string          { return bigName }

// Cycles are inserted in parts of at most maxInsertBytes: with AS and rDNS
// names on every hop a 31 MiB request made one 500 MB insert. A failure
// part-way stores the parts before it, and the retry only the rest.
func TestCyclesInsertedInParts(t *testing.T) {
	st := newHookStore()
	in := NewIngester(st, bigEnrich{}, quietLogger(), nil)
	spec := make([]string, 255)
	for i := range spec {
		spec[i] = "198.18.120.1"
	}
	var recs []*record.Record
	for seq := uint64(1); seq <= 200; seq++ { // about 100 KiB per row
		recs = append(recs, mk("lab", seq, spec...))
	}
	var inserts atomic.Int64
	st.insert = func(table string, _ int) error {
		if table == "cycles" && inserts.Add(1) == 2 {
			return errors.New("clickhouse: connection reset")
		}
		return nil
	}
	if _, err := in.Ingest(t.Context(), recs); err == nil {
		t.Fatal("the failed part was not reported")
	}
	res, err := in.Ingest(t.Context(), recs)
	if err != nil {
		t.Fatal(err)
	}
	stored := len(st.table("cycles"))
	if stored != len(recs) || res.Duplicates == 0 || res.Accepted+res.Duplicates != len(recs) {
		t.Errorf("%d cycles stored, retry %+v", stored, res)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.sizes["cycles"]) < 3 {
		t.Errorf("inserts %v", st.sizes["cycles"])
	}
	for _, n := range st.sizes["cycles"] {
		if n > maxInsertBytes+200<<10 {
			t.Errorf("an insert of %d bytes", n)
		}
	}
}

// Agent states are bounded like streams: a request may name 16 new
// agent_ids, and each used to keep its state for an hour.
func TestIngesterAgentsBounded(t *testing.T) {
	st := newFakeStore()
	in := NewIngester(st, nil, quietLogger(), nil)
	now := t0
	in.now = func() time.Time { return now }
	in.maxAgents = 100
	for r := range 50 {
		var recs []*record.Record
		for a := range 16 {
			recs = append(recs, mk(fmt.Sprintf("a%d-%d", r, a), 1, pathA...))
		}
		if _, err := in.Ingest(t.Context(), recs); err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Second)
	}
	_, _ = in.Ingest(t.Context(), nil) // the next request's sweep
	in.mu.Lock()
	n := len(in.agents)
	in.mu.Unlock()
	if n > 100 {
		t.Errorf("%d agent states, max 100", n)
	}
	if s := in.nstreams.Load(); s > int64(n) {
		t.Errorf("%d streams counted for %d agents", s, n)
	}
	// A dropped agent resumes from storage: its seqs are still duplicates.
	res, err := in.Ingest(t.Context(), []*record.Record{mk("a0-0", 1, pathA...)})
	if err != nil || res.Duplicates != 1 {
		t.Errorf("dropped agent: %+v %v", res, err)
	}
}

// Bodies being read hold memory only within the read buffer: a client that
// announces a large body and stalls holds the chunks it reserved until its
// read timeout, and others wait for them rather than adding memory (64 slow
// clients could otherwise hold 64 x max_body_bytes).
func TestReadBufferBounded(t *testing.T) {
	st := newFakeStore()
	s := New(Options{Store: st, Logger: quietLogger(), MaxBodyBytes: 1 << 20})
	s.ingester.now = func() time.Time { return t0.Add(time.Hour) }
	s.bodyTimeout = 500 * time.Millisecond
	s.readBuf = newChunkPool(16) // 1 MiB
	s.SetReady()
	srv := httptestServer(t, s.Handler())
	c, err := net.Dial("tcp", strings.TrimPrefix(srv, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "POST /api/v1/ingest HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-ndjson\r\nContent-Length: 1000000\r\n\r\n")
	_, _ = c.Write(bytes.Repeat([]byte(" "), 4*readChunk)) // then nothing
	time.Sleep(100 * time.Millisecond)
	if n := s.readBuf.inUse(); n != 16 {
		t.Fatalf("stalled client holds %d chunks, want all 16", n)
	}
	// An agent waits for the stalled client's chunks (released at its read
	// timeout, 400 ms from now), then gets through.
	start := time.Now()
	if code, res := post(t, srv, "", ndjson(mk("lab", 1, pathA...))); code != 200 || res.Accepted != 1 ||
		time.Since(start) < 300*time.Millisecond {
		t.Errorf("behind a full read buffer: %d %+v after %v", code, res, time.Since(start))
	}
	if n := s.readBuf.inUse(); n != 0 {
		t.Errorf("%d chunks still held", n)
	}
}
