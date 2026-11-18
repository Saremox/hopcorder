package server

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/output"
	"github.com/saremox/hopcorder/internal/record"
)

// fakeStore keeps inserted rows in memory and answers the handful of
// queries the ingester issues from them, like ClickHouse would. The SQL
// itself is covered by the clickhouse-tagged integration tests.
type fakeStore struct {
	mu      sync.Mutex
	rows    map[string][]map[string]any
	fail    map[string]int // table -> inserts still to fail
	queries []string
	execs   []string                // statements run with Exec (it has no views to fill in)
	answer  func(q string) []string // optional canned answers for read-API queries
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: map[string][]map[string]any{}, fail: map[string]int{}}
}

func (f *fakeStore) InsertJSON(_ context.Context, table string, rows [][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[table] > 0 {
		f.fail[table]--
		return errors.New("clickhouse: connection refused")
	}
	for _, r := range rows {
		var m map[string]any
		if err := json.Unmarshal(r, &m); err != nil {
			return err
		}
		f.rows[table] = append(f.rows[table], m)
	}
	return nil
}

func (f *fakeStore) Exec(_ context.Context, q string, _ clickhouse.Params) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, q)
	return nil
}

func num(v any) uint64 { f, _ := v.(float64); return uint64(f) }

func (f *fakeStore) stream(m map[string]any, p clickhouse.Params) bool {
	return m["agent_id"] == p["agent"] && m["target"] == p["target"] && m["protocol"] == p["protocol"] &&
		strconv.FormatUint(num(m["flow_id"]), 10) == p["flow"]
}

func (f *fakeStore) Query(_ context.Context, q string, p clickhouse.Params, row func(json.RawMessage) error) error {
	f.mu.Lock()
	f.queries = append(f.queries, q)
	var out []string
	before, _ := strconv.ParseUint(p["before"], 10, 64)
	switch {
	case strings.Contains(q, "FROM agent_seq"):
		var m uint64
		for _, c := range f.rows["agent_seq"] {
			if c["agent_id"] == p["agent"] {
				m = max(m, num(c["max_seq"]))
			}
		}
		out = []string{fmt.Sprintf(`{"m":%d}`, m)}
	case strings.Contains(q, "SELECT DISTINCT seq FROM cycles"): // findStored
		lo, _ := strconv.ParseUint(p["lo"], 10, 64)
		hi, _ := strconv.ParseUint(p["hi"], 10, 64)
		seen := map[uint64]bool{}
		for _, c := range f.rows["cycles"] {
			s, ts := num(c["seq"]), c["ts"].(string)
			if c["agent_id"] == p["agent"] && s >= lo && s <= hi && ts >= p["from"] && ts <= p["to"] && !seen[s] {
				seen[s] = true
				out = append(out, fmt.Sprintf(`{"seq":%d}`, s))
			}
		}
	case strings.Contains(q, "SELECT DISTINCT seq, toUnixTimestamp64Milli(ts) AS ms FROM cycles"): // holdsAll
		lo, _ := strconv.ParseUint(p["lo"], 10, 64)
		hi, _ := strconv.ParseUint(p["hi"], 10, 64)
		seen := map[string]bool{}
		for _, c := range f.rows["cycles"] {
			s, ts := num(c["seq"]), c["ts"].(string)
			if c["agent_id"] == p["agent"] && s >= lo && s <= hi && ts >= p["from"] && ts <= p["to"] {
				at, err := time.Parse("2006-01-02 15:04:05.000", ts)
				if err != nil {
					f.mu.Unlock()
					return err
				}
				if l := fmt.Sprintf(`{"seq":%d,"ms":%d}`, s, at.UnixMilli()); !seen[l] {
					seen[l] = true
					out = append(out, l)
				}
			}
		}
	case strings.Contains(q, "AS n_reached"), strings.Contains(q, "AS since_last"): // restore: an outage's cycles
		start, _ := strconv.ParseUint(p["start"], 10, 64)
		closeSeq, _ := strconv.ParseUint(p["close"], 10, 64)
		last, _ := strconv.ParseUint(p["last"], 10, 64)
		var n, reached, closed, sinceLast int
		var first, lastReached uint64
		closeTS := ""
		for _, c := range f.rows["cycles"] {
			s := num(c["seq"])
			if !f.stream(c, p) || s < start || s >= before || c["ts"].(string) < p["start_ts"] {
				continue
			}
			n++
			if c["reached"] == true {
				reached++
				if first == 0 || s < first {
					first, closeTS = s, c["ts"].(string)
				}
				lastReached = max(lastReached, s)
			}
			if s < closeSeq {
				closed++
			}
			if s > last {
				sinceLast++
			}
		}
		if strings.Contains(q, "AS since_last") {
			out = []string{fmt.Sprintf(`{"closed":%d,"since_last":%d}`, closed, sinceLast)}
		} else {
			b, _ := json.Marshal(map[string]any{"n": n, "n_reached": reached, "close_seq": first, "close_ts": closeTS,
				"last_reached": lastReached})
			out = []string{string(b)}
		}
	case strings.Contains(q, "ORDER BY seq LIMIT 1"): // restore: the first cycle of an outage
		last, _ := strconv.ParseUint(p["last"], 10, 64)
		var best map[string]any
		for _, c := range f.rows["cycles"] {
			s := num(c["seq"])
			if f.stream(c, p) && s > last && s < before && c["ts"].(string) >= p["start_ts"] &&
				(best == nil || s < num(best["seq"])) {
				best = c
			}
		}
		if best != nil {
			b, _ := json.Marshal(best)
			out = []string{string(b)}
		}
	case strings.Contains(q, "FROM cycles WHERE agent_id = {agent:String}"): // restore
		var cs []map[string]any
		for _, c := range f.rows["cycles"] {
			// ts is stored as clickhouse.Time, which sorts like the time.
			if f.stream(c, p) && num(c["seq"]) < before && c["ts"].(string) >= p["since"] {
				cs = append(cs, c)
			}
		}
		slices.SortFunc(cs, func(a, b map[string]any) int { return cmp.Compare(num(b["seq"]), num(a["seq"])) })
		for _, c := range cs[:min(len(cs), replayDepth)] {
			b, _ := json.Marshal(c)
			out = append(out, string(b))
		}
	case strings.Contains(q, "FROM outages FINAL WHERE agent_id") && strings.Contains(q, "start_seq >= {lo:UInt64}"): // heldOpen
		lo, _ := strconv.ParseUint(p["lo"], 10, 64)
		hi, _ := strconv.ParseUint(p["hi"], 10, 64)
		type k struct {
			target, protocol string
			flow, start      uint64
		}
		latest := map[k]map[string]any{}
		for _, o := range f.rows["outages"] {
			s := num(o["start_seq"])
			if o["agent_id"] != p["agent"] || s < lo || s > hi {
				continue
			}
			key := k{o["target"].(string), o["protocol"].(string), num(o["flow_id"]), s}
			if cur, ok := latest[key]; !ok || num(o["version"]) >= num(cur["version"]) {
				latest[key] = o
			}
		}
		for _, o := range latest {
			b, _ := json.Marshal(map[string]any{"target": o["target"], "protocol": o["protocol"],
				"flow_id": o["flow_id"], "start_seq": o["start_seq"], "end_ts": o["end_ts"]})
			out = append(out, string(b))
		}
	case strings.Contains(q, "FROM outages FINAL WHERE agent_id"): // restore: the newest outage, open or closed
		latest := map[uint64]map[string]any{}
		for _, o := range f.rows["outages"] {
			if f.stream(o, p) {
				if cur, ok := latest[num(o["start_seq"])]; !ok || num(o["version"]) >= num(cur["version"]) {
					latest[num(o["start_seq"])] = o
				}
			}
		}
		var best map[string]any
		after, _ := strconv.ParseUint(p["after"], 10, 64)
		for _, o := range latest {
			if num(o["start_seq"]) < before && num(o["start_seq"]) > after &&
				(best == nil || num(o["start_seq"]) > num(best["start_seq"])) {
				best = o
			}
		}
		if best != nil {
			b, _ := json.Marshal(best)
			out = []string{string(b)}
		}
	case f.answer != nil:
		out = f.answer(q)
	}
	f.mu.Unlock()
	for _, l := range out {
		if err := row(json.RawMessage(l)); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeStore) table(name string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.rows[name])
}

type fakeEnrich struct{}

func (fakeEnrich) ASN(ip netip.Addr) (uint32, string) {
	if strings.HasPrefix(ip.String(), "198.18.") {
		return 64510, "EXAMPLE-TRANSIT"
	}
	return 0, ""
}
func (fakeEnrich) Name(ip netip.Addr) string {
	if ip.String() == "10.0.0.1" {
		return "gw.lan"
	}
	return ""
}

var t0 = time.Date(2026, 11, 16, 23, 0, 0, 0, time.UTC)

// mk builds a finalized record. Hop spec: "*" silent, "=IP" destination
// reply, anything else ttl-expired from that IP.
func mk(agent string, seq uint64, spec ...string) *record.Record {
	r := &record.Record{Schema: 1, AgentID: agent, Seq: seq, TS: t0.Add(time.Duration(seq) * 5 * time.Second),
		Target: "dc1.example", TargetIP: netip.MustParseAddr("192.0.2.9"), AF: 4, Protocol: "icmp", FlowID: 33434}
	for i, s := range spec {
		h := record.Hop{TTL: i + 1, Status: record.StatusNoReply}
		switch {
		case s == "*":
		case strings.HasPrefix(s, "="):
			h.Status, h.IP, h.RTTMicros = record.StatusReply, netip.MustParseAddr(s[1:]), 20000
		default:
			h.Status, h.IP, h.RTTMicros = record.StatusTTLExpired, netip.MustParseAddr(s), 1000
		}
		r.Hops = append(r.Hops, h)
	}
	r.Finalize(2)
	return r
}

var (
	pathA   = []string{"10.0.0.1", "198.18.120.1", "=192.0.2.9"}
	pathB   = []string{"10.0.0.1", "198.18.120.7", "=192.0.2.9"}
	flaky   = []string{"10.0.0.1", "*", "=192.0.2.9"}
	brokenA = []string{"10.0.0.1", "198.18.120.1", "*", "*"} // pathA, destination silent
	brokenB = []string{"10.0.0.1", "198.18.120.7", "*", "*"} // pathB, destination silent
)

func quietLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestIngestDedup(t *testing.T) {
	st := newFakeStore()
	in := NewIngester(st, fakeEnrich{}, quietLogger(), nil)
	batch := func(seqs ...uint64) []*record.Record {
		var out []*record.Record
		for _, s := range seqs {
			out = append(out, mk("lab", s, pathA...))
		}
		return out
	}
	res, err := in.Ingest(t.Context(), batch(1, 2, 3))
	if err != nil || res.Accepted != 3 || res.Duplicates != 0 {
		t.Fatalf("first batch: %+v %v", res, err)
	}
	// A retry with more records appended (the response was lost), out of
	// order and with an in-batch duplicate.
	res, err = in.Ingest(t.Context(), batch(5, 1, 2, 3, 4, 4))
	if err != nil || res.Accepted != 2 || res.Duplicates != 4 {
		t.Fatalf("retry: %+v %v", res, err)
	}
	var seqs []uint64
	for _, c := range st.table("cycles") {
		seqs = append(seqs, num(c["seq"]))
	}
	if fmt.Sprint(seqs) != "[1 2 3 4 5]" {
		t.Errorf("stored seqs %v", seqs)
	}
	// A new ingester (server restart) learns the high-water mark from storage.
	in2 := NewIngester(st, fakeEnrich{}, quietLogger(), nil)
	res, _ = in2.Ingest(t.Context(), batch(4, 5, 6))
	if res.Accepted != 1 || res.Duplicates != 2 {
		t.Errorf("after restart: %+v", res)
	}
	// Agents are independent.
	res, _ = in2.Ingest(t.Context(), []*record.Record{mk("other", 1, pathA...)})
	if res.Accepted != 1 {
		t.Errorf("other agent: %+v", res)
	}
}

func TestIngestStorageFailureIsRetryable(t *testing.T) {
	st := newFakeStore()
	in := NewIngester(st, nil, quietLogger(), nil)
	st.fail["cycles"] = 1
	if _, err := in.Ingest(t.Context(), []*record.Record{mk("lab", 1, pathA...), mk("lab", 2, pathA...)}); err == nil {
		t.Fatal("storage failure not reported")
	}
	res, err := in.Ingest(t.Context(), []*record.Record{mk("lab", 1, pathA...), mk("lab", 2, pathA...)})
	if err != nil || res.Accepted != 2 {
		t.Fatalf("retry: %+v %v", res, err)
	}
	if n := len(st.table("cycles")); n != 2 {
		t.Errorf("%d cycles stored, want 2", n)
	}
}

func TestCycleRowEnrichment(t *testing.T) {
	st := newFakeStore()
	in := NewIngester(st, fakeEnrich{}, quietLogger(), nil)
	r := mk("lab", 1, "10.0.0.1", "198.18.120.1", "198.18.120.2", "198.18.120.1", "*")
	r.Hops[1].MPLS = []record.MPLSLabel{{Label: 24005, S: true, TTL: 1}}
	if _, err := in.Ingest(t.Context(), []*record.Record{r}); err != nil {
		t.Fatal(err)
	}
	c := st.table("cycles")[0]
	got := func(k string) string { b, _ := json.Marshal(c[k]); return string(b) }
	// (The fake re-encodes rows from maps, so object keys come out sorted.)
	for k, want := range map[string]string{
		"hops.ttl":     "[1,2,3,4,5]",
		"hops.status":  `["ttl-expired","ttl-expired","ttl-expired","ttl-expired","no-reply"]`,
		"hops.ip":      `["10.0.0.1","198.18.120.1","198.18.120.2","198.18.120.1",""]`,
		"hops.asn":     "[0,64510,64510,64510,0]",
		"hops.as_name": `["","EXAMPLE-TRANSIT","EXAMPLE-TRANSIT","EXAMPLE-TRANSIT",""]`,
		"hops.name":    `["gw.lan","","","",""]`,
		"hops.code":    "[-1,-1,-1,-1,-1]",
		"hops.mpls":    `[[],[{"label":24005,"s":true,"tc":0,"ttl":1}],[],[],[]]`,
		"loops.ip":     `["198.18.120.1"]`,
		"loops.ttls":   "[[2,4]]",
		"ts":           `"2026-11-16 23:00:05.000"`,
	} {
		if g := got(k); g != want {
			t.Errorf("%s = %s, want %s", k, g, want)
		}
	}
}

func TestEvents(t *testing.T) {
	st := newFakeStore()
	in := NewIngester(st, nil, quietLogger(), nil)
	seq := uint64(0)
	feed := func(spec []string) {
		seq++
		if _, err := in.Ingest(t.Context(), []*record.Record{mk("lab", seq, spec...)}); err != nil {
			t.Fatal(err)
		}
	}
	feed(pathA)
	feed(flaky) // rate-limited hop: not a change
	feed(pathA)
	feed(pathB) // seq 4: router change at TTL 2
	for range 3 {
		feed(brokenB) // seq 5-7: outage on the same path
	}
	feed(pathB) // seq 8: recovered

	ch := st.table("path_changes")
	if len(ch) != 1 {
		t.Fatalf("path changes %v", ch)
	}
	c := ch[0]
	if num(c["seq"]) != 4 || c["reason"] != "router" || fmt.Sprint(c["ttls"]) != "[2]" ||
		fmt.Sprint(c["from_ips"]) != "[198.18.120.1]" || fmt.Sprint(c["to_ips"]) != "[198.18.120.7]" || num(c["prev_seq"]) != 3 {
		t.Errorf("path change %v", c)
	}
	out := st.table("outages")
	if len(out) != 4 {
		t.Fatalf("outage rows %v", out)
	}
	// The open row is written after each batch the outage opened or went on
	// in, with its cycles so far and the seq of its last cycle as version.
	for i, open := range out[:3] {
		if num(open["start_seq"]) != 5 || open["end_ts"] != nil || num(open["cycles"]) != uint64(i+1) ||
			num(open["version"]) != uint64(5+i) || open["last_hop_ip"] != "198.18.120.7" || num(open["last_hop_ttl"]) != 2 {
			t.Errorf("open row %d: %v", i, open)
		}
	}
	closed := out[3]
	if num(closed["start_seq"]) != 5 || num(closed["end_seq"]) != 8 || closed["end_ts"] == nil ||
		num(closed["cycles"]) != 3 || num(closed["version"]) <= num(out[2]["version"]) {
		t.Errorf("close row %v", closed)
	}
}

// An ongoing outage reports the cycles it has so far (review round seven:
// the open row was written once, with 1 cycle, and the count stayed in
// memory until the outage closed, so the read API, the UI and Grafana said
// "1 cycles" for as long as it lasted). The review's scenario: 50 cycles
// that reach the destination, then 100 that do not, in batches, with a
// restart in between. Every open row's version stays below the close
// row's, so FINAL reads the close row whatever order the rows were
// inserted in (a retried or late insert).
func TestOpenOutageCountsItsCycles(t *testing.T) {
	st := newFakeStore()
	reg := prometheus.NewRegistry()
	in := NewIngester(st, nil, quietLogger(), newServerMetrics(reg))
	ingest := func(from, to uint64, spec []string) {
		t.Helper()
		var recs []*record.Record
		for seq := from; seq <= to; seq++ {
			recs = append(recs, mk("lab", seq, spec...))
		}
		if _, err := in.Ingest(t.Context(), recs); err != nil {
			t.Fatal(err)
		}
	}
	ingest(1, 50, pathA)
	for from := uint64(51); from <= 150; from += 10 {
		if from == 121 {
			in = NewIngester(st, nil, quietLogger(), nil) // a restart: the open outage is restored
		}
		ingest(from, from+9, brokenA)
		want := fmt.Sprintf("end 0, %d cycles", from+9-50)
		if got := outagesOf(st.table("outages")); len(got) != 1 || got[51] != want {
			t.Fatalf("after seq %d: outages %v, want one from 51: %s", from+9, got, want)
		}
	}
	if n := counter(t, reg, "hopcorder_server_outages_total"); n != 1 {
		t.Errorf("%v outages opened, want 1 (rewriting the open row opens none)", n)
	}
	ingest(151, 151, pathA)
	rows := st.table("outages")
	if got := outagesOf(rows); len(got) != 1 || got[51] != "end 151, 100 cycles" {
		t.Errorf("outages %v, want one from 51 to 151 over 100 cycles", got)
	}
	slices.Reverse(rows)
	if got := outagesOf(rows); len(got) != 1 || got[51] != "end 151, 100 cycles" {
		t.Errorf("with the rows inserted in reverse: outages %v, want the close row", got)
	}
}

// dualStack is the review's scenario (round five): n cycles of one agent
// alternating between the IPv4 target of dual.example (odd seqs, reached)
// and its IPv6 target (even seqs, IPv6 down after the second hop), on the
// given flows.
func dualStack(n, flow4, flow6 int) []*record.Record {
	var recs []*record.Record
	for seq := 1; seq <= n; seq++ {
		var r *record.Record
		if seq%2 == 1 {
			r = mk("lab", uint64(seq), pathA...)
			r.FlowID = flow4
		} else {
			r = mk("lab", uint64(seq), "2001:db8:1::1", "2001:db8:2::1", "*", "*")
			r.TargetIP, r.AF, r.FlowID = netip.MustParseAddr("2001:db8::9"), 6, flow6
		}
		r.Target = "dual.example"
		recs = append(recs, r)
	}
	return recs
}

// Two targets of one host on their own flows are two streams with their
// own events: the IPv6 one an outage from its first cycle, neither a route
// change. On one flow (what the agent sent before it refused such targets)
// the server has no way to tell them apart, and the review's numbers come
// out: every cycle after the first a "target" route change, every IPv6
// cycle a new outage.
func TestDualStackStreams(t *testing.T) {
	ingest := func(flow6 int) *fakeStore {
		st := newFakeStore()
		in := NewIngester(st, nil, quietLogger(), nil)
		if _, err := in.Ingest(t.Context(), dualStack(40, 33434, flow6)); err != nil {
			t.Fatal(err)
		}
		return st
	}
	starts := func(st *fakeStore) map[uint64]bool {
		m := map[uint64]bool{}
		for _, o := range st.table("outages") {
			m[num(o["start_seq"])] = true
		}
		return m
	}

	st := ingest(33435)
	if ch := st.table("path_changes"); len(ch) != 0 {
		t.Errorf("own flows: %d route changes, want none: %v", len(ch), ch)
	}
	out := st.table("outages")
	if len(out) != 1 || num(out[0]["flow_id"]) != 33435 || num(out[0]["start_seq"]) != 2 || out[0]["end_ts"] != nil {
		t.Errorf("own flows: outages %v, want one open from seq 2 on flow 33435", out)
	}

	st = ingest(33434)
	ch := st.table("path_changes")
	if len(ch) != 39 || ch[0]["reason"] != "target" {
		t.Errorf("one flow: %d route changes, want 39 target changes: %v", len(ch), ch)
	}
	if n := len(starts(st)); n != 20 {
		t.Errorf("one flow: %d outages, want 20", n)
	}
}

func TestRestoreAfterRestart(t *testing.T) {
	st := newFakeStore()
	in := NewIngester(st, nil, quietLogger(), nil)
	for i, spec := range [][]string{pathA, pathA, brokenA, brokenA} {
		if _, err := in.Ingest(t.Context(), []*record.Record{mk("lab", uint64(i+1), spec...)}); err != nil {
			t.Fatal(err)
		}
	}
	// Restart: a new ingester restores the tracker and the open outage.
	in2 := NewIngester(st, nil, quietLogger(), nil)
	if _, err := in2.Ingest(t.Context(), []*record.Record{mk("lab", 5, pathA...)}); err != nil {
		t.Fatal(err)
	}
	if ch := st.table("path_changes"); len(ch) != 0 {
		t.Errorf("restart produced spurious path changes: %v", ch)
	}
	out := st.table("outages")
	last := out[len(out)-1]
	if len(out) != 3 || num(out[1]["cycles"]) != 2 || out[1]["end_ts"] != nil ||
		num(last["start_seq"]) != 3 || num(last["end_seq"]) != 5 || num(last["cycles"]) != 2 {
		t.Errorf("outage not resumed across restart: %v", out)
	}
	// The restored tracker still knows TTL 2, so a new router there is a change.
	if _, err := in2.Ingest(t.Context(), []*record.Record{mk("lab", 6, pathB...)}); err != nil {
		t.Fatal(err)
	}
	if ch := st.table("path_changes"); len(ch) != 1 || num(ch[0]["prev_seq"]) != 5 {
		t.Errorf("path changes after restart: %v", ch)
	}
}

func TestEventInsertFailureDoesNotFailIngest(t *testing.T) {
	st := newFakeStore()
	in := NewIngester(st, nil, quietLogger(), nil)
	st.fail["outages"] = 10
	res, err := in.Ingest(t.Context(), []*record.Record{mk("lab", 1, brokenA...)})
	if err != nil || res.Accepted != 1 {
		t.Errorf("ingest failed because of derived events: %+v %v", res, err)
	}
}

// --- HTTP ---

func newTestServer(t *testing.T, st *fakeStore) (*Server, *httptest.Server) {
	t.Helper()
	return newTestServerAt(t, st, t0.Add(time.Hour))
}

// newTestServerAt is newTestServer with its clock at now (records' ts must
// lie within a month before it).
func newTestServerAt(t *testing.T, st Store, now time.Time) (*Server, *httptest.Server) {
	t.Helper()
	s := New(Options{Store: st, Enricher: fakeEnrich{}, Logger: quietLogger(), Registry: prometheus.NewRegistry(),
		MaxBodyBytes: 1 << 20, MaxDecodedBytes: 4 << 20})
	s.ingester.now = func() time.Time { return now }
	s.SetReady()
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts
}

func lines(recs ...*record.Record) [][]byte {
	var out [][]byte
	for _, r := range recs {
		b, _ := json.Marshal(r)
		out = append(out, b)
	}
	return out
}

// The agent's real HTTP output talks to the server.
func TestIngestFromAgentOutput(t *testing.T) {
	st := newFakeStore()
	_, ts := newTestServer(t, st)
	h, err := output.NewHTTP(output.HTTPConfig{URL: ts.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err := h.Deliver(t.Context(), lines(mk("lab", 1, pathA...), mk("lab", 2, pathA...))); err != nil {
		t.Fatal(err)
	}
	if err := h.Deliver(t.Context(), lines(mk("lab", 2, pathA...), mk("lab", 3, pathB...))); err != nil {
		t.Fatal(err)
	}
	if n := len(st.table("cycles")); n != 3 {
		t.Errorf("%d cycles stored, want 3", n)
	}
}

func post(t *testing.T, url, encoding string, body []byte) (int, IngestResult) {
	t.Helper()
	return postAs(t, url, "application/x-ndjson", encoding, body)
}

func postAs(t *testing.T, url, contentType, encoding string, body []byte) (int, IngestResult) {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, url+"/api/v1/ingest", bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var res IngestResult
	_ = json.NewDecoder(resp.Body).Decode(&res)
	return resp.StatusCode, res
}

func ndjson(recs ...*record.Record) []byte {
	var b bytes.Buffer
	for _, l := range lines(recs...) {
		b.Write(l)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

func TestIngestHTTP(t *testing.T) {
	st := newFakeStore()
	_, ts := newTestServer(t, st)

	var gz bytes.Buffer
	gw := gzip.NewWriter(&gz)
	_, _ = gw.Write(ndjson(mk("lab", 1, pathA...)))
	_ = gw.Close()
	if code, res := post(t, ts.URL, "gzip", gz.Bytes()); code != 200 || res.Accepted != 1 {
		t.Errorf("gzip: %d %+v", code, res)
	}
	if code, res := post(t, ts.URL, "", ndjson(mk("lab", 2, pathA...))); code != 200 || res.Accepted != 1 {
		t.Errorf("identity: %d %+v", code, res)
	}

	// Invalid records are rejected for good; valid ones in the same batch stored.
	bad := mk("lab", 4, pathA...)
	bad.PathLen = 99
	once := mk("lab", 5, pathA...)
	once.Seq = 0
	body := append(ndjson(mk("lab", 3, pathA...), bad, once), []byte("not json\n{\"schema\":2}\n\n")...)
	code, res := post(t, ts.URL, "", body)
	if code != 200 || res.Accepted != 1 || res.Rejected != 4 || len(res.Errors) != 4 {
		t.Errorf("mixed batch: %d %+v", code, res)
	}

	if code, _ := post(t, ts.URL, "zstd", []byte("not zstd at all")); code != http.StatusBadRequest {
		t.Errorf("bad zstd: %d", code)
	}
	if code, _ := post(t, ts.URL, "br", []byte("x")); code != http.StatusUnsupportedMediaType {
		t.Errorf("unknown encoding: %d", code)
	}
	if code, _ := post(t, ts.URL, "", bytes.Repeat([]byte("x"), 2<<20)); code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body: %d", code)
	}
	// A small zstd body that inflates past the decoded limit.
	enc, _ := zstd.NewWriter(nil)
	bomb := enc.EncodeAll(bytes.Repeat([]byte("{}\n"), 3<<20), nil)
	if code, _ := post(t, ts.URL, "zstd", bomb); code != http.StatusRequestEntityTooLarge {
		t.Errorf("decompression bomb: %d", code)
	}

	st.fail["cycles"] = 1
	if code, _ := post(t, ts.URL, "", ndjson(mk("lab", 6, pathA...))); code != http.StatusServiceUnavailable {
		t.Errorf("storage failure: %d", code)
	}

	// Content types a web page can send without a CORS preflight are refused.
	for _, ct := range []string{"", "text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x"} {
		if code, _ := postAs(t, ts.URL, ct, "", ndjson(mk("lab", 7, pathA...))); code != http.StatusUnsupportedMediaType {
			t.Errorf("Content-Type %q: %d", ct, code)
		}
	}
	if code, res := postAs(t, ts.URL, "application/x-ndjson; charset=utf-8", "", ndjson(mk("lab", 7, pathA...))); code != 200 || res.Accepted != 1 {
		t.Errorf("ndjson with parameters: %d %+v", code, res)
	}

	// A zstd frame declaring a huge window is refused before it is allocated.
	big := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x88, 0x01, 0x00, 0x00} // magic, window 128 MiB, empty raw block
	if code, _ := post(t, ts.URL, "zstd", big); code != http.StatusRequestEntityTooLarge && code != http.StatusBadRequest {
		t.Errorf("huge zstd window: %d", code)
	}

	// More agents in one request than an agent would send.
	var many []*record.Record
	for i := range maxAgentsPerRequest + 1 {
		many = append(many, mk(fmt.Sprintf("a%d", i), 1, pathA...))
	}
	if code, _ := post(t, ts.URL, "", ndjson(many...)); code != http.StatusBadRequest {
		t.Errorf("%d agents in one request: %d", len(many), code)
	}
	resp, _ := http.Get(ts.URL + "/api/v1/ingest")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET ingest: %d", resp.StatusCode)
	}
}

type fakePinger struct{ err error }

func (p fakePinger) Ping(context.Context) error { return p.err }

func TestHealthAndReady(t *testing.T) {
	get := func(h http.Handler, path string) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}
	s := New(Options{Store: newFakeStore(), Pinger: fakePinger{}, Logger: quietLogger(), Registry: prometheus.NewRegistry()})
	h := s.Handler()
	if get(h, "/healthz") != 200 || get(h, "/readyz") != 503 {
		t.Error("before migrations: healthy but not ready expected")
	}
	if code, _ := post(t, httptest.NewServer(h).URL, "", nil); code != http.StatusServiceUnavailable {
		t.Errorf("ingest before ready: %d", code)
	}
	s.SetReady()
	if get(h, "/readyz") != 200 || get(h, "/metrics") != 200 {
		t.Error("ready server not ready")
	}
	s2 := New(Options{Store: newFakeStore(), Pinger: fakePinger{errors.New("down")}, Logger: quietLogger()})
	s2.SetReady()
	if get(s2.Handler(), "/readyz") != 503 {
		t.Error("ready despite ClickHouse being unreachable")
	}
	if get(s2.Handler(), "/metrics") != 404 {
		t.Error("/metrics served without a registry")
	}
}

func TestQueryAPI(t *testing.T) {
	st := newFakeStore()
	st.answer = func(q string) []string {
		if strings.Contains(q, "hops.mpls") {
			return []string{`{"agent_id":"lab","seq":7,"ts":"2026-11-16T23:00:35.000Z","target":"dc1.example",
				"target_ip":"192.0.2.9","af":4,"protocol":"icmp","flow_id":33434,"reached":true,"path_len":2,
				"path_hash":"x","loops.ip":[],"loops.ttls":[],"hops.ttl":[1,2],"hops.status":["unreachable","reply"],
				"hops.ip":["10.0.0.1","192.0.2.9"],"hops.rtt_us":[900,20000],"hops.code":[0,-1],"hops.asn":[0,64500],
				"hops.as_name":["","EXAMPLE"],"hops.name":["gw.lan",""],"hops.mpls":[[],[]]}`}
		}
		return []string{`{"x":1}`}
	}
	_, ts := newTestServer(t, st)
	get := func(path string) (int, map[string]any) {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		b, _ := io.ReadAll(resp.Body)
		_ = json.Unmarshal(b, &m)
		return resp.StatusCode, m
	}
	for _, p := range []string{"/api/v1/query/targets", "/api/v1/query/path-changes?agent=lab",
		"/api/v1/query/loops?from=2026-11-16T00:00:00Z", "/api/v1/query/outages?to=1800000000000"} {
		if code, m := get(p); code != 200 || len(m["rows"].([]any)) != 1 {
			t.Errorf("%s: %d %v", p, code, m)
		}
	}
	code, m := get("/api/v1/query/cycles?agent=lab&target=dc1.example")
	if code != 200 {
		t.Fatalf("cycles: %d %v", code, m)
	}
	hops := m["rows"].([]any)[0].(map[string]any)["hops"].([]any)
	h0, h1 := hops[0].(map[string]any), hops[1].(map[string]any)
	if h0["code"] != 0.0 || h0["name"] != "gw.lan" || h1["code"] != nil || h1["asn"] != 64500.0 {
		t.Errorf("hops %v", hops)
	}
	for _, p := range []string{"/api/v1/query/cycles?agent=lab", "/api/v1/query/targets?limit=0",
		"/api/v1/query/cycles?agent=lab&target=dc1.example&limit=1001",
		"/api/v1/query/targets?from=yesterday", "/api/v1/query/targets?from=2026-11-17T00:00:00Z&to=2026-11-16T00:00:00Z"} {
		if code, _ := get(p); code != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", p, code)
		}
	}
	// Every read query uses typed parameters, never interpolated values.
	if _, _ = get("/api/v1/query/targets?agent=x'%20OR%201=1--"); true {
		for _, q := range st.queries {
			if strings.Contains(q, "OR 1=1") {
				t.Errorf("parameter interpolated into SQL: %s", q)
			}
		}
	}
}

// A batch far below the agent's stored seqs is refused (409), not silently
// dropped as duplicates: the agent keeps it and its operator notices.
func TestStaleBatchRefused(t *testing.T) {
	st := newFakeStore()
	_, ts := newTestServerAt(t, st, t0.Add((staleWindow+50)*5*time.Second)) // the first record's ts
	if code, res := post(t, ts.URL, "", ndjson(mk("lab", staleWindow+50, pathA...))); code != 200 || res.Accepted != 1 {
		t.Fatalf("first: %d %+v", code, res)
	}
	if code, res := post(t, ts.URL, "", ndjson(mk("lab", staleWindow+50, pathA...))); code != 200 || res.Duplicates != 1 {
		t.Errorf("retry: %d %+v", code, res)
	}
	if code, _ := post(t, ts.URL, "", ndjson(mk("lab", 1, pathA...), mk("lab", 2, pathA...))); code != http.StatusConflict {
		t.Errorf("stale batch: %d", code)
	}
}

// A batch far below the agent's mark that the server holds cycle for
// cycle (seq and ts) is one it stored before, delivered again: answered as
// duplicates, not refused (review, round eight). Since then an agent's
// seqs jump at every open of its spool (they go on from the clock), so the
// backlog of a spool that went back to an earlier state, or a retry cut
// shorter, can lie that far below. A batch with a seq the server holds as
// another cycle (another ts), or does not hold, is still refused.
func TestStaleRedeliveryIsDuplicates(t *testing.T) {
	st := newFakeStore()
	_, ts := newTestServerAt(t, st, t0.Add((staleWindow+50)*5*time.Second))
	old := func() []*record.Record { return []*record.Record{mk("lab", 10, pathA...), mk("lab", 11, pathA...)} }
	if code, res := post(t, ts.URL, "", ndjson(old()...)); code != 200 || res.Accepted != 2 {
		t.Fatalf("first: %d %+v", code, res)
	}
	if code, res := post(t, ts.URL, "", ndjson(mk("lab", staleWindow+50, pathA...))); code != 200 || res.Accepted != 1 {
		t.Fatalf("after the jump: %d %+v", code, res)
	}
	if code, res := post(t, ts.URL, "", ndjson(old()...)); code != 200 || res.Duplicates != 2 {
		t.Errorf("delivered again: %d %+v, want 200 and 2 duplicates", code, res)
	}
	other := old()
	other[1].TS = other[1].TS.Add(time.Second) // another cycle at a seq the server holds
	for name, recs := range map[string][]*record.Record{
		"another cycle": other,
		"one not held":  {mk("lab", 10, pathA...), mk("lab", 12, pathA...)},
		"none held":     {mk("lab", 12, pathA...)},
	} {
		if code, _ := post(t, ts.URL, "", ndjson(recs...)); code != http.StatusConflict {
			t.Errorf("%s: %d, want 409", name, code)
		}
	}
	if n := len(st.table("cycles")); n != 3 {
		t.Errorf("%d cycles stored, want 3", n)
	}
}

// Stream state is bounded: idle streams and agents are dropped, and the
// least recently used beyond the limit.
func TestIngesterStateBounded(t *testing.T) {
	st := newFakeStore()
	in := NewIngester(st, fakeEnrich{}, quietLogger(), nil)
	now := t0
	in.now = func() time.Time { return now }
	in.maxStreams = 10
	ingest := func(agent string, seq uint64, target string) {
		t.Helper()
		r := mk(agent, seq, pathA...)
		r.Target = target
		if _, err := in.Ingest(t.Context(), []*record.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 30 {
		ingest("lab", uint64(i+1), fmt.Sprintf("t%d.example", i))
		now = now.Add(2 * time.Second)
	}
	if n := in.nstreams.Load(); n > 10 {
		t.Errorf("%d streams tracked, max 10", n)
	}
	// Everything idle for longer than the TTL goes, agents included.
	now = now.Add(in.idleTTL + 2*sweepInterval)
	ingest("other", 1, "x.example")
	in.mu.Lock()
	_, kept := in.agents["lab"]
	in.mu.Unlock()
	if kept || in.nstreams.Load() != 1 {
		t.Errorf("after idling: lab kept=%v, %d streams", kept, in.nstreams.Load())
	}
	// An evicted agent resumes from storage: its high-water mark is reloaded.
	ingest("lab", 30, "t29.example")
	r, err := in.Ingest(t.Context(), []*record.Record{mk("lab", 29, pathA...)})
	if err != nil || r.Duplicates != 1 {
		t.Errorf("after eviction, an old seq is a duplicate: %+v %v", r, err)
	}
}
