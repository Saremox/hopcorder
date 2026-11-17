package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// measureStore stores nothing and, at each insert, notes its size and the
// live heap (after a collection) while the insert's body exists, as the
// ClickHouse client builds it.
type measureStore struct {
	nullStore
	mu    sync.Mutex
	peak  uint64
	sizes map[string][]int
}

func (m *measureStore) InsertJSON(_ context.Context, table string, rows [][]byte) error {
	var body bytes.Buffer
	for _, r := range rows {
		body.Write(r)
		body.WriteByte('\n')
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.mu.Lock()
	m.peak = max(m.peak, ms.HeapAlloc)
	m.sizes[table] = append(m.sizes[table], body.Len())
	m.mu.Unlock()
	runtime.KeepAlive(body.Bytes())
	return nil
}

// flipRecord is the cheapest record that derives events (fourth security
// review): its target address alternates, so every record is a route
// change, and it reaches its destination every other time, so every record
// opens or closes an outage. Before, it needed no hops at all: reached was
// not checked against them.
func flipRecord(seq uint64) *record.Record {
	r := &record.Record{Schema: record.SchemaVersion, AgentID: "a", Seq: seq, TS: t0.Add(time.Duration(seq) * time.Millisecond),
		Target: "t", TargetIP: netip.MustParseAddr("192.0.2.10"), AF: 4, Protocol: "icmp", Hops: []record.Hop{}}
	if seq%2 == 0 {
		r.TargetIP = netip.MustParseAddr("192.0.2.9")
		r.Hops = []record.Hop{{TTL: 1, Status: record.StatusReply, IP: r.TargetIP, RTTMicros: 1}}
	}
	r.Finalize(2)
	return r
}

// One request of max_decoded_bytes of such records made 208k path changes
// and outage rows, which went to ClickHouse in one insert per table (48 and
// 43 MiB), all held at once: 213 MiB live beyond the body, and four such
// requests OOM-killed the server under the chart's 1 GiB. Events are now
// derived, inserted and dropped part by part, every insert within
// maxInsertBytes, and the request stays within three times its decoded
// size plus a few parts.
func TestRequestMemoryEventFlood(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and stores 32 MiB requests")
	}
	// The review's second shape: two IPv6 paths of 255 hops in turn, a
	// route change at every TTL in every record (path changes of 20 KB).
	v6 := func(seq uint64) *record.Record {
		r := &record.Record{Schema: record.SchemaVersion, AgentID: "lab", Seq: seq, TS: t0.Add(time.Duration(seq) * time.Millisecond),
			Target: "dc1.example", TargetIP: netip.MustParseAddr("2001:db8::9"), AF: 6, Protocol: "icmp"}
		for i := range record.MaxHops {
			r.Hops = append(r.Hops, record.Hop{TTL: i + 1, Status: record.StatusTTLExpired, RTTMicros: 1,
				IP: netip.MustParseAddr(fmt.Sprintf("2001:1db8:aaaa:bbbb:cccc:dddd:%04x:%04x", 0x1000+seq%2, 0x1000+i))})
		}
		r.Finalize(2)
		return r
	}
	for name, mkRec := range map[string]func(uint64) *record.Record{"flip": flipRecord, "v6 route changes": v6} {
		t.Run(name, func(t *testing.T) {
			st := &measureStore{sizes: map[string][]int{}}
			s := New(Options{Store: st, Logger: quietLogger()})
			s.ingester.now = func() time.Time { return t0.Add(time.Hour) }
			var b bytes.Buffer
			for seq := uint64(1); ; seq++ {
				line, _ := json.Marshal(mkRec(seq))
				if b.Len()+len(line)+1 > DefaultMaxDecodedBytes {
					break
				}
				b.Write(line)
				b.WriteByte('\n')
			}
			raw := b.Bytes()
			runtime.GC()
			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)
			recs, res, code, _ := s.decode("", bytes.NewReader(raw))
			if code != 0 || res.Rejected != 0 {
				t.Fatalf("decode: %d %v", code, res.Errors)
			}
			runtime.GC()
			runtime.ReadMemStats(&m1)
			ir, err := s.ingester.Ingest(t.Context(), recs)
			if err != nil {
				t.Fatal(err)
			}
			mib := func(a, b uint64) float64 { return float64(int64(a)-int64(b)) / (1 << 20) }
			beyond := mib(st.peak, m0.HeapAlloc)
			t.Logf("%d records (%.1f MiB, %.0f MiB decoded), peak live beyond the body %.0f MiB; "+
				"inserts: %d cycles, %d path_changes, %d outages", ir.Accepted, float64(len(raw))/(1<<20),
				mib(m1.HeapAlloc, m0.HeapAlloc), beyond, len(st.sizes["cycles"]), len(st.sizes["path_changes"]),
				len(st.sizes["outages"]))
			if want := len(recs); ir.Accepted != want {
				t.Errorf("%d accepted, want %d", ir.Accepted, want)
			}
			for table, sizes := range st.sizes {
				if n := slices.Max(sizes); n > maxInsertBytes+64<<10 {
					t.Errorf("an insert into %s of %d bytes, more than %d", table, n, maxInsertBytes)
				}
			}
			if len(st.sizes["path_changes"]) < 2 {
				t.Errorf("events in %d inserts: not per part", len(st.sizes["path_changes"]))
			}
			// The records, up to three times their decoded size, and
			// while a part is stored its rows, its events and an insert's
			// body.
			if limit := 3*float64(DefaultMaxDecodedBytes)/(1<<20) + 32; beyond > limit {
				t.Errorf("one request held %.0f MiB beyond its body, more than %.0f MiB", beyond, limit)
			}
			runtime.KeepAlive(raw)
			runtime.KeepAlive(recs)
		})
	}
}

// The flip records' smallest form is valid only as an agent sends it.
func TestFlipRecordsValid(t *testing.T) {
	for seq := uint64(1); seq <= 2; seq++ {
		if err := flipRecord(seq).Validate(); err != nil {
			t.Errorf("seq %d: %v", seq, err)
		}
	}
	r := flipRecord(1)
	r.Reached = true // what the review sent: reached, without a reply
	if r.Validate() == nil {
		t.Error("reached without hops accepted")
	}
}

// commitFailStore is a fakeStore whose chosen cycles inserts store their
// rows and then fail, as when a view fails after ClickHouse committed the
// cycles part, or the client gives up on an insert that commits later.
type commitFailStore struct {
	*fakeStore
	mu      sync.Mutex
	inserts int
	failAt  map[int]bool // cycles inserts (1-based) that fail after storing
}

func (c *commitFailStore) InsertJSON(ctx context.Context, table string, rows [][]byte) error {
	if err := c.fakeStore.InsertJSON(ctx, table, rows); err != nil || table != "cycles" {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inserts++
	if c.failAt[c.inserts] {
		return errors.New("clickhouse: code 241: memory limit exceeded: while pushing to view hops_mv")
	}
	return nil
}

// outageParamStore is a fakeStore that keeps the parameters of its queries.
type outageParamStore struct {
	*fakeStore
	mu     sync.Mutex
	params map[string][]clickhouse.Params // by query
}

func (p *outageParamStore) Query(ctx context.Context, q string, params clickhouse.Params, row func(json.RawMessage) error) error {
	p.mu.Lock()
	p.params[q] = append(p.params[q], params)
	p.mu.Unlock()
	return p.fakeStore.Query(ctx, q, params, row)
}

// A restore looks for the stream's open outage only after the last cycle
// it replayed that reached the destination: the outage started after it.
// The lookup used to read every outage of the stream, one per two cycles
// for a stream that flaps (or that someone makes flap).
func TestRestoreOutageLookupBounded(t *testing.T) {
	st := &outageParamStore{fakeStore: newFakeStore(), params: map[string][]clickhouse.Params{}}
	in := NewIngester(st, nil, quietLogger(), nil)
	ingestOK := func(recs ...*record.Record) {
		t.Helper()
		if _, err := in.Ingest(t.Context(), recs); err != nil {
			t.Fatal(err)
		}
	}
	ingestOK(mk("lab", 1, pathA...), mk("lab", 2, brokenA...), mk("lab", 3, pathA...), mk("lab", 4, brokenA...),
		mk("lab", 5, brokenA...))
	in = NewIngester(st, nil, quietLogger(), nil) // a restart: the outage open since 4 is restored
	ingestOK(mk("lab", 6, brokenA...), mk("lab", 7, pathA...))
	var lookups []clickhouse.Params
	for q, ps := range st.params {
		for _, p := range ps {
			if strings.Contains(q, "FROM outages FINAL") && p["before"] == "6" {
				lookups = append(lookups, p)
			}
		}
	}
	if len(lookups) != 1 || lookups[0]["after"] != "3" {
		t.Errorf("open-outage lookups %v, want one after seq 3", lookups)
	}
	if got := outagesOf(st.table("outages")); got[4] != "end 7, 3 cycles" || len(got) != 2 {
		t.Errorf("outages %v", got)
	}
}

// outagesOf returns the latest version of each outage, as FINAL reads them.
func outagesOf(rows []map[string]any) map[uint64]string {
	latest := map[uint64]map[string]any{}
	for _, o := range rows {
		if cur, ok := latest[num(o["start_seq"])]; !ok || num(o["version"]) >= num(cur["version"]) {
			latest[num(o["start_seq"])] = o
		}
	}
	out := map[uint64]string{}
	for s, o := range latest {
		out[s] = fmt.Sprintf("end %d, %d cycles", num(o["end_seq"]), num(o["cycles"]))
	}
	return out
}

// lab is the batch of seqs from to to of agent "lab": a route change at 6
// and an outage from 8 to 11.
func lab(from, to uint64) []*record.Record {
	var recs []*record.Record
	for seq := from; seq <= to; seq++ {
		switch {
		case seq >= 8 && seq < 12:
			recs = append(recs, mk("lab", seq, brokenB...))
		case seq >= 6:
			recs = append(recs, mk("lab", seq, pathB...))
		default:
			recs = append(recs, mk("lab", seq, pathA...))
		}
	}
	return recs
}

// A cycles insert that fails after ClickHouse committed it: the retry used
// to be answered "duplicates" (the mark came from a view of cycles) and the
// events of those cycles were never derived; or, had the mark not moved,
// they were stored twice. Now the retry stops what may still run, finds
// them stored, fills in their hop and loop rows and derives their events,
// once, whatever the retry's boundaries and across a restart.
func TestRetryAfterCommittedFailure(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			ref := newFakeStore()
			if _, err := NewIngester(ref, fakeEnrich{}, quietLogger(), nil).Ingest(t.Context(), lab(1, 14)); err != nil {
				t.Fatal(err)
			}
			st := &commitFailStore{fakeStore: newFakeStore(), failAt: map[int]bool{2: true}}
			in := NewIngester(st, fakeEnrich{}, quietLogger(), nil)
			if _, err := in.Ingest(t.Context(), lab(1, 4)); err != nil {
				t.Fatal(err)
			}
			if _, err := in.Ingest(t.Context(), lab(5, 12)); err == nil {
				t.Fatal("the failed insert was not reported")
			}
			if restart {
				in = NewIngester(st, fakeEnrich{}, quietLogger(), nil)
			}
			// The agent's retries: a part of the failed batch, then more.
			for _, b := range [][]*record.Record{lab(5, 9), lab(5, 14)} {
				if _, err := in.Ingest(t.Context(), b); err != nil {
					t.Fatal(err)
				}
			}
			var seqs []uint64
			for _, c := range st.table("cycles") {
				seqs = append(seqs, num(c["seq"]))
			}
			if want := "[1 2 3 4 5 6 7 8 9 10 11 12 13 14]"; fmt.Sprint(seqs) != want {
				t.Errorf("cycles stored %v, want %s", seqs, want)
			}
			if got, want := len(st.table("path_changes")), len(ref.table("path_changes")); got != want || got == 0 {
				t.Errorf("%d path changes, want %d", got, want)
			}
			if got, want := outagesOf(st.table("outages")), outagesOf(ref.table("outages")); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("outages %v, want %v", got, want)
			}
			var mark uint64
			for _, m := range st.table("agent_seq") {
				mark = max(mark, num(m["max_seq"]))
			}
			if mark != 14 {
				t.Errorf("stored mark %d, want 14", mark)
			}
			st.mu.Lock()
			defer st.mu.Unlock()
			kill := slices.IndexFunc(st.execs, func(q string) bool { return strings.HasPrefix(q, "KILL QUERY") })
			fill := slices.IndexFunc(st.execs, func(q string) bool { return strings.HasPrefix(q, "INSERT INTO hops") })
			if kill < 0 || fill < kill {
				t.Errorf("statements %q: want the agent's writes stopped, then its hop rows filled in", st.execs)
			}
		})
	}
}
