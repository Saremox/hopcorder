//go:build clickhouse

package server

import (
	"log/slog"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// Restores read a bounded window of ClickHouse (the sort key is agent,
// target, protocol, ts; without a ts bound a restore read the whole history
// of every flow): first the day before the new cycle, then, if that holds
// none of the stream's cycles, max_record_age. An outage left open is
// resumed even when its cycles lie beyond both.
func TestRestoreWindowClickHouse(t *testing.T) {
	c := testClickHouse(t)
	at := func(seq uint64, ts time.Time, spec ...string) *record.Record {
		r := mk("lab", seq, spec...)
		r.TS = ts
		return r
	}
	ingest := func(in *Ingester, recs ...*record.Record) {
		t.Helper()
		if _, err := in.Ingest(t.Context(), recs); err != nil {
			t.Fatal(err)
		}
	}
	fresh := func() *Ingester {
		in := NewIngester(c, nil, slog.New(slog.DiscardHandler), nil)
		in.maxRecordAge = 7 * 24 * time.Hour
		return in
	}
	// Cycles three days before the next one: found by the second query, so
	// the new router at TTL 2 is a change.
	ingest(fresh(), at(1, t0.Add(-72*time.Hour), pathA...), at(2, t0.Add(-72*time.Hour+time.Minute), pathA...))
	ingest(fresh(), at(3, t0, pathB...))
	if n := count(t, c, "SELECT count() AS n FROM path_changes FINAL WHERE seq = 3 AND prev_seq = 2"); n != 1 {
		t.Errorf("change across a three-day gap: %d rows", n)
	}
	// An outage opened ten days before (beyond max_record_age): nothing is
	// replayed, but the open outage is resumed and closed, not left open.
	r := at(4, t0.Add(-240*time.Hour), brokenA...)
	r.Target = "dc2.example"
	ingest(fresh(), r)
	r = at(5, t0, pathA...)
	r.Target = "dc2.example"
	ingest(fresh(), r)
	if n := count(t, c, "SELECT count() AS n FROM outages FINAL WHERE target = 'dc2.example' AND start_seq = 4 AND end_seq = 5"); n != 1 {
		t.Errorf("outage beyond the window not closed: %d rows", n)
	}
	if n := count(t, c, "SELECT count() AS n FROM outages FINAL WHERE end_ts IS NULL"); n != 0 {
		t.Errorf("%d outages left open", n)
	}
}

// hops_mv copied a cycle's loops onto every hop row to look up each hop's
// loop TTLs: an 8 MiB insert of valid records with 255 hops and 255 loops
// of 255 TTLs needed over 1.6 GiB in ClickHouse 25.8 and failed. The TTLs
// now come with each hop, and an insert is sized by what the view writes
// too: the same records need a few tens of MiB, and the hop rows are as
// before.
func TestLoopRecordsInsertClickHouse(t *testing.T) {
	c := testClickHouse(t)
	in := NewIngester(c, nil, slog.New(slog.DiscardHandler), nil)
	spec := make([]string, record.MaxHops)
	ttls := make([]int, record.MaxHops)
	text := make([]string, record.MaxHops)
	for i := range spec {
		spec[i], ttls[i], text[i] = "198.18.120.1", i+1, strconv.Itoa(i+1)
	}
	var recs []*record.Record
	for seq := uint64(1); seq <= 20; seq++ {
		r := mk("lab", seq, spec...)
		r.Loops = make([]record.Loop, record.MaxHops)
		for i := range r.Loops {
			r.Loops[i] = record.Loop{IP: netip.MustParseAddr("198.18.120.1"), TTLs: ttls}
		}
		recs = append(recs, r)
	}
	if _, err := in.Ingest(t.Context(), recs); err != nil {
		t.Fatal(err)
	}
	if n := count(t, c, "SELECT count() AS n FROM path_probes WHERE is_loop = 1 AND loop_ttls = '"+
		strings.Join(text, ",")+"'"); n != 20*record.MaxHops {
		t.Errorf("%d looping hop rows, want %d", n, 20*record.MaxHops)
	}
	if err := c.ExecServer(t.Context(), "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	type row struct {
		N   int   `json:"n"`
		Mem int64 `json:"mem"`
	}
	rows, err := clickhouse.QueryInto[row](t.Context(), c, `SELECT count() AS n, max(memory_usage) AS mem
		FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish'
		AND query LIKE 'INSERT INTO `+"`cycles`"+`%'`, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cycles inserts: %+v", rows)
	if len(rows) != 1 || rows[0].N < 2 || rows[0].Mem > 256<<20 {
		t.Errorf("cycles inserts: %+v (want at least 2, each under 256 MiB)", rows)
	}
}
