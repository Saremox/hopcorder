//go:build clickhouse

package server

import (
	"log/slog"
	"testing"
	"time"

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
