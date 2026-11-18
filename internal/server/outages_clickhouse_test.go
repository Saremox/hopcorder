//go:build clickhouse

package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/saremox/hopcorder/dashboards"
	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// An ongoing outage reports the cycles it has so far, in the read API and
// in Grafana's Outages panel, across a restart; once it closes, the close
// row is what FINAL reads, even when an open row lands after it (review
// round seven: the open row was written once, with 1 cycle, until the
// outage closed). The review's scenario: 50 cycles that reach the
// destination, then 100 that do not, in batches of 10.
func TestOpenOutageCyclesClickHouse(t *testing.T) {
	c := testClickHouse(t)
	in := NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
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
	s := New(Options{Store: c, Pinger: c, Enricher: fakeEnrich{}, Logger: slog.New(slog.DiscardHandler)})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	outages := func() string {
		t.Helper()
		resp, err := http.Get(ts.URL + "/api/v1/query/outages?agent=lab&target=dc1.example&from=2026-11-16T22:00:00Z&to=2026-11-17T01:00:00Z")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var body struct {
			Rows []map[string]any `json:"rows"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("outages: %d %v", resp.StatusCode, err)
		}
		var out string
		for _, r := range body.Rows {
			out += fmt.Sprintf("[%v to %v (%v): %v cycles]", r["start_seq"], r["end_seq"], r["end_ts"], r["cycles"])
		}
		return out
	}

	ingest(1, 50, pathA)
	for from := uint64(51); from <= 150; from += 10 {
		if from == 101 {
			in = NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil) // a restart
		}
		ingest(from, from+9, brokenA)
		if got, want := outages(), fmt.Sprintf("[51 to 0 (<nil>): %d cycles]", from+9-50); got != want {
			t.Fatalf("after seq %d: outages %s, want %s", from+9, got, want)
		}
	}

	d, err := dashboards.Load("hopcorder.json")
	if err != nil {
		t.Fatal(err)
	}
	panel := false
	for _, q := range d.Queries() {
		if q.Where != "panel Outages" {
			continue
		}
		panel = true
		sql := dashboards.Expand(q.SQL, map[string]string{"agent": "lab"}, t0.Add(-time.Hour), t0.Add(2*time.Hour), time.Minute)
		rows, err := clickhouse.QueryInto[map[string]any](t.Context(), c, sql, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0]["end"] != nil || rows[0]["cycles"] != 100.0 {
			t.Errorf("Grafana's Outages panel: %v, want one open outage of 100 cycles", rows)
		}
	}
	if !panel {
		t.Error("no Outages panel in the dashboard")
	}

	ingest(151, 151, pathA)
	const want = "[51 to 151 (2026-11-16T23:12:35.000Z): 100 cycles]"
	if got := outages(); got != want {
		t.Errorf("closed: outages %s, want %s", got, want)
	}
	// The newest open row inserted again after the close row, as an insert
	// that outlived its timeout would land: the close row's version is
	// higher, so FINAL still reads it, and so do merges.
	if err := c.Exec(t.Context(), `INSERT INTO outages SELECT * FROM outages WHERE end_ts IS NULL
		ORDER BY version DESC LIMIT 1`, nil); err != nil {
		t.Fatal(err)
	}
	if n := count(t, c, "SELECT count() AS n FROM outages WHERE end_ts IS NULL AND cycles = 100"); n != 2 {
		t.Fatalf("%d open rows of 100 cycles, want 2 (the ingester's and the late one)", n)
	}
	if got := outages(); got != want {
		t.Errorf("with an open row inserted late: outages %s, want %s", got, want)
	}
	if err := c.Exec(t.Context(), "OPTIMIZE TABLE outages FINAL", nil); err != nil {
		t.Fatal(err)
	}
	if n := count(t, c, "SELECT count() AS n FROM outages WHERE end_seq = 151 AND cycles = 100"); n != 1 ||
		count(t, c, "SELECT count() AS n FROM outages") != 1 {
		t.Errorf("after merging: not just the close row left")
	}
}
