//go:build clickhouse

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/record"
)

// A query whose caller gave up stops in ClickHouse. ClickHouse cancels HTTP
// queries on disconnect only with readonly > 0; without it this one ran on
// until max_execution_time (25 s here), and a reader who disconnected at
// once could keep any number of queries running past query.max_concurrent.
func TestQueryStopsWhenCallerGivesUp(t *testing.T) {
	c := testClickHouse(t)
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	tag := "cancel_" + hex.EncodeToString(b)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	err := c.Query(ctx, "SELECT sum(number) FROM numbers(1000000000000) /* "+tag+" */", nil,
		func(json.RawMessage) error { return nil })
	if err == nil {
		t.Fatal("a query over 10^12 rows finished in half a second")
	}
	running := "SELECT count() AS n FROM system.processes WHERE query LIKE '%" + tag + "%' AND query NOT LIKE '%system.processes%'"
	deadline := time.Now().Add(10 * time.Second)
	for count(t, c, running) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("the query still runs 10 s after its caller gave up")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// The read API's settings (readonly, memory and result limits) are accepted
// by ClickHouse, and a response that does not fit its budget comes back
// truncated, newest rows first, as valid JSON.
func TestReadBudgetClickHouse(t *testing.T) {
	c := testClickHouse(t)
	long := make([]string, 0, 40)
	for i := range 39 {
		long = append(long, fmt.Sprintf("198.18.%d.%d", i, i+1))
	}
	long = append(long, "=192.0.2.9")
	var recs []*record.Record
	for seq := uint64(1); seq <= 200; seq++ {
		recs = append(recs, mk("lab", seq, long...))
	}
	if _, err := NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil).Ingest(t.Context(), recs); err != nil {
		t.Fatal(err)
	}
	get := func(budget int64, path string, v any) {
		t.Helper()
		s := New(Options{Store: c, Pinger: c, Enricher: fakeEnrich{}, Logger: slog.New(slog.DiscardHandler),
			MaxResponseBytes: budget})
		ts := httptest.NewServer(s.Handler())
		defer ts.Close()
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, b)
		}
		if err := json.Unmarshal(b, v); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	const cycles = "/api/v1/query/cycles?agent=lab&target=dc1.example&from=2026-11-16T00:00:00Z&to=2026-11-18T00:00:00Z&limit=1000"
	type page struct {
		Rows []struct {
			Seq  uint64            `json:"seq"`
			Hops []json.RawMessage `json:"hops"`
		} `json:"rows"`
		Truncated bool `json:"truncated"`
	}
	var all, cut page
	get(DefaultMaxResponseBytes, cycles, &all)
	get(64<<10, cycles, &cut)
	if len(all.Rows) != 200 || all.Truncated || len(all.Rows[0].Hops) != 40 {
		t.Errorf("whole: %d rows, truncated %v", len(all.Rows), all.Truncated)
	}
	if len(cut.Rows) == 0 || len(cut.Rows) >= 200 || !cut.Truncated || cut.Rows[0].Seq != 200 ||
		cut.Rows[len(cut.Rows)-1].Seq != uint64(201-len(cut.Rows)) {
		t.Errorf("cut: %d rows, truncated %v", len(cut.Rows), cut.Truncated)
	}
	var topo struct {
		Truncated bool              `json:"truncated"`
		Nodes     []json.RawMessage `json:"nodes"`
		Edges     []json.RawMessage `json:"edges"`
		Timeline  []json.RawMessage `json:"timeline"`
	}
	get(DefaultMaxResponseBytes, "/api/v1/query/topology?agent=lab&target=dc1.example&from=2026-11-16T00:00:00Z&to=2026-11-18T00:00:00Z", &topo)
	if topo.Truncated || len(topo.Nodes) != 40 || len(topo.Edges) != 39 || len(topo.Timeline) == 0 {
		t.Errorf("topology: truncated %v, %d nodes, %d edges, %d buckets", topo.Truncated, len(topo.Nodes),
			len(topo.Edges), len(topo.Timeline))
	}
}
