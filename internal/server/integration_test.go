//go:build clickhouse

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/output"
	"github.com/saremox/hopcorder/internal/record"
)

// These tests need a real ClickHouse:
//
//	HOPCORDER_CLICKHOUSE_URL=http://127.0.0.1:8123 go test -tags clickhouse ./internal/server/
//
// Each test works in a fresh database that is dropped afterwards.

func testClickHouse(t *testing.T) *clickhouse.Client {
	t.Helper()
	url := os.Getenv("HOPCORDER_CLICKHOUSE_URL")
	if url == "" {
		t.Skip("HOPCORDER_CLICKHOUSE_URL not set")
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	db := "hopcorder_test_" + hex.EncodeToString(b)
	c, err := clickhouse.New(clickhouse.Config{URL: url, Database: db, User: os.Getenv("HOPCORDER_CLICKHOUSE_USER"),
		Password: os.Getenv("HOPCORDER_CLICKHOUSE_PASSWORD")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.ExecServer(context.Background(), "DROP DATABASE IF EXISTS "+clickhouse.QuoteIdent(db)); err != nil {
			t.Errorf("drop test database: %v", err)
		}
	})
	if err := Migrate(t.Context(), c, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	return c
}

func count(t *testing.T, c *clickhouse.Client, query string) int {
	t.Helper()
	type row struct {
		N int `json:"n"`
	}
	rows, err := clickhouse.QueryInto[row](t.Context(), c, query, nil)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return rows[0].N
}

func TestMigrateIdempotent(t *testing.T) {
	c := testClickHouse(t)
	if err := Migrate(t.Context(), c, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	migs, _ := loadMigrations()
	if n := count(t, c, "SELECT count() AS n FROM schema_migrations"); n != len(migs) {
		t.Errorf("%d migrations recorded, want %d", n, len(migs))
	}
	for _, tbl := range []string{"cycles", "hops", "loop_events", "agent_seq", "path_changes", "outages"} {
		if n := count(t, c, "SELECT count() AS n FROM system.tables WHERE database = currentDatabase() AND name = '"+tbl+"'"); n != 1 {
			t.Errorf("table %s missing", tbl)
		}
	}
}

func TestIngestEndToEndClickHouse(t *testing.T) {
	c := testClickHouse(t)
	newSrv := func() *httptest.Server {
		s := New(Options{Store: c, Pinger: c, Enricher: fakeEnrich{}, Logger: slog.New(slog.DiscardHandler)})
		s.SetReady()
		ts := httptest.NewServer(s.Handler())
		t.Cleanup(ts.Close)
		return ts
	}
	ts := newSrv()
	h, err := output.NewHTTP(output.HTTPConfig{URL: ts.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	loop := mk("lab", 3, "10.0.0.1", "198.18.120.1", "198.18.120.2", "198.18.120.1", "198.18.120.2", "*")
	loop.Hops[1].MPLS = []record.MPLSLabel{{Label: 24005, S: true, TTL: 1}}
	batch1 := lines(mk("lab", 1, pathA...), mk("lab", 2, flaky...), loop)
	if err := h.Deliver(t.Context(), batch1); err != nil {
		t.Fatal(err)
	}
	// Retry with more appended: 1-3 are duplicates.
	batch2 := slices.Concat(batch1, lines(mk("lab", 4, pathA...), mk("lab", 5, brokenA...), mk("lab", 6, brokenA...)))
	if err := h.Deliver(t.Context(), batch2); err != nil {
		t.Fatal(err)
	}

	// Restart: a new server restores the open outage and the tracker.
	ts2 := newSrv()
	h2, _ := output.NewHTTP(output.HTTPConfig{URL: ts2.URL})
	defer h2.Close()
	if err := h2.Deliver(t.Context(), lines(mk("lab", 6, brokenA...), mk("lab", 7, pathB...))); err != nil {
		t.Fatal(err)
	}

	checks := map[string]int{
		"SELECT count() AS n FROM cycles":                                                7,
		"SELECT count() AS n FROM hops":                                                  3 + 3 + 6 + 3 + 4 + 4 + 3,
		"SELECT count() AS n FROM hops WHERE hop_asn = 64510":                             1 + 4 + 1 + 2 + 1, // 198.18.x hops
		"SELECT count() AS n FROM hops WHERE hop_name = 'gw.lan'":                        7,
		"SELECT count() AS n FROM hops WHERE length(mpls) = 1 AND mpls[1].label = 24005": 1,
		"SELECT count() AS n FROM loop_events":                                           2,
		"SELECT count() AS n FROM path_changes FINAL":                                    3, // into the loop (3), out of it (4), new router (7)
		"SELECT count() AS n FROM outages FINAL":                                         2, // the loop (3) and 5-6
		"SELECT count() AS n FROM outages FINAL WHERE end_seq = 7 AND cycles = 2":        1,
		"SELECT max(max_seq) AS n FROM agent_seq":                                        7,
	}
	for q, want := range checks {
		if got := count(t, c, q); got != want {
			t.Errorf("%s = %d, want %d", q, got, want)
		}
	}

	// Every read-API endpoint runs against the real schema.
	for _, p := range []string{
		"/api/v1/query/targets?from=2026-11-16T00:00:00Z&to=2026-11-17T00:00:00Z",
		"/api/v1/query/cycles?agent=lab&target=dc1.example&from=2026-11-16T00:00:00Z&to=2026-11-17T00:00:00Z",
		"/api/v1/query/path-changes?agent=lab&from=2026-11-16T00:00:00Z&to=2026-11-17T00:00:00Z",
		"/api/v1/query/loops?from=2026-11-16T00:00:00Z&to=2026-11-17T00:00:00Z",
		"/api/v1/query/outages?from=2026-11-16T00:00:00Z&to=2026-11-17T00:00:00Z",
	} {
		resp, err := http.Get(ts2.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Rows  []map[string]any `json:"rows"`
			Error string           `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || len(body.Rows) == 0 {
			t.Errorf("%s: %d %q rows=%d", p, resp.StatusCode, body.Error, len(body.Rows))
			continue
		}
		t.Logf("%s first row: %v", p, body.Rows[0])
	}
	resp, err := http.Get(ts2.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("readyz: %d", resp.StatusCode)
	}
}
