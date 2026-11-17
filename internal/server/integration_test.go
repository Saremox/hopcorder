//go:build clickhouse

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/dashboards"
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
	for _, tbl := range []string{"cycles", "hops", "loop_events", "agent_seq", "path_changes", "outages", "path_probes"} {
		if n := count(t, c, "SELECT count() AS n FROM system.tables WHERE database = currentDatabase() AND name = '"+tbl+"'"); n != 1 {
			t.Errorf("table %s missing", tbl)
		}
	}
}

func TestIngestEndToEndClickHouse(t *testing.T) {
	c := testClickHouse(t)
	newSrv := func() *httptest.Server {
		s := New(Options{Store: c, Pinger: c, Enricher: fakeEnrich{}, Logger: slog.New(slog.DiscardHandler)})
		s.ingester.now = func() time.Time { return t0.Add(time.Hour) } // the records' ts are fixed
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
		"SELECT count() AS n FROM hops WHERE hop_asn = 64510":                            1 + 4 + 1 + 2 + 1, // 198.18.x hops
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

// prototypeQueries returns the analysis queries of docs/prototype/schema.sql
// with only names substituted: the database, the targets and Grafana's time
// filter macro.
func prototypeQueries(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile("../../docs/prototype/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, stmt := range splitStatements(string(b)) {
		if !strings.HasPrefix(stmt, "SELECT") {
			continue
		}
		q := strings.NewReplacer(
			"netpath.path_probes", "path_probes",
			"'dc1.example'", "'dc1.example'",
			"'target-a'", "'dc1.example'",
			"'target-b'", "'dc2.example'",
			"$__timeFilter(ts)", "ts >= now() - INTERVAL 3650 DAY",
		).Replace(stmt)
		// The queries look back from now() (1 or 3 days), and the seeded
		// cycles are at the fixed t0: "now" is an hour after t0, or the
		// test failed a day after t0 (review, round seven).
		q = strings.ReplaceAll(q, "now()", seedNow)
		if strings.Contains(q, "neighbor(") {
			// The prototype's Q3 fails on ClickHouse 25.8 even against its
			// own netpath.path_probes table: "min(ts) AS ts" shadows the
			// column in the same subquery's WHERE (ILLEGAL_AGGREGATION).
			// Renaming the alias is the whole fix.
			q = regexp.MustCompile(`min\(ts\)\s+AS ts`).ReplaceAllString(q, "min(ts) AS first_ts")
			q = regexp.MustCompile(`SELECT\s+ts,\s+path,`).ReplaceAllString(q, "SELECT first_ts AS ts, path,")
			// neighbor() is deprecated in current ClickHouse but still works.
			q += " SETTINGS allow_deprecated_error_prone_window_functions = 1"
		}
		out = append(out, q)
	}
	return out
}

// seedNow is the time the prototype's queries run at: an hour after the
// seeded cycles (t0), whatever the clock says.
var seedNow = fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", t0.Add(time.Hour).Format("2006-01-02 15:04:05.000"))

// seedTwoTargets ingests cycles of agent "lab" to dc1.example and
// dc2.example: a stable path, a loop (dc1), an outage on both and a route
// change on both.
func seedTwoTargets(t *testing.T, c *clickhouse.Client) {
	t.Helper()
	in := NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
	var recs []*record.Record
	add := func(target string, spec ...string) {
		r := mk("lab", uint64(len(recs)+1), spec...)
		r.Target = target
		recs = append(recs, r)
	}
	for range 3 {
		add("dc1.example", pathA...)
		add("dc2.example", pathA...)
	}
	add("dc1.example", "10.0.0.1", "198.18.120.1", "198.18.120.2", "198.18.120.1", "198.18.120.2", "*") // loop
	add("dc1.example", brokenA...)
	add("dc2.example", brokenA...)
	add("dc1.example", pathB...)
	add("dc2.example", pathB...)
	if _, err := in.Ingest(t.Context(), recs); err != nil {
		t.Fatal(err)
	}
}

// The prototype's seven analysis queries run unchanged (but for names)
// against hopcorder data through the path_probes compatibility view.
func TestPrototypeQueriesRun(t *testing.T) {
	c := testClickHouse(t)
	seedTwoTargets(t, c)

	queries := prototypeQueries(t)
	if len(queries) != 7 {
		t.Fatalf("found %d prototype queries, want 7", len(queries))
	}
	for i, q := range queries {
		rows, err := clickhouse.QueryInto[map[string]any](t.Context(), c, q, nil)
		if err != nil {
			t.Errorf("Q%d failed: %v\n%s", i+1, err, q)
			continue
		}
		if len(rows) == 0 {
			t.Errorf("Q%d returned no rows", i+1)
			continue
		}
		t.Logf("Q%d: %d rows, first %v", i+1, len(rows), rows[0])
	}

	// Loop columns on the per-hop rows, as the prototype has them.
	if n := count(t, c, "SELECT count() AS n FROM path_probes WHERE is_loop = 1 AND loop_ttls IN ('2,4', '3,5')"); n != 4 {
		t.Errorf("%d looping hop rows, want 4", n)
	}
}

// Every query of the Grafana dashboards (panels, variables, annotations)
// runs against the real schema, expanded as Grafana and the ClickHouse
// plugin would, and finds the seeded data.
func TestDashboardQueriesRun(t *testing.T) {
	c := testClickHouse(t)
	seedTwoTargets(t, c)
	vars := map[string]string{"datasource": "hopcorder-clickhouse", "agent": "lab", "target": "dc1.example",
		"protocol": "icmp", "target_b": "dc2.example"}
	for _, name := range dashboards.Names() {
		d, err := dashboards.Load(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range d.Queries() {
			sql := dashboards.Expand(q.SQL, vars, t0.Add(-time.Hour), t0.Add(time.Hour), time.Minute)
			rows, err := clickhouse.QueryInto[map[string]any](t.Context(), c, sql, nil)
			if err != nil {
				t.Errorf("%s %s: %v\n%s", name, q.Where, err, sql)
				continue
			}
			if len(rows) == 0 {
				t.Errorf("%s %s: no rows\n%s", name, q.Where, sql)
				continue
			}
			t.Logf("%s: %d rows, first %v", q.Where, len(rows), rows[0])
			check := func(ok bool) {
				if !ok {
					t.Errorf("%s: unexpected rows %v", q.Where, rows)
				}
			}
			switch q.Where {
			case "panel Where failures stop":
				// On dc1, brokenA stops after 198.18.120.1 at TTL 2, the loop
				// after 198.18.120.2 at TTL 5.
				stops := map[string]any{}
				for _, r := range rows {
					stops[fmt.Sprint(r["ttl"], " ", r["hop"], " ", r["as_name"])] = r["failed_cycles"]
				}
				check(len(stops) == 2 && stops["2 198.18.120.1 EXAMPLE-TRANSIT"] == float64(1) && stops["5 198.18.120.2 EXAMPLE-TRANSIT"] == float64(1))
			case "panel Route changes":
				// Into the loop, out of it, then pathA to pathB.
				check(len(rows) == 3 && rows[0]["before"] != "" && rows[0]["after"] != "" && rows[0]["changed_hops"] != "")
			case "panel Loops":
				check(len(rows) == 2 && rows[0]["loop_ttls"] != "")
			case "panel Outages":
				// dc1: the loop and brokenA in a row, 2 cycles; dc2: brokenA.
				check(len(rows) == 2 && rows[1]["target"] == "dc1.example" && rows[1]["cycles"] == float64(2))
			case "variable target_b":
				check(len(rows) == 1 && rows[0]["target"] == "dc2.example")
			}
		}
	}
}

// The UI's topology aggregation against the real schema: nodes per TTL and
// address, edges between consecutive answering hops (across silent TTLs),
// and the reachability timeline.
func TestTopologyClickHouse(t *testing.T) {
	c := testClickHouse(t)
	seedTwoTargets(t, c)
	flaky := mk("lab", 12, flaky...)
	flaky.Target = "dc3.example"
	if _, err := NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil).Ingest(t.Context(), []*record.Record{flaky}); err != nil {
		t.Fatal(err)
	}
	s := New(Options{Store: c, Pinger: c, Enricher: fakeEnrich{}, Logger: slog.New(slog.DiscardHandler)})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)

	type topo struct {
		BucketMS  int64 `json:"bucket_ms"`
		Truncated bool  `json:"truncated"`
		Nodes     []struct {
			TTL         int    `json:"ttl"`
			IP          string `json:"ip"`
			Name        string `json:"name"`
			ASName      string `json:"as_name"`
			Cycles      int    `json:"cycles"`
			Destination bool   `json:"destination"`
			LoopCycles  int    `json:"loop_cycles"`
			RTT         int    `json:"rtt_p50_us"`
		} `json:"nodes"`
		Edges []struct {
			FromTTL int    `json:"from_ttl"`
			FromIP  string `json:"from_ip"`
			ToTTL   int    `json:"to_ttl"`
			ToIP    string `json:"to_ip"`
			Cycles  int    `json:"cycles"`
		} `json:"edges"`
		Timeline []struct {
			Cycles     int `json:"cycles"`
			Reached    int `json:"reached_cycles"`
			LoopCycles int `json:"loop_cycles"`
			RTT        int `json:"rtt_p50_us"`
		} `json:"timeline"`
	}
	get := func(target string, window ...string) topo {
		t.Helper()
		if window == nil {
			window = []string{"2026-11-16T22:00:00Z", "2026-11-17T00:00:00Z"}
		}
		resp, err := http.Get(ts.URL + "/api/v1/query/topology?agent=lab&protocol=icmp&target=" + target +
			"&from=" + window[0] + "&to=" + window[1])
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var v topo
		if err := json.NewDecoder(resp.Body).Decode(&v); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d %v", target, resp.StatusCode, err)
		}
		return v
	}

	// dc1: pathA x3, the loop, brokenA, pathB.
	d := get("dc1.example")
	nodes := map[string]string{}
	for _, n := range d.Nodes {
		nodes[fmt.Sprint(n.TTL, " ", n.IP)] = fmt.Sprintf("%d %s %s dest=%v loops=%d", n.Cycles, n.Name, n.ASName, n.Destination, n.LoopCycles)
	}
	for k, want := range map[string]string{
		"1 10.0.0.1":     "6 gw.lan  dest=false loops=0",
		"2 198.18.120.1": "5  EXAMPLE-TRANSIT dest=false loops=1",
		"2 198.18.120.7": "1  EXAMPLE-TRANSIT dest=false loops=0",
		"3 192.0.2.9":    "4   dest=true loops=0",
		"3 198.18.120.2": "1  EXAMPLE-TRANSIT dest=false loops=1",
		"4 198.18.120.1": "1  EXAMPLE-TRANSIT dest=false loops=1",
	} {
		if nodes[k] != want {
			t.Errorf("node %s = %q, want %q", k, nodes[k], want)
		}
	}
	if len(d.Nodes) != 7 { // plus 198.18.120.2 at TTL 5
		t.Errorf("%d nodes: %v", len(d.Nodes), nodes)
	}
	edges := map[string]int{}
	for _, e := range d.Edges {
		edges[fmt.Sprintf("%d %s > %d %s", e.FromTTL, e.FromIP, e.ToTTL, e.ToIP)] = e.Cycles
	}
	for k, want := range map[string]int{
		"1 10.0.0.1 > 2 198.18.120.1":     5,
		"2 198.18.120.1 > 3 192.0.2.9":    3,
		"1 10.0.0.1 > 2 198.18.120.7":     1,
		"2 198.18.120.7 > 3 192.0.2.9":    1,
		"3 198.18.120.2 > 4 198.18.120.1": 1,
	} {
		if edges[k] != want {
			t.Errorf("edge %s = %d, want %d (all: %v)", k, edges[k], want, edges)
		}
	}
	var cycles, reached, loops int
	for _, b := range d.Timeline {
		cycles, reached, loops = cycles+b.Cycles, reached+b.Reached, loops+b.LoopCycles
	}
	if cycles != 6 || reached != 4 || loops != 1 || d.BucketMS != 30000 || d.Truncated || d.Timeline[0].RTT != 20000 {
		t.Errorf("timeline %+v bucket %d truncated %v", d.Timeline, d.BucketMS, d.Truncated)
	}

	// 5 s buckets: the loop and brokenA cycles are buckets without a reached
	// cycle, so no destination RTT.
	d = get("dc1.example", "2026-11-16T23:00:00Z", "2026-11-16T23:01:00Z")
	unreachedBuckets := 0
	for _, b := range d.Timeline {
		if b.Reached == 0 {
			unreachedBuckets++
			if b.RTT != 0 {
				t.Errorf("RTT %d in a bucket without a reached cycle", b.RTT)
			}
		}
	}
	if d.BucketMS != 5000 || unreachedBuckets != 2 {
		t.Errorf("5 s buckets: bucket %d, %d unreached buckets: %+v", d.BucketMS, unreachedBuckets, d.Timeline)
	}

	// A silent TTL between two answers: one edge across it.
	d = get("dc3.example")
	if len(d.Edges) != 1 || d.Edges[0].FromTTL != 1 || d.Edges[0].ToTTL != 3 || d.Edges[0].ToIP != "192.0.2.9" {
		t.Errorf("dc3 edges %+v", d.Edges)
	}
}

// Parameter values reach ClickHouse unchanged, backslashes and control
// characters included (ClickHouse parses them in its escaped format).
func TestQueryParamsRoundTrip(t *testing.T) {
	c := testClickHouse(t)
	for _, v := range []string{`corp\lab1`, `ends\`, "tab\there", "new\nline", `quote'd`, `\\`} {
		type row struct {
			V string `json:"v"`
		}
		rows, err := clickhouse.QueryInto[row](t.Context(), c, "SELECT {x:String} AS v", clickhouse.Params{"x": v})
		if err != nil || len(rows) != 1 || rows[0].V != v {
			t.Errorf("%q: %v %v", v, rows, err)
		}
	}
}

// The review's injection: Grafana's sqlstring left a backslash in place, so
// a value like this ended the string literal. Every dashboard query runs it
// as plain data: no error, and nothing but the (empty) data it filters for.
func TestDashboardQueriesInjection(t *testing.T) {
	c := testClickHouse(t)
	seedTwoTargets(t, c)
	evil := `!\' OR 1=1 UNION ALL SELECT name FROM system.users -- `
	vars := map[string]string{"datasource": "hopcorder-clickhouse", "agent": evil, "target": evil,
		"protocol": evil, "target_b": evil}
	for _, name := range dashboards.Names() {
		d, err := dashboards.Load(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range d.Queries() {
			if len(dashboards.Variables(q.SQL)) == 0 {
				continue
			}
			sql := dashboards.Expand(q.SQL, vars, t0.Add(-time.Hour), t0.Add(time.Hour), time.Minute)
			rows, err := clickhouse.QueryInto[map[string]any](t.Context(), c, sql, nil)
			if err != nil || len(rows) != 0 {
				t.Errorf("%s: %d rows, %v\n%s", q.Where, len(rows), err, sql)
			}
		}
	}
}

func TestApplyRetention(t *testing.T) {
	c := testClickHouse(t)
	ttlTables := func() int {
		return count(t, c, `SELECT count() AS n FROM system.tables WHERE database = currentDatabase()
			AND position(create_table_query, ' TTL ') > 0`)
	}
	for range 2 { // idempotent
		if err := ApplyRetention(t.Context(), c, 90); err != nil {
			t.Fatal(err)
		}
	}
	if n := ttlTables(); n != len(retentionTables) {
		t.Errorf("%d tables with a TTL, want %d", n, len(retentionTables))
	}
	if n := count(t, c, `SELECT count() AS n FROM system.tables WHERE database = currentDatabase() AND name = 'hops'
		AND position(create_table_query, 'toIntervalDay(90)') > 0`); n != 1 {
		t.Error("hops has no 90-day TTL")
	}
	for range 2 {
		if err := ApplyRetention(t.Context(), c, 0); err != nil {
			t.Fatal(err)
		}
	}
	if n := ttlTables(); n != 0 {
		t.Errorf("%d tables still have a TTL after retention 0", n)
	}
}

// The edges of long paths cost ClickHouse memory in proportion to their
// hops, not to the square of them. The query paired consecutive hops with
// a lambda over the hop indexes that captured both hop arrays, and
// ClickHouse copies captured arrays for every element: 20,000 cycles of
// 255 answering hops took 3.1 GiB and 29 s (now 49 MiB and 0.4 s), so an
// agent's long paths, real or sent to that end, broke the topology view at
// query.max_memory_bytes and held that much memory per request.
func TestTopologyLongPathsClickHouse(t *testing.T) {
	c := testClickHouse(t)
	spec := make([]string, 255)
	for i := range 254 {
		spec[i] = fmt.Sprintf("10.1.%d.%d", i/200, i%200+1)
	}
	spec[254] = "=192.0.2.9"
	recs := make([]*record.Record, 2000)
	for i := range recs {
		recs[i] = mk("lab", uint64(i+1), spec...)
	}
	if _, err := NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil).Ingest(t.Context(), recs); err != nil {
		t.Fatal(err)
	}
	s := New(Options{Store: c, Pinger: c, Enricher: fakeEnrich{}, Logger: slog.New(slog.DiscardHandler),
		MaxQueryMemory: 128 << 20})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/api/v1/query/topology?agent=lab&protocol=icmp&target=dc1.example" +
		"&from=2026-11-16T22:00:00Z&to=2026-11-17T03:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var v struct {
		Edges []struct {
			FromTTL int `json:"from_ttl"`
			ToTTL   int `json:"to_ttl"`
			Cycles  int `json:"cycles"`
		} `json:"edges"`
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("topology of long paths with 128 MiB per query: %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	if len(v.Edges) != 254 {
		t.Fatalf("%d edges, want 254", len(v.Edges))
	}
	for _, e := range v.Edges {
		if e.ToTTL != e.FromTTL+1 || e.Cycles != len(recs) {
			t.Errorf("edge %d > %d in %d cycles, want consecutive TTLs in %d", e.FromTTL, e.ToTTL, e.Cycles, len(recs))
		}
	}
}
