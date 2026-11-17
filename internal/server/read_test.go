package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/saremox/hopcorder/internal/clickhouse"
)

// funcStore answers every query with q.
type funcStore struct {
	*fakeStore
	q func(ctx context.Context, query string, row func(json.RawMessage) error) error
}

func (f funcStore) Query(ctx context.Context, query string, _ clickhouse.Params, row func(json.RawMessage) error) error {
	return f.q(ctx, query, row)
}

func readServer(t *testing.T, st Store, o Options) *httptest.Server {
	t.Helper()
	o.Store, o.Enricher, o.Logger = st, fakeEnrich{}, quietLogger()
	s := New(o)
	s.SetReady()
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func bigRow(i int) string { return fmt.Sprintf(`{"i":%d,"pad":"%s"}`, i, strings.Repeat("x", 1000)) }

// A response stops at its byte budget, as valid JSON that says it was cut.
func TestReadResponseBudget(t *testing.T) {
	const n = 300 // about 300 KB of rows
	st := funcStore{newFakeStore(), func(_ context.Context, _ string, row func(json.RawMessage) error) error {
		for i := range n {
			if err := row(json.RawMessage(bigRow(i))); err != nil {
				return err
			}
		}
		return nil
	}}
	for _, tc := range []struct {
		budget    int64
		truncated bool
	}{{100 << 10, true}, {1 << 20, false}} {
		ts := readServer(t, st, Options{MaxResponseBytes: tc.budget})
		for _, path := range []string{"/api/v1/query/targets", "/api/v1/query/loops"} {
			resp, err := http.Get(ts.URL + path)
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			var body struct {
				Rows []struct {
					I int `json:"i"`
				} `json:"rows"`
				Truncated bool `json:"truncated"`
			}
			if err := json.Unmarshal(b, &body); err != nil {
				t.Fatalf("budget %d %s: %v", tc.budget, path, err)
			}
			want, left := 0, tc.budget // each row takes its length and a comma
			for want < n && int64(len(bigRow(want))+1) <= left {
				left -= int64(len(bigRow(want)) + 1)
				want++
			}
			if body.Truncated != tc.truncated || len(body.Rows) != want || int64(len(b)) > tc.budget+100 ||
				body.Rows[len(body.Rows)-1].I != want-1 {
				t.Errorf("budget %d %s: truncated %v, %d rows (want %d), %d bytes", tc.budget, path, body.Truncated,
					len(body.Rows), want, len(b))
			}
		}
	}
}

// Rows reach the client while the query still delivers them: the server
// keeps 64 KiB at a time, not the response.
func TestReadResponseStreams(t *testing.T) {
	release := make(chan struct{})
	st := funcStore{newFakeStore(), func(ctx context.Context, _ string, row func(json.RawMessage) error) error {
		for i := range 200 { // about 200 KB
			if err := row(json.RawMessage(bigRow(i))); err != nil {
				return err
			}
		}
		select { // the client must have the first rows by now
		case <-release:
		case <-ctx.Done():
		case <-time.After(10 * time.Second):
		}
		return row(json.RawMessage(bigRow(200)))
	}}
	ts := readServer(t, st, Options{})
	resp, err := http.Get(ts.URL + "/api/v1/query/targets")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 64<<10)
	start := time.Now()
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 5*time.Second {
		t.Error("the first rows arrived only when the query ended")
	}
	close(release)
	rest, _ := io.ReadAll(resp.Body)
	var body struct {
		Rows      []json.RawMessage `json:"rows"`
		Truncated bool              `json:"truncated"`
	}
	if err := json.Unmarshal(append(first, rest...), &body); err != nil || len(body.Rows) != 201 || body.Truncated {
		t.Errorf("%d rows, truncated %v, %v", len(body.Rows), body.Truncated, err)
	}
}

// A query that fails before anything was sent gets 502; one that fails
// after rows went out breaks the response instead of ending it cleanly.
func TestReadQueryFailure(t *testing.T) {
	failAfter := 0
	st := funcStore{newFakeStore(), func(_ context.Context, _ string, row func(json.RawMessage) error) error {
		for i := range failAfter {
			if err := row(json.RawMessage(bigRow(i))); err != nil {
				return err
			}
		}
		return errors.New("clickhouse: connection reset")
	}}
	ts := readServer(t, st, Options{})
	var e errorBody
	if code := getJSON(t, ts.URL+"/api/v1/query/targets", &e); code != http.StatusBadGateway || e.Error != "query failed" {
		t.Errorf("%d %q", code, e.Error)
	}
	failAfter = 200
	resp, err := http.Get(ts.URL + "/api/v1/query/outages")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Error("a response that failed part-way ended cleanly")
	}
}

// Read queries run with the memory and result-size settings; the
// ingester's do not get them.
func TestReadQuerySettings(t *testing.T) {
	var mu sync.Mutex
	var got []clickhouse.Settings
	st := funcStore{newFakeStore(), func(ctx context.Context, _ string, _ func(json.RawMessage) error) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, clickhouse.SettingsFrom(ctx))
		return nil
	}}
	ts := readServer(t, st, Options{MaxResponseBytes: 4 << 20, MaxQueryMemory: 512 << 20})
	for _, p := range []string{"/api/v1/query/targets", "/api/v1/query/topology?agent=lab&target=dc1.example"} {
		if resp, err := http.Get(ts.URL + p); err == nil {
			_ = resp.Body.Close()
		}
	}
	if len(got) != 4 {
		t.Fatalf("%d queries", len(got))
	}
	for _, s := range got {
		if s["max_memory_usage"] != "536870912" || s["max_result_bytes"] != "8388608" || s["result_overflow_mode"] != "break" ||
			// Round four: what a query costs ClickHouse depends neither on
			// the machine's cores nor on formatting whole blocks at once,
			// and the result backstop can stop between small blocks.
			s["max_threads"] != "4" || s["max_block_size"] != "1024" || s["output_format_parallel_formatting"] != "0" ||
			s["max_bytes_before_remerge_sort"] != "134217728" {
			t.Errorf("settings %v", s)
		}
	}
}

// The GROUP BYs of the read API have a group limit (with overflow mode
// any), above the row caps, so that a response it cuts has more rows than
// its cap and says truncated: without one, 20,000 planted cycles of new
// addresses cost ClickHouse 793 MiB (nodes) and 681 MiB (edges), and
// 660,000 planted streams 494 MiB (targets).
func TestReadGroupLimits(t *testing.T) {
	if topologyMaxGroups <= topologyMaxNodes+1 || topologyMaxGroups <= topologyMaxEdges+1 {
		t.Fatalf("topologyMaxGroups %d must exceed the caps", topologyMaxGroups)
	}
	var mu sync.Mutex
	var got []string
	st := funcStore{newFakeStore(), func(_ context.Context, q string, _ func(json.RawMessage) error) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, q)
		return nil
	}}
	ts := readServer(t, st, Options{MaxStreams: 30000})
	for _, p := range []string{"/api/v1/query/topology?agent=lab&target=dc1.example", "/api/v1/query/targets?limit=10000",
		"/api/v1/query/targets?limit=10000&agent=lab"} {
		resp, err := http.Get(ts.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	limited := 0
	for _, q := range got {
		switch {
		case strings.Contains(q, "FROM hops"), strings.Contains(q, "ARRAY JOIN"):
			if !strings.Contains(q, "max_rows_to_group_by = 20000, group_by_overflow_mode = 'any'") {
				t.Errorf("topology query without the group limit: %s", q)
			}
			limited++
		case strings.Contains(q, "GROUP BY agent_id, target, protocol, flow_id"):
			// as many as the ingester tracks (ingest.max_streams)
			if !strings.Contains(q, "max_rows_to_group_by = 30000, group_by_overflow_mode = 'any'") {
				t.Errorf("targets query without the group limit: %s", q)
			}
			limited++
		}
	}
	if limited != 4 {
		t.Errorf("%d of the queries with a group limit: %q", limited, got)
	}
}

// paramStore answers with rows(query, params) and records every query's
// params.
type paramStore struct {
	*fakeStore
	mu     sync.Mutex
	params []clickhouse.Params
	rows   func(q string, p clickhouse.Params) []string
}

func (p *paramStore) Query(_ context.Context, q string, params clickhouse.Params, row func(json.RawMessage) error) error {
	p.mu.Lock()
	p.params = append(p.params, maps.Clone(params))
	p.mu.Unlock()
	for _, r := range p.rows(q, params) {
		if err := row(json.RawMessage(r)); err != nil {
			return err
		}
	}
	return nil
}

// path-changes first reads the times and sizes of the newest rows, then
// only the rows that can fit the response: ClickHouse sorted whole rows of
// every change in the window, 537 MiB for 10,000 changes of 255 TTLs.
func TestPathChangesReadOnlyWhatFits(t *testing.T) {
	const n = 50 // rows of about 100 KB, 5 MB in all
	var phases []string
	st := &paramStore{fakeStore: newFakeStore()}
	st.rows = func(q string, p clickhouse.Params) []string {
		var out []string
		if strings.Contains(q, "AS bytes") {
			phases = append(phases, "sizes")
			for i := range n {
				out = append(out, fmt.Sprintf(`{"ts":"2026-11-16T23:%02d:00.000Z","bytes":100000}`, 59-i))
			}
			return out
		}
		phases = append(phases, "rows")
		limit, _ := strconv.Atoi(p["limit"])
		for i := range limit {
			out = append(out, fmt.Sprintf(`{"seq":%d,"pad":"%s"}`, n-i, strings.Repeat("x", 100000)))
		}
		return out
	}
	ts := readServer(t, st, Options{MaxResponseBytes: 1 << 20})
	var body struct {
		Rows []struct {
			Seq int `json:"seq"`
		} `json:"rows"`
		Truncated bool `json:"truncated"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/query/path-changes?agent=lab&limit=10000", &body); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	// 1 MiB holds 10 rows of 100 KB; the eleventh is the first past it.
	if len(phases) != 2 || phases[0] != "sizes" || st.params[1]["limit"] != "11" ||
		st.params[1]["oldest"] != "2026-11-16 23:49:00.000" {
		t.Errorf("queries %v, params of the second %v", phases, st.params[len(st.params)-1])
	}
	if !body.Truncated || len(body.Rows) != 10 || body.Rows[0].Seq != n {
		t.Errorf("truncated %v, %d rows", body.Truncated, len(body.Rows))
	}
}

// Times outside what records can hold, or a span over 400 days, are the
// client's error (400), not a failed query (502, an ERROR logged and
// counted for each request), and no query runs.
func TestReadTimeRange(t *testing.T) {
	st := newFakeStore()
	_, ts := newTestServer(t, st)
	for _, q := range []string{
		"to=9223372036854775807", "from=-1&to=1000", "from=-9223372036854775808", "to=9999-01-01T00:00:00Z",
		"from=1969-12-31T23:59:59Z&to=1970-01-02T00:00:00Z", "from=2261-12-31T00:00:00Z&to=2262-01-01T00:00:01Z",
		"from=2025-01-01T00:00:00Z&to=2026-02-05T00:00:01Z", // 400 days and a second
	} {
		for _, p := range []string{"targets", "cycles", "path-changes", "loops", "outages", "topology"} {
			var e errorBody
			if code := getJSON(t, ts.URL+"/api/v1/query/"+p+"?agent=lab&target=dc1.example&"+q, &e); code != http.StatusBadRequest ||
				!strings.Contains(e.Error, "from and to") {
				t.Errorf("%s?%s: %d %q", p, q, code, e.Error)
			}
		}
	}
	for _, q := range []string{"from=0&to=86400000", "from=2025-01-01T00:00:00Z&to=2026-02-05T00:00:00Z",
		"from=2261-12-31T00:00:00Z&to=2262-01-01T00:00:00Z"} {
		var body map[string]any
		if code := getJSON(t, ts.URL+"/api/v1/query/targets?"+q, &body); code != http.StatusOK {
			t.Errorf("targets?%s: %d %v", q, code, body)
		}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.queries) != 3 {
		t.Errorf("%d queries, want only the 3 valid ones", len(st.queries))
	}
}

// Read-API responses, errors included, are neither cached nor sniffed.
func TestReadAPIHeaders(t *testing.T) {
	st := newFakeStore()
	st.answer = func(q string) []string {
		if strings.Contains(q, "hops.mpls") {
			return []string{`[]`} // the reshape of cycles fails: 502
		}
		return []string{`{"x":1}`}
	}
	_, ts := newTestServer(t, st)
	for path, want := range map[string]int{
		"/api/v1/query/targets":                               http.StatusOK,
		"/api/v1/query/topology?agent=lab&target=dc1.example": http.StatusOK,
		"/api/v1/query/targets?limit=0":                       http.StatusBadRequest,
		"/api/v1/query/cycles?agent=lab&target=dc1.example":   http.StatusBadGateway,
	} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != want || resp.Header.Get("Cache-Control") != "no-store" ||
			resp.Header.Get("X-Content-Type-Options") != "nosniff" || resp.Header.Get("Content-Type") != "application/json" {
			t.Errorf("%s: %d %v", path, resp.StatusCode, resp.Header)
		}
	}
}

// Topology stays within the budget too: the timeline comes first, and the
// parts after the one that filled the budget are empty.
func TestTopologyByteBudget(t *testing.T) {
	ts := readServer(t, funcStore{newFakeStore(), func(_ context.Context, q string, row func(json.RawMessage) error) error {
		n, mk := 1, func(i int) string { return fmt.Sprintf(`{"t":"2026-11-16T00:00:00.000Z","cycles":%d}`, i) }
		if !strings.Contains(q, "toStartOfInterval") {
			n, mk = 2000, func(i int) string { return fmt.Sprintf(`{"ttl":1,"ip":"%d","name":"%s"}`, i, strings.Repeat("n", 500)) }
		}
		for i := range n {
			if err := row(json.RawMessage(mk(i))); err != nil {
				return err
			}
		}
		return nil
	}}, Options{MaxResponseBytes: 64 << 10})
	var body struct {
		Truncated bool              `json:"truncated"`
		Nodes     []json.RawMessage `json:"nodes"`
		Edges     []json.RawMessage `json:"edges"`
		Timeline  []json.RawMessage `json:"timeline"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/query/topology?agent=lab&target=dc1.example", &body); code != 200 ||
		!body.Truncated || len(body.Timeline) != 1 || len(body.Nodes) == 0 || len(body.Nodes) > 130 || len(body.Edges) != 0 {
		t.Errorf("%d: truncated %v, %d nodes, %d edges, %d buckets", code, body.Truncated, len(body.Nodes),
			len(body.Edges), len(body.Timeline))
	}
}

// With allowed hosts, other Host headers get 421 (DNS rebinding), except on
// the health and metrics endpoints; without, every host is served. A
// "*.domain" entry allows one label below the domain, like an Ingress host.
func TestHostCheck(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := New(Options{Store: newFakeStore(), Logger: quietLogger(), Registry: reg, UI: http.NotFoundHandler(),
		AllowedHosts: []string{"localhost", "127.0.0.1", "::1", "Hopcorder.Example.", "*.Lab.Example"}})
	s.SetReady()
	h := s.Handler()
	for _, tc := range []struct {
		host, path string
		want       int
	}{
		{"localhost:8080", "/api/v1/query/targets", 200},
		{"127.0.0.1", "/api/v1/query/targets", 200},
		{"[::1]:8080", "/api/v1/query/targets", 200},
		{"hopcorder.example", "/api/v1/query/targets", 200},
		{"HOPCORDER.example.:443", "/api/v1/query/targets", 200},
		{"rebind.attacker.example:8080", "/api/v1/query/targets", 421},
		{"rebind.attacker.example", "/api/v1/ingest", 421},
		{"rebind.attacker.example", "/ui/", 421},
		{"127.0.0.2:8080", "/", 421},
		{"", "/api/v1/query/targets", 421},
		{"hc.lab.example", "/api/v1/query/targets", 200},
		{"HC.Lab.Example.:443", "/ui/", 404}, // served (by the test's UI handler)
		{"lab.example", "/api/v1/query/targets", 421},
		{".lab.example", "/api/v1/query/targets", 421},
		{"a.hc.lab.example", "/api/v1/query/targets", 421},
		{"hc.lab.example.attacker.example", "/api/v1/query/targets", 421},
		{"10.1.2.3:8080", "/healthz", 200},
		{"10.1.2.3:8080", "/readyz", 200},
		{"10.1.2.3:8080", "/metrics", 200},
	} {
		r := httptest.NewRequest(http.MethodGet, tc.path, nil)
		if tc.path == "/api/v1/ingest" {
			r = httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader("\n"))
			r.Header.Set("Content-Type", "application/x-ndjson")
		}
		r.Host = tc.host
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Code != tc.want {
			t.Errorf("Host %q %s: %d, want %d", tc.host, tc.path, rec.Code, tc.want)
		}
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	refused := -1.0
	for _, mf := range mfs {
		if mf.GetName() == "hopcorder_server_host_refused_total" {
			refused = mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	if refused != 9 {
		t.Errorf("hopcorder_server_host_refused_total = %v, want 9", refused)
	}

	open := New(Options{Store: newFakeStore(), Logger: quietLogger()}).Handler()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/query/targets", nil)
	r.Host = "rebind.attacker.example"
	rec := httptest.NewRecorder()
	open.ServeHTTP(rec, r)
	if rec.Code != 200 {
		t.Errorf("without allowed hosts: %d", rec.Code)
	}
}
