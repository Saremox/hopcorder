package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/clickhouse"
)

func TestTimelineBucket(t *testing.T) {
	t0 := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		span time.Duration
		want time.Duration
	}{
		{time.Minute, 5 * time.Second},
		{30 * time.Minute, 5 * time.Second}, // 360 buckets
		{time.Hour, 10 * time.Second},
		{24 * time.Hour, 5 * time.Minute},
		{7 * 24 * time.Hour, 30 * time.Minute},
		{90 * 24 * time.Hour, 6 * time.Hour},
		{3650 * 24 * time.Hour, 24 * time.Hour}, // capped at a day
	} {
		if got := timelineBucket(t0, t0.Add(c.span)); got != c.want {
			t.Errorf("span %v: bucket %v, want %v", c.span, got, c.want)
		}
	}
}

func topologyServer(t *testing.T, answer func(q string) []string) *httptest.Server {
	t.Helper()
	st := newFakeStore()
	st.answer = answer
	s := New(Options{Store: st, Enricher: fakeEnrich{}, Logger: quietLogger()})
	s.SetReady()
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func getJSON(t *testing.T, url string, v any) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode
}

func TestTopologyResponse(t *testing.T) {
	var seen []string
	ts := topologyServer(t, func(q string) []string {
		seen = append(seen, q)
		switch {
		case strings.Contains(q, "FROM hops"):
			rows := make([]string, topologyMaxNodes+1) // one too many: truncated
			for i := range rows {
				rows[i] = fmt.Sprintf(`{"ttl":%d,"ip":"10.0.0.%d","cycles":1}`, i%30+1, i%250)
			}
			return rows
		case strings.Contains(q, "ARRAY JOIN"):
			return []string{`{"from_ttl":1,"from_ip":"10.0.0.1","to_ttl":3,"to_ip":"10.0.0.3","cycles":7}`}
		default:
			return []string{`{"t":"2026-11-16T00:00:00.000Z","cycles":12,"reached_cycles":11}`}
		}
	})
	var body struct {
		BucketMS  int64            `json:"bucket_ms"`
		Truncated bool             `json:"truncated"`
		Nodes     []map[string]any `json:"nodes"`
		Edges     []map[string]any `json:"edges"`
		Timeline  []map[string]any `json:"timeline"`
	}
	code := getJSON(t, ts.URL+"/api/v1/query/topology?agent=lab&target=dc1.example&protocol=icmp"+
		"&from=2026-11-16T00:00:00Z&to=2026-11-17T00:00:00Z", &body)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if body.BucketMS != (5*time.Minute).Milliseconds() || !body.Truncated || len(body.Nodes) != topologyMaxNodes ||
		len(body.Edges) != 1 || body.Edges[0]["to_ttl"] != float64(3) || len(body.Timeline) != 1 {
		t.Errorf("body: bucket %d truncated %v nodes %d edges %v timeline %v",
			body.BucketMS, body.Truncated, len(body.Nodes), body.Edges, body.Timeline)
	}
	if len(seen) != 3 {
		t.Errorf("%d queries, want 3", len(seen))
	}

	// Empty results are empty arrays, not null.
	ts = topologyServer(t, func(string) []string { return nil })
	var raw map[string]json.RawMessage
	getJSON(t, ts.URL+"/api/v1/query/topology?agent=lab&target=dc1.example", &raw)
	for _, k := range []string{"nodes", "edges", "timeline"} {
		if string(raw[k]) != "[]" {
			t.Errorf("%s = %s, want []", k, raw[k])
		}
	}
	if string(raw["truncated"]) != "false" {
		t.Errorf("truncated = %s", raw["truncated"])
	}
}

func TestTopologyNeedsStream(t *testing.T) {
	ts := topologyServer(t, nil)
	var body errorBody
	if code := getJSON(t, ts.URL+"/api/v1/query/topology?agent=lab", &body); code != http.StatusBadRequest ||
		!strings.Contains(body.Error, "agent and target") {
		t.Errorf("%d %q", code, body.Error)
	}
}

type failingStore struct{ *fakeStore }

func (failingStore) Query(context.Context, string, clickhouse.Params, func(json.RawMessage) error) error {
	return errors.New("clickhouse: connection refused")
}

func TestTopologyQueryFailure(t *testing.T) {
	s := New(Options{Store: failingStore{newFakeStore()}, Enricher: fakeEnrich{}, Logger: quietLogger()})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	var body errorBody
	if code := getJSON(t, ts.URL+"/api/v1/query/topology?agent=lab&target=dc1.example", &body); code != http.StatusBadGateway ||
		body.Error != "query failed" {
		t.Errorf("%d %q", code, body.Error)
	}
}

func TestCyclesOrder(t *testing.T) {
	var got []string
	ts := topologyServer(t, func(q string) []string {
		got = append(got, q)
		return nil
	})
	base := ts.URL + "/api/v1/query/cycles?agent=lab&target=dc1.example"
	var body map[string]any
	for _, c := range []struct {
		query string
		code  int
		order string
	}{{"", 200, "ORDER BY ts DESC"}, {"&order=asc", 200, "ORDER BY ts ASC"}, {"&order=up", 400, ""}} {
		got = nil
		if code := getJSON(t, base+c.query, &body); code != c.code {
			t.Errorf("%q: status %d", c.query, code)
		}
		if c.order != "" && (len(got) != 1 || !strings.Contains(got[0], c.order)) {
			t.Errorf("%q: queries %v", c.query, got)
		}
	}
}

func TestUIRoutes(t *testing.T) {
	ui := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ui " + r.URL.Path)) })
	s := New(Options{Store: newFakeStore(), Enricher: fakeEnrich{}, Logger: quietLogger(), UI: ui})
	h := s.Handler()
	for path, want := range map[string]string{"/": "redirect", "/ui": "redirect", "/ui/": "ui /ui/", "/ui/assets/x.js": "ui /ui/assets/x.js", "/other": "404"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		var got string
		switch {
		case rec.Code == http.StatusFound && rec.Header().Get("Location") == "ui/":
			got = "redirect"
		case rec.Code == http.StatusNotFound:
			got = "404"
		default:
			got = rec.Body.String()
		}
		if got != want {
			t.Errorf("%s: %s (%d), want %s", path, got, rec.Code, want)
		}
	}
	// Without a UI, / stays unrouted.
	rec := httptest.NewRecorder()
	New(Options{Store: newFakeStore(), Enricher: fakeEnrich{}, Logger: quietLogger()}).Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ui/", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("no UI: /ui/ = %d", rec.Code)
	}
}
