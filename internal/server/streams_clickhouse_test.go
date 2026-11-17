//go:build clickhouse

package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

// The review's dual-stack scenario (round five) against the real schema:
// IPv4 and IPv6 of one name, on their own flows, are two streams with
// their own events, and every read endpoint reads one flow by flow_id, or
// all of them without it. Before, the read API ignored flow_id: the UI's
// view of either flow showed both, 50% reached, with IPv4 and IPv6 hops in
// one graph.
func TestDualStackStreamsClickHouse(t *testing.T) {
	c := testClickHouse(t)
	if _, err := NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil).Ingest(t.Context(), dualStack(40, 33434, 33435)); err != nil {
		t.Fatal(err)
	}
	s := New(Options{Store: c, Pinger: c, Enricher: fakeEnrich{}, Logger: slog.New(slog.DiscardHandler)})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	const window = "from=2026-11-16T22:00:00Z&to=2026-11-17T00:00:00Z"
	get := func(path string, v any) {
		t.Helper()
		resp, err := http.Get(ts.URL + "/api/v1/query/" + path + "&" + window)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d %v", path, resp.StatusCode, err)
		}
	}
	type rows struct {
		Rows []map[string]any `json:"rows"`
	}

	var targets rows
	get("targets?agent=lab", &targets)
	var got []string
	for _, r := range targets.Rows {
		got = append(got, fmt.Sprintf("%v %v flow %v af %v: %v cycles, reached %v", r["target"], r["protocol"],
			r["flow_id"], r["last_af"], r["cycles"], r["reach_ratio"]))
	}
	if want := []string{
		"dual.example icmp flow 33434 af 4: 20 cycles, reached 1",
		"dual.example icmp flow 33435 af 6: 20 cycles, reached 0",
	}; !slices.Equal(got, want) {
		t.Errorf("targets %q, want %q", got, want)
	}
	get("targets?agent=lab&flow_id=33435", &targets)
	if len(targets.Rows) != 1 || targets.Rows[0]["flow_id"] != 33435.0 {
		t.Errorf("targets of flow 33435: %v", targets.Rows)
	}

	const stream = "agent=lab&target=dual.example"
	for q, want := range map[string]string{
		"cycles?" + stream + "&flow_id=33435":               "20 af [6] flows [33435]",
		"cycles?" + stream + "&protocol=icmp&flow_id=33435": "20 af [6] flows [33435]",
		"cycles?" + stream + "&protocol=icmp&flow_id=33434": "20 af [4] flows [33434]",
		"cycles?" + stream + "&protocol=icmp&flow_id=1":     "0 af [] flows []",
		"cycles?" + stream:                                   "40 af [4 6] flows [33434 33435]",
		"cycles?" + stream + "&protocol=icmp":                "40 af [4 6] flows [33434 33435]",
		"path-changes?" + stream:                             "0 af [] flows []",
		"loops?" + stream + "&flow_id=33435":                 "0 af [] flows []",
		"outages?" + stream + "&protocol=icmp&flow_id=33434": "0 af [] flows []",
		"outages?" + stream + "&protocol=icmp&flow_id=33435": "1 af [] flows [33435]",
	} {
		var r rows
		get(q, &r)
		var afs, flows []int
		for _, row := range r.Rows {
			if af, ok := row["af"].(float64); ok && !slices.Contains(afs, int(af)) {
				afs = append(afs, int(af))
			}
			if f := int(row["flow_id"].(float64)); !slices.Contains(flows, f) {
				flows = append(flows, f)
			}
		}
		slices.Sort(afs)
		slices.Sort(flows)
		if s := fmt.Sprintf("%d af %v flows %v", len(r.Rows), afs, flows); s != want {
			t.Errorf("%s: %s, want %s", q, s, want)
		}
	}
	var out rows
	get("outages?"+stream+"&flow_id=33435", &out)
	if len(out.Rows) == 1 && (out.Rows[0]["end_ts"] != nil || out.Rows[0]["start_seq"] != 2.0) {
		t.Errorf("outage %v, want one open from seq 2", out.Rows[0])
	}

	for flow, want := range map[string]string{
		"33434": "20 cycles, 20 reached; nodes [1 10.0.0.1 2 198.18.120.1 3 192.0.2.9]",
		"33435": "20 cycles, 0 reached; nodes [1 2001:db8:1::1 2 2001:db8:2::1]",
	} {
		var topo struct {
			Nodes []struct {
				TTL int    `json:"ttl"`
				IP  string `json:"ip"`
			} `json:"nodes"`
			Timeline []struct {
				Cycles  int `json:"cycles"`
				Reached int `json:"reached_cycles"`
			} `json:"timeline"`
		}
		get("topology?"+stream+"&protocol=icmp&flow_id="+flow, &topo)
		var cycles, reached int
		for _, b := range topo.Timeline {
			cycles, reached = cycles+b.Cycles, reached+b.Reached
		}
		var nodes []any
		for _, n := range topo.Nodes {
			nodes = append(nodes, n.TTL, n.IP)
		}
		if s := fmt.Sprintf("%d cycles, %d reached; nodes %v", cycles, reached, nodes); s != want {
			t.Errorf("topology of flow %s: %s, want %s", flow, s, want)
		}
	}

	for q, want := range map[string]int{
		"SELECT count() AS n FROM path_changes FINAL":                                     0,
		"SELECT count() AS n FROM outages FINAL":                                          1,
		"SELECT count() AS n FROM outages FINAL WHERE flow_id = 33435 AND end_ts IS NULL": 1,
	} {
		if n := count(t, c, q); n != want {
			t.Errorf("%s = %d, want %d", q, n, want)
		}
	}
}
