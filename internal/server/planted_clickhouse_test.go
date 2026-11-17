//go:build clickhouse

package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/saremox/hopcorder/internal/clickhouse"
)

// Data planted through ingest made the read views cost ClickHouse about
// 1 GiB each (security review, round four; ClickHouse 25.8): topology's
// nodes and edges of 20,000 cycles of 255 new addresses each 793 and
// 681 MiB, 10,000 path changes of 255 TTLs 537 MiB (the response was cut
// at 32 MiB), 1000 of 1500 maximal cycles over 500 MiB and the stream list of
// 660,000 streams 494 MiB. With the budget the chart gives the server with
// its bundled ClickHouse (273 MiB a query, for 2Gi), each failed. Now every
// view answers, cut to its caps and the response budget, within that budget.
func TestReadViewsOfPlantedDataClickHouse(t *testing.T) {
	c := testClickHouse(t)
	plant := func(sql string) {
		t.Helper()
		if err := c.Exec(t.Context(), sql, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Columns of a cycle that do not matter here. In parts of 1000 cycles,
	// so that hops_mv's blocks stay small.
	const hops = `"hops.ttl", "hops.status", "hops.ip", "hops.rtt_us", "hops.code", "hops.asn", "hops.as_name",
		"hops.name", "hops.mpls", "hops.loop_ttls"`
	const cols = `agent_id, seq, ts, target, target_ip, af, protocol, flow_id, reached, path_len, path_hash, ` + hops
	const same = `arrayMap(i -> toUInt8(i), range(1, 256)), arrayResize(['ttl-expired'], 255, 'ttl-expired'), %s,
		arrayResize([toUInt32(1000)], 255, 1000), arrayResize([toInt16(-1)], 255, -1), arrayResize([toUInt32(0)], 255, 0),
		arrayResize([''], 255, ''), arrayResize([''], 255, ''), %s,
		arrayResize(CAST([], 'Array(Array(UInt8))'), 255)`
	const noMPLS = `arrayResize(CAST([], 'Array(Array(Tuple(UInt32, UInt8, Bool, UInt8)))'), 255)`
	const ts = `toDateTime64('2026-11-16 12:00:00', 3, 'UTC') + number`
	for part := range 20 {
		// dc1: 20,000 cycles of 255 answering hops, every address new, and a
		// route change of all 255 TTLs after each.
		where := fmt.Sprintf("FROM numbers(%d, 1000)", part*1000)
		plant(`INSERT INTO cycles (` + cols + `) SELECT 'lab', number + 1, ` + ts + `, 'dc1.example', '192.0.2.9', 4,
			'icmp', 33434, false, 255, toString(number), ` +
			fmt.Sprintf(same, `arrayMap(i -> IPv4NumToString(toUInt32(167772160 + number * 255 + i)), range(255))`, noMPLS) +
			" " + where)
		plant(`INSERT INTO path_changes SELECT ` + ts + `, 'lab', 'dc1.example', 'icmp', 33434, number + 1, 'router',
			arrayMap(i -> toUInt8(i), range(1, 256)),
			arrayMap(i -> IPv4NumToString(toUInt32(167772160 + (number - 1) * 255 + i)), range(255)),
			arrayMap(i -> IPv4NumToString(toUInt32(167772160 + number * 255 + i)), range(255)),
			number, toString(number - 1), toString(number), 255, 255 ` + where)
	}
	// dc2: 1500 maximal cycles (255 IPv6 hops with 16 MPLS labels each), in
	// parts of 100.
	for part := range 15 {
		plant(`INSERT INTO cycles (` + cols + `) SELECT 'lab', 100000 + number, ` + ts + `, 'dc2.example', '2001:db8::1',
			6, 'icmp', 33434, false, 255, toString(number), ` +
			fmt.Sprintf(same, `arrayMap(i -> concat('2001:db8:ffff:ffff:ffff:ffff:ffff:', hex(i)), range(255))`,
				`arrayResize([arrayResize([tuple(toUInt32(1048575), toUInt8(7), false, toUInt8(255))], 16,
					tuple(toUInt32(1048575), toUInt8(7), false, toUInt8(255)))], 255,
					arrayResize([tuple(toUInt32(1048575), toUInt8(7), false, toUInt8(255))], 16,
					tuple(toUInt32(1048575), toUInt8(7), false, toUInt8(255))))`) +
			fmt.Sprintf(" FROM numbers(%d, 100)", part*100))
	}
	// 660,000 streams of one cycle each.
	plant(`INSERT INTO cycles (agent_id, seq, ts, target, target_ip, af, protocol, flow_id, reached, path_len, path_hash,
		"hops.ttl", "hops.status", "hops.ip", "hops.rtt_us", "hops.code", "hops.asn", "hops.as_name", "hops.name",
		"hops.mpls", "hops.loop_ttls")
		SELECT concat('s', toString(intDiv(number, 256))), 1, toDateTime64('2026-11-16 12:00:00', 3, 'UTC') + number % 3600,
		concat('t', toString(number % 256), '.example'),
		'192.0.2.9', 4, 'icmp', 33434, true, 1, 'h', [1], ['reply'], ['192.0.2.9'], [100], [-1], [0], [''], [''], [[]], [[]]
		FROM numbers(660000)`)

	s := New(Options{Store: c, Pinger: c, Enricher: fakeEnrich{}, Logger: slog.New(slog.DiscardHandler),
		MaxQueryMemory: 273 << 20, MaxConcurrentQueries: 2})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	const window = "&from=2026-11-16T00:00:00Z&to=2026-11-18T00:00:00Z"
	// get reports a failed request and returns false, so that every view
	// is tried.
	get := func(path string, v any) bool {
		t.Helper()
		resp, err := http.Get(srv.URL + "/api/v1/query/" + path + window)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: %d %s", path, resp.StatusCode, b)
			return false
		}
		if len(b) > DefaultMaxResponseBytes+1000 {
			t.Errorf("%s: %d bytes, over the response budget", path, len(b))
		}
		if err := json.Unmarshal(b, v); err != nil {
			t.Errorf("%s: %v", path, err)
			return false
		}
		return true
	}

	var topo struct {
		Truncated bool              `json:"truncated"`
		Nodes     []json.RawMessage `json:"nodes"`
		Edges     []json.RawMessage `json:"edges"`
		Timeline  []json.RawMessage `json:"timeline"`
	}
	if get("topology?agent=lab&target=dc1.example&protocol=icmp", &topo) && (!topo.Truncated ||
		len(topo.Nodes) != topologyMaxNodes || len(topo.Edges) != topologyMaxEdges || len(topo.Timeline) == 0) {
		t.Errorf("topology: truncated %v, %d nodes, %d edges, %d buckets", topo.Truncated, len(topo.Nodes),
			len(topo.Edges), len(topo.Timeline))
	}

	type rows struct {
		Rows []struct {
			Seq    uint64 `json:"seq"`
			Target string `json:"target"`
		} `json:"rows"`
		Truncated bool `json:"truncated"`
	}
	var changes rows
	if get("path-changes?agent=lab&target=dc1.example&limit=10000", &changes) {
		if n := len(changes.Rows); !changes.Truncated || n < 1000 || n >= 10000 || changes.Rows[0].Seq != 20000 ||
			changes.Rows[n-1].Seq != uint64(20001-n) {
			t.Errorf("path-changes: truncated %v, %d rows", changes.Truncated, n)
		}
	}

	// With and without protocol (the second sorts), newest and oldest first.
	for _, tc := range []struct {
		query string
		first uint64
	}{
		{"&protocol=icmp", 101499}, {"", 101499}, {"&protocol=icmp&order=asc", 100000}, {"&order=asc", 100000},
	} {
		var cycles rows
		if get("cycles?agent=lab&target=dc2.example&limit=1000"+tc.query, &cycles) &&
			(len(cycles.Rows) < 100 || !cycles.Truncated || cycles.Rows[0].Seq != tc.first) {
			t.Errorf("cycles%s: truncated %v, %d rows, first %+v", tc.query, cycles.Truncated, len(cycles.Rows),
				cycles.Rows[:min(1, len(cycles.Rows))])
		}
	}

	var streams rows
	if get("targets?limit=10000", &streams) && len(streams.Rows) != 10000 {
		t.Errorf("targets: %d rows", len(streams.Rows))
	}

	// What each query took, for the record.
	if err := c.ExecServer(t.Context(), "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	type usage struct {
		Query string `json:"q"`
		Mem   int64  `json:"mem"`
	}
	used, err := clickhouse.QueryInto[usage](t.Context(), c, `SELECT substring(replaceRegexpAll(query, '\\s+', ' '), 1, 50) AS q,
		memory_usage AS mem FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish'
		AND Settings['result_overflow_mode'] = 'break' ORDER BY event_time_microseconds`, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range used {
		t.Logf("%4d MiB  %s", u.Mem>>20, u.Query)
	}
}
