//go:build clickhouse

package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/dashboards"
	"github.com/saremox/hopcorder/internal/clickhouse"
)

// pluginClient sends queries the way Grafana's ClickHouse plugin
// (grafana-clickhouse-datasource 4.23.5, over clickhouse-go 2.50.0's HTTP
// protocol) does with the provisioned data source: database,
// default_format and client_protocol_version as request parameters, and
// max_execution_time from the plugin's 60 s query timeout plus 5; no
// custom settings and no row limit (`limit`). The plugin asks for Native;
// JSONEachRow here, to count rows: both are formats, not settings.
type pluginClient struct {
	url, db, user, password string
}

func (p pluginClient) query(ctx context.Context, sql string, extra url.Values) (int, error) {
	q := url.Values{"database": {p.db}, "default_format": {"JSONEachRow"}, "client_protocol_version": {"54460"},
		"max_execution_time": {"65"}}
	for k, v := range extra {
		q[k] = v
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url+"/?"+q.Encode(), strings.NewReader(sql))
	if err != nil {
		return 0, err
	}
	req.SetBasicAuth(p.user, p.password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, &clickhouse.Error{Status: resp.StatusCode, Code: resp.Header.Get("X-ClickHouse-Exception-Code"),
			Text: strings.TrimSpace(string(b))}
	}
	if q.Get("default_format") != "JSONEachRow" {
		return len(b), nil
	}
	return bytes.Count(b, []byte("\n")), nil
}

// refused reports whether err is ClickHouse refusing a setting: READONLY
// (readonly = 1 and not CHANGEABLE_IN_READONLY) or
// SETTING_CONSTRAINT_VIOLATION (outside its MIN/MAX, or CONST).
func refused(err error) bool {
	var ce *clickhouse.Error
	return errors.As(err, &ce) && (ce.Code == "164" || ce.Code == "452")
}

// Grafana's read-only user as the compose stack creates it
// (deploy/compose/clickhouse-init.sql, its names made unique to the test):
// every dashboard query still runs with what the plugin sends, and the
// user can set no other setting. With readonly = 2 and a list of forbidden
// ones, the settings left off the list let one query OOM-kill ClickHouse
// (max_untracked_memory, memory_profiler_step; max_threads with
// use_concurrency_control = 0), pin memory in the query cache for good, fill
// the disk with ClickHouse's own logs (the profilers, log_*) or hide a query
// from query_log; earlier, the spill settings filled the data volume with
// temporary data. Hyperscan is off: its compiled patterns stay in a global
// cache outside every memory limit. The Helm chart's read-only user has the same profile in
// XML; deploy/helm/ci/smoke.sh checks that one.
func TestGrafanaUserClickHouse(t *testing.T) {
	c := testClickHouse(t)
	seedTwoTargets(t, c)
	user := c.Database() + "_grafana"
	profile := c.Database() + "_limits"

	b, err := os.ReadFile("../../deploy/compose/clickhouse-init.sql")
	if err != nil {
		t.Fatal(err)
	}
	var sql strings.Builder
	for line := range strings.Lines(string(b)) {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			sql.WriteString(line)
		}
	}
	script := sql.String()
	for _, r := range []struct{ re, to string }{
		{`\bgrafana_limits\b`, profile},
		{`\bgrafana\b`, user}, // the user name and its password
		{`\bhopcorder\.\*`, c.Database() + ".*"},
	} {
		re := regexp.MustCompile(r.re)
		if !re.MatchString(script) {
			t.Fatalf("%s not found in clickhouse-init.sql: update this test", r.re)
		}
		script = re.ReplaceAllString(script, r.to)
	}
	t.Cleanup(func() {
		for _, q := range []string{"DROP USER IF EXISTS " + user, "DROP SETTINGS PROFILE IF EXISTS " + profile} {
			if err := c.ExecServer(context.Background(), q); err != nil {
				t.Errorf("%s: %v", q, err)
			}
		}
	})
	for stmt := range strings.SplitSeq(script, ";") {
		if stmt = strings.TrimSpace(stmt); stmt == "" {
			continue
		}
		if err := c.ExecServer(t.Context(), stmt); err != nil {
			t.Fatalf("%v (the test's ClickHouse user needs access management)\n%s", err, stmt)
		}
	}

	g := pluginClient{url: strings.TrimRight(os.Getenv("HOPCORDER_CLICKHOUSE_URL"), "/"), db: c.Database(), user: user, password: user}
	query := func(sql string) error {
		_, err := g.query(t.Context(), sql, nil)
		return err
	}

	// clickhouse-go's handshake, with the plugin's exact parameters.
	if _, err := g.query(t.Context(), "SELECT displayName(), version(), revision(), timezone()",
		url.Values{"default_format": {"Native"}}); err != nil {
		t.Errorf("the plugin's first query: %v", err)
	}
	vars := map[string]string{"datasource": "hopcorder-clickhouse", "agent": "lab", "target": "dc1.example",
		"protocol": "icmp", "target_b": "dc2.example"}
	queries := 0
	for _, name := range dashboards.Names() {
		d, err := dashboards.Load(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range d.Queries() {
			sql := dashboards.Expand(q.SQL, vars, t0.Add(-time.Hour), t0.Add(time.Hour), time.Minute)
			rows, err := g.query(t.Context(), sql, nil)
			switch {
			case err != nil:
				t.Errorf("%s %s as the Grafana user: %v", name, q.Where, err)
			case rows == 0:
				t.Errorf("%s %s as the Grafana user: no rows", name, q.Where)
			}
			queries++
		}
	}
	if queries == 0 {
		t.Fatal("no dashboard queries")
	}

	// readonly = 1, which the cancel setting needs, and memory limits that
	// leave the chart's bundled ClickHouse more than half of its memory for
	// the server (the chart's defaults, which the script uses).
	const budget = 2 << 30 / 10 * 8 // resources.limits.memory 2Gi, max_server_memory_usage_to_ram_ratio 0.8
	var got struct{ Readonly, Cancel, Query, User int64 }
	for _, s := range []struct {
		name string
		v    *int64
	}{{"readonly", &got.Readonly}, {"cancel_http_readonly_queries_on_client_close", &got.Cancel},
		{"max_memory_usage", &got.Query}, {"max_memory_usage_for_user", &got.User}} {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, g.url+"/?default_format=TSV",
			strings.NewReader(fmt.Sprintf("SELECT toInt64(getSetting('%s'))", s.name)))
		req.SetBasicAuth(user, user)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if *s.v, err = strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err != nil {
			t.Fatalf("%s: %s", s.name, b)
		}
	}
	if got.Readonly != 1 || got.Cancel != 1 || got.Query < 1 || got.User < got.Query || got.User >= budget/2 {
		t.Errorf("the Grafana user's settings: %+v", got)
	}

	// What the plugin sends works: max_execution_time from its query
	// timeout (MIN 1 MAX 300), as a parameter or in the query.
	for _, s := range []url.Values{{"max_execution_time": {"300"}}, {"max_execution_time": {"1"}}} {
		if _, err := g.query(t.Context(), "SELECT 1", s); err != nil {
			t.Errorf("%v: %v", s, err)
		}
	}
	if err := query("SELECT 1 SETTINGS max_execution_time = 10"); err != nil {
		t.Errorf("SETTINGS max_execution_time = 10: %v", err)
	}

	// Nothing else. Each would raise a limit or switch a protection off.
	settings := []string{
		// No limit on time, memory or concurrency (0 means unlimited).
		"max_execution_time = 0", "max_execution_time = 301",
		"max_memory_usage = 0", "max_memory_usage = 10000000000",
		"max_memory_usage_for_user = 0", "max_concurrent_queries_for_user = 1000",
		"readonly = 0", "readonly = 2",
		"cancel_http_readonly_queries_on_client_close = 0",
		// Memory ClickHouse does not track: with both, one query
		// OOM-killed a 2 GiB ClickHouse; so did 14 with 1000 threads each
		// and no concurrency control.
		"max_untracked_memory = 100000000000", "memory_profiler_step = 0",
		"max_threads = 1000", "use_concurrency_control = 0",
		// Memory pinned in the query cache for as long as the TTL.
		"use_query_cache = 1", "query_cache_ttl = 100000000",
		// ClickHouse's own logs at MB/s (a trace_log row per allocation).
		"memory_profiler_sample_probability = 1", "max_untracked_memory = 1", "memory_profiler_step = 1",
		"trace_profile_events = 1", "query_profiler_real_time_period_ns = 1000000",
		"query_profiler_cpu_time_period_ns = 1000000",
		"log_processors_profiles = 0", // on by default in 25.8: "setting" it to 1 changes nothing

		"opentelemetry_start_trace_probability = 1", "opentelemetry_trace_processors = 1",
		"log_query_threads = 1", "log_query_views = 0", "query_metric_log_interval = 1",
		// A query kept out of query_log.
		"log_queries = 0", "log_queries_min_type = 'EXCEPTION_WHILE_PROCESSING'",
		"log_queries_probability = 0.5", "log_queries_min_query_duration_ms = 100000",
		// Temporary data on disk: external sort and GROUP BY (by bytes or
		// by ratio of the memory limit), and the joins that spill. One
		// query filled ClickHouse's data volume with them.
		"max_bytes_before_external_sort = 1000000",
		"max_bytes_ratio_before_external_sort = 0.1",
		"max_bytes_before_external_group_by = 1000000",
		"max_bytes_ratio_before_external_group_by = 0.1",
		// Hyperscan, whose compiled patterns stay in a global cache
		// outside every memory limit (below).
		"allow_hyperscan = 1",
		// Generated CapnProto and Protobuf schemas, which left a file per
		// failed query on the data volume (below).
		"format_capn_proto_use_autogenerated_schema = 1", "format_protobuf_use_autogenerated_schema = 1",
	}
	for _, alg := range []string{"grace_hash", "partial_merge", "auto", "full_sorting_merge", "prefer_partial_merge", "hash,grace_hash"} {
		settings = append(settings, "join_algorithm = '"+alg+"'")
	}
	for _, s := range settings {
		if err := query("SELECT count() FROM numbers(10) a JOIN numbers(10) b USING number SETTINGS " + s); !refused(err) {
			t.Errorf("SETTINGS %s as the Grafana user: %v, want READONLY or SETTING_CONSTRAINT_VIOLATION", s, err)
		}
	}
	// The same as request parameters (the data source's custom settings).
	for _, s := range []string{"max_untracked_memory=100000000000", "max_threads=1000", "use_query_cache=1",
		"log_queries=0", "max_bytes_before_external_sort=1000000", "readonly=2"} {
		k, v, _ := strings.Cut(s, "=")
		if _, err := g.query(t.Context(), "SELECT 1", url.Values{k: {v}}); !refused(err) {
			t.Errorf("parameter %s as the Grafana user: %v, want READONLY or SETTING_CONSTRAINT_VIOLATION", s, err)
		}
	}

	// No hyperscan function runs: ClickHouse keeps each compiled pattern
	// set in a global cache, outside every query and user memory limit, for
	// good; ten queries of 1,500 fuzzy patterns each OOM-killed a 2 GiB
	// ClickHouse while every one stayed under max_memory_usage.
	for _, q := range []string{
		"SELECT multiFuzzyMatchAny('hopcorder', 1, ['hopcordr', 'agent'])",
		"SELECT multiMatchAny('hopcorder', ['hop.*', 'server'])",
		"SELECT multiSearchAnyCaseInsensitiveUTF8('hopcorder', ['HOP'])", // not hyperscan: allowed
	} {
		err := query(q)
		var ce *clickhouse.Error
		notAllowed := errors.As(err, &ce) && ce.Code == "446"
		hyperscan := !strings.Contains(q, "multiSearch")
		switch {
		case hyperscan && !notAllowed:
			t.Errorf("%s as the Grafana user: %v, want FUNCTION_NOT_ALLOWED", q, err)
		case !hyperscan && err != nil:
			t.Errorf("%s as the Grafana user: %v", q, err)
		}
	}

	// No generated CapnProto or Protobuf schema: ClickHouse writes it to a
	// temporary file under format_schemas, and one that fails to generate
	// (a field named "1") is never removed, a file per query on the data
	// volume until it runs out of inodes (review round seven). The profile
	// turns generation off, so these are refused before any file: the
	// first one fails either way, the other two ran before.
	for _, q := range []string{
		"SELECT formatRow('CapnProto', 1)",
		"SELECT length(formatRow('CapnProto', toUInt8(1) AS a))",
		"SELECT length(formatRow('Protobuf', toUInt8(1) AS a))",
	} {
		if err := query(q); err == nil || !strings.Contains(err.Error(), "requires a schema") {
			t.Errorf("%s as the Grafana user: %v, want refused for want of a schema", q, err)
		}
	}

	// A query stops when Grafana gives up on it.
	id := user + "_cancel"
	running := func() bool {
		type row struct {
			N int `json:"n"`
		}
		rows, err := clickhouse.QueryInto[row](t.Context(), c,
			"SELECT count() AS n FROM system.processes WHERE query_id = {id:String}", clickhouse.Params{"id": id})
		if err != nil {
			t.Fatal(err)
		}
		return len(rows) == 1 && rows[0].N > 0
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := g.query(ctx, "SELECT count() FROM numbers(1000000000000) WHERE NOT ignore(sipHash64(number))",
			url.Values{"query_id": {id}})
		done <- err
	}()
	for deadline := time.Now().Add(10 * time.Second); !running(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the long query did not start")
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("the cancelled query: %v", err)
	}
	for deadline := time.Now().Add(5 * time.Second); running(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Error("the query kept running after the client went away")
			_ = c.ExecServer(context.Background(), "KILL QUERY WHERE query_id = '"+id+"' SYNC")
			break
		}
	}

	// The server's own read queries use another user and are unaffected.
	if _, err := clickhouse.QueryInto[map[string]any](t.Context(), c,
		"SELECT count() FROM numbers(10) a JOIN numbers(10) b USING number SETTINGS join_algorithm = 'grace_hash'", nil); err != nil {
		t.Errorf("the server's user: %v", err)
	}
}
