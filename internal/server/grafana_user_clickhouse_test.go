//go:build clickhouse

package server

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/dashboards"
	"github.com/saremox/hopcorder/internal/clickhouse"
)

// Grafana's read-only user as the compose stack creates it
// (deploy/compose/clickhouse-init.sql, its names made unique to the test):
// every dashboard query still runs with it, and it cannot set any of the
// settings that make ClickHouse spill temporary data to disk. With one of
// them a single query filled ClickHouse's data volume and the server's
// inserts failed. The Helm chart's read-only user has the same profile in
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

	g, err := clickhouse.New(clickhouse.Config{URL: os.Getenv("HOPCORDER_CLICKHOUSE_URL"), Database: c.Database(),
		User: user, Password: user})
	if err != nil {
		t.Fatal(err)
	}
	query := func(sql string) error {
		_, err := clickhouse.QueryInto[map[string]any](t.Context(), g, sql, nil)
		return err
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
			rows, err := clickhouse.QueryInto[map[string]any](t.Context(), g, sql, nil)
			switch {
			case err != nil:
				t.Errorf("%s %s as the Grafana user: %v", name, q.Where, err)
			case len(rows) == 0:
				t.Errorf("%s %s as the Grafana user: no rows", name, q.Where)
			}
			queries++
		}
	}
	if queries == 0 {
		t.Fatal("no dashboard queries")
	}

	// Lowering a limit, and setting a constant to its own value, still work.
	for _, s := range []string{"max_execution_time = 10", "max_memory_usage = 100000000",
		"max_bytes_before_external_sort = 0", "join_algorithm = 'direct,parallel_hash,hash'"} {
		if err := query("SELECT count() FROM numbers(10) SETTINGS " + s); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
	// Every way to spill: external sort and GROUP BY (by bytes or by ratio
	// of the memory limit), and the joins that write to disk.
	spill := []string{
		"max_bytes_before_external_sort = 1000000",
		"max_bytes_ratio_before_external_sort = 0.1",
		"max_bytes_before_external_group_by = 1000000",
		"max_bytes_ratio_before_external_group_by = 0.1",
	}
	for _, alg := range []string{"grace_hash", "partial_merge", "auto", "full_sorting_merge", "prefer_partial_merge", "hash,grace_hash"} {
		spill = append(spill, "join_algorithm = '"+alg+"'")
	}
	for _, s := range spill {
		err := query("SELECT count() FROM numbers(10) a JOIN numbers(10) b USING number SETTINGS " + s)
		var ce *clickhouse.Error
		if !errors.As(err, &ce) || ce.Code != "452" { // SETTING_CONSTRAINT_VIOLATION
			t.Errorf("SETTINGS %s as the Grafana user: %v, want SETTING_CONSTRAINT_VIOLATION", s, err)
		}
	}
	// The server's own read queries use another user and are unaffected.
	if _, err := clickhouse.QueryInto[map[string]any](t.Context(), c,
		"SELECT count() FROM numbers(10) a JOIN numbers(10) b USING number SETTINGS join_algorithm = 'grace_hash'", nil); err != nil {
		t.Errorf("the server's user: %v", err)
	}
}
