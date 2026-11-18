//go:build clickhouse

package clickhouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
)

// These tests need a real ClickHouse, and a user there that may create
// databases, users and settings profiles (HOPCORDER_CLICKHOUSE_USER and
// HOPCORDER_CLICKHOUSE_PASSWORD; default: ClickHouse's default user):
//
//	HOPCORDER_CLICKHOUSE_URL=http://127.0.0.1:8123 go test -tags clickhouse ./internal/clickhouse/

// progressUser creates a database, and a user whose profile has settings,
// both named name and dropped after the test, with the tables the tests
// use: src, whose materialized view into dst fails for x = 3; checked,
// whose constraint fails for x >= 5; and big, 100,000 rows. It returns a
// client of that user.
func progressUser(t *testing.T, name, settings string) *Client {
	t.Helper()
	url := os.Getenv("HOPCORDER_CLICKHOUSE_URL")
	if url == "" {
		t.Skip("HOPCORDER_CLICKHOUSE_URL not set")
	}
	admin, err := New(Config{URL: url, Database: name, User: os.Getenv("HOPCORDER_CLICKHOUSE_USER"),
		Password: os.Getenv("HOPCORDER_CLICKHOUSE_PASSWORD")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, q := range []string{"DROP DATABASE IF EXISTS " + name, "DROP USER IF EXISTS " + name,
			"DROP SETTINGS PROFILE IF EXISTS " + name} {
			if err := admin.ExecServer(context.Background(), q); err != nil {
				t.Errorf("%s: %v", q, err)
			}
		}
	})
	for _, q := range []string{
		"CREATE DATABASE " + name,
		"CREATE SETTINGS PROFILE " + name + " SETTINGS " + settings,
		"CREATE USER " + name + " IDENTIFIED WITH plaintext_password BY '" + name + "' SETTINGS PROFILE " + name,
		"GRANT SELECT, INSERT ON " + name + ".* TO " + name,
	} {
		if err := admin.ExecServer(t.Context(), q); err != nil {
			t.Fatalf("%v (the test's ClickHouse user needs access management)\n%s", err, q)
		}
	}
	for _, q := range []string{
		"CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x",
		"CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x",
		"CREATE MATERIALIZED VIEW mv TO dst AS SELECT x + throwIf(x = 3, 'the view fails') AS x FROM src",
		"CREATE TABLE checked (x UInt64, CONSTRAINT small CHECK x < 5) ENGINE = MergeTree ORDER BY x",
		"CREATE TABLE big (x UInt64) ENGINE = MergeTree ORDER BY x",
		"INSERT INTO big SELECT number FROM numbers(100000)",
	} {
		if err := admin.Exec(t.Context(), q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	c, err := New(Config{URL: url, Database: name, User: name, Password: name})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func testName(t *testing.T) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "hopcorder_test_" + hex.EncodeToString(b)
}

func xRows(from, to int) [][]byte {
	var rows [][]byte
	for x := from; x <= to; x++ {
		rows = append(rows, fmt.Appendf(nil, `{"x":%d}`, x))
	}
	return rows
}

// With send_progress_in_http_headers = 1 in the user's profile (some
// operators set it to keep long HTTP queries alive through a load
// balancer), ClickHouse sends a progress header once a request has run for
// http_headers_progress_interval_ms (100 ms): an insert whose body takes
// that long to send, a fill or query that reads that long. Its status is
// then 200, and an exception after it went into the body: failed inserts,
// views and fills looked stored, the ingester moved its mark past cycles
// without their hop rows, and the agent's retries were duplicates (review,
// round six). The interval of 0 here sends the header at once, for every
// request. Every failure must be an error.
func TestProgressHeadersHideNoFailure(t *testing.T) {
	c := progressUser(t, testName(t), "send_progress_in_http_headers = 1, http_headers_progress_interval_ms = 0")
	ctx := t.Context()
	rows := 0
	for _, tc := range []struct {
		name, code string
		run        func() error
	}{
		{"insert", "469", func() error { return c.InsertJSON(ctx, "checked", xRows(1, 10)) }},
		{"materialized view", "395", func() error { return c.InsertJSON(ctx, "src", xRows(1, 10)) }},
		{"fill", "395", func() error {
			return c.Exec(ctx, "INSERT INTO dst SELECT x + throwIf(x = 50000, 'the fill fails') FROM big "+
				"SETTINGS max_block_size = 1000", nil)
		}},
		{"statement without a database", "395", func() error {
			return c.ExecServer(ctx, "INSERT INTO "+c.Database()+".dst SELECT x + throwIf(x = 50000, 'the fill fails') "+
				"FROM "+c.Database()+".big SETTINGS max_block_size = 1000")
		}},
		{"query", "395", func() error {
			return c.Query(ctx, "SELECT x, throwIf(x = 50000, 'the query fails') AS t FROM big SETTINGS max_block_size = 1000",
				nil, func(json.RawMessage) error { rows++; return nil })
		}},
	} {
		var ce *Error
		if err := tc.run(); !errors.As(err, &ce) || ce.Code != tc.code {
			t.Errorf("%s: %v, want ClickHouse's exception %s", tc.name, err, tc.code)
		}
	}
	if rows != 0 {
		t.Errorf("the query's exception was passed on as %d rows", rows)
	}
	// What succeeds still does.
	if err := c.InsertJSON(ctx, "src", xRows(4, 6)); err != nil {
		t.Errorf("insert: %v", err)
	}
	type n struct {
		N int `json:"n"`
	}
	if got, err := QueryInto[n](ctx, c, "SELECT count() AS n FROM dst WHERE x >= 4", nil); err != nil || len(got) != 1 || got[0].N != 3 {
		t.Errorf("rows the view wrote: %v, %v; want 3", got, err)
	}
}

// A profile that makes the setting constant at 1 refuses the client's 0:
// every request fails, loudly, rather than hiding failures. At 0 it is no
// change, and allowed.
func TestProgressHeadersConstant(t *testing.T) {
	for _, tc := range []struct {
		value string
		code  string // "": no error
	}{{"1", "452"}, {"0", ""}} {
		t.Run(tc.value, func(t *testing.T) {
			c := progressUser(t, testName(t), "send_progress_in_http_headers = "+tc.value+" CONST")
			ctx := t.Context()
			for _, run := range []struct {
				name string
				run  func() error
			}{
				{"insert", func() error { return c.InsertJSON(ctx, "dst", xRows(1, 3)) }},
				{"statement", func() error { return c.Exec(ctx, "INSERT INTO dst SELECT x FROM big LIMIT 3", nil) }},
				{"query", func() error {
					return c.Query(ctx, "SELECT 1", nil, func(json.RawMessage) error { return nil })
				}},
			} {
				err := run.run()
				var ce *Error
				switch {
				case tc.code == "" && err != nil:
					t.Errorf("%s: %v", run.name, err)
				case tc.code != "" && (!errors.As(err, &ce) || ce.Code != tc.code):
					t.Errorf("%s: %v, want ClickHouse's exception %s", run.name, err, tc.code)
				}
			}
		})
	}
}
