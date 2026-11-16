//go:build clickhouse

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"os"
	"testing"

	"github.com/saremox/hopcorder/internal/clickhouse"
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
