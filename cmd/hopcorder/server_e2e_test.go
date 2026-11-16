//go:build clickhouse

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/config"
	"github.com/saremox/hopcorder/internal/spool"
)

// TestAgentServerClickHouse runs the agent daemon (fake prober, real spool
// and HTTP output) against the real server code and a real ClickHouse:
//
//	HOPCORDER_CLICKHOUSE_URL=http://127.0.0.1:8123 go test -tags clickhouse ./cmd/hopcorder/
//
// HOPCORDER_CLICKHOUSE_USER and HOPCORDER_CLICKHOUSE_PASSWORD, if set, are the
// ClickHouse credentials (see the Makefile's clickhouse target).
func TestAgentServerClickHouse(t *testing.T) {
	url := os.Getenv("HOPCORDER_CLICKHOUSE_URL")
	if url == "" {
		t.Skip("HOPCORDER_CLICKHOUSE_URL not set")
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	db := "hopcorder_e2e_" + hex.EncodeToString(b)
	user, password := os.Getenv("HOPCORDER_CLICKHOUSE_USER"), os.Getenv("HOPCORDER_CLICKHOUSE_PASSWORD")
	ch, err := clickhouse.New(clickhouse.Config{URL: url, Database: db, User: user, Password: password})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.ExecServer(context.Background(), "DROP DATABASE IF EXISTS "+clickhouse.QuoteIdent(db)) })

	scfg, err := config.ParseServer([]byte(fmt.Sprintf(`
listen: 127.0.0.1:0
clickhouse: {url: %q, database: %q, user: %q, password: %q}
enrich:
  rdns: {enabled: false}
  asn: {enabled: false}
`, url, db, user, password)))
	if err != nil {
		t.Fatal(err)
	}
	if err := scfg.Finalize(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ready := make(chan string, 1)
	served := make(chan error, 1)
	go func() { served <- serve(ctx, scfg, slog.New(slog.DiscardHandler), ready) }()
	addr := <-ready
	waitFor(t, 10*time.Second, func() bool {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, "server ready")

	dir := t.TempDir()
	writeTestFile(t, dir+"/token", "unused")
	acfg := daemonConfig(t, dir+"/spool", "http://"+addr, freeAddr(t), dir+"/token")
	shutdownGrace = 5 * time.Second
	stopAgent := startDaemon(t, acfg, fakeNetwork())
	time.Sleep(1500 * time.Millisecond)
	stopAgent()

	sp, err := spool.Open(spool.Options{Dir: dir + "/spool", MaxAge: time.Hour, MaxBytes: 1 << 30, Consumers: []string{"http"}})
	if err != nil {
		t.Fatal(err)
	}
	last := sp.Stats().LastSeq
	pending := sp.Pending("http")
	_ = sp.Close()

	type counts struct {
		N        int    `json:"n"`
		Distinct int    `json:"d"`
		Max      uint64 `json:"m"`
		Hops     int    `json:"h"`
	}
	rows, err := clickhouse.QueryInto[counts](t.Context(), ch, `SELECT count() AS n, uniqExact(seq) AS d, max(seq) AS m,
		(SELECT count() FROM hops) AS h FROM cycles WHERE agent_id = 'e2e'`, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := rows[0]
	t.Logf("agent spooled %d records (pending %d); ClickHouse has %d cycles, %d hops", last, pending, c.N, c.Hops)
	if last < 20 || pending != 0 || c.N != int(last) || c.Distinct != c.N || c.Max != last || c.Hops == 0 {
		t.Errorf("want all %d records exactly once, got %+v (pending %d)", last, c, pending)
	}

	// The read API serves them back.
	resp, err := http.Get("http://" + addr + "/api/v1/query/targets")
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Rows []map[string]any `json:"rows"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	_ = resp.Body.Close()
	var targets []string
	for _, r := range body.Rows {
		targets = append(targets, fmt.Sprint(r["target"], "/", r["protocol"]))
	}
	if strings.Join(targets, ",") != "192.0.2.1/icmp,192.0.2.2/icmp,192.0.2.3/udp" {
		t.Errorf("targets %v", targets)
	}
	cancel()
	if err := <-served; err != nil {
		t.Errorf("serve: %v", err)
	}
}
