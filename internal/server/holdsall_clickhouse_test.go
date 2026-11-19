//go:build clickhouse

package server

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// rowCounter counts the rows ClickHouse hands the ingester.
type rowCounter struct {
	Store
	rows atomic.Int64
}

func (s *rowCounter) Query(ctx context.Context, q string, p clickhouse.Params, row func(json.RawMessage) error) error {
	return s.Store.Query(ctx, q, p, func(m json.RawMessage) error {
		s.rows.Add(1)
		return row(m)
	})
}

// A batch far below an agent's mark is looked up in cycles (holdsAll) by
// its seq and ts window. A two-record batch whose records lie a month and
// most of the agent's seqs apart set that window to the agent's whole
// history, and the lookup handed every stored cycle in it to the server:
// with a few million cycles a 690-byte request OOM-killed a server at
// 1 GiB (review round seventeen, H1). The lookup now stops one row past
// the batch's size, which then cannot be a re-delivery of it.
func TestStaleBatchLookupIsBoundedByTheBatch(t *testing.T) {
	c := testClickHouse(t)
	const n = 1000000
	now := time.Now().UTC()
	if err := c.Exec(t.Context(), `INSERT INTO cycles (agent_id, seq, ts, target, target_ip, af, protocol, flow_id,
		reached, path_len, path_hash) SELECT 'a', 1 + number, now64(3) - toIntervalMillisecond(2505600000)
		+ toIntervalMillisecond(intDiv(number * 2505600000, `+strconv.Itoa(n)+`)), 't', '192.0.2.1', 4, 'icmp',
		1, true, 0, 'x' FROM numbers(`+strconv.Itoa(n)+`)`, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(t.Context(), "INSERT INTO agent_seq SELECT 'a', "+strconv.Itoa(n), nil); err != nil {
		t.Fatal(err)
	}
	st := &rowCounter{Store: c}
	in := clickhouseIngester(st)
	rec := func(seq uint64, ts time.Time) *record.Record {
		return &record.Record{Schema: 1, AgentID: "a", Seq: seq, TS: ts, Target: "t", Protocol: "icmp", FlowID: 1, AF: 4}
	}
	batch := []*record.Record{rec(1, now.Add(-29*24*time.Hour-12*time.Hour)), rec(n-staleWindow-1000, now.Add(23*time.Hour))}
	if _, err := in.Ingest(t.Context(), batch); !errors.Is(err, ErrStaleBatch) {
		t.Errorf("err %v, want ErrStaleBatch", err)
	}
	if got := st.rows.Load(); got > int64(len(batch))+2 {
		t.Errorf("ClickHouse handed the server %d rows for a batch of %d records", got, len(batch))
	}
}
