package server

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// Store is the part of the ClickHouse client the ingester uses; tests
// substitute a fake. It is not a storage abstraction: the SQL is
// ClickHouse's.
type Store interface {
	InsertJSON(ctx context.Context, table string, rows [][]byte) error
	Query(ctx context.Context, query string, params clickhouse.Params, row func(json.RawMessage) error) error
}

// replayDepth is how many past cycles of a stream are replayed into its
// path tracker after a server restart.
const replayDepth = 30

// Ingester turns validated records into rows and events.
//
// Deduplication on (agent_id, seq): an agent ships its records in seq order
// and only moves on once a batch was accepted, so everything at or below the
// highest seq stored for that agent has been stored already. Each agent's
// batches are processed one at a time under its own lock, the high-water
// mark moves only after the cycles insert succeeded, and so a retried batch
// (say, after a lost response) is stored exactly once.
type Ingester struct {
	store   Store
	enrich  Enricher
	log     *slog.Logger
	metrics *serverMetrics

	mu     sync.Mutex
	agents map[string]*agentState
}

type agentState struct {
	mu        sync.Mutex
	loaded    bool
	highWater uint64
	streams   map[streamKey]*stream
}

// IngestResult is returned to the agent.
type IngestResult struct {
	Accepted   int      `json:"accepted"`
	Duplicates int      `json:"duplicates"`
	Rejected   int      `json:"rejected"`
	Errors     []string `json:"errors,omitempty"`
}

// NewIngester returns an ingester writing to store.
func NewIngester(store Store, en Enricher, log *slog.Logger, m *serverMetrics) *Ingester {
	if en == nil {
		en = noEnrich{}
	}
	if log == nil {
		log = slog.Default()
	}
	if m == nil {
		m = newServerMetrics(nil)
	}
	return &Ingester{store: store, enrich: en, log: log, metrics: m, agents: map[string]*agentState{}}
}

func (in *Ingester) agent(id string) *agentState {
	in.mu.Lock()
	defer in.mu.Unlock()
	a, ok := in.agents[id]
	if !ok {
		a = &agentState{streams: map[streamKey]*stream{}}
		in.agents[id] = a
	}
	return a
}

// Ingest stores recs (already validated). It returns an error only when
// storage failed, in which case nothing was stored for the failing agent
// and the batch should be retried.
func (in *Ingester) Ingest(ctx context.Context, recs []*record.Record) (IngestResult, error) {
	var res IngestResult
	byAgent := map[string][]*record.Record{}
	for _, r := range recs {
		byAgent[r.AgentID] = append(byAgent[r.AgentID], r)
	}
	ids := make([]string, 0, len(byAgent))
	for id := range byAgent {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		acc, dup, err := in.ingestAgent(ctx, id, byAgent[id])
		res.Accepted += acc
		res.Duplicates += dup
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

func (in *Ingester) ingestAgent(ctx context.Context, id string, recs []*record.Record) (accepted, dups int, err error) {
	a := in.agent(id)
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.loaded {
		hw, err := in.loadHighWater(ctx, id)
		if err != nil {
			return 0, 0, fmt.Errorf("load high-water mark for %s: %w", id, err)
		}
		a.highWater, a.loaded = hw, true
	}

	slices.SortStableFunc(recs, func(x, y *record.Record) int { return cmp.Compare(x.Seq, y.Seq) })
	fresh := recs[:0:0]
	last := a.highWater
	for _, r := range recs {
		if r.Seq <= last {
			dups++
			continue
		}
		fresh = append(fresh, r)
		last = r.Seq
	}
	if len(fresh) == 0 {
		if n := len(recs); n > 0 && recs[n-1].Seq+100000 < a.highWater {
			in.log.Warn("agent sends seqs far below what is stored; was its spool reset? Its records are dropped as duplicates",
				"agent_id", id, "batch_max_seq", recs[n-1].Seq, "stored_max_seq", a.highWater)
		}
		in.metrics.records(0, dups)
		return 0, dups, nil
	}

	rows := make([][]byte, len(fresh))
	for i, r := range fresh {
		if rows[i], err = buildCycleRow(r, in.enrich); err != nil {
			return 0, dups, fmt.Errorf("encode record %s/%d: %w", id, r.Seq, err)
		}
	}
	start := time.Now()
	if err := in.store.InsertJSON(ctx, "cycles", rows); err != nil {
		in.metrics.insertError("cycles")
		return 0, dups, fmt.Errorf("insert cycles: %w", err)
	}
	in.metrics.insertDuration(time.Since(start))
	a.highWater = fresh[len(fresh)-1].Seq
	in.metrics.records(len(fresh), dups)

	// Derived events. The cycles are committed at this point; events are
	// derived data, so a failure here is logged rather than failing (and
	// re-sending) a batch that is already stored.
	var ev events
	for _, r := range fresh {
		s := a.stream(keyOf(r))
		if !s.loaded {
			in.restore(ctx, s, r.Seq)
		}
		s.observe(r, &ev)
	}
	in.insertEvents(ctx, ev)
	return len(fresh), dups, nil
}

func (a *agentState) stream(k streamKey) *stream {
	s, ok := a.streams[k]
	if !ok {
		s = &stream{key: k}
		a.streams[k] = s
	}
	return s
}

func (in *Ingester) loadHighWater(ctx context.Context, id string) (uint64, error) {
	type row struct {
		Max uint64 `json:"m"`
	}
	var hw uint64
	err := in.store.Query(ctx, "SELECT max(max_seq) AS m FROM agent_seq WHERE agent_id = {agent:String}",
		clickhouse.Params{"agent": id}, func(m json.RawMessage) error {
			var r row
			if err := json.Unmarshal(m, &r); err != nil {
				return err
			}
			hw = r.Max
			return nil
		})
	return hw, err
}

// restore primes a stream from storage after a server restart: the last
// cycles before seq are replayed into the path tracker, and an outage left
// open is resumed. Failures leave the stream starting fresh.
func (in *Ingester) restore(ctx context.Context, s *stream, before uint64) {
	s.loaded = true
	k := s.key
	params := clickhouse.Params{"agent": k.Agent, "target": k.Target, "protocol": k.Protocol,
		"flow": strconv.Itoa(k.Flow), "before": strconv.FormatUint(before, 10), "depth": strconv.Itoa(replayDepth)}
	where := `agent_id = {agent:String} AND target = {target:String} AND protocol = {protocol:String}
		AND flow_id = {flow:UInt16}`

	type cyc struct {
		Seq       uint64   `json:"seq"`
		TargetIP  string   `json:"target_ip"`
		Reached   bool     `json:"reached"`
		PathLen   int      `json:"path_len"`
		PathHash  string   `json:"path_hash"`
		HopTTL    []int    `json:"hops.ttl"`
		HopStatus []string `json:"hops.status"`
		HopIP     []string `json:"hops.ip"`
	}
	rows, err := clickhouseQuery[cyc](ctx, in.store, `SELECT seq, target_ip, reached, path_len, path_hash,
		hops.ttl, hops.status, hops.ip FROM cycles WHERE `+where+` AND seq < {before:UInt64}
		ORDER BY seq DESC LIMIT {depth:UInt32}`, params)
	if err != nil {
		in.log.Warn("cannot restore stream state; path changes restart from scratch", "stream", k, "err", err)
		return
	}
	slices.Reverse(rows)
	recs := make([]*record.Record, 0, len(rows))
	for _, c := range rows {
		r := &record.Record{Seq: c.Seq, Reached: c.Reached, PathLen: c.PathLen, PathHash: c.PathHash}
		r.TargetIP, _ = netip.ParseAddr(c.TargetIP)
		for i := range c.HopTTL {
			h := record.Hop{TTL: c.HopTTL[i], Status: record.Status(c.HopStatus[i])}
			if i < len(c.HopIP) {
				h.IP, _ = netip.ParseAddr(c.HopIP[i])
			}
			r.Hops = append(r.Hops, h)
		}
		recs = append(recs, r)
	}
	s.replay(recs)
	if len(recs) == 0 || recs[len(recs)-1].Reached {
		return
	}
	// The stream was down when the server stopped: resume its outage.
	open, err := clickhouseQuery[outageRow](ctx, in.store, `SELECT agent_id, target, protocol, flow_id, start_seq,
		toString(start_ts) AS start_ts, end_seq, NULL AS end_ts, cycles, last_hop_ttl, last_hop_ip, version
		FROM outages FINAL WHERE `+where+` AND end_ts IS NULL AND start_seq < {before:UInt64}
		ORDER BY start_seq DESC LIMIT 1`, params)
	if err != nil || len(open) == 0 {
		if err != nil {
			in.log.Warn("cannot restore open outage", "stream", k, "err", err)
		}
		return
	}
	type n struct {
		N int `json:"n"`
	}
	cnt, err := clickhouseQuery[n](ctx, in.store, `SELECT count() AS n FROM cycles WHERE `+where+`
		AND seq >= {start:UInt64} AND seq < {before:UInt64}`,
		withParam(params, "start", strconv.FormatUint(open[0].StartSeq, 10)))
	if err == nil && len(cnt) == 1 {
		open[0].Cycles = cnt[0].N
	}
	s.outage = &open[0]
}

func withParam(p clickhouse.Params, k, v string) clickhouse.Params {
	out := clickhouse.Params{k: v}
	for kk, vv := range p {
		out[kk] = vv
	}
	return out
}

func clickhouseQuery[T any](ctx context.Context, st Store, q string, p clickhouse.Params) ([]T, error) {
	var out []T
	err := st.Query(ctx, q, p, func(m json.RawMessage) error {
		var v T
		if err := json.Unmarshal(m, &v); err != nil {
			return err
		}
		out = append(out, v)
		return nil
	})
	return out, err
}

func (in *Ingester) insertEvents(ctx context.Context, ev events) {
	insert := func(table string, rows [][]byte) {
		if len(rows) == 0 {
			return
		}
		var err error
		for attempt := range 3 {
			if err = in.store.InsertJSON(ctx, table, rows); err == nil {
				return
			}
			in.metrics.insertError(table)
			if ctx.Err() != nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
		}
		in.log.Error("cannot store derived events; they are lost (the cycles are stored)", "table", table,
			"events", len(rows), "err", err)
	}
	var changes, outages [][]byte
	for _, c := range ev.changes {
		b, _ := json.Marshal(c)
		changes = append(changes, b)
	}
	for _, o := range ev.outages {
		b, _ := json.Marshal(o)
		outages = append(outages, b)
		if o.EndTS == nil {
			in.metrics.outage("open")
		} else {
			in.metrics.outage("close")
		}
	}
	in.metrics.pathChanges(len(ev.changes))
	insert("path_changes", changes)
	insert("outages", outages)
}
