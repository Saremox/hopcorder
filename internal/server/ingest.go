package server

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
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
// mark moves only after a cycles insert succeeded, and so a retried batch
// (say, after a lost response) is stored exactly once.
type Ingester struct {
	store   Store
	enrich  Enricher
	log     *slog.Logger
	metrics *serverMetrics

	// State is kept per agent and per stream, keyed by what agents send, so
	// it is bounded. There is state for at most maxStreams streams, and for
	// at most streamsPerAgent of one agent: a stream beyond them keeps none
	// (its cycles are stored but derive no events). Streams and agents idle
	// for idleTTL are dropped (a stream that comes back is restored from
	// storage, as after a restart); at maxStreams the agents with the most
	// streams drop their least recently used, and beyond maxAgents the
	// least recently used agents go.
	maxStreams         int
	maxStreamsPerAgent int // 0: the default (see streamsPerAgent)
	maxAgents          int
	idleTTL            time.Duration
	// maxRecordAge is how old a record may be (the handler refuses older
	// ones); restores look no further back than that.
	maxRecordAge time.Duration
	// Each request restores at most maxRestores streams, within
	// restoreTimeout of its first restore.
	maxRestores    int
	restoreTimeout time.Duration
	now            func() time.Time
	nstreams       atomic.Int64

	mu        sync.Mutex
	agents    map[string]*agentState
	lastSweep time.Time
}

type agentState struct {
	mu        sync.Mutex
	evicted   bool // dropped from Ingester.agents; look the agent up again
	loaded    bool
	highWater uint64
	streams   map[streamKey]*stream
	used      time.Time
	// nstreams is len(streams), for sweeps that find the agent locked.
	nstreams atomic.Int64
	// keep, unless negative, is how many streams a sweep left the agent
	// while it was locked; ingestAgent drops the rest before unlocking.
	keep atomic.Int64
}

func newAgentState() *agentState {
	a := &agentState{streams: map[streamKey]*stream{}}
	a.keep.Store(-1)
	return a
}

// Defaults for the ingester's state bounds.
const (
	// DefaultMaxStreams: a stream costs about 1 KiB (15 IPv4 hops) to 9 KiB
	// (255 IPv6 hops and a 255-character target), so 20,000 hold at most
	// about 170 MiB.
	DefaultMaxStreams = 20000
	// DefaultMaxStreamsPerAgent is as many streams as one request may
	// carry; an agent has a stream per target, protocol and flow. (At most
	// half of max_streams by default: see streamsPerAgent.)
	DefaultMaxStreamsPerAgent = maxStreamsPerRequest
	// DefaultMaxAgents: an agent's state without its streams is under
	// 1 KiB; agents normally have several streams each, so this only binds
	// when someone sends many agent_ids.
	DefaultMaxAgents     = 10000
	DefaultStreamIdleTTL = time.Hour
	// DefaultMaxRecordAge bounds how old a record may be; the agent's spool
	// keeps records for 7 days by default.
	DefaultMaxRecordAge = 30 * 24 * time.Hour
	sweepInterval       = time.Minute
	// staleWindow: a batch whose seqs all lie this far below the agent's
	// stored ones is not a retry but a reset spool or a second agent with
	// the same agent_id.
	staleWindow = 100000
	// insertTimeout bounds the cycles inserts of one agent's batch, which
	// are not cancelled when the agent disconnects: an insert that
	// ClickHouse committed must be accounted for.
	insertTimeout = 30 * time.Second
	// maxInsertBytes bounds one cycles insert: its rows' JSON, and what
	// hops_mv makes of them, hopRowBytes per hop row plus the hop's loop
	// TTLs as text (see buildCycleRow). Enrichment makes a row up to about
	// seven times its record (AS and rDNS names on every hop), and a hop's
	// loop TTLs can be 900 bytes on each of 255 hops, so a large batch is
	// inserted in parts; an agent's normal batch (1 MiB) is one insert.
	maxInsertBytes = 8 << 20
	// hopRowBytes is about what a hop row costs ClickHouse besides its
	// strings: the cycle's columns repeated on it and its own fixed-size
	// ones (about 80 bytes written per row with 25.8).
	hopRowBytes = 64
	// A request restores at most maxRestores streams, within restoreTimeout
	// of its first restore. Each restore is one to four queries, run while
	// the request holds an ingest slot; the streams left over are restored
	// with their next batch.
	maxRestores    = 256
	restoreTimeout = 5 * time.Second
	// restoreRecent is where a restore looks for a stream's previous cycles
	// first, before the new cycle; only if it finds none does it look back
	// maxRecordAge. A restore reads every cycle of the agent, target and
	// protocol in its window, and the stream's last cycles are almost
	// always within a day of its next one.
	restoreRecent = 24 * time.Hour
	// eventsTimeout bounds storing the derived events of one batch.
	eventsTimeout = 10 * time.Second
)

// ErrStaleBatch is returned for a batch whose records all lie far below the
// agent's stored seqs; storing nothing and reporting success would make the
// agent discard them silently.
var ErrStaleBatch = errors.New("seqs far below the ones stored for this agent_id: was the agent's spool reset, " +
	"or does another agent use the same agent_id? Give it a new agent_id")

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
	return &Ingester{store: store, enrich: en, log: log, metrics: m, agents: map[string]*agentState{},
		maxStreams: DefaultMaxStreams, maxAgents: DefaultMaxAgents, idleTTL: DefaultStreamIdleTTL,
		maxRecordAge: DefaultMaxRecordAge, maxRestores: maxRestores, restoreTimeout: restoreTimeout, now: time.Now}
}

// lockAgent returns the agent's state, locked.
func (in *Ingester) lockAgent(id string) *agentState {
	for {
		in.mu.Lock()
		a, ok := in.agents[id]
		if !ok {
			a = newAgentState()
			in.agents[id] = a
		}
		in.mu.Unlock()
		a.mu.Lock()
		if !a.evicted {
			return a
		}
		a.mu.Unlock() // swept between lookup and lock
	}
}

// streamsPerAgent is how many streams one agent may have state for:
// maxStreamsPerAgent (at most maxStreams), by default 4096 or half of
// maxStreams if that is less, so that no one agent can fill the state.
func (in *Ingester) streamsPerAgent() int {
	if in.maxStreamsPerAgent > 0 {
		return min(in.maxStreamsPerAgent, in.maxStreams)
	}
	return max(1, min(DefaultMaxStreamsPerAgent, in.maxStreams/2))
}

// sweep drops idle streams and agents, the least recently used agents
// beyond maxAgents (down to 90%), and, once the streams reach maxStreams,
// streams down to 90% of it: the agents with the most streams give up
// their least recently used first (see waterLevel), so one agent sending
// many streams pays for them, not everyone else. An agent being ingested
// right now cannot be swept; it is told how many streams to keep and drops
// the rest itself before it unlocks (ingestAgent). It walks all state,
// which the limits bound, at most every sweepInterval, or every second
// while at a limit.
func (in *Ingester) sweep() {
	now := in.now()
	in.mu.Lock()
	defer in.mu.Unlock()
	overStreams, overAgents := in.nstreams.Load() >= int64(in.maxStreams), len(in.agents) > in.maxAgents
	over := overStreams || overAgents
	if since := now.Sub(in.lastSweep); since < sweepInterval && (!over || since < time.Second) {
		return
	}
	in.lastSweep = now
	cutoff := now.Add(-in.idleTTL)
	type lruAgent struct {
		id   string
		a    *agentState
		used time.Time
	}
	type holder struct {
		a *agentState
		n int
	}
	var agents []lruAgent
	var holders []holder
	for id, a := range in.agents {
		if !a.mu.TryLock() {
			if n := int(a.nstreams.Load()); overStreams && n > 0 {
				holders = append(holders, holder{a, n})
			}
			continue
		}
		a.keep.Store(-1) // a share left from an earlier sweep: this one decides
		for k, s := range a.streams {
			if s.used.Before(cutoff) {
				in.dropStream(a, k)
			}
		}
		if len(a.streams) == 0 && a.used.Before(cutoff) {
			a.evicted = true
			delete(in.agents, id)
		} else {
			if overAgents {
				agents = append(agents, lruAgent{id, a, a.used})
			}
			if overStreams && len(a.streams) > 0 {
				holders = append(holders, holder{a, len(a.streams)})
			}
		}
		a.mu.Unlock()
	}
	if excess := len(in.agents) - in.maxAgents*9/10; overAgents && excess > 0 {
		slices.SortFunc(agents, func(x, y lruAgent) int { return x.used.Compare(y.used) })
		n := 0
		for _, l := range agents[:min(excess, len(agents))] {
			if l.a.mu.TryLock() {
				l.a.evicted = true
				delete(in.agents, l.id)
				in.nstreams.Add(-int64(len(l.a.streams)))
				l.a.nstreams.Store(0)
				l.a.streams = nil
				l.a.mu.Unlock()
				n++
			}
		}
		in.metrics.evicted("agent", n)
		in.log.Warn("tracking too many agents; dropped the least recently used (they are restored from storage when they come back)",
			"max_agents", in.maxAgents, "dropped", n)
	}
	if excess := in.nstreams.Load() - int64(in.maxStreams)*9/10; overStreams && excess > 0 {
		// Agents just dropped above (only sweeps set evicted, under in.mu)
		// have no streams left to give.
		holders = slices.DeleteFunc(holders, func(h holder) bool { return h.a.evicted })
		counts := make([]int, len(holders))
		for i, h := range holders {
			counts[i] = h.n
		}
		keep := waterLevel(counts, excess)
		n, busy := 0, 0
		for _, h := range holders {
			if h.n <= keep {
				continue
			}
			if h.a.mu.TryLock() {
				n += in.trimLocked(h.a, keep)
				h.a.mu.Unlock()
			} else {
				h.a.keep.Store(int64(keep))
				busy++
			}
		}
		in.metrics.evicted("stream", n)
		in.log.Warn("tracking too many streams; the agents with the most dropped their least recently used "+
			"(they are restored from storage when they come back)", "max_streams", in.maxStreams, "dropped", n,
			"kept_per_agent", keep, "agents_dropping_later", busy)
	}
}

// waterLevel returns the highest level such that cutting every count above
// it down to it removes at least excess: the largest counts are cut first,
// and no count at or below the level is touched.
func waterLevel(counts []int, excess int64) int {
	counts = slices.Clone(counts)
	slices.SortFunc(counts, func(x, y int) int { return y - x })
	var sum int64
	for i, c := range counts {
		sum += int64(c)
		next := 0
		if i+1 < len(counts) {
			next = counts[i+1]
		}
		// Levelling the i+1 largest at L removes sum - (i+1)*L.
		if level := (sum - excess) / int64(i+1); level >= int64(next) {
			return int(max(level, 0))
		}
	}
	return 0
}

// trimLocked drops a's least recently used streams beyond keep and returns
// how many it dropped; a must be locked.
func (in *Ingester) trimLocked(a *agentState, keep int) int {
	n := len(a.streams) - keep
	if n <= 0 {
		return 0
	}
	type lru struct {
		k    streamKey
		used time.Time
	}
	all := make([]lru, 0, len(a.streams))
	for k, s := range a.streams {
		all = append(all, lru{k, s.used})
	}
	slices.SortFunc(all, func(x, y lru) int { return x.used.Compare(y.used) })
	for _, l := range all[:n] {
		in.dropStream(a, l.k)
	}
	return n
}

// unlockAgent drops the streams a sweep found a (agent id) should give up
// while it was locked, then unlocks it.
func (in *Ingester) unlockAgent(id string, a *agentState) {
	if keep := a.keep.Swap(-1); keep >= 0 {
		if n := in.trimLocked(a, int(keep)); n > 0 {
			in.metrics.evicted("stream", n)
			in.log.Warn("agent has more streams than tracking leaves it room for; dropped its least recently used",
				"agent_id", id, "dropped", n, "kept", keep, "max_streams", in.maxStreams)
		}
	}
	a.mu.Unlock()
}

// restoreBudget bounds the stream restores of one request.
type restoreBudget struct {
	left     int
	timeout  time.Duration
	deadline time.Time // set by the first restore
}

func (in *Ingester) newRestoreBudget() *restoreBudget {
	return &restoreBudget{left: in.maxRestores, timeout: in.restoreTimeout}
}

// take reports whether another restore may start, and until when it may run.
func (b *restoreBudget) take() (time.Time, bool) {
	now := time.Now()
	if b.deadline.IsZero() {
		b.deadline = now.Add(b.timeout)
	}
	if b.left <= 0 || !now.Before(b.deadline) {
		return time.Time{}, false
	}
	b.left--
	return b.deadline, true
}

// Ingest stores recs (already validated). It returns an error only when
// storage failed, in which case the failing agent's records from the first
// failed insert on are not stored and the batch should be retried.
func (in *Ingester) Ingest(ctx context.Context, recs []*record.Record) (IngestResult, error) {
	in.sweep()
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
	rb := in.newRestoreBudget()
	for _, id := range ids {
		acc, dup, err := in.ingestAgent(ctx, id, byAgent[id], rb)
		res.Accepted += acc
		res.Duplicates += dup
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

func (in *Ingester) ingestAgent(ctx context.Context, id string, recs []*record.Record, rb *restoreBudget) (accepted, dups int, err error) {
	a := in.lockAgent(id)
	defer in.unlockAgent(id, a)
	now := in.now()
	a.used = now
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
		if n := len(recs); n > 0 && recs[n-1].Seq < a.highWater && a.highWater-recs[n-1].Seq > staleWindow {
			in.log.Warn("agent sends seqs far below what is stored; refusing the batch so the agent keeps it",
				"agent_id", id, "batch_max_seq", recs[n-1].Seq, "stored_max_seq", a.highWater)
			return 0, 0, ErrStaleBatch
		}
		in.metrics.records(0, dups)
		return 0, dups, nil
	}

	stored, err := in.insertCycles(ctx, a, fresh)
	in.metrics.records(stored, dups)
	// Derived events of what was stored, even if a later part failed: the
	// retry sees those records as duplicates.
	in.derive(ctx, a, fresh[:stored], now, rb)
	return stored, dups, err
}

// insertCycles inserts recs (in seq order) in parts of at most
// maxInsertBytes (the rows and the hop rows the view makes of them, see
// buildCycleRow) and moves a's high-water mark after each. It returns how
// many records were stored. Not cancelled with the request: if the agent
// disconnects after ClickHouse committed, the high-water mark must still
// move.
func (in *Ingester) insertCycles(ctx context.Context, a *agentState, recs []*record.Record) (int, error) {
	ictx, cancel := context.WithTimeout(context.WithoutCancel(ctx), insertTimeout)
	defer cancel()
	stored := 0
	for stored < len(recs) {
		var rows [][]byte
		size, end := 0, stored
		for end < len(recs) && size < maxInsertBytes {
			b, cost, err := buildCycleRow(recs[end], in.enrich)
			if err != nil {
				return stored, fmt.Errorf("encode record %s/%d: %w", recs[end].AgentID, recs[end].Seq, err)
			}
			rows = append(rows, b)
			size += cost
			end++
		}
		start := time.Now()
		if err := in.store.InsertJSON(ictx, "cycles", rows); err != nil {
			in.metrics.insertError("cycles")
			// The insert may have been committed all the same: re-read the
			// high-water mark before the next batch rather than trusting ours.
			a.loaded = false
			return stored, fmt.Errorf("insert cycles: %w", err)
		}
		in.metrics.insertDuration(time.Since(start))
		a.highWater = recs[end-1].Seq
		stored = end
	}
	return stored, nil
}

// derive feeds stored records to their streams and stores the events they
// produce. The cycles are committed at this point; events are derived data,
// so a failure here is logged rather than failing (and re-sending) a batch
// that is already stored. A stream whose state cannot be restored now
// (budget used up, or storage failing) skips this batch's cycles and is
// restored with its next batch, from storage, which then holds these
// cycles: it never starts from scratch while ClickHouse has its history.
func (in *Ingester) derive(ctx context.Context, a *agentState, recs []*record.Record, now time.Time, rb *restoreBudget) {
	var ev events
	tried := map[*stream]bool{}
	var skipped, failed, noRoom int
	for _, r := range recs {
		s := in.stream(a, keyOf(r))
		if s == nil {
			noRoom++
			continue
		}
		s.used = now
		if !s.loaded && !tried[s] {
			tried[s] = true
			if deadline, ok := rb.take(); ok {
				if err := in.restore(ctx, deadline, s, r, &ev); err != nil {
					failed++
					in.log.Debug("cannot restore stream state", "stream", s.key, "err", err)
				}
			}
		}
		if !s.loaded {
			skipped++
			continue
		}
		s.observe(r, &ev)
	}
	if skipped > 0 {
		in.metrics.unrestoredCycles(skipped)
		in.log.Warn("stream state not restored in time; these cycles derive no path changes or outages "+
			"(the streams are restored with their next batch)", "agent_id", recs[0].AgentID, "cycles", skipped,
			"failed_restores", failed)
	}
	if noRoom > 0 {
		in.metrics.unrestoredCycles(noRoom)
		in.log.Warn("no room to track more streams; these cycles derive no path changes or outages "+
			"(raise ingest.max_streams or max_streams_per_agent if this agent's streams are real)",
			"agent_id", recs[0].AgentID, "cycles", noRoom, "agent_streams", len(a.streams),
			"max_streams_per_agent", in.streamsPerAgent(), "max_streams", in.maxStreams)
	}
	ectx, cancel := context.WithTimeout(context.WithoutCancel(ctx), eventsTimeout)
	defer cancel()
	in.insertEvents(ectx, ev)
}

// stream returns a's stream k, creating it if there is room: nil if a has
// streamsPerAgent streams already, or all agents together maxStreams. a
// must be locked.
func (in *Ingester) stream(a *agentState, k streamKey) *stream {
	if s, ok := a.streams[k]; ok {
		return s
	}
	if len(a.streams) >= in.streamsPerAgent() {
		return nil
	}
	for n := in.nstreams.Load(); ; n = in.nstreams.Load() {
		if n >= int64(in.maxStreams) {
			return nil
		}
		if in.nstreams.CompareAndSwap(n, n+1) {
			break
		}
	}
	s := &stream{key: k}
	a.streams[k] = s
	a.nstreams.Add(1)
	return s
}

// dropStream forgets a's stream k; a must be locked.
func (in *Ingester) dropStream(a *agentState, k streamKey) {
	delete(a.streams, k)
	a.nstreams.Add(-1)
	in.nstreams.Add(-1)
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

// restore primes stream s from storage before its cycle first (after a
// server restart or after s was dropped): the stream's last cycles are
// replayed into the path tracker, and an outage left open is resumed. Only
// a complete restore marks s loaded; on an error s is left as it was.
// Queries run until deadline, not cancelled with the request.
func (in *Ingester) restore(ctx context.Context, deadline time.Time, s *stream, first *record.Record, ev *events) error {
	ctx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	k := s.key
	params := clickhouse.Params{"agent": k.Agent, "target": k.Target, "protocol": k.Protocol,
		"flow": strconv.Itoa(k.Flow), "before": strconv.FormatUint(first.Seq, 10), "depth": strconv.Itoa(replayDepth)}
	where := `agent_id = {agent:String} AND target = {target:String} AND protocol = {protocol:String}
		AND flow_id = {flow:UInt16}`

	type cyc struct {
		Seq       uint64   `json:"seq"`
		TS        string   `json:"ts"`
		TargetIP  string   `json:"target_ip"`
		Reached   bool     `json:"reached"`
		PathLen   int      `json:"path_len"`
		PathHash  string   `json:"path_hash"`
		HopTTL    []int    `json:"hops.ttl"`
		HopStatus []string `json:"hops.status"`
		HopIP     []string `json:"hops.ip"`
	}
	// ts bounds what ClickHouse reads: the sort key is (agent_id, target,
	// protocol, ts), so without it a restore reads the whole history of
	// every flow of the agent, target and protocol.
	var rows []cyc
	for _, back := range []time.Duration{min(restoreRecent, in.maxRecordAge), in.maxRecordAge} {
		var err error
		rows, err = clickhouseQuery[cyc](ctx, in.store, `SELECT seq, ts, target_ip, reached, path_len,
			path_hash, hops.ttl, hops.status, hops.ip FROM cycles WHERE `+where+` AND seq < {before:UInt64}
			AND ts >= {since:DateTime64(3)} ORDER BY seq DESC LIMIT {depth:UInt32}`,
			withParam(params, "since", clickhouse.Time(first.TS.Add(-back))))
		if err != nil {
			return fmt.Errorf("replay cycles: %w", err)
		}
		if len(rows) > 0 || back >= in.maxRecordAge {
			break
		}
	}
	slices.Reverse(rows)
	recs := make([]*record.Record, 0, len(rows))
	for _, c := range rows {
		r := &record.Record{AgentID: k.Agent, Target: k.Target, Protocol: k.Protocol, FlowID: k.Flow,
			Seq: c.Seq, Reached: c.Reached, PathLen: c.PathLen, PathHash: c.PathHash}
		r.TS = parseTime64(c.TS)
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
	restored := stream{key: k}
	restored.replay(recs)
	if len(recs) == 0 || !recs[len(recs)-1].Reached {
		// The stream may have been down when its state was lost (or its
		// last cycles lie beyond the window): resume its open outage. Not
		// bounded by time: an outage can be older than any window, and
		// outages has a row per outage, not per cycle.
		open, err := clickhouseQuery[outageRow](ctx, in.store, `SELECT agent_id, target, protocol, flow_id, start_seq,
			toString(start_ts) AS start_ts, end_seq, NULL AS end_ts, cycles, last_hop_ttl, last_hop_ip, version
			FROM outages FINAL WHERE `+where+` AND end_ts IS NULL AND start_seq < {before:UInt64}
			ORDER BY start_seq DESC LIMIT 1`, params)
		if err != nil {
			return fmt.Errorf("find open outage: %w", err)
		}
		switch {
		case len(open) == 1:
			type n struct {
				N int `json:"n"`
			}
			cnt, err := clickhouseQuery[n](ctx, in.store, `SELECT count() AS n FROM cycles WHERE `+where+`
				AND seq >= {start:UInt64} AND seq < {before:UInt64} AND ts >= {start_ts:DateTime64(3)}`,
				withParam(withParam(params, "start", strconv.FormatUint(open[0].StartSeq, 10)), "start_ts", open[0].StartTS))
			if err != nil {
				return fmt.Errorf("count outage cycles: %w", err)
			}
			if len(cnt) == 1 {
				open[0].Cycles = cnt[0].N
			}
			restored.outage = &open[0]
		case len(recs) > 0:
			// Down, but no open outage stored: its start was in cycles that
			// derived no events (a batch whose restore did not happen in
			// time, or whose events could not be stored). It starts at the
			// first of the trailing unreached cycles replayed.
			i := len(recs)
			for i > 0 && !recs[i-1].Reached {
				i--
			}
			o := newOutage(recs[i], len(recs)-i)
			restored.outage = &o
			ev.outages = append(ev.outages, o)
		}
	}
	s.tracker, s.last, s.outage, s.loaded = restored.tracker, restored.last, restored.outage, true
	return nil
}

// parseTime64 reads a DateTime64 as the client returns it (ISO 8601), or
// as clickhouse.Time writes it.
func parseTime64(s string) time.Time {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	t, _ := time.Parse("2006-01-02 15:04:05.000", s)
	return t
}

func withParam(p clickhouse.Params, k, v string) clickhouse.Params {
	out := clickhouse.Params{k: v}
	for kk, vv := range p {
		if kk != k {
			out[kk] = vv
		}
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
