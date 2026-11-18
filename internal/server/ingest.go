package server

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
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
	Exec(ctx context.Context, query string, params clickhouse.Params) error
	Query(ctx context.Context, query string, params clickhouse.Params, row func(json.RawMessage) error) error
}

// replayDepth is how many past cycles of a stream are replayed into its
// path tracker after a server restart.
const replayDepth = 30

// Ingester turns validated records into rows and events.
//
// Deduplication on (agent_id, seq): an agent ships its records in seq order
// and only moves on once a batch was accepted. Each agent's batches are
// processed one at a time under its own lock, in parts (see nextPart) that
// are inserted in seq order, each into one partition of cycles and so
// stored there whole or not at all. The agent's mark (highWater, written to
// agent_seq by the server) moves past a part only once the part is stored
// with its hop and loop rows and its events were derived: records at or
// below it are duplicates.
//
// A cycles insert is not all-or-nothing beyond cycles: ClickHouse commits
// the cycles part before it runs the views, so a failing view (memory, disk,
// a restart) leaves the part stored without some of its hop or loop rows,
// and an insert can outlive the client's timeout and commit later. So after
// any failed write, and when the agent's state is loaded (after a restart,
// or after it was dropped), the agent is unsure: its next batch first stops
// whatever of its statements still runs in ClickHouse (settle), then looks
// up which of its records cycles already holds (findStored). Those are not
// inserted again; their missing hop and loop rows are written from cycles
// (fillViews) and their events are derived with the rest. Since parts are
// stored whole and in seq order, a part whose last record is not stored has
// nothing of the agent stored above it, and the agent is sure again.
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
	// partBytes bounds one insert (maxInsertBytes; tests lower it).
	partBytes int
	now       func() time.Time
	nstreams  atomic.Int64

	mu        sync.Mutex
	agents    map[string]*agentState
	lastSweep time.Time
}

type agentState struct {
	mu      sync.Mutex
	evicted bool // dropped from Ingester.agents; look the agent up again
	loaded  bool
	// highWater is the agent's mark: its cycles up to this seq are stored
	// with their hop and loop rows, and their events were derived.
	highWater uint64
	// unsure: cycles of the agent above highWater may be stored (a write
	// failed or timed out, or the state was just loaded); see Ingester.
	unsure  bool
	streams map[streamKey]*stream
	used    time.Time
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
	// the same agent_id, unless the server holds every one of its cycles
	// (see holdsAll).
	staleWindow = 100000
	// insertTimeout bounds the writes of one agent's batch (cycles, hop
	// and loop rows filled in, the mark), which are not cancelled when the
	// agent disconnects: an insert that ClickHouse committed must be
	// accounted for.
	insertTimeout = 30 * time.Second
	// maxInsertBytes bounds one cycles insert: its rows' JSON, and what
	// hops_mv makes of them, hopRowBytes per hop row plus the hop's loop
	// TTLs as text (see buildCycleRow). Enrichment makes a row up to about
	// seven times its record (AS and rDNS names on every hop), and a hop's
	// loop TTLs can be 900 bytes on each of 255 hops, so a large batch is
	// inserted in parts; an agent's normal batch (1 MiB) is one insert.
	// The events of a part are inserted after it, in inserts of at most
	// this many bytes of JSON too.
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
	// queryIDPrefix starts the query_id of every write of an agent's
	// cycles and of the hop and loop rows filled in for them, followed by
	// the agent_id and "/" (not in agent_ids), so that settle can stop the
	// agent's writes that are still running, a previous process's too.
	queryIDPrefix = "hopcorder/"
)

// ErrStaleBatch is returned for a batch whose records all lie far below the
// agent's stored seqs, and which the server does not hold as they are (see
// holdsAll); storing nothing and reporting success would make the agent
// discard them silently.
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
		maxRecordAge: DefaultMaxRecordAge, maxRestores: maxRestores, restoreTimeout: restoreTimeout,
		partBytes: maxInsertBytes, now: time.Now}
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
// storage failed; the failing agent's records from the part that failed on
// may then be stored or not, and the batch should be retried, which is safe
// (see Ingester).
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
		// A previous process (or this one, before it dropped the agent's
		// state) may have left writes above the mark.
		a.highWater, a.loaded, a.unsure = hw, true, true
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
			held, err := in.holdsAll(ctx, id, recs)
			if err != nil {
				return 0, 0, err
			}
			if !held {
				in.log.Warn("agent sends seqs far below what is stored; refusing the batch so the agent keeps it",
					"agent_id", id, "batch_max_seq", recs[n-1].Seq, "stored_max_seq", a.highWater)
				return 0, 0, ErrStaleBatch
			}
		}
		in.metrics.records(0, dups)
		return 0, dups, nil
	}

	stored, err := in.storeRecords(ctx, a, id, fresh, now, rb)
	in.metrics.records(stored, dups)
	return stored, dups, err
}

// storeRecords stores recs (agent id's, in seq order, above its mark) part
// by part: once a part is stored, its events are derived and stored and the
// mark moves past it, so the memory a batch takes beyond its records is one
// part's. It returns how many of recs are stored. The writes are not
// cancelled with the request: if the agent disconnects after ClickHouse
// committed, the mark must still move.
func (in *Ingester) storeRecords(ctx context.Context, a *agentState, id string, recs []*record.Record, now time.Time,
	rb *restoreBudget) (int, error) {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), insertTimeout)
	defer cancel()
	// Events have their own budget, from the first part's on.
	ectx := &lazyTimeout{parent: context.WithoutCancel(ctx), d: eventsTimeout}
	defer ectx.stop()
	var st deriveStats
	defer in.reportDerive(a, id, &st)

	if a.unsure {
		if err := in.settle(wctx, id); err != nil {
			return 0, err
		}
	}
	stored := 0
	for stored < len(recs) {
		end, rows, err := in.nextPart(recs, stored)
		if err != nil {
			return stored, err
		}
		part := recs[stored:end]
		last := part[len(part)-1].Seq
		sure := !a.unsure
		if a.unsure {
			found, err := in.findStored(wctx, id, part)
			if err != nil {
				return stored, err
			}
			if len(found) > 0 {
				if err := in.fillViews(wctx, id, part, found); err != nil {
					return stored, err
				}
				in.metrics.recovered(len(found))
				in.log.Info("cycles of a failed or interrupted insert were stored after all; not inserting them again",
					"agent_id", id, "cycles", len(found))
				keep := rows[:0]
				for i, r := range part {
					if !found[r.Seq] {
						keep = append(keep, rows[i])
					}
				}
				rows = keep
			}
			// Parts are stored whole and in seq order: if this one's last
			// record is not stored, nothing of the agent above it is.
			sure = !found[last]
		}
		if err := in.insertPart(wctx, id, rows); err != nil {
			a.unsure = true
			return stored, err
		}
		in.insertEvents(ectx.get(), in.derive(ctx, a, part, now, rb, &st))
		in.writeMark(wctx, id, last)
		a.highWater, a.unsure = last, !sure
		stored = end
	}
	return stored, nil
}

// lazyTimeout is a context with a timeout that starts at its first use.
type lazyTimeout struct {
	parent context.Context
	d      time.Duration
	ctx    context.Context
	cancel context.CancelFunc
}

func (l *lazyTimeout) get() context.Context {
	if l.ctx == nil {
		l.ctx, l.cancel = context.WithTimeout(l.parent, l.d)
	}
	return l.ctx
}

func (l *lazyTimeout) stop() {
	if l.cancel != nil {
		l.cancel()
	}
}

// nextPart returns where the part of recs that starts at start ends, and
// its cycles rows: at most partBytes (the rows and the hop rows the view
// makes of them, see buildCycleRow), within one month. A month is a
// partition of cycles, so ClickHouse stores the part in cycles as one part
// of the table: whole or not at all.
func (in *Ingester) nextPart(recs []*record.Record, start int) (int, [][]byte, error) {
	var rows [][]byte
	size, end := 0, start
	month := monthOf(recs[start].TS)
	for end < len(recs) && size < in.partBytes && monthOf(recs[end].TS) == month {
		b, cost, err := buildCycleRow(recs[end], in.enrich)
		if err != nil {
			return end, nil, fmt.Errorf("encode record %s/%d: %w", recs[end].AgentID, recs[end].Seq, err)
		}
		rows = append(rows, b)
		size += cost
		end++
	}
	return end, rows, nil
}

// monthOf is toYYYYMM(ts), the partition of a cycle and its hop and loop
// rows, as a number.
func monthOf(t time.Time) int {
	t = t.UTC()
	return t.Year()*12 + int(t.Month())
}

func (in *Ingester) insertPart(ctx context.Context, id string, rows [][]byte) error {
	if len(rows) == 0 {
		return nil
	}
	start := time.Now()
	if err := in.store.InsertJSON(writeCtx(ctx, id), "cycles", rows); err != nil {
		in.metrics.insertError("cycles")
		return fmt.Errorf("insert cycles: %w", err)
	}
	in.metrics.insertDuration(time.Since(start))
	return nil
}

// writeCtx gives a write of agent id's cycles (or of their hop and loop
// rows) a query_id that settle finds it by, and the settings the ingester
// relies on whatever the user's profile says: the insert is the client's
// own (not buffered with others'), and it fails if a view fails.
func writeCtx(ctx context.Context, id string) context.Context {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return clickhouse.WithWriteSettings(ctx, clickhouse.Settings{
		"query_id":                         queryIDPrefix + id + "/" + hex.EncodeToString(b[:]),
		"async_insert":                     "0",
		"materialized_views_ignore_errors": "0",
	})
}

// settle stops the writes of agent id that still run in ClickHouse (an
// insert of this process that outlived its timeout, or one of a process
// before a restart) and waits until they have ended, so that what
// findStored sees no longer changes. (KILL QUERY needs the server's
// ClickHouse user to read query_id, user and query of system.processes;
// Migrate checks that.)
func (in *Ingester) settle(ctx context.Context, id string) error {
	if err := in.store.Exec(ctx, "KILL QUERY WHERE startsWith(query_id, {prefix:String}) SYNC",
		clickhouse.Params{"prefix": queryIDPrefix + id + "/"}); err != nil {
		return fmt.Errorf("stop earlier writes of %s: %w", id, err)
	}
	return nil
}

// partWindow selects an agent's rows in cycles, hops or loop_events by seq
// and ts, both within a part's: ts is in the sort keys, seq is not. A cycle
// is stored with the ts clickhouse.Time gives (in UTC, to the
// millisecond), as here, so the window holds it whatever ClickHouse's
// time zone.
const partWindow = `agent_id = {agent:String} AND seq >= {lo:UInt64} AND seq <= {hi:UInt64}
	AND ts >= {from:DateTime64(3, 'UTC')} AND ts <= {to:DateTime64(3, 'UTC')}`

func windowParams(id string, recs []*record.Record) clickhouse.Params {
	from, to := recs[0].TS, recs[0].TS
	for _, r := range recs {
		if r.TS.Before(from) {
			from = r.TS
		}
		if r.TS.After(to) {
			to = r.TS
		}
	}
	return clickhouse.Params{"agent": id, "lo": strconv.FormatUint(recs[0].Seq, 10),
		"hi": strconv.FormatUint(recs[len(recs)-1].Seq, 10), "from": clickhouse.Time(from), "to": clickhouse.Time(to)}
}

// findStored returns the seqs of part's records that cycles holds already.
func (in *Ingester) findStored(ctx context.Context, id string, part []*record.Record) (map[uint64]bool, error) {
	type row struct {
		Seq uint64 `json:"seq"`
	}
	rows, err := clickhouseQuery[row](ctx, in.store, "SELECT DISTINCT seq FROM cycles WHERE "+partWindow,
		windowParams(id, part))
	if err != nil {
		return nil, fmt.Errorf("look up stored cycles of %s: %w", id, err)
	}
	found := map[uint64]bool{}
	for _, r := range rows {
		if _, ok := slices.BinarySearchFunc(part, r.Seq, func(x *record.Record, s uint64) int { return cmp.Compare(x.Seq, s) }); ok {
			found[r.Seq] = true
		}
	}
	return found, nil
}

// holdsAll reports whether cycles holds every one of recs (agent id's, in
// seq order) as it is: a cycle at its seq with its ts, to the millisecond
// that cycles keeps. A batch far below the agent's mark that it holds so
// is one the server stored before, delivered again, not a reset spool's
// or another agent's (review, round eight): an agent's seqs jump at every
// open of its spool (they go on from the clock), so the batch of a spool
// that went back to an earlier state, or a retry cut shorter, can lie
// that far below.
func (in *Ingester) holdsAll(ctx context.Context, id string, recs []*record.Record) (bool, error) {
	type row struct {
		Seq uint64 `json:"seq"`
		MS  int64  `json:"ms"`
	}
	rows, err := clickhouseQuery[row](ctx, in.store,
		"SELECT DISTINCT seq, toUnixTimestamp64Milli(ts) AS ms FROM cycles WHERE "+partWindow, windowParams(id, recs))
	if err != nil {
		return false, fmt.Errorf("look up stored cycles of %s: %w", id, err)
	}
	held := make(map[row]bool, len(rows))
	for _, r := range rows {
		held[r] = true
	}
	for _, r := range recs {
		if !held[row{r.Seq, r.TS.UnixMilli()}] {
			return false, nil
		}
	}
	return true, nil
}

// The hop and loop rows of stored cycles that hops_mv and loop_events_mv
// did not write (one failed, or its insert was stopped), made from cycles
// as those views make them, and only where they are missing, so that a
// fill can run any number of times. They must change with the views
// (TestFillMatchesViews checks that they agree).
const (
	fillHops = `INSERT INTO hops (ts, agent_id, seq, target, target_ip, af, protocol, flow_id, reached, path_len,
		path_hash, ttl, status, hop_ip, rtt_us, code, hop_asn, hop_as_name, hop_name, mpls, is_loop, loop_ttls)
	SELECT ts, agent_id, seq, target, target_ip, af, protocol, flow_id, reached, path_len, path_hash,
		hops.ttl AS ttl, hops.status AS status, hops.ip AS hop_ip, hops.rtt_us AS rtt_us, hops.code AS code,
		hops.asn AS hop_asn, hops.as_name AS hop_as_name, hops.name AS hop_name, hops.mpls AS mpls,
		notEmpty(hops.loop_ttls) AS is_loop,
		arrayStringConcat(arrayMap(x -> toString(x), hops.loop_ttls), ',') AS loop_ttls
	FROM cycles ARRAY JOIN hops
	WHERE ` + partWindow + ` AND (seq, ttl) NOT IN (SELECT seq, ttl FROM hops WHERE ` + partWindow + `)`
	fillLoops = `INSERT INTO loop_events (ts, agent_id, seq, target, protocol, flow_id, loop_ip, ttls)
	SELECT ts, agent_id, seq, target, protocol, flow_id, loops.ip AS loop_ip, loops.ttls AS ttls
	FROM cycles ARRAY JOIN loops
	WHERE ` + partWindow + ` AND (seq, loop_ip) NOT IN (SELECT seq, loop_ip FROM loop_events WHERE ` + partWindow + `)`
)

// fillViews writes the missing hop and loop rows of part's records that
// cycles holds (found).
func (in *Ingester) fillViews(ctx context.Context, id string, part []*record.Record, found map[uint64]bool) error {
	var stored []*record.Record
	var hops, loops bool
	for _, r := range part {
		if found[r.Seq] {
			stored = append(stored, r)
			hops, loops = hops || len(r.Hops) > 0, loops || len(r.Loops) > 0
		}
	}
	p := windowParams(id, stored)
	for _, f := range []struct {
		need         bool
		table, query string
	}{{hops, "hops", fillHops}, {loops, "loop_events", fillLoops}} {
		if !f.need {
			continue
		}
		if err := in.store.Exec(writeCtx(ctx, id), f.query, p); err != nil {
			in.metrics.insertError(f.table)
			return fmt.Errorf("fill in %s of stored cycles of %s: %w", f.table, id, err)
		}
	}
	return nil
}

// writeMark stores agent id's mark. If that fails, the mark kept in memory
// moves all the same: the stored one only lags, which costs the next
// process (or this one, once it dropped the agent's state) a findStored of
// records it has.
func (in *Ingester) writeMark(ctx context.Context, id string, seq uint64) {
	b, err := json.Marshal(struct {
		AgentID string `json:"agent_id"`
		MaxSeq  uint64 `json:"max_seq"`
	}{id, seq})
	if err == nil {
		err = in.store.InsertJSON(ctx, "agent_seq", [][]byte{b})
	}
	if err != nil {
		in.metrics.insertError("agent_seq")
		in.log.Warn("cannot store the agent's high-water mark; it is kept in memory", "agent_id", id, "seq", seq, "err", err)
	}
}

// deriveStats counts the cycles of a batch that derived no events.
type deriveStats struct{ skipped, failed, noRoom int }

func (in *Ingester) reportDerive(a *agentState, id string, st *deriveStats) {
	if st.skipped > 0 {
		in.metrics.unrestoredCycles(st.skipped)
		in.log.Warn("stream state not restored in time; these cycles derive no path changes or outages "+
			"(the streams are restored with their next batch)", "agent_id", id, "cycles", st.skipped,
			"failed_restores", st.failed)
	}
	if st.noRoom > 0 {
		in.metrics.unrestoredCycles(st.noRoom)
		in.log.Warn("no room to track more streams; these cycles derive no path changes or outages "+
			"(raise ingest.max_streams or max_streams_per_agent if this agent's streams are real)",
			"agent_id", id, "cycles", st.noRoom, "agent_streams", len(a.streams),
			"max_streams_per_agent", in.streamsPerAgent(), "max_streams", in.maxStreams)
	}
}

// derive feeds stored records to their streams and returns the events they
// produce. A stream whose state cannot be restored now (budget used up, or
// storage failing) skips these cycles and is restored with its next batch,
// from storage, which then holds them: it never starts from scratch while
// ClickHouse has its history. Every record comes here once: in seq order,
// right after its part is stored (or found stored), when the mark moves
// past it. A stream that is down after a cycle of recs has its open outage's
// row written after them, once, with its cycles so far (review round seven:
// it was written once, at open, and showed 1 cycle until it closed).
func (in *Ingester) derive(ctx context.Context, a *agentState, recs []*record.Record, now time.Time, rb *restoreBudget,
	st *deriveStats) events {
	var ev events
	tried := map[*stream]bool{}
	down := map[*stream]bool{}
	var open []*stream // the streams in down, in order
	for _, r := range recs {
		s := in.stream(a, keyOf(r))
		if s == nil {
			st.noRoom++
			continue
		}
		s.used = now
		if !s.loaded && !tried[s] {
			tried[s] = true
			if deadline, ok := rb.take(); ok {
				if err := in.restore(ctx, deadline, s, r, &ev); err != nil {
					st.failed++
					in.log.Debug("cannot restore stream state", "stream", s.key, "err", err)
				}
			}
		}
		if !s.loaded {
			st.skipped++
			continue
		}
		s.observe(r, &ev)
		if s.outage != nil && !down[s] {
			down[s] = true
			open = append(open, s)
		}
	}
	for _, s := range open {
		if s.outage != nil { // still down at the end of recs
			ev.outages = append(ev.outages, *s.outage)
		}
	}
	return ev
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

// restoreWhere selects a stream's rows in a restore's queries.
const restoreWhere = `agent_id = {agent:String} AND target = {target:String} AND protocol = {protocol:String}
	AND flow_id = {flow:UInt16}`

// storedCycle is a stored cycle as a restore reads it back (restoreCols).
type storedCycle struct {
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

const restoreCols = `seq, ts, target_ip, reached, path_len, path_hash, hops.ttl, hops.status, hops.ip`

// record is c as stream k's record, with what a restore uses of it.
func (c storedCycle) record(k streamKey) *record.Record {
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
	return r
}

// restore primes stream s from storage before its cycle first (after a
// server restart or after s was dropped): the stream's last cycles are
// replayed into the path tracker, an outage left open is resumed, and one
// left open that had ended is closed (restoreOutage). Only a complete
// restore marks s loaded and adds to ev; on an error s is left as it was.
// Queries run until deadline, not cancelled with the request.
func (in *Ingester) restore(ctx context.Context, deadline time.Time, s *stream, first *record.Record, ev *events) error {
	ctx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	k := s.key
	params := clickhouse.Params{"agent": k.Agent, "target": k.Target, "protocol": k.Protocol,
		"flow": strconv.Itoa(k.Flow), "before": strconv.FormatUint(first.Seq, 10), "depth": strconv.Itoa(replayDepth)}

	// ts bounds what ClickHouse reads: the sort key is (agent_id, target,
	// protocol, ts), so without it a restore reads the whole history of
	// every flow of the agent, target and protocol.
	var rows []storedCycle
	for _, back := range []time.Duration{min(restoreRecent, in.maxRecordAge), in.maxRecordAge} {
		var err error
		rows, err = clickhouseQuery[storedCycle](ctx, in.store, `SELECT `+restoreCols+` FROM cycles WHERE `+restoreWhere+`
			AND seq < {before:UInt64} AND ts >= {since:DateTime64(3, 'UTC')} ORDER BY seq DESC LIMIT {depth:UInt32}`,
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
		recs = append(recs, c.record(k))
	}
	restored := stream{key: k}
	restored.replay(recs)
	var found events
	o, err := in.restoreOutage(ctx, k, params, first, recs, &found)
	if err != nil {
		return err
	}
	ev.outages = append(ev.outages, found.outages...)
	ev.opened += found.opened
	s.tracker, s.last, s.outage, s.loaded = restored.tracker, restored.last, o, true
	return nil
}

// restoreOutage returns the outage that stream k, restored before its cycle
// first, goes on with, if any; recs are its replayed cycles (its last
// stored cycles before first, oldest first).
//
// An outage that storage holds open although a cycle after its start
// reached the destination ended at the first such cycle, in cycles that
// derived no events: those of a batch in which the stream could not be
// restored (review, round nine; before, it stayed ongoing for good, and a
// restore of the stream down for its whole replay took it up again). Its
// close row is added to ev, as derive writes one, and if the stream is
// down, the outage it is in begins with the first cycle after the last
// that reached.
func (in *Ingester) restoreOutage(ctx context.Context, k streamKey, params clickhouse.Params, first *record.Record,
	recs []*record.Record, ev *events) (*outageRow, error) {
	// newest is the stream's newest outage that started after lo and before
	// hi, open or closed. (Not "end_ts IS NULL" with "NULL AS end_ts"
	// selected, as until review round eight: ClickHouse reads a SELECT alias
	// in WHERE, so that filter was always true and a closed outage came back
	// as open.)
	newest := func(lo, hi uint64) (*outageRow, error) {
		rows, err := clickhouseQuery[outageRow](ctx, in.store, `SELECT agent_id, target, protocol, flow_id, start_seq,
			toString(start_ts) AS start_ts, end_seq, end_ts, cycles, last_hop_ttl, last_hop_ip, version
			FROM outages FINAL WHERE `+restoreWhere+` AND start_seq < {before:UInt64}
			AND start_seq > {after:UInt64} ORDER BY start_seq DESC LIMIT 1`,
			withParam(withParam(params, "before", strconv.FormatUint(hi, 10)), "after", strconv.FormatUint(lo, 10)))
		if err != nil || len(rows) == 0 {
			return nil, err
		}
		return &rows[0], nil
	}
	after := uint64(0) // the last replayed cycle that reached the destination
	for _, r := range slices.Backward(recs) {
		if r.Reached {
			after = r.Seq
			break
		}
	}
	down := len(recs) == 0 || !recs[len(recs)-1].Reached
	var o *outageRow
	current, reopened, newestKnown := false, false, false
	if down {
		// The stream may have been down when its state was lost (or its
		// last cycles lie beyond the window): resume its open outage. It
		// started after the last replayed cycle that reached the
		// destination, if there is one. Otherwise it is not bounded: an
		// outage can be older than any window, and a stream that was down
		// for the whole replay has a row there per outage, not per cycle (a
		// stream that flaps has one per two cycles, so the bound matters).
		found, err := newest(after, first.Seq)
		if err != nil {
			return nil, fmt.Errorf("find open outage: %w", err)
		}
		// It goes on if it is open, or if it closed at or after first: then
		// first's part is derived again (after a crash before the mark), and
		// closes it there again, with the same row. One that closed before
		// first is over; resumed, its next rows (with higher versions) would
		// replace its close row.
		switch {
		case found == nil:
		case found.EndTS == nil:
			o, current = found, true
		case found.EndSeq >= first.Seq:
			found.EndTS, found.EndSeq = nil, 0
			o, current, reopened = found, true, true
		}
		newestKnown = found != nil
	}
	if !newestKnown && after > 0 {
		// The stream's newest outage started before the last replayed cycle
		// that reached, so it ended by then: if storage holds it open, its
		// end derived no events. Looked for in the replay first, which
		// bounds the lookup for a stream that flaps (round four), then
		// before it.
		var found *outageRow
		var err error
		if slices.ContainsFunc(recs, func(r *record.Record) bool { return !r.Reached }) {
			found, err = newest(recs[0].Seq-1, after)
		}
		if err == nil && found == nil {
			found, err = newest(0, recs[0].Seq)
		}
		if err != nil {
			return nil, fmt.Errorf("find outage left open: %w", err)
		}
		if found != nil && found.EndTS == nil {
			o = found
		}
	}

	if o != nil {
		sp, err := in.outageSpan(ctx, params, o)
		if err != nil {
			return nil, err
		}
		switch {
		case reopened || (current && sp.Reached == 0):
			o.Cycles = sp.N
			return o, nil
		case sp.Reached > 0:
			// It ended at the first of those. A stream down for its whole
			// replay is in an outage that began after the last of them,
			// which can lie before the replay; a stream down after a
			// replayed cycle that reached is in one opened below, as one
			// whose start derived no events.
			n, err := in.closeLeftOpen(ctx, k, params, *o, sp, down && after == 0 && len(recs) > 0, ev)
			if err != nil {
				return nil, err
			}
			if n != nil {
				n.Version = recs[len(recs)-1].Seq
				ev.opened++
				return n, nil
			}
		}
	}
	if !down || len(recs) == 0 {
		return nil, nil
	}
	// Down, but no open outage stored: its start was in cycles that derived
	// no events (a batch whose restore did not happen in time, or whose
	// events could not be stored). It starts at the first of the trailing
	// unreached cycles replayed; its row is written after the part, as any
	// open outage's (derive).
	i := len(recs)
	for i > 0 && !recs[i-1].Reached {
		i--
	}
	n := newOutage(recs[i], len(recs)-i)
	n.Version = recs[len(recs)-1].Seq
	ev.opened++
	return &n, nil
}

// outageSpan is what storage holds of a stream's cycles from an outage's
// start to the cycle being restored before.
type outageSpan struct {
	N           int    `json:"n"`
	Reached     int    `json:"n_reached"`    // of them, how many reached the destination
	CloseSeq    uint64 `json:"close_seq"`    // the first of those
	CloseTS     string `json:"close_ts"`     // and its ts
	LastReached uint64 `json:"last_reached"` // the last of those
}

// outageSpanFrom is the window of outageSpan's queries. (Their aliases are
// no column names: ClickHouse reads an alias in WHERE and in the other
// expressions before a column of the same name; review, round eight.)
const outageSpanFrom = ` FROM cycles WHERE ` + restoreWhere + ` AND seq >= {start:UInt64} AND seq < {before:UInt64}
	AND ts >= {start_ts:DateTime64(3, 'UTC')}`

func outageParams(params clickhouse.Params, o *outageRow) clickhouse.Params {
	return withParam(withParam(params, "start", strconv.FormatUint(o.StartSeq, 10)), "start_ts", o.StartTS)
}

func (in *Ingester) outageSpan(ctx context.Context, params clickhouse.Params, o *outageRow) (outageSpan, error) {
	rows, err := clickhouseQuery[outageSpan](ctx, in.store, `SELECT count() AS n, countIf(reached) AS n_reached,
		minIf(seq, reached) AS close_seq, argMinIf(ts, seq, reached) AS close_ts,
		maxIf(seq, reached) AS last_reached`+outageSpanFrom, outageParams(params, o))
	if err != nil {
		return outageSpan{}, fmt.Errorf("count outage cycles: %w", err)
	}
	if len(rows) != 1 {
		return outageSpan{N: o.Cycles}, nil
	}
	return rows[0], nil
}

// closeLeftOpen adds to ev the close row of outage o, which storage holds
// open although sp has a cycle after its start that reached: o ended at
// the first, as observe would have closed it. With downSince (the stream
// was down for its whole replay), it also returns the outage the stream is
// in at the cycle being restored before: from the first cycle after the
// last that reached, with its cycles until then, still to be given its
// version.
func (in *Ingester) closeLeftOpen(ctx context.Context, k streamKey, params clickhouse.Params, o outageRow,
	sp outageSpan, downSince bool, ev *events) (*outageRow, error) {
	p := withParam(withParam(outageParams(params, &o), "close", strconv.FormatUint(sp.CloseSeq, 10)),
		"last", strconv.FormatUint(sp.LastReached, 10))
	type counts struct {
		Closed    int `json:"closed"`
		SinceLast int `json:"since_last"`
	}
	cnt, err := clickhouseQuery[counts](ctx, in.store, `SELECT countIf(seq < {close:UInt64}) AS closed,
		countIf(seq > {last:UInt64}) AS since_last`+outageSpanFrom, p)
	if err != nil {
		return nil, fmt.Errorf("count cycles of an outage left open: %w", err)
	}
	if len(cnt) != 1 {
		return nil, errors.New("count cycles of an outage left open: no row")
	}
	o.EndSeq, o.EndTS, o.Version, o.Cycles = sp.CloseSeq, tsPtr(parseTime64(sp.CloseTS)), sp.CloseSeq, cnt[0].Closed
	ev.outages = append(ev.outages, o)
	if !downSince {
		return nil, nil
	}
	rows, err := clickhouseQuery[storedCycle](ctx, in.store, `SELECT `+restoreCols+` FROM cycles WHERE `+restoreWhere+`
		AND seq > {last:UInt64} AND seq < {before:UInt64} AND ts >= {start_ts:DateTime64(3, 'UTC')}
		ORDER BY seq LIMIT 1`, p)
	if err != nil {
		return nil, fmt.Errorf("find the first cycle of the outage: %w", err)
	}
	if len(rows) != 1 {
		return nil, nil
	}
	n := newOutage(rows[0].record(k), cnt[0].SinceLast)
	return &n, nil
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

// insertEvents stores events in inserts of at most partBytes of JSON each,
// encoding an insert's rows only when it is due.
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
	write := func(table string, n int, row func(int) []byte) {
		var rows [][]byte
		size := 0
		for i := range n {
			b := row(i)
			rows, size = append(rows, b), size+len(b)+1
			if size >= in.partBytes {
				insert(table, rows)
				rows, size = nil, 0
			}
		}
		insert(table, rows)
	}
	for range ev.opened {
		in.metrics.outage("open")
	}
	for _, o := range ev.outages {
		if o.EndTS != nil {
			in.metrics.outage("close")
		}
	}
	in.metrics.pathChanges(len(ev.changes))
	write("path_changes", len(ev.changes), func(i int) []byte { b, _ := json.Marshal(ev.changes[i]); return b })
	write("outages", len(ev.outages), func(i int) []byte { b, _ := json.Marshal(ev.outages[i]); return b })
}
