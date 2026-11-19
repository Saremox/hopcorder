package server

import (
	"slices"
	"time"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// streamKey identifies one measurement stream: what one agent records about
// one target with one protocol and flow. Route changes and outages are
// tracked per stream.
type streamKey struct {
	Agent, Target, Protocol string
	Flow                    int
}

func keyOf(rec *record.Record) streamKey {
	return streamKey{Agent: rec.AgentID, Target: rec.Target, Protocol: rec.Protocol, Flow: rec.FlowID}
}

// stream holds the derived-event state of one stream. Records must be fed
// in seq order; the agent ships them that way and the ingester sorts each
// batch.
type stream struct {
	key     streamKey
	loaded  bool // state was restored from storage (or there was none)
	tracker record.PathTracker
	last    lastCycle  // what a route change reports about the cycle before it
	outage  *outageRow // open outage, if any
	used    time.Time  // last record, for evicting idle streams
	// pending holds close rows of the stream's outages that could not be
	// stored, to write again with its next part (at most maxPending).
	pending []outageRow
}

// keepPending adds a lost close row o to s.pending, within maxPending. Past
// it, a row of an outage that storage has nothing of (pendOwn) is dropped,
// lost with its insert as round six accepted, so that one storage may hold
// open (pendKeep) is kept. A part adds few such rows a stream: the outage
// open when the part began, and in a part derived again those an earlier
// derivation left open in storage (heldOpen; at most one for each part it
// was derived in). Only past maxPending such rows (eight failed parts in a
// row, each of which closed an outage begun before it) is a row that must
// be kept dropped, the newest; the oldest, which storage holds open if any
// does, stays. In a part derived again, an outage that began before the
// part is pendKeep even if the earlier derivation stored it closed, so the
// row heldOpen found open is dropped when at least 9 of the retry's failed
// parts lie in the region found stored before its close: that region is
// the killed process's unmarked parts, one after a kill (more only if its
// mark inserts failed too), and a retry's parts are no smaller than the
// killed attempt's (round sixteen: 8 parts held, 9 did not).
func (s *stream) keepPending(o outageRow) {
	if len(s.pending) < maxPending {
		s.pending = append(s.pending, o)
		return
	}
	if o.pend != pendKeep {
		return
	}
	for i := len(s.pending) - 1; i >= 0; i-- {
		if s.pending[i].pend == pendOwn {
			s.pending = append(slices.Delete(s.pending, i, i+1), o)
			return
		}
	}
}

// lastCycle is what the stream keeps of its previous cycle: not the record
// itself, so a stream's memory does not grow with what an agent sends.
type lastCycle struct {
	ok       bool
	seq      uint64
	pathHash string
	pathLen  int
}

func lastOf(r *record.Record) lastCycle {
	return lastCycle{ok: true, seq: r.Seq, pathHash: r.PathHash, pathLen: r.PathLen}
}

// events produced by one record.
type events struct {
	changes []pathChangeRow
	outages []outageRow
	opened  int // outages opened (for the metric: open rows are written after the part, see derive)
	resent  int // close rows in outages written again (pending), not counted again
}

// observe feeds rec to the stream and returns the events it produces. An
// outage that opens or goes on is left in s.outage, and its open row is
// written after the part (derive), with the cycles it has then. Its version
// is the seq of its last cycle so far, the close row's the seq of the cycle
// that reached again: higher than every open row's, so FINAL reads the
// close row whatever order the inserts land in.
func (s *stream) observe(rec *record.Record, ev *events) {
	prev := s.last
	if ch, changed := s.tracker.ObserveChange(rec); changed && prev.ok {
		row := pathChangeRow{
			TS: clickhouse.Time(rec.TS), AgentID: rec.AgentID, Target: rec.Target, Protocol: rec.Protocol,
			FlowID: rec.FlowID, Seq: rec.Seq, Reason: string(ch.Reason),
			TTLs: ch.TTLs, FromIPs: addrStrings(ch.From), ToIPs: addrStrings(ch.To),
			PrevSeq: prev.seq, PrevPathHash: prev.pathHash, PathHash: rec.PathHash,
			PrevPathLen: prev.pathLen, PathLen: rec.PathLen,
		}
		if row.TTLs == nil {
			row.TTLs = []int{}
		}
		ev.changes = append(ev.changes, row)
	}
	s.last = lastOf(rec)

	switch {
	case !rec.Reached && s.outage == nil:
		o := newOutage(rec, 1)
		s.outage = &o
		ev.opened++
	case !rec.Reached:
		s.outage.Cycles++
		s.outage.Version = rec.Seq
	case s.outage != nil:
		o := *s.outage
		o.EndSeq, o.EndTS, o.Version = rec.Seq, tsPtr(rec.TS), rec.Seq
		s.outage = nil
		ev.outages = append(ev.outages, o)
	}
}

// newOutage is the open outage starting with rec, cycles long so far.
// last_hop is where it broke: the last responding hop of its first cycle.
func newOutage(rec *record.Record, cycles int) outageRow {
	ttl, ip := lastResponding(rec.Hops)
	return outageRow{
		AgentID: rec.AgentID, Target: rec.Target, Protocol: rec.Protocol, FlowID: rec.FlowID,
		StartSeq: rec.Seq, StartTS: clickhouse.Time(rec.TS), Cycles: cycles,
		LastHopTTL: ttl, LastHopIP: ip, Version: rec.Seq,
	}
}

// replay primes the stream with records it has already seen (after a
// server restart) without producing events.
func (s *stream) replay(recs []*record.Record) {
	for _, r := range recs {
		s.tracker.Observe(r)
		s.last = lastOf(r)
	}
}
