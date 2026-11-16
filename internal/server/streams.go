package server

import (
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
}

// observe feeds rec to the stream and returns the events it produces.
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
		ttl, ip := lastResponding(rec.Hops)
		o := outageRow{
			AgentID: rec.AgentID, Target: rec.Target, Protocol: rec.Protocol, FlowID: rec.FlowID,
			StartSeq: rec.Seq, StartTS: clickhouse.Time(rec.TS), Cycles: 1,
			LastHopTTL: ttl, LastHopIP: ip, Version: rec.Seq,
		}
		s.outage = &o
		ev.outages = append(ev.outages, o)
	case !rec.Reached:
		s.outage.Cycles++
	case s.outage != nil:
		o := *s.outage
		o.EndSeq, o.EndTS, o.Version = rec.Seq, tsPtr(rec.TS), rec.Seq
		s.outage = nil
		ev.outages = append(ev.outages, o)
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
