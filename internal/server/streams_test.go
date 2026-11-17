package server

import (
	"net/netip"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/record"
)

// One lost (or late) destination reply, with the next TTL's reply in,
// made two route changes in path_changes on an unchanged path, there and
// back (review, round six): about 400 a day per target at 1% loss.
func TestLostDestinationReplyIsNoRouteChange(t *testing.T) {
	dst := netip.MustParseAddr("192.0.2.9")
	cycle := func(seq uint64, lost bool) *record.Record {
		r := &record.Record{Schema: 1, AgentID: "a", Seq: seq, TS: time.Unix(int64(seq), 0), Target: "t",
			TargetIP: dst, AF: 4, Protocol: "icmp", FlowID: 33434}
		for ttl, ip := range []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"} {
			r.Hops = append(r.Hops, record.Hop{TTL: ttl + 1, Status: record.StatusTTLExpired,
				IP: netip.MustParseAddr(ip), RTTMicros: 1000})
		}
		if lost {
			r.Hops = append(r.Hops, record.Hop{TTL: 4, Status: record.StatusNoReply})
		}
		r.Hops = append(r.Hops, record.Hop{TTL: len(r.Hops) + 1, Status: record.StatusReply, IP: dst, RTTMicros: 20000})
		r.Finalize(2)
		return r
	}
	var s stream
	var ev events
	for seq := uint64(1); seq <= 100; seq++ {
		s.observe(cycle(seq, seq%10 == 5), &ev)
	}
	if len(ev.changes) != 0 {
		t.Errorf("%d route changes on an unchanged path, the first %+v", len(ev.changes), ev.changes[0])
	}
	if len(ev.outages) != 0 {
		t.Errorf("outages: %+v", ev.outages)
	}
}
