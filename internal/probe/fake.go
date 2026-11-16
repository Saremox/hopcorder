package probe

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saremox/hopcorder/internal/record"
)

// Fake is a scripted Prober for tests of everything above this package. Each
// target follows a list of Scenarios; the one with the latest From not after
// the current time is active, which models a path change at time T.
//
// It can simulate rate-limited hops (FakeHop.ReplyEvery), ECMP alternation
// (FakeHop.IPs with more than one address), loops (Scenario.Loop), a full
// black hole (Scenario.Reachable false), unreachable routers, late replies
// (FakeHop.Late) and routing failures (Scenario.Err).
type Fake struct {
	// Now returns the current time; it defaults to time.Now.
	Now func() time.Time

	mu        sync.Mutex
	scenarios map[netip.Addr][]Scenario
	cycles    map[netip.Addr]int
	requests  []TraceRequest

	late  atomic.Uint64
	sent  atomic.Uint64
	inUse atomic.Int64
	peak  atomic.Int64
}

// Scenario is the simulated path to a target from time From onward.
type Scenario struct {
	From time.Time
	// Hops are the routers at TTL 1..len(Hops).
	Hops []FakeHop
	// Reachable makes the destination answer every TTL above len(Hops).
	// Without it (and without Loop) TTLs above len(Hops) go unanswered.
	Reachable bool
	// DestRTT is the destination's RTT when Reachable.
	DestRTT time.Duration
	// Loop, if set, makes TTLs above len(Hops) cycle through these routers,
	// each answering with time exceeded.
	Loop []netip.Addr
	// Err makes Trace fail with this error (e.g. ErrNoRoute).
	Err error
	// Duration is how long Trace takes (honouring ctx), to exercise
	// scheduling and overruns.
	Duration time.Duration
}

// FakeHop is one simulated router.
type FakeHop struct {
	// IPs are the router addresses. With more than one, the hop is an ECMP
	// group: paris requests always see the same member (chosen by flow ID);
	// classic requests see a different member per TTL and cycle.
	IPs []netip.Addr
	RTT time.Duration
	// ReplyEvery models ICMP rate limiting: the hop answers only in every
	// Nth cycle (values <= 1 mean always).
	ReplyEvery int
	// Silent hops never answer.
	Silent bool
	// Late hops answer after the timeout: no-reply plus a late-reply count.
	Late bool
	// Unreachable, if set, makes the hop answer destination unreachable with
	// this code for its TTL and every higher one.
	Unreachable *int
	// MPLS is the label stack the hop reports in its ICMP errors.
	MPLS []record.MPLSLabel
}

// NewFake returns an empty Fake. Targets without scenarios fail Trace.
func NewFake() *Fake {
	return &Fake{scenarios: map[netip.Addr][]Scenario{}, cycles: map[netip.Addr]int{}}
}

// Set replaces the scenarios for target.
func (f *Fake) Set(target netip.Addr, scenarios ...Scenario) {
	s := slices.Clone(scenarios)
	slices.SortStableFunc(s, func(a, b Scenario) int { return a.From.Compare(b.From) })
	f.mu.Lock()
	f.scenarios[target] = s
	f.mu.Unlock()
}

// Requests returns every request Trace has received, in order.
func (f *Fake) Requests() []TraceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

// Stats implements StatsProvider.
func (f *Fake) Stats() Stats {
	return Stats{ProbesSent: f.sent.Load(), LateReplies: f.late.Load()}
}

// PeakConcurrency reports the largest number of Trace calls that were in
// flight at the same time.
func (f *Fake) PeakConcurrency() int { return int(f.peak.Load()) }

// Trace implements Prober.
func (f *Fake) Trace(ctx context.Context, req TraceRequest) ([]HopResult, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	n := f.inUse.Add(1)
	defer f.inUse.Add(-1)
	for {
		p := f.peak.Load()
		if n <= p || f.peak.CompareAndSwap(p, n) {
			break
		}
	}

	now := time.Now
	if f.Now != nil {
		now = f.Now
	}
	f.mu.Lock()
	f.requests = append(f.requests, req)
	sc, ok := f.active(req.Target, now())
	cycle := f.cycles[req.Target]
	f.cycles[req.Target]++
	f.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("fake: no scenario for %s", req.Target)
	}

	if sc.Duration > 0 {
		t := time.NewTimer(sc.Duration)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sc.Err != nil {
		return nil, sc.Err
	}
	first := req.FirstTTL()
	f.sent.Add(uint64(req.MaxTTL - first + 1)) //nolint:gosec // validated range
	return f.simulate(sc, req, cycle)[first-1:], nil
}

func (f *Fake) active(target netip.Addr, now time.Time) (Scenario, bool) {
	list := f.scenarios[target]
	for i := len(list) - 1; i >= 0; i-- {
		if !list[i].From.After(now) {
			return list[i], true
		}
	}
	return Scenario{}, false
}

func (f *Fake) simulate(sc Scenario, req TraceRequest, cycle int) []HopResult {
	out := NoReplies(req.MaxTTL)
	var unreach *HopResult
	for i := range out {
		ttl := i + 1
		res := &out[i]
		if unreach != nil {
			*res = *unreach
			res.TTL = ttl
			continue
		}
		if ttl <= len(sc.Hops) {
			h := sc.Hops[ttl-1]
			if len(h.IPs) == 0 || h.Silent || (h.ReplyEvery > 1 && cycle%h.ReplyEvery != 0) {
				continue
			}
			if h.Late {
				f.late.Add(1)
				continue
			}
			res.From = pickECMP(h.IPs, req, ttl, cycle)
			res.MPLS = h.MPLS
			res.RTT = h.RTT
			if res.RTT <= 0 {
				res.RTT = time.Millisecond
			}
			if h.Unreachable != nil {
				res.Status, res.Code = record.StatusUnreachable, *h.Unreachable
				r := *res
				unreach = &r
				continue
			}
			res.Status = record.StatusTTLExpired
			continue
		}
		beyond := ttl - len(sc.Hops) - 1
		switch {
		case len(sc.Loop) > 0:
			res.Status = record.StatusTTLExpired
			res.From = sc.Loop[beyond%len(sc.Loop)]
			res.RTT = time.Millisecond
		case sc.Reachable:
			res.Status = record.StatusReply
			res.From = req.Target
			res.RTT = sc.DestRTT
			if res.RTT <= 0 {
				res.RTT = time.Millisecond
			}
		}
	}
	return out
}

func pickECMP(ips []netip.Addr, req TraceRequest, ttl, cycle int) netip.Addr {
	if len(ips) == 1 {
		return ips[0]
	}
	if req.Paris {
		return ips[int(req.FlowID)%len(ips)]
	}
	return ips[(ttl+cycle)%len(ips)]
}
