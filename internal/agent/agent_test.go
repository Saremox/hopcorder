package agent

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/probe"
	"github.com/saremox/hopcorder/internal/record"
)

type memSink struct {
	mu   sync.Mutex
	seq  uint64
	recs []*record.Record
}

func (s *memSink) Publish(_ context.Context, rec *record.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	rec.Seq = s.seq
	s.recs = append(s.recs, rec)
	return nil
}

func (s *memSink) byTarget() map[string][]*record.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]*record.Record{}
	for _, r := range s.recs {
		out[r.Target] = append(out[r.Target], r)
	}
	return out
}

type countObserver struct {
	done, failed, overrun, resolve atomic.Int64
}

func (o *countObserver) CycleDone(*Target, *record.Record) { o.done.Add(1) }
func (o *countObserver) CycleFailed(*Target)               { o.failed.Add(1) }
func (o *countObserver) CycleOverrun(*Target)              { o.overrun.Add(1) }
func (o *countObserver) ResolveFailed(*Target)             { o.resolve.Add(1) }

func addr(s string) netip.Addr { return netip.MustParseAddr(s) }

func hop(ips ...string) probe.FakeHop {
	h := probe.FakeHop{RTT: 2 * time.Millisecond}
	for _, s := range ips {
		h.IPs = append(h.IPs, addr(s))
	}
	return h
}

func target(host string, interval time.Duration) Target {
	return Target{Host: host, Protocol: probe.ICMP, FlowID: probe.DefaultFlowID, Paris: true,
		Interval: interval, Timeout: interval / 2, MaxTTL: 8, ProbePast: 2}
}

func runFor(t *testing.T, a *Agent, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(d + 2*time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestRunRecordsAndStagger(t *testing.T) {
	f := probe.NewFake()
	targets := []Target{
		target("192.0.2.1", 90*time.Millisecond),
		target("192.0.2.2", 90*time.Millisecond),
		target("192.0.2.3", 90*time.Millisecond),
	}
	for _, tg := range targets {
		f.Set(addr(tg.Host), probe.Scenario{Hops: []probe.FakeHop{hop("10.0.0.1"), hop("10.0.0.2")}, Reachable: true})
	}
	sink := &memSink{}
	obs := &countObserver{}
	a, err := New(Options{AgentID: "test", Targets: targets, Prober: f, Sink: sink, Observer: obs})
	if err != nil {
		t.Fatal(err)
	}
	runFor(t, a, 500*time.Millisecond)

	got := sink.byTarget()
	for _, tg := range targets {
		recs := got[tg.Host]
		if len(recs) < 4 || len(recs) > 7 {
			t.Errorf("%s: %d records in 500ms at 90ms interval", tg.Host, len(recs))
		}
		for _, r := range recs {
			if err := r.Validate(); err != nil {
				t.Errorf("%s: invalid record: %v", tg.Host, err)
			}
			if !r.Reached || r.PathLen != 3 || r.AgentID != "test" || r.FlowID != probe.DefaultFlowID {
				t.Errorf("%s: unexpected record %+v", tg.Host, r)
			}
		}
	}
	// Targets start interval/3 apart.
	first := func(h string) time.Time { return got[h][0].TS }
	for i, gap := range []time.Duration{first("192.0.2.2").Sub(first("192.0.2.1")), first("192.0.2.3").Sub(first("192.0.2.2"))} {
		if gap < 15*time.Millisecond || gap > 50*time.Millisecond {
			t.Errorf("stagger gap %d = %v, want about 30ms", i, gap)
		}
	}
	if obs.done.Load() != int64(len(sink.recs)) || obs.failed.Load() != 0 {
		t.Errorf("observer done=%d failed=%d, records=%d", obs.done.Load(), obs.failed.Load(), len(sink.recs))
	}
}

func TestOverrunStartsNextCycleImmediately(t *testing.T) {
	f := probe.NewFake()
	tg := target("192.0.2.1", 50*time.Millisecond)
	f.Set(addr(tg.Host), probe.Scenario{Duration: 120 * time.Millisecond, Reachable: true})
	sink, obs := &memSink{}, &countObserver{}
	a, _ := New(Options{AgentID: "test", Targets: []Target{tg}, Prober: f, Sink: sink, Observer: obs})
	runFor(t, a, 650*time.Millisecond)
	n := len(sink.byTarget()[tg.Host])
	// Back-to-back 120ms cycles: about 5 in 650ms, never the 13 a queue of
	// 50ms ticks would produce.
	if n < 4 || n > 6 {
		t.Errorf("%d cycles, want about 5", n)
	}
	if obs.overrun.Load() < int64(n-1) {
		t.Errorf("overruns = %d for %d overrunning cycles", obs.overrun.Load(), n)
	}
	if f.PeakConcurrency() != 1 {
		t.Errorf("cycles of one target overlapped (peak %d)", f.PeakConcurrency())
	}
}

func TestSlowTargetDoesNotDelayOthers(t *testing.T) {
	f := probe.NewFake()
	slow, fast := target("192.0.2.1", 100*time.Millisecond), target("192.0.2.2", 50*time.Millisecond)
	f.Set(addr(slow.Host), probe.Scenario{Duration: time.Hour})
	f.Set(addr(fast.Host), probe.Scenario{Reachable: true})
	sink := &memSink{}
	a, _ := New(Options{AgentID: "test", Targets: []Target{slow, fast}, Prober: f, Sink: sink})
	start := time.Now()
	runFor(t, a, 400*time.Millisecond)
	if d := time.Since(start); d > time.Second {
		t.Errorf("shutdown with an in-flight hour-long cycle took %v", d)
	}
	got := sink.byTarget()
	if n := len(got[fast.Host]); n < 6 {
		t.Errorf("fast target got %d records", n)
	}
	if n := len(got[slow.Host]); n != 0 {
		t.Errorf("abandoned cycle produced %d records", n)
	}
}

type fakeResolver struct {
	mu    sync.Mutex
	addrs map[string][]netip.Addr
	err   error
	calls map[string]int
}

func (r *fakeResolver) LookupNetIP(_ context.Context, network, host string) ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls == nil {
		r.calls = map[string]int{}
	}
	r.calls[network+" "+host]++
	if r.err != nil {
		return nil, r.err
	}
	return r.addrs[host], nil
}

func (r *fakeResolver) set(host string, addrs []netip.Addr, err error) {
	r.mu.Lock()
	r.addrs[host], r.err = addrs, err
	r.mu.Unlock()
}

func TestDNSRefresh(t *testing.T) {
	f := probe.NewFake()
	old, nw := addr("192.0.2.10"), addr("192.0.2.20")
	f.Set(old, probe.Scenario{Reachable: true})
	f.Set(nw, probe.Scenario{Reachable: true})
	res := &fakeResolver{addrs: map[string][]netip.Addr{"svc.example": {addr("2001:db8::1"), old}}}
	tg := target("svc.example", 40*time.Millisecond)
	sink, obs := &memSink{}, &countObserver{}
	a, _ := New(Options{AgentID: "test", Targets: []Target{tg}, DNSTTL: 100 * time.Millisecond,
		Prober: f, Sink: sink, Observer: obs, Resolver: res})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { _ = a.Run(ctx); close(done) }()
	time.Sleep(150 * time.Millisecond)
	res.set("svc.example", nil, errors.New("SERVFAIL")) // resolver outage: keep the old address
	time.Sleep(200 * time.Millisecond)
	res.set("svc.example", []netip.Addr{nw}, nil)
	time.Sleep(250 * time.Millisecond)
	cancel()
	<-done

	recs := sink.byTarget()["svc.example"]
	var sawOld, sawNew bool
	for _, r := range recs {
		switch r.TargetIP {
		case old:
			if sawNew {
				t.Error("old address used after the change")
			}
			sawOld = true
		case nw:
			sawNew = true
		default:
			t.Errorf("unexpected target_ip %s (IPv4 must be preferred)", r.TargetIP)
		}
	}
	if !sawOld || !sawNew {
		t.Errorf("sawOld=%v sawNew=%v over %d records", sawOld, sawNew, len(recs))
	}
	if obs.resolve.Load() == 0 {
		t.Error("resolver failure not reported")
	}
	if obs.failed.Load() != 0 {
		t.Error("resolver failure with a known address failed cycles")
	}
}

func TestAddressFamilySelection(t *testing.T) {
	res := &fakeResolver{addrs: map[string][]netip.Addr{"dual.example": {addr("2001:db8::1")}}}
	f := probe.NewFake()
	f.Set(addr("2001:db8::1"), probe.Scenario{Reachable: true})
	tg := target("dual.example", time.Second)
	tg.AF = 6
	a, _ := New(Options{AgentID: "test", Targets: []Target{tg}, Prober: f, Resolver: res})
	recs, err := a.RunOnce(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if recs[0].AF != 6 || recs[0].TargetIP != addr("2001:db8::1") {
		t.Errorf("record %+v", recs[0])
	}
	if res.calls["ip6 dual.example"] != 1 {
		t.Errorf("resolver calls %v, want one ip6 lookup", res.calls)
	}
}

func TestNoRouteRecordsSilentCycle(t *testing.T) {
	f := probe.NewFake()
	tg := target("192.0.2.1", time.Second)
	f.Set(addr(tg.Host), probe.Scenario{Err: fmt.Errorf("sendto: %w", probe.ErrNoRoute)})
	a, _ := New(Options{AgentID: "test", Targets: []Target{tg}, Prober: f})
	recs, err := a.RunOnce(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r := recs[0]
	if r.Reached || r.PathLen != tg.ProbePast || len(r.Hops) != 2 || r.Hops[0].Status != record.StatusNoReply {
		t.Errorf("record %+v", r)
	}
	if err := r.Validate(); err != nil {
		t.Error(err)
	}
}

func TestRunOnce(t *testing.T) {
	f := probe.NewFake()
	good, bad := target("192.0.2.1", time.Second), target("192.0.2.99", time.Second)
	f.Set(addr(good.Host), probe.Scenario{Reachable: true})
	a, _ := New(Options{AgentID: "test", Targets: []Target{bad, good}, Prober: f})
	recs, err := a.RunOnce(t.Context())
	if err == nil {
		t.Error("expected an error for the target without a scenario")
	}
	if len(recs) != 1 || recs[0].Target != good.Host || recs[0].Seq != 0 {
		t.Errorf("records %+v", recs)
	}
}

func TestRecordAssembly(t *testing.T) {
	f := probe.NewFake()
	tg := target("198.51.100.7", time.Second)
	tg.MaxTTL = 10
	f.Set(addr(tg.Host), probe.Scenario{
		Hops: []probe.FakeHop{
			{IPs: []netip.Addr{addr("10.0.0.1")}, RTT: 400 * time.Nanosecond}, // rounds up to 1µs
			hop("62.115.120.1"), hop("62.115.120.2"), hop("62.115.120.1"),
			{IPs: []netip.Addr{addr("62.115.120.9")}, Unreachable: record.IntPtr(0)},
		},
	})
	a, _ := New(Options{AgentID: "test", Targets: []Target{tg}, Prober: f})
	recs, err := a.RunOnce(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r := recs[0]
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	if r.Hops[0].RTTMicros != 1 {
		t.Errorf("sub-microsecond rtt recorded as %d", r.Hops[0].RTTMicros)
	}
	last := r.Hops[len(r.Hops)-1]
	if r.PathLen != 5 || last.Status != record.StatusUnreachable || last.Code == nil || *last.Code != 0 {
		t.Errorf("path_len %d, last hop %+v", r.PathLen, last)
	}
	if len(r.Loops) != 1 || r.Loops[0].IP != addr("62.115.120.1") {
		t.Errorf("loops %+v", r.Loops)
	}
	if r.TS.Nanosecond()%int(time.Millisecond) != 0 || r.TS.Location() != time.UTC {
		t.Errorf("ts %v not UTC milliseconds", r.TS)
	}
}

// Rate-limited hops must not register as path changes; a real path change
// at time T must.
func TestPathHashSemantics(t *testing.T) {
	f := probe.NewFake()
	t0 := time.Now()
	now := t0
	var mu sync.Mutex
	f.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	tg := target("198.51.100.7", time.Second)
	f.Set(addr(tg.Host),
		probe.Scenario{Hops: []probe.FakeHop{hop("10.0.0.1"), {IPs: []netip.Addr{addr("10.0.0.2")}, ReplyEvery: 3}, hop("10.0.0.3")}, Reachable: true},
		probe.Scenario{From: t0.Add(time.Hour), Hops: []probe.FakeHop{hop("10.0.0.1"), hop("10.9.9.9"), hop("10.0.0.3")}, Reachable: true},
	)
	a, _ := New(Options{AgentID: "test", Targets: []Target{tg}, Prober: f})
	hashes := map[string]int{}
	for range 6 {
		recs, err := a.RunOnce(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		hashes[recs[0].PathHash]++
	}
	// The flickering hop alternates between its IP and "*", which are two
	// distinct snapshots, but never more than that.
	if len(hashes) > 2 {
		t.Errorf("rate limiting produced %d distinct hashes", len(hashes))
	}
	mu.Lock()
	now = t0.Add(time.Hour)
	mu.Unlock()
	recs, _ := a.RunOnce(t.Context())
	if _, seen := hashes[recs[0].PathHash]; seen {
		t.Error("path change at T did not change path_hash")
	}
}

func TestRunNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	f := probe.NewFake()
	var targets []Target
	for i := range 20 {
		tg := target(fmt.Sprintf("192.0.2.%d", i+1), 20*time.Millisecond)
		targets = append(targets, tg)
		f.Set(addr(tg.Host), probe.Scenario{Reachable: true, Duration: time.Duration(i) * time.Millisecond})
	}
	a, _ := New(Options{AgentID: "test", Targets: targets, Prober: f, Sink: &memSink{}})
	runFor(t, a, 300*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+1 {
		t.Errorf("goroutines %d -> %d", before, after)
	}
}

func TestNewValidates(t *testing.T) {
	f := probe.NewFake()
	if _, err := New(Options{Targets: []Target{target("x", time.Second)}, Prober: f}); err == nil {
		t.Error("empty agent id accepted")
	}
	if _, err := New(Options{AgentID: "a", Prober: f}); err == nil {
		t.Error("no targets accepted")
	}
	if _, err := New(Options{AgentID: "a", Targets: []Target{target("x", time.Second)}}); err == nil {
		t.Error("no prober accepted")
	}
	a, _ := New(Options{AgentID: "a", Targets: []Target{target("x", time.Second)}, Prober: f})
	if err := a.Run(t.Context()); err == nil {
		t.Error("Run without sink accepted")
	}
}
