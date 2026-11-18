package agent

import (
	"context"
	"errors"
	"maps"
	"net/netip"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/probe"
	"github.com/saremox/hopcorder/internal/record"
)

// memStore is an AddressStore in memory.
type memStore struct {
	mu sync.Mutex
	m  map[string]netip.Addr
}

func (s *memStore) Addresses() (map[string]netip.Addr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.m), nil
}

func (s *memStore) SaveAddress(key string, a netip.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = a
	return nil
}

func (s *memStore) KeepAddresses(keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	maps.DeleteFunc(s.m, func(k string, _ netip.Addr) bool { return !slices.Contains(keys, k) })
	return nil
}

func (s *memStore) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.m))
}

func (a *Addresses) keys() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Sorted(maps.Keys(a.m))
}

var (
	pathUp   = probe.Scenario{Hops: []probe.FakeHop{hop("10.0.0.1"), hop("10.0.0.2")}, Reachable: true}
	pathDown = probe.Scenario{Hops: []probe.FakeHop{hop("10.0.0.1")}} // dropped past the first router
)

// started runs a until the returned stop is called.
func started(t *testing.T, a *Agent) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	return func() {
		t.Helper()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not return after cancellation")
		}
	}
}

// A reload (the next Agent with the same Addresses) while DNS is down with
// the network keeps recording the name target, unreached, at the address it
// had, as the target given as an address is; once DNS answers again, with
// another address, that is taken. Before, the new configuration's runners
// had no address and every cycle failed to resolve: no record of the name
// for the rest of the outage (review, round six).
func TestReloadKeepsAddressWhileDNSFails(t *testing.T) {
	f := probe.NewFake()
	old, nw, lit := addr("192.0.2.10"), addr("192.0.2.20"), addr("192.0.2.1")
	f.Set(old, pathUp)
	f.Set(lit, pathUp)
	res := &fakeResolver{addrs: map[string][]netip.Addr{"dst.example": {old}}}
	targets := []Target{target("dst.example", 40*time.Millisecond), target(lit.String(), 40*time.Millisecond)}
	addrs := NewAddresses()
	newAgent := func(dnsTTL time.Duration) (*Agent, *memSink, *countObserver) {
		sink, obs := &memSink{}, &countObserver{}
		a, err := New(Options{AgentID: "test", Targets: targets, DNSTTL: dnsTTL, Prober: f, Sink: sink,
			Observer: obs, Resolver: res, Addresses: addrs})
		if err != nil {
			t.Fatal(err)
		}
		return a, sink, obs
	}

	// The first configuration: the outage begins while it runs, and it
	// keeps the address.
	a, sink, _ := newAgent(time.Hour)
	stop := started(t, a)
	eventually(t, 3*time.Second, func() bool { return len(sink.byTarget()["dst.example"]) >= 2 })
	f.Set(old, pathDown)
	f.Set(lit, pathDown)
	res.set("dst.example", nil, errors.New("SERVFAIL"))
	eventually(t, 3*time.Second, func() bool {
		recs := sink.byTarget()["dst.example"]
		return !recs[len(recs)-1].Reached
	})
	stop()
	if recs := sink.byTarget()["dst.example"]; !recs[0].Reached || recs[len(recs)-1].TargetIP != old {
		t.Fatalf("before the reload: first reached %v, last at %s", recs[0].Reached, recs[len(recs)-1].TargetIP)
	}

	// The reload, with the outage going on: the name keeps its address.
	a, sink, obs := newAgent(100 * time.Millisecond)
	stop = started(t, a)
	eventually(t, 5*time.Second, func() bool { return len(sink.byTarget()[lit.String()]) >= 10 })
	during := sink.byTarget()
	if n, want := len(during["dst.example"]), len(during[lit.String()]); n < want-1 {
		t.Errorf("after the reload, while DNS fails: %d records of the name, %d of the address", n, want)
	}
	for _, r := range during["dst.example"] {
		if r.Reached || r.TargetIP != old {
			t.Errorf("after the reload, while DNS fails: reached %v at %s, want unreached at %s", r.Reached, r.TargetIP, old)
		}
	}
	if obs.failed.Load() != 0 || obs.resolve.Load() == 0 {
		t.Errorf("failed cycles %d (want 0), resolver failures %d (want some)", obs.failed.Load(), obs.resolve.Load())
	}

	// DNS and the path come back, the name with another address: taken on
	// the next lookup.
	f.Set(nw, pathUp)
	res.set("dst.example", []netip.Addr{nw}, nil)
	eventually(t, 3*time.Second, func() bool {
		recs := sink.byTarget()["dst.example"]
		return len(recs) > 0 && recs[len(recs)-1].Reached && recs[len(recs)-1].TargetIP == nw
	})
	stop()

	// The next reload uses the new address at once, refreshes it in the
	// background at the first cycle and then every DNSTTL (an hour here).
	res.mu.Lock()
	clear(res.calls)
	res.mu.Unlock()
	a, sink, _ = newAgent(time.Hour)
	stop = started(t, a)
	eventually(t, 3*time.Second, func() bool { return len(sink.byTarget()["dst.example"]) >= 5 })
	stop()
	if recs := sink.byTarget()["dst.example"]; recs[0].TargetIP != nw || !recs[0].Reached {
		t.Errorf("after the next reload: the first record reached %v at %s, want reached at %s",
			recs[0].Reached, recs[0].TargetIP, nw)
	}
	if n := res.calls["ip dst.example"]; n != 1 {
		t.Errorf("%d lookups after the next reload, want 1 (the refresh at its first cycle)", n)
	}
}

// After a restart (a new Addresses, the store's addresses) a name whose
// first lookup fails is probed at the address it had: its outage is
// recorded. The store's address is not used when the lookup answers, nor
// for a lookup of another family; addresses of names no target has any
// more are deleted from the store.
func TestSavedAddressIsTheFallbackAfterARestart(t *testing.T) {
	f := probe.NewFake()
	saved, fresh, lit := addr("192.0.2.10"), addr("192.0.2.20"), addr("192.0.2.1")
	f.Set(saved, pathDown)
	f.Set(fresh, pathUp)
	f.Set(lit, pathDown)
	st := &memStore{m: map[string]netip.Addr{
		"ip dst.example":   saved,
		"ip4 v4.example":   addr("2001:db8::1"), // not an IPv4 address: not used
		"ip6 gone.example": addr("2001:db8::2"), // no such target any more
	}}
	res := &fakeResolver{addrs: map[string][]netip.Addr{}, err: errors.New("SERVFAIL")}
	v4 := target("v4.example", 40*time.Millisecond)
	v4.AF = 4
	targets := []Target{target("dst.example", 40*time.Millisecond), v4, target(lit.String(), 40*time.Millisecond)}
	// run runs an Agent (a start) until the target given as an address has
	// ten records.
	run := func() (map[string][]*record.Record, *countObserver) {
		sink, obs := &memSink{}, &countObserver{}
		a, err := New(Options{AgentID: "test", Targets: targets, Prober: f, Sink: sink, Observer: obs,
			Resolver: res, AddressStore: st})
		if err != nil {
			t.Fatal(err)
		}
		stop := started(t, a)
		eventually(t, 5*time.Second, func() bool { return len(sink.byTarget()[lit.String()]) >= 10 })
		stop()
		return sink.byTarget(), obs
	}

	got, obs := run()
	if n, want := len(got["dst.example"]), len(got[lit.String()]); n < want-1 {
		t.Errorf("after a restart, while DNS fails: %d records of the name, %d of the address", n, want)
	}
	for _, r := range got["dst.example"] {
		if r.Reached || r.TargetIP != saved {
			t.Errorf("after a restart, while DNS fails: reached %v at %s, want unreached at %s", r.Reached, r.TargetIP, saved)
		}
	}
	if n := len(got["v4.example"]); n != 0 {
		t.Errorf("%d records of v4.example: an IPv6 address was taken for an ip4 lookup", n)
	}
	if obs.resolve.Load() == 0 || obs.failed.Load() == 0 {
		t.Errorf("resolver failures %d, failed cycles %d (v4.example's): want some of each", obs.resolve.Load(), obs.failed.Load())
	}
	if got, want := st.keys(), []string{"ip dst.example", "ip4 v4.example"}; !slices.Equal(got, want) {
		t.Errorf("store keys %v, want %v", got, want)
	}

	// DNS answers at the next start: the lookup goes first, the store's
	// address is not probed, and the new one is saved.
	res.set("dst.example", []netip.Addr{fresh}, nil)
	got, _ = run()
	recs := got["dst.example"]
	if len(recs) < 8 {
		t.Fatalf("%d records of dst.example", len(recs))
	}
	for _, r := range recs {
		if r.TargetIP != fresh || !r.Reached {
			t.Errorf("reached %v at %s, want reached at %s (the lookup's)", r.Reached, r.TargetIP, fresh)
		}
	}
	if m, _ := st.Addresses(); m["ip dst.example"] != fresh {
		t.Errorf("saved %s, want %s", m["ip dst.example"], fresh)
	}
}

// Addresses and the store keep only the names of the targets that run:
// reloads that keep changing the targets do not grow them. Targets given
// as addresses have none.
func TestAddressesForgetRemovedTargets(t *testing.T) {
	f := probe.NewFake()
	res := &fakeResolver{addrs: map[string][]netip.Addr{}}
	f.Set(addr("192.0.2.1"), pathUp)
	addrs, st := NewAddresses(), &memStore{m: map[string]netip.Addr{}}
	run := func(targets ...Target) {
		t.Helper()
		a, err := New(Options{AgentID: "test", Targets: targets, Prober: f, Sink: &memSink{}, Resolver: res,
			Addresses: addrs, AddressStore: st})
		if err != nil {
			t.Fatal(err)
		}
		runFor(t, a, 60*time.Millisecond)
	}
	for i := range 20 {
		ip := netip.AddrFrom4([4]byte{192, 0, 2, byte(10 + i)})
		host := "n" + ip.String() + ".example"
		res.set(host, []netip.Addr{ip, addr("2001:db8::1")}, nil)
		f.Set(ip, pathUp)
		v6 := target(host, 20*time.Millisecond)
		v6.AF = 6
		run(target(host, 20*time.Millisecond), v6, target("192.0.2.1", 20*time.Millisecond))
		want := []string{"ip " + host, "ip6 " + host}
		if got := addrs.keys(); !slices.Equal(got, want) {
			t.Fatalf("configuration %d: addresses of %v, want %v", i, got, want)
		}
		if got := st.keys(); !slices.Equal(got, want) {
			t.Fatalf("configuration %d: store has %v, want %v", i, got, want)
		}
	}
}

func TestUsableSavedAddress(t *testing.T) {
	for _, c := range []struct {
		key, ip string
		want    bool
	}{
		{"ip a", "192.0.2.1", true},
		{"ip a", "2001:db8::1", true},
		{"ip4 a", "192.0.2.1", true},
		{"ip4 a", "2001:db8::1", false},
		{"ip6 a", "2001:db8::1", true},
		{"ip6 a", "fe80::1%eth0", true},
		{"ip6 a", "fe80::1%eth/0", false}, // a record may not carry that zone
		{"ip6 a", "192.0.2.1", false},
		{"ip a", "::ffff:192.0.2.1", false}, // pickAddr unmaps
	} {
		if got := usable(c.key, netip.MustParseAddr(c.ip)); got != c.want {
			t.Errorf("usable(%q, %s) = %v, want %v", c.key, c.ip, got, c.want)
		}
	}
	if usable("ip a", netip.Addr{}) {
		t.Error("the zero address is usable")
	}
}
