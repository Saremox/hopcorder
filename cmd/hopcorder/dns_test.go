package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/config"
	"github.com/saremox/hopcorder/internal/probe"
	"github.com/saremox/hopcorder/internal/record"
)

// dnsFake is a resolver whose answers a test changes: each name's address,
// or for every name a failure (a server that answers SERVFAIL) or no answer
// until the lookup times out (a server behind the outage).
type dnsFake struct {
	mu    sync.Mutex
	addrs map[string]netip.Addr
	mode  string // "" (answers), "fail" or "hang"
}

func (d *dnsFake) LookupNetIP(ctx context.Context, _, host string) ([]netip.Addr, error) {
	d.mu.Lock()
	a, ok := d.addrs[host]
	mode := d.mode
	d.mu.Unlock()
	switch {
	case mode == "fail":
		return nil, &net.DNSError{Err: "server misbehaving", Name: host, Server: "192.0.2.53:53", IsTemporary: true}
	case mode == "hang":
		<-ctx.Done()
		return nil, &net.DNSError{Err: "i/o timeout", Name: host, Server: "192.0.2.53:53", IsTimeout: true}
	case !ok:
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return []netip.Addr{a}, nil
}

func (d *dnsFake) set(mode, host string, a netip.Addr) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mode = mode
	if host != "" {
		d.addrs[host] = a
	}
}

// useDNS makes the agent resolve names with a dnsFake for the rest of the
// test. Call it before starting a daemon, so that the daemon stops before
// the resolver is reset.
func useDNS(t *testing.T, addrs map[string]netip.Addr) *dnsFake {
	t.Helper()
	d := &dnsFake{addrs: addrs}
	resolver = d
	t.Cleanup(func() { resolver = nil })
	return d
}

var (
	dnsOld, dnsNew, dnsLit = netip.MustParseAddr("192.0.2.10"), netip.MustParseAddr("192.0.2.20"), netip.MustParseAddr("192.0.2.1")
	pathUp                 = probe.Scenario{Hops: []probe.FakeHop{{IPs: []netip.Addr{netip.MustParseAddr("10.0.0.1")}, RTT: time.Millisecond}}, Reachable: true}
	pathDown               = probe.Scenario{Hops: []probe.FakeHop{{IPs: []netip.Addr{netip.MustParseAddr("10.0.0.1")}, RTT: time.Millisecond}}} // dropped past r1
)

// nameTargetConfig has a target given by name, dst.example, and one given
// as an address, 192.0.2.1.
func nameTargetConfig(t *testing.T, spoolDir, out string) *config.Config {
	t.Helper()
	c, err := config.Parse(fmt.Appendf(nil, "agent_id: r\ninterval: 100ms\ntimeout: 50ms\nmax_ttl: 6\ndns_ttl: 1s\n"+
		"spool: {dir: %q}\ntargets: [{host: dst.example}, {host: %s}]\noutputs: {jsonl: {path: %q}}\n", spoolDir, dnsLit, out))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(config.FinalizeOptions{}); err != nil {
		t.Fatal(err)
	}
	return c
}

// since returns the records of target in the JSONL file at path whose cycle
// started at t0 or later.
func since(t *testing.T, path, target string, t0 time.Time) []record.Record {
	t.Helper()
	var out []record.Record
	for _, r := range readRecords(t, path) {
		if r.Target == target && !r.TS.Before(t0) {
			out = append(out, r)
		}
	}
	return out
}

// checkOutageRecorded checks that the name target recorded the outage after
// t0, unreached at its old address, about as often as the target given as
// an address.
func checkOutageRecorded(t *testing.T, path string, t0 time.Time, after string) {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool { return len(since(t, path, dnsLit.String(), t0)) >= 10 },
		"records of the address after the "+after)
	lit, name := since(t, path, dnsLit.String(), t0), since(t, path, "dst.example", t0)
	if len(name) < len(lit)-2 {
		t.Fatalf("after the %s, while DNS fails: %d records of dst.example, %d of %s", after, len(name), len(lit), dnsLit)
	}
	for _, r := range name {
		if r.Reached || r.TargetIP != dnsOld {
			t.Errorf("after the %s, while DNS fails: dst.example reached %v at %s, want unreached at %s", after, r.Reached, r.TargetIP, dnsOld)
		}
	}
}

// A reload during an outage that takes DNS down with it keeps recording the
// name target, unreached at the address it had, like the target given as an
// address; once DNS answers, with another address, that is taken. Before,
// the new configuration's runners had no address and could not resolve one:
// no record of dst.example for the rest of the outage, while 192.0.2.1
// recorded it (review, round six: the real binary, SIGHUP 8 s into the
// outage, 10 s without a record of the name). The DNS server is behind the
// outage, so lookups hang until they time out (a second here): the
// supervisor's address is used at once, where the spool's, a restart's
// fallback, would wait for the first lookup to time out.
func TestReloadKeepsNameTargetsRecordedWhileDNSFails(t *testing.T) {
	dns := useDNS(t, map[string]netip.Addr{"dst.example": dnsOld})
	dir := t.TempDir()
	a, b := nameTargetConfig(t, dir+"/spool", dir+"/a.jsonl"), nameTargetConfig(t, dir+"/spool", dir+"/b.jsonl")
	f := probe.NewFake()
	f.Set(dnsOld, pathUp)
	f.Set(dnsLit, pathUp)
	shutdownGrace = time.Second
	ctx, cancel := context.WithCancel(t.Context())
	reload, done := make(chan os.Signal, 1), make(chan error, 1)
	quiet := func(*config.Config) *slog.Logger { return slog.New(slog.DiscardHandler) }
	go func() {
		done <- superviseDaemon(ctx, a, f, reload, func() (*config.Config, error) { return b, nil }, quiet)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("supervisor: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("supervisor did not stop")
			}
		})
	}
	t.Cleanup(stop)
	waitFor(t, 3*time.Second, func() bool { return len(since(t, dir+"/a.jsonl", "dst.example", time.Time{})) >= 3 }, "records")

	// The outage: the path is dropped past r1, and DNS is down too. The
	// running configuration keeps the address.
	t0 := time.Now()
	f.Set(dnsOld, pathDown)
	f.Set(dnsLit, pathDown)
	dns.set("hang", "", netip.Addr{})
	waitFor(t, 3*time.Second, func() bool {
		r := since(t, dir+"/a.jsonl", "dst.example", t0.Add(200*time.Millisecond))
		return len(r) >= 2 && !r[len(r)-1].Reached
	}, "unreached records of dst.example before the reload")

	t1 := time.Now().UTC().Truncate(time.Millisecond)
	reload <- syscall.SIGHUP
	checkOutageRecorded(t, dir+"/b.jsonl", t1, "reload")

	// DNS answers again, with another address, and the path is back.
	f.Set(dnsNew, pathUp)
	dns.set("", "dst.example", dnsNew)
	waitFor(t, 5*time.Second, func() bool {
		r := since(t, dir+"/b.jsonl", "dst.example", t1)
		return len(r) > 0 && r[len(r)-1].TargetIP == dnsNew && r[len(r)-1].Reached
	}, "records of dst.example at its new address")
	running(t, done)
	stop()
}

// The same for a restart: the spool keeps the address the name resolved to,
// and a first lookup that fails takes it. Before, a restart while DNS was
// down left the name target without records until DNS answered. (A lookup
// that hangs instead makes the first cycle wait for its timeout first.)
func TestRestartKeepsNameTargetsRecordedWhileDNSFails(t *testing.T) {
	dns := useDNS(t, map[string]netip.Addr{"dst.example": dnsOld})
	dir := t.TempDir()
	f := probe.NewFake()
	f.Set(dnsOld, pathUp)
	f.Set(dnsLit, pathUp)
	shutdownGrace = time.Second
	stop := startDaemon(t, nameTargetConfig(t, dir+"/spool", dir+"/a.jsonl"), f)
	waitFor(t, 3*time.Second, func() bool { return len(since(t, dir+"/a.jsonl", "dst.example", time.Time{})) >= 3 }, "records")
	stop()

	f.Set(dnsOld, pathDown)
	f.Set(dnsLit, pathDown)
	dns.set("fail", "", netip.Addr{})
	t1 := time.Now().UTC().Truncate(time.Millisecond)
	stop = startDaemon(t, nameTargetConfig(t, dir+"/spool", dir+"/b.jsonl"), f)
	checkOutageRecorded(t, dir+"/b.jsonl", t1, "restart")
	stop()

	// With DNS answering, a restart takes the new answer from its first
	// cycle on.
	f.Set(dnsNew, pathUp)
	dns.set("", "dst.example", dnsNew)
	t2 := time.Now().UTC().Truncate(time.Millisecond)
	stop = startDaemon(t, nameTargetConfig(t, dir+"/spool", dir+"/c.jsonl"), f)
	waitFor(t, 3*time.Second, func() bool { return len(since(t, dir+"/c.jsonl", "dst.example", t2)) >= 3 }, "records")
	stop()
	for _, r := range since(t, dir+"/c.jsonl", "dst.example", t2) {
		if r.TargetIP != dnsNew || !r.Reached {
			t.Errorf("with DNS answering: reached %v at %s, want reached at %s", r.Reached, r.TargetIP, dnsNew)
		}
	}
}
