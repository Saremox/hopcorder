//go:build rawsock && linux

package probe

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/record"
)

// These tests open real raw sockets. They skip themselves unless the test
// binary has CAP_NET_RAW:
//
//	go test -c -tags rawsock -o probe.test ./internal/probe
//	sudo setcap cap_net_raw+ep probe.test && ./probe.test -test.v
//
// TestRawNetns additionally needs CAP_SYS_ADMIN and iproute2 to build a
// routed topology out of network namespaces; it skips otherwise.

func newRawOrSkip(t *testing.T) *Raw {
	t.Helper()
	r := NewRaw(nil)
	if err := r.Check(); err != nil {
		if errors.Is(err, ErrPermission) {
			t.Skipf("no raw socket permission: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

func skipWithoutIPv6(t *testing.T, r *Raw) {
	t.Helper()
	if _, err := r.open6(true); err != nil {
		if errors.Is(err, syscall.EAFNOSUPPORT) {
			t.Skip("kernel has no IPv6")
		}
		t.Fatal(err)
	}
	c, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("no ::1: %v", err)
	}
	_ = c.Close()
}

// closedUDPPort returns a local UDP port nothing listens on, so a probe to it
// draws a port unreachable.
func closedUDPPort(t *testing.T, network, addr string) uint16 {
	t.Helper()
	c, err := net.ListenPacket(network, addr)
	if err != nil {
		t.Fatal(err)
	}
	port := c.LocalAddr().(*net.UDPAddr).Port
	_ = c.Close()
	return uint16(port)
}

func TestRawLoopback(t *testing.T) {
	r := newRawOrSkip(t)
	cases := []struct {
		name   string
		target string
		proto  Protocol
	}{
		{"icmp4", "127.0.0.1", ICMP},
		{"udp4", "127.0.0.1", UDP},
		{"tcp4", "127.0.0.1", TCP},
		{"icmp6", "::1", ICMP},
		{"udp6", "::1", UDP},
		{"tcp6", "::1", TCP},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dst := netip.MustParseAddr(c.target)
			if dst.Is6() {
				skipWithoutIPv6(t, r)
			}
			req := TraceRequest{Target: dst, Protocol: c.proto, FlowID: DefaultFlowID, Paris: true,
				MaxTTL: 3, Timeout: time.Second}
			switch c.proto {
			case UDP:
				if dst.Is4() {
					req.Port = closedUDPPort(t, "udp4", "127.0.0.1:0")
				} else {
					req.Port = closedUDPPort(t, "udp6", "[::1]:0")
				}
			case TCP:
				req.Port = closedTCPPort(t, dst) // answered by RST
			}
			start := time.Now()
			res, err := r.Trace(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			// Local delivery does not decrement the TTL, so every probe
			// reaches the destination.
			for _, h := range res {
				if h.Status != record.StatusReply || h.From != dst || h.RTT <= 0 || h.RTT > time.Second {
					t.Errorf("ttl %d: %+v", h.TTL, h)
				}
			}
			if d := time.Since(start); d > 500*time.Millisecond {
				t.Errorf("trace took %v; it should end as soon as all probes are answered", d)
			}
			if n := r.eng.pendingLen(); n != 0 {
				t.Errorf("%d probes left pending", n)
			}
		})
	}
	if r.kernelStamped.Load() == 0 {
		t.Error("no reply carried a kernel receive timestamp")
	}
}

// closedTCPPort returns a local TCP port nothing listens on.
func closedTCPPort(t *testing.T, dst netip.Addr) uint16 {
	t.Helper()
	ln, err := net.Listen("tcp", netip.AddrPortFrom(dst, 0).String())
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return uint16(port)
}

// An open port answers the SYN with a SYN-ACK.
func TestRawTCPOpenPort(t *testing.T) {
	r := newRawOrSkip(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := uint16(ln.Addr().(*net.TCPAddr).Port)
	res, err := r.Trace(t.Context(), TraceRequest{Target: netip.MustParseAddr("127.0.0.1"), Protocol: TCP, Port: port,
		FlowID: 40123, Paris: true, MaxTTL: 2, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range res {
		if h.Status != record.StatusReply {
			t.Errorf("ttl %d: %+v", h.TTL, h)
		}
	}
}

func TestRawConcurrentAndSoak(t *testing.T) {
	r := newRawOrSkip(t)
	dst := netip.MustParseAddr("127.0.0.1")
	port := closedUDPPort(t, "udp4", "127.0.0.1:0")
	before := runtime.NumGoroutine()
	for round := range 50 {
		var wg sync.WaitGroup
		errs := make(chan error, 16)
		for i := range 16 {
			proto := ICMP
			if i%2 == 1 {
				proto = UDP
			}
			wg.Go(func() {
				res, err := r.Trace(t.Context(), TraceRequest{Target: dst, Protocol: proto, Port: port,
					FlowID: uint16(40000 + i), Paris: true, MaxTTL: 4, Timeout: time.Second})
				if err != nil {
					errs <- err
					return
				}
				for _, h := range res {
					if h.Status != record.StatusReply {
						errs <- fmt.Errorf("round %d %s flow %d ttl %d: %+v", round, proto, 40000+i, h.TTL, h)
						return
					}
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	}
	if n := r.eng.pendingLen(); n != 0 {
		t.Errorf("%d probes left pending", n)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Errorf("goroutines grew from %d to %d", before, after)
	}
}

func TestRawNoRoute(t *testing.T) {
	r := newRawOrSkip(t)
	// 192.0.2.0/24 (TEST-NET-1) via a blackhole-free lookup: on most hosts it
	// follows the default route, so this only checks the error path when
	// there is no default route at all (as in an isolated namespace).
	if os.Getenv("HOPCORDER_NETNS_INNER") == "" {
		t.Skip("only meaningful inside the isolated test namespace")
	}
	_, err := r.Trace(t.Context(), TraceRequest{Target: netip.MustParseAddr("192.0.2.1"), Protocol: ICMP,
		FlowID: 1, Paris: true, MaxTTL: 2, Timeout: 100 * time.Millisecond})
	if !errors.Is(err, ErrNoRoute) {
		t.Fatalf("err = %v, want ErrNoRoute", err)
	}
}

// netns topology, all /24:
//
//	agent 10.99.1.2 -- 10.99.1.1 r1 10.99.2.1 -- 10.99.2.2 r2 10.99.3.1 -- 10.99.3.2 target
//
// and the same with fd99:N::/64 for IPv6 where the kernel supports it.
type netnsTopo struct {
	prefix string
	ns     []string
}

func run(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func setupNetns(t *testing.T) *netnsTopo {
	t.Helper()
	if _, err := exec.LookPath("ip"); err != nil {
		t.Skip("iproute2 not installed")
	}
	// HOPCORDER_NETNS_PREFIX tells concurrent runs on one machine apart
	// beyond the process id (at most 5 characters: link names are short).
	prefix := os.Getenv("HOPCORDER_NETNS_PREFIX")
	if prefix == "" {
		prefix = "hc"
	}
	topo := &netnsTopo{prefix: fmt.Sprintf("%.5s%d", prefix, os.Getpid())}
	names := []string{"agent", "r1", "r2", "target"}
	for _, n := range names {
		ns := topo.prefix + n
		if out, err := exec.Command("ip", "netns", "add", ns).CombinedOutput(); err != nil {
			topo.cleanup()
			t.Skipf("cannot create network namespaces (need CAP_SYS_ADMIN): %v %s", err, out)
		}
		topo.ns = append(topo.ns, ns)
	}
	t.Cleanup(topo.cleanup)
	ns := func(i int) string { return topo.ns[i] }
	in := func(i int, args ...string) { run(t, append([]string{"ip", "netns", "exec", ns(i)}, args...)...) }

	v6 := ipv6Supported()
	for i := range 3 { // link i joins ns i and ns i+1, subnet 10.99.(i+1).0/24
		a, b := fmt.Sprintf("%sl%da", topo.prefix, i), fmt.Sprintf("%sl%db", topo.prefix, i)
		run(t, "ip", "link", "add", a, "netns", ns(i), "type", "veth", "peer", "name", b, "netns", ns(i+1))
		left, right := "2", "1" // the agent side of link 0 is .2, routers are .1 towards the agent
		if i > 0 {
			left, right = "1", "2"
		}
		in(i, "ip", "addr", "add", fmt.Sprintf("10.99.%d.%s/24", i+1, left), "dev", a)
		in(i+1, "ip", "addr", "add", fmt.Sprintf("10.99.%d.%s/24", i+1, right), "dev", b)
		if v6 {
			in(i, "ip", "addr", "add", fmt.Sprintf("fd99:%d::%s/64", i+1, left), "dev", a, "nodad")
			in(i+1, "ip", "addr", "add", fmt.Sprintf("fd99:%d::%s/64", i+1, right), "dev", b, "nodad")
		}
		in(i, "ip", "link", "set", a, "up")
		in(i+1, "ip", "link", "set", b, "up")
	}
	for i := range 4 {
		in(i, "ip", "link", "set", "lo", "up")
	}
	for _, i := range []int{1, 2} {
		in(i, "sysctl", "-qw", "net.ipv4.ip_forward=1")
		if v6 {
			in(i, "sysctl", "-qw", "net.ipv6.conf.all.forwarding=1")
		}
	}
	// Static routes: agent -> r1 -> r2 -> target and back.
	in(0, "ip", "route", "add", "10.99.0.0/16", "via", "10.99.1.1")
	in(1, "ip", "route", "add", "10.99.3.0/24", "via", "10.99.2.2")
	in(2, "ip", "route", "add", "10.99.1.0/24", "via", "10.99.2.1")
	in(3, "ip", "route", "add", "10.99.0.0/16", "via", "10.99.3.1")
	if v6 {
		in(0, "ip", "-6", "route", "add", "fd99::/16", "via", "fd99:1::1")
		in(1, "ip", "-6", "route", "add", "fd99:3::/64", "via", "fd99:2::2")
		in(2, "ip", "-6", "route", "add", "fd99:1::/64", "via", "fd99:2::1")
		in(3, "ip", "-6", "route", "add", "fd99::/16", "via", "fd99:3::1")
	}
	return topo
}

func (n *netnsTopo) cleanup() {
	for _, ns := range n.ns {
		_ = exec.Command("ip", "netns", "del", ns).Run()
	}
	n.ns = nil
}

func ipv6Supported() bool {
	fd, err := syscall.Socket(syscall.AF_INET6, syscall.SOCK_DGRAM, 0)
	if err != nil {
		return false
	}
	_ = syscall.Close(fd)
	return true
}

// TestRawNetns re-runs this test binary inside the agent namespace so that
// route lookups and sockets all live there.
func TestRawNetns(t *testing.T) {
	if os.Getenv("HOPCORDER_NETNS_INNER") != "" {
		t.Skip("inner run")
	}
	topo := setupNetns(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	inner := func(t *testing.T, filter string) {
		t.Helper()
		cmd := exec.Command("ip", "netns", "exec", topo.ns[0], exe,
			"-test.run=^("+filter+")$", "-test.v", "-test.count=1")
		cmd.Env = append(os.Environ(), "HOPCORDER_NETNS_INNER=1")
		out, err := cmd.CombinedOutput()
		t.Logf("inner run:\n%s", out)
		if err != nil {
			t.Fatalf("inner run failed: %v", err)
		}
		if !strings.Contains(string(out), "--- PASS") {
			t.Fatal("inner run passed nothing")
		}
	}
	t.Run("path", func(t *testing.T) { inner(t, "TestInnerPath|TestRawNoRoute") })
	t.Run("hold-and-forge", func(t *testing.T) {
		if _, err := exec.LookPath("iptables"); err != nil {
			t.Skip("iptables not installed")
		}
		in := func(i int, args ...string) { run(t, append([]string{"ip", "netns", "exec", topo.ns[i]}, args...)...) }
		// Ping sockets for every group in the agent namespace (systemd's
		// default), so the echo identifier can be held, and could be
		// claimed by an unprivileged process otherwise.
		in(0, "sysctl", "-qw", "net.ipv4.ping_group_range=0 2147483647")
		// r2 stays silent, like many routers: a forgery has no race to win.
		rule := []string{"OUTPUT", "-p", "icmp", "--icmp-type", "time-exceeded", "-j", "DROP"}
		in(2, append([]string{"iptables", "-A"}, rule...)...)
		defer in(2, append([]string{"iptables", "-D"}, rule...)...)
		in(3, "sysctl", "-qw", "net.ipv4.icmp_ratelimit=0")
		inner(t, "TestInnerHoldFlows|TestInnerForgeRefused")
	})
	t.Run("tcp-hold", func(t *testing.T) {
		addrs := []string{"10.99.3.2"}
		if ipv6Supported() {
			addrs = append(addrs, "fd99:3::2")
		}
		startTCPListener(t, topo.ns[3], addrs...)
		t.Setenv("HOPCORDER_NETNS_TARGET", topo.ns[3])
		inner(t, "TestInnerTCPHold")
	})
	t.Run("spoof", func(t *testing.T) {
		if _, err := exec.LookPath("iptables"); err != nil {
			t.Skip("iptables not installed")
		}
		in := func(i int, args ...string) { run(t, append([]string{"ip", "netns", "exec", topo.ns[i]}, args...)...) }
		// Every hop and the target silent, so any answer is a forgery.
		silent := map[int][]string{
			1: {"OUTPUT", "-p", "icmp", "--icmp-type", "time-exceeded", "-j", "DROP"},
			2: {"OUTPUT", "-p", "icmp", "--icmp-type", "time-exceeded", "-j", "DROP"},
			3: {"INPUT", "-s", "10.99.1.2", "-j", "DROP"},
		}
		for i, rule := range silent {
			in(i, append([]string{"iptables", "-A"}, rule...)...)
			defer in(i, append([]string{"iptables", "-D"}, rule...)...)
		}
		t.Setenv("HOPCORDER_NETNS_SPOOFER", topo.ns[3]) // the spoofer sends from the target's namespace
		inner(t, "TestInnerSpoofRefused")
	})
	t.Run("loop", func(t *testing.T) {
		// Make r2 send target traffic back to r1: a two-router loop. Host
		// routes, because the longest prefix beats r2's connected route to
		// the target subnet whatever the metrics (an IPv6 /64 added with
		// "replace" gets metric 1024 and loses to the connected /64 at 256).
		run(t, "ip", "netns", "exec", topo.ns[2], "ip", "route", "add", "10.99.3.2/32", "via", "10.99.2.1")
		if ipv6Supported() {
			run(t, "ip", "netns", "exec", topo.ns[2], "ip", "-6", "route", "add", "fd99:3::2/128", "via", "fd99:2::1")
		}
		inner(t, "TestInnerLoop")
	})
}

func innerOnly(t *testing.T) {
	if os.Getenv("HOPCORDER_NETNS_INNER") == "" {
		t.Skip("runs inside the namespace started by TestRawNetns")
	}
}

type topoCase struct {
	name   string
	target string
	hops   []string // routers at TTL 1, 2
	// r1Link2 is r1's address on the r1–r2 link. A router may source an
	// ICMP error from the interface the offending packet arrived on (Linux
	// does for IPv6), so on the loop legs r1 can answer from this address.
	r1Link2 string
}

func topoCases() []topoCase {
	cs := []topoCase{{"v4", "10.99.3.2", []string{"10.99.1.1", "10.99.2.2"}, "10.99.2.1"}}
	if ipv6Supported() {
		cs = append(cs, topoCase{"v6", "fd99:3::2", []string{"fd99:1::1", "fd99:2::2"}, "fd99:2::1"})
	}
	return cs
}

func TestInnerPath(t *testing.T) {
	innerOnly(t)
	r := newRawOrSkip(t)
	for _, c := range topoCases() {
		for _, proto := range []Protocol{ICMP, UDP, TCP} {
			for _, paris := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/paris=%v", c.name, proto, paris), func(t *testing.T) {
					dst := netip.MustParseAddr(c.target)
					res, err := r.Trace(t.Context(), TraceRequest{Target: dst, Protocol: proto, Port: 33434,
						FlowID: DefaultFlowID, Paris: paris, MaxTTL: 5, Timeout: time.Second})
					if err != nil {
						t.Fatal(err)
					}
					want := []string{"ttl-expired " + c.hops[0], "ttl-expired " + c.hops[1],
						"reply " + c.target, "reply " + c.target, "reply " + c.target}
					for i, h := range res {
						if got := fmt.Sprintf("%s %s", h.Status, h.From); got != want[i] {
							t.Errorf("ttl %d: got %q, want %q", h.TTL, got, want[i])
						}
					}
				})
				// Routers rate-limit ICMP errors per destination (a burst of
				// ~6, then 1/s); give the bucket time to refill between cases.
				time.Sleep(1100 * time.Millisecond)
			}
		}
	}
}

func TestInnerLoop(t *testing.T) {
	innerOnly(t)
	r := newRawOrSkip(t)
	for _, c := range topoCases() {
		t.Run(c.name, func(t *testing.T) {
			dst := netip.MustParseAddr(c.target)
			res, err := r.Trace(t.Context(), TraceRequest{Target: dst, Protocol: ICMP,
				FlowID: DefaultFlowID, Paris: true, MaxTTL: 6, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			hops := make([]record.Hop, len(res))
			for i, h := range res {
				hops[i] = record.Hop{TTL: h.TTL, Status: h.Status, IP: h.From, RTTMicros: 1}
			}
			// r2 answers every even TTL, r1 every odd one beyond the first,
			// from whichever of its addresses its kernel picks.
			loops := record.DetectLoops(record.Trim(hops, 2))
			var r1, r2 bool
			for _, l := range loops {
				ttls := fmt.Sprint(l.TTLs)
				switch ip := l.IP.String(); {
				case ip == c.hops[1] && ttls == "[2 4 6]":
					r2 = true
				case (ip == c.hops[0] && ttls == "[1 3 5]") || (ip == c.r1Link2 && ttls == "[3 5]"):
					r1 = true
				}
			}
			if len(loops) != 2 || !r1 || !r2 {
				t.Errorf("detected loops %v in %+v, want r1 (%s or %s) and r2 (%s) looping",
					loops, res, c.hops[0], c.r1Link2, c.hops[1])
			}
			time.Sleep(1100 * time.Millisecond)
		})
	}
}
