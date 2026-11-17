//go:build linux

package probe

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// What Trace checks is held (holdKeys) and what HoldFlows holds for a flow
// (NewFlow, tcpPorts) are exactly the source ports the engine sends TCP
// probes from, in paris and classic mode (which walks the source port).
func TestHoldKeysAreTheProbesPorts(t *testing.T) {
	for _, target := range []string{"198.51.100.7", "2001:db8:1::7"} {
		for _, paris := range []bool{true, false} {
			for _, flowID := range []uint16{4321, 65533} { // 65533: classic wraps through port 0
				t.Run(fmt.Sprintf("%s/paris=%v/%d", target, paris, flowID), func(t *testing.T) {
					s := newSim()
					dst := netip.MustParseAddr(target)
					s.setPath(dst, &simPath{routers: addrs("*", "*", "*"), blackhole: true})
					req := TraceRequest{Target: dst, Protocol: TCP, Port: 443, FlowID: flowID, Paris: paris,
						MinTTL: 3, MaxTTL: 8, Timeout: 5 * time.Millisecond}
					if _, err := s.eng.trace(t.Context(), req); err != nil {
						t.Fatal(err)
					}
					var sent []holdKey
					for _, p := range s.sent {
						port := uint16(p.l4[0])<<8 | uint16(p.l4[1])
						if k := (holdKey{proto: TCP, v6: dst.Is6(), id: port}); port != 0 && !slices.Contains(sent, k) {
							sent = append(sent, k)
						}
					}
					if got := holdKeys(req); fmt.Sprint(got) != fmt.Sprint(sent) {
						t.Errorf("holdKeys %v, probes sent from %v", got, sent)
					}
					held := NewFlow(TCP, flowID, paris, req.MaxTTL).tcpPorts()
					for _, k := range sent {
						if !slices.Contains(held, k.id) {
							t.Errorf("probe sent from port %d, which the flow's hold (%v) lacks", k.id, held)
						}
					}
					if paris && len(held) != 1 {
						t.Errorf("paris flow holds %v", held)
					}
				})
			}
		}
	}
	for _, f := range []Flow{NewFlow(ICMP, 1, false, 30), NewFlow(UDP, 1, false, 30), NewFlow(TCP, 1, true, 30)} {
		if f.Ports != 0 {
			t.Errorf("%+v: only classic TCP flows use more than one port", f)
		}
	}
	if f := NewFlow(TCP, 1, false, 30); f.Ports != 30 {
		t.Errorf("classic TCP flow %+v, want 30 ports", f)
	}
}

// listenTCP4 binds 127.0.0.1:port the way most programs do (net.Listen
// sets SO_REUSEADDR).
func listenTCP4(port uint16) (net.Listener, error) {
	return net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
}

// freeTCPPorts returns the first of n consecutive TCP ports nothing has.
func freeTCPPorts(t *testing.T, n int) uint16 {
	t.Helper()
	for range 50 {
		var ls []net.Listener
		ok := true
		l, err := listenTCP4(0)
		if err != nil {
			t.Fatal(err)
		}
		first := uint16(l.Addr().(*net.TCPAddr).Port) //nolint:gosec // a port
		ls = append(ls, l)
		for i := 1; i < n && ok; i++ {
			if int(first)+i > 65535 {
				ok = false
				break
			}
			l, err := listenTCP4(first + uint16(i)) //nolint:gosec // checked above
			if err != nil {
				ok = false
				break
			}
			ls = append(ls, l)
		}
		for _, l := range ls {
			_ = l.Close()
		}
		if ok {
			return first
		}
	}
	t.Fatal("no free port range")
	return 0
}

// The agent holds TCP flows' source ports with unprivileged sockets: one
// another socket has (here this test's listener, as another process's
// connection or listener would) refuses only that flow's targets of that
// address family, is retried by each trace, and is held once free; nothing
// else can bind a held port. No raw socket is needed for any of this.
func TestRawHoldsTCPPorts(t *testing.T) {
	r := NewRaw(slog.New(slog.DiscardHandler))
	t.Cleanup(func() { _ = r.Close() })
	other, err := listenTCP4(freeTCPPorts(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	port := uint16(other.Addr().(*net.TCPAddr).Port) //nolint:gosec // a port
	v4 := TraceRequest{Target: netip.MustParseAddr("192.0.2.1"), Protocol: TCP, Port: 443, FlowID: port, Paris: true,
		MaxTTL: 3, Timeout: time.Second}
	v6, udp := v4, v4
	v6.Target = netip.MustParseAddr("2001:db8::1")
	udp.Protocol = UDP

	err = r.HoldFlows([]Flow{{Protocol: TCP, ID: port}, {Protocol: UDP, ID: port}})
	want := fmt.Sprintf("tcp flow_id %d: IPv4 TCP port %d is in use by another process", port, port)
	if !errors.Is(err, ErrFlowInUse) || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "IPv6") {
		t.Errorf("hold with another socket on the port: %v; want only %q", err, want)
	}
	for range 2 {
		if err := r.flowReady(v4); !errors.Is(err, ErrFlowInUse) {
			t.Errorf("IPv4 TCP target while another socket has the port: %v", err)
		}
	}
	// The IPv6 port is held (IPv6-only, it does not collide with the
	// IPv4 listener) or there is no IPv6; UDP ports are not held.
	if err := r.flowReady(v6); err != nil {
		t.Errorf("IPv6 TCP target: %v", err)
	}
	if err := r.flowReady(udp); err != nil {
		t.Errorf("UDP target on the same port number: %v", err)
	}
	if l, err := net.ListenPacket("udp4", fmt.Sprintf("127.0.0.1:%d", port)); err != nil {
		t.Errorf("UDP port of the flow: %v", err)
	} else {
		_ = l.Close()
	}

	// Once the other socket is gone, the next trace holds the port.
	_ = other.Close()
	if err := r.flowReady(v4); err != nil {
		t.Errorf("IPv4 TCP target once the port is free: %v", err)
	}
	for _, network := range []string{"tcp4", "tcp"} { // "tcp": dual-stack where there is IPv6
		if l, err := net.Listen(network, fmt.Sprintf(":%d", port)); !errors.Is(err, syscall.EADDRINUSE) {
			t.Errorf("%s socket on the held port: %v", network, err)
			if err == nil {
				_ = l.Close()
			}
		}
	}

	// A classic flow holds a port for every TTL.
	base := freeTCPPorts(t, 3)
	if err := r.HoldFlows([]Flow{NewFlow(TCP, base, false, 3)}); err != nil {
		t.Fatal(err)
	}
	for p := base; p < base+3; p++ {
		if l, err := listenTCP4(p); !errors.Is(err, syscall.EADDRINUSE) {
			t.Errorf("socket on the classic flow's port %d: %v", p, err)
			if err == nil {
				_ = l.Close()
			}
		}
	}
	// The paris flow's port, no longer listed, is released.
	if l, err := listenTCP4(port); err != nil {
		t.Errorf("released port: %v", err)
	} else {
		_ = l.Close()
	}
	_ = r.Close()
	if l, err := listenTCP4(base); err != nil {
		t.Errorf("port after close: %v", err)
	} else {
		_ = l.Close()
	}
}
