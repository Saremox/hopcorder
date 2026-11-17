//go:build rawsock && linux

package probe

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/record"
)

// tcpServicePort is the open port of the target's listener in
// TestRawNetns/tcp-hold (TestTCPListenerChild); tcpServicePort+1 is closed.
const tcpServicePort = 8443

// startTCPListener runs TestTCPListenerChild in namespace ns, serving
// tcpServicePort on addrs, until the test ends.
func startTCPListener(t *testing.T, ns string, addrs ...string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("ip", "netns", "exec", ns, exe, "-test.run=^TestTCPListenerChild$")
	cmd.Env = append(os.Environ(), "HOPCORDER_TCP_LISTENER="+strings.Join(addrs, " "))
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	if sc := bufio.NewScanner(out); !sc.Scan() || sc.Text() != "listening" {
		t.Fatalf("listener did not start: %q", sc.Text())
	}
}

// TestTCPListenerChild is a server on the target: it accepts connections on
// tcpServicePort and keeps them open, as a TLS server waiting for a
// ClientHello would.
func TestTCPListenerChild(t *testing.T) {
	addrs := os.Getenv("HOPCORDER_TCP_LISTENER")
	if addrs == "" {
		t.Skip("helper process")
	}
	for _, a := range strings.Fields(addrs) {
		ln, err := net.Listen("tcp", netip.AddrPortFrom(netip.MustParseAddr(a), tcpServicePort).String())
		if err != nil {
			fmt.Println("listen:", err)
			os.Exit(1)
		}
		go func() {
			var keep []net.Conn // open until the process is killed
			defer func() {
				for _, c := range keep {
					_ = c.Close()
				}
			}()
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				keep = append(keep, c)
			}
		}()
	}
	fmt.Println("listening")
	select {}
}

// TestInnerTCPHold is the round-six review's proof of concept: a local user
// who holds a connection from a TCP flow's source port to the target's
// service (`nc -p 33434 target 443`) made the target answer every probe SYN
// with a challenge ACK for that connection (RFC 5961), which the agent never
// sees, so every cycle was recorded as not reaching the target, with no
// error and no metric. The agent now holds the flow's port: a connection
// that was there first refuses the flow's targets (visibly, retried every
// cycle) until it is gone, and once the agent holds the port nobody else
// can bind it or be given it by the kernel.
func TestInnerTCPHold(t *testing.T) {
	innerOnly(t)
	targetNS := os.Getenv("HOPCORDER_NETNS_TARGET")
	if targetNS == "" {
		t.Skip("runs in TestRawNetns/tcp-hold")
	}
	r := newRawOrSkip(t)
	u := startUnprivileged(t)
	type family struct {
		name string
		dst  netip.Addr
	}
	families := []family{{"v4", netip.MustParseAddr("10.99.3.2")}}
	if ipv6Supported() {
		families = append(families, family{"v6", netip.MustParseAddr("fd99:3::2")})
	}
	// trace returns the destination's answers (TTLs 3 and 4) or the error.
	trace := func(p *Raw, dst netip.Addr, flowID, port uint16, paris bool) (string, error) {
		t.Helper()
		res, err := p.Trace(t.Context(), TraceRequest{Target: dst, Protocol: TCP, Port: port, FlowID: flowID,
			Paris: paris, MaxTTL: 4, Timeout: time.Second})
		if err != nil {
			return "", err
		}
		var got []string
		for _, h := range res[2:] {
			got = append(got, fmt.Sprintf("%s %v", h.Status, h.From))
		}
		return strings.Join(got, ", "), nil
	}
	reached := func(dst netip.Addr) string {
		return fmt.Sprintf("%s %v, %s %v", record.StatusReply, dst, record.StatusReply, dst)
	}
	inUse := func(got string) bool { return strings.HasSuffix(got, ": address already in use") }

	for i, fam := range families {
		t.Run(fam.name, func(t *testing.T) {
			dst := fam.dst
			attacked := uint16(4250 + 10*i) // the attacker is there first
			held := attacked + 1            // the agent is there first

			// The review's attack, the attacker first: the flow is
			// refused, visibly, instead of recorded as unreached.
			got := u.do("connect-tcp %d %s %d", attacked, dst, tcpServicePort)
			if !strings.HasPrefix(got, fmt.Sprintf("connect-tcp %d: <nil>", attacked)) {
				t.Fatalf("attacker's connection: %q", got)
			}
			err := r.HoldFlows([]Flow{{Protocol: TCP, ID: attacked}})
			if !errors.Is(err, ErrFlowInUse) || !strings.Contains(err.Error(), fmt.Sprintf("TCP port %d", attacked)) {
				t.Errorf("hold with the attacker's connection open: %v", err)
			}
			for range 3 {
				if got, err := trace(r, dst, attacked, tcpServicePort, true); !errors.Is(err, ErrFlowInUse) {
					t.Errorf("trace with the attacker's connection open: %q %v; want the flow refused", got, err)
				}
			}
			// Once the connection is gone, the next trace holds the port
			// and probes.
			if got := u.do("reset-tcp %d", attacked); got != fmt.Sprintf("reset-tcp %d: <nil>", attacked) {
				t.Fatalf("reset: %q", got)
			}
			if got, err := trace(r, dst, attacked, tcpServicePort, true); err != nil || got != reached(dst) {
				t.Errorf("trace once the connection is gone: %q %v", got, err)
			}
			if got := u.do("connect-tcp %d %s %d", attacked, dst, tcpServicePort); !inUse(got) {
				t.Errorf("attacker's connection once the trace took the port back: %q", got)
			}

			// The agent first: nobody else can take the port, and every
			// cycle reaches the target.
			if err := r.HoldFlows([]Flow{{Protocol: TCP, ID: held}}); err != nil {
				t.Fatal(err)
			}
			for _, cmd := range []string{
				fmt.Sprintf("connect-tcp %d %s %d", held, dst, tcpServicePort),
				fmt.Sprintf("bind-tcp %d 4", held),
				fmt.Sprintf("bind-tcp %d 6", held),
			} {
				got := u.do("%s", cmd)
				if cmd == fmt.Sprintf("bind-tcp %d 6", held) && strings.HasSuffix(got, "address family not supported by protocol") {
					continue
				}
				if !inUse(got) {
					t.Errorf("%s while the agent holds the port: %q", cmd, got)
				}
			}
			for _, port := range []uint16{tcpServicePort, tcpServicePort + 1} { // SYN-ACK, RST
				for range 3 {
					if got, err := trace(r, dst, held, port, true); err != nil || got != reached(dst) {
						t.Errorf("trace to port %d on the held port: %q %v", port, got, err)
					}
				}
			}
			// The held socket does not listen or connect: the agent's
			// kernel still answers the SYN-ACKs with a RST, so the target
			// keeps no half-open connection from the probes.
			time.Sleep(100 * time.Millisecond)
			if out, err := exec.Command("ip", "netns", "exec", targetNS, "ss", "-Htan",
				fmt.Sprintf("( dport = :%d )", held)).CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "" {
				t.Errorf("target's sockets from the held port: %v\n%s", err, out)
			}
		})
	}

	// Classic mode walks the source port: TTL n is sent from flow_id+n-1,
	// and every one of those ports is held. A connection from the port of
	// the target's TTL (3) made that probe go unanswered and the next one
	// reach, so the target looked one hop further away, behind a silent hop.
	dst := families[0].dst
	const classic = 4270
	got := u.do("connect-tcp %d %s %d", classic+2, dst, tcpServicePort)
	if !strings.HasPrefix(got, fmt.Sprintf("connect-tcp %d: <nil>", classic+2)) {
		t.Fatalf("attacker's connection: %q", got)
	}
	err := r.HoldFlows([]Flow{NewFlow(TCP, classic, false, 4)})
	if !errors.Is(err, ErrFlowInUse) || !strings.Contains(err.Error(), fmt.Sprintf("IPv4 TCP port %d", classic+2)) {
		t.Errorf("classic hold with a connection from the TTL 3 port open: %v", err)
	}
	if got, err := trace(r, dst, classic, tcpServicePort, false); !errors.Is(err, ErrFlowInUse) {
		t.Errorf("classic trace with a connection from the TTL 3 port open: %q %v; want the flow refused", got, err)
	}
	if got := u.do("reset-tcp %d", classic+2); got != fmt.Sprintf("reset-tcp %d: <nil>", classic+2) {
		t.Fatalf("reset: %q", got)
	}
	if got, err := trace(r, dst, classic, tcpServicePort, false); err != nil || got != reached(dst) {
		t.Errorf("classic trace once the connection is gone: %q %v", got, err)
	}
	for p := classic; p < classic+4; p++ {
		if got := u.do("connect-tcp %d %s %d", p, dst, tcpServicePort); !inUse(got) {
			t.Errorf("connection from the classic flow's port %d: %q", p, got)
		}
	}

	// The kernel never gives a held port to a connection (the agent's own
	// HTTP output to a TCP target's host:port, say): only the held port is
	// left to choose from here.
	const ephemeral = 4290
	run(t, "sysctl", "-qw", fmt.Sprintf("net.ipv4.ip_local_port_range=%d %d", ephemeral, ephemeral))
	defer run(t, "sysctl", "-qw", "net.ipv4.ip_local_port_range=32768 60999")
	if got := u.do("connect-tcp 0 %s %d", dst, tcpServicePort); got != fmt.Sprintf("connect-tcp 0: <nil> from %d", ephemeral) {
		t.Fatalf("connection given the only port: %q", got)
	}
	if got := u.do("reset-tcp %d", ephemeral); got != fmt.Sprintf("reset-tcp %d: <nil>", ephemeral) {
		t.Fatalf("reset: %q", got)
	}
	if err := r.HoldFlows([]Flow{{Protocol: TCP, ID: ephemeral}}); err != nil {
		t.Fatal(err)
	}
	if got := u.do("connect-tcp 0 %s %d", dst, tcpServicePort); got != "connect-tcp 0: cannot assign requested address" {
		t.Errorf("connection while the agent holds the only port: %q", got)
	}

	// Releasing a flow frees its port; Close frees the rest.
	if err := r.HoldFlows([]Flow{{Protocol: TCP, ID: 4251}}); err != nil {
		t.Fatal(err)
	}
	if got := u.do("bind-tcp %d 4", ephemeral); got != fmt.Sprintf("bind-tcp %d 4: <nil>", ephemeral) {
		t.Errorf("released port: %q", got)
	}
	_ = r.Close()
	if got := u.do("bind-tcp 4251 4"); got != "bind-tcp 4251 4: <nil>" {
		t.Errorf("port after close: %q", got)
	}
}
