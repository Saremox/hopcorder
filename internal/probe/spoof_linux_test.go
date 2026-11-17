//go:build rawsock && linux

package probe

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestInnerSpoofRefused is the round-three review's proof of concept: an
// off-path sender that sees no probe forges answers for guessed probe ids,
// many thousands a second, while every hop on the path and the target stay
// silent. It runs in the target's namespace (TestSpooferChild) and builds
// its packets as the review's script did, for the probe layout before round
// three: ICMPv4 errors quoting the guess as UDP checksum and IP ID, echo
// replies carrying the guess as sequence number and ^guess plus the magic as
// data. Every forgery whose guess hits a probe in flight must be refused (and
// counted); before round three they made fake hops and fake destination
// replies in nearly every cycle.
func TestInnerSpoofRefused(t *testing.T) {
	innerOnly(t)
	ns := os.Getenv("HOPCORDER_NETNS_SPOOFER")
	if ns == "" {
		t.Skip("runs in TestRawNetns/spoof")
	}
	r := newRawOrSkip(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	agent, dst := netip.MustParseAddr("10.99.1.2"), netip.MustParseAddr("10.99.3.2")
	const cycles = 3
	for _, c := range []struct {
		mode  string
		proto Protocol
		pps   int
	}{
		{"udp-te", UDP, 20000},
		{"icmp-echo", ICMP, 10000},
	} {
		t.Run(c.mode, func(t *testing.T) {
			secs := float64(cycles)*1.1 + 0.5
			cmd := exec.Command("ip", "netns", "exec", ns, exe, "-test.run=^TestSpooferChild$")
			cmd.Env = append(os.Environ(), fmt.Sprintf("HOPCORDER_SPOOF=%s %s %s %d %g", c.mode, agent, dst, c.pps, secs))
			cmd.Stderr = os.Stderr
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			lines := bufio.NewScanner(out)
			if !lines.Scan() || lines.Text() != "spoofing" {
				_ = cmd.Wait()
				t.Fatalf("spoofer did not start: %q", lines.Text())
			}
			before := r.Stats().MismatchedReplies
			for i := range cycles {
				res, err := r.Trace(t.Context(), TraceRequest{Target: dst, Protocol: c.proto, Port: DefaultUDPPort,
					FlowID: DefaultFlowID, Paris: true, MaxTTL: 30, Timeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				var fake []string
				for _, h := range res {
					if h.Status.Responded() {
						fake = append(fake, fmt.Sprintf("ttl %d %s %s", h.TTL, h.Status, h.From))
					}
				}
				if len(fake) > 0 {
					t.Errorf("cycle %d: forged answers taken: %v", i+1, fake)
				}
			}
			var sent string
			for lines.Scan() {
				if l := lines.Text(); strings.HasPrefix(l, "sent ") {
					sent = l
				}
			}
			if err := cmd.Wait(); err != nil {
				t.Fatalf("spoofer: %v", err)
			}
			refused := r.Stats().MismatchedReplies - before
			t.Logf("spoofer %s; %d forgeries hit a probe in flight and were refused", sent, refused)
			if refused == 0 {
				t.Error("no forgery hit a probe in flight: did they reach the agent?")
			}
		})
	}
}

// TestSpooferChild is the spoofer: HOPCORDER_SPOOF="<mode> <agent> <target>
// <packets per second> <seconds>". It needs CAP_NET_RAW (raw IPv4 sockets).
func TestSpooferChild(t *testing.T) {
	spec := strings.Fields(os.Getenv("HOPCORDER_SPOOF"))
	if len(spec) != 5 {
		t.Skip("helper process for TestInnerSpoofRefused")
	}
	mode := spec[0]
	agent, target := netip.MustParseAddr(spec[1]), netip.MustParseAddr(spec[2])
	pps, _ := strconv.Atoi(spec[3])
	secs, _ := strconv.ParseFloat(spec[4], 64)
	// A router that is not on the path, in the target's subnet so that no
	// reverse path filter on the way drops it.
	fakeRouter := netip.MustParseAddr("10.99.3.66")
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_RAW, syscall.IPPROTO_RAW)
	if err != nil {
		fmt.Println("socket:", err)
		os.Exit(1)
	}
	to := &syscall.SockaddrInet4{Addr: agent.As4()}
	magic := []byte("hopcorder\x00")
	pkt := func(g uint16) []byte {
		var src netip.Addr
		var icmp []byte
		switch mode {
		case "udp-te": // time exceeded quoting a UDP probe: checksum = IP ID = g
			icmp = []byte{icmp4TimeExceeded, 0, 0, 0, 0, 0, 0, 0}
			icmp = append(icmp, buildIPv4(agent, target, protoUDP, 1, g, 8+12)...)
			icmp = binary.BigEndian.AppendUint16(icmp, DefaultFlowID)
			icmp = binary.BigEndian.AppendUint16(icmp, DefaultUDPPort)
			icmp = binary.BigEndian.AppendUint16(icmp, 8+12)
			icmp = binary.BigEndian.AppendUint16(icmp, g)
			src = fakeRouter
		case "icmp-echo": // echo reply from the target: seq = g, data = ^g + magic
			icmp = []byte{icmp4EchoReply, 0, 0, 0}
			icmp = binary.BigEndian.AppendUint16(icmp, DefaultFlowID)
			icmp = binary.BigEndian.AppendUint16(icmp, g)
			icmp = binary.BigEndian.AppendUint16(icmp, ^g)
			icmp = append(icmp, magic...)
			src = target
		default:
			fmt.Println("unknown mode", mode)
			os.Exit(1)
		}
		binary.BigEndian.PutUint16(icmp[2:], checksum(0, icmp))
		return append(buildIPv4(src, agent, protoICMP, 64, uint16(rand.Uint32()), len(icmp)), icmp...)
	}
	fmt.Println("spoofing")
	start := time.Now()
	n := 0
	for time.Since(start).Seconds() < secs {
		for range 100 {
			if err := syscall.Sendto(fd, pkt(uint16(1+rand.IntN(0xfffe))), 0, to); err != nil {
				fmt.Println("send:", err)
				os.Exit(1)
			}
			n++
		}
		if lag := time.Until(start.Add(time.Duration(n) * time.Second / time.Duration(pps))); lag > 0 {
			time.Sleep(lag)
		}
	}
	fmt.Printf("sent %d packets in %.1fs\n", n, time.Since(start).Seconds())
}
