//go:build rawsock && linux

package probe

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// These run inside the agent namespace of TestRawNetns ("hold-and-forge"),
// where ping sockets are permitted for every group (systemd's default) and
// r2 drops its own time-exceeded messages, like many routers.

// unprivileged is a child process running as uid 65534 without
// capabilities, driven line by line (see TestUnprivilegedChild).
type unprivileged struct {
	t   *testing.T
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Scanner
	mu  sync.Mutex
}

func startUnprivileged(t *testing.T) *unprivileged {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestUnprivilegedChild$")
	cmd.Env = append(os.Environ(), "HOPCORDER_UNPRIVILEGED_CHILD=1")
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	u := &unprivileged{t: t, cmd: cmd, in: in, out: bufio.NewScanner(out)}
	t.Cleanup(func() {
		_ = in.Close()
		_ = cmd.Wait()
	})
	if got := u.do("whoami"); got != "uid 65534 capeff 0000000000000000" {
		t.Fatalf("child: %q", got)
	}
	return u
}

// do sends one command and returns the child's one-line answer.
func (u *unprivileged) do(format string, args ...any) string {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, err := fmt.Fprintf(u.in, format+"\n", args...); err != nil {
		u.t.Fatal(err)
	}
	if !u.out.Scan() {
		u.t.Fatalf("child exited: %v", u.out.Err())
	}
	return u.out.Text()
}

// TestUnprivilegedChild is the child half: it drops to uid 65534 and then
// uses only what any local user may.
func TestUnprivilegedChild(t *testing.T) {
	if os.Getenv("HOPCORDER_UNPRIVILEGED_CHILD") == "" {
		t.Skip("helper process")
	}
	for _, err := range []error{syscall.Setgroups(nil), syscall.Setgid(65534), syscall.Setuid(65534)} {
		if err != nil {
			fmt.Println("drop privileges:", err)
			os.Exit(1)
		}
	}
	udp := -1
	squatted := map[int]int{} // identifier -> ping socket
	conns := map[int]int{}    // local port -> connected TCP socket
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		switch f[0] {
		case "bind-tcp": // bind-tcp <port> <4|6>: bind a TCP socket as hard as a user can, then close it
			port, _ := strconv.Atoi(f[1])
			fmt.Printf("%s %d %s: %v\n", f[0], port, f[2], bindTCP(port, f[2] == "6"))
		case "connect-tcp": // connect-tcp <local port, 0: the kernel's choice> <dst> <dport>; keeps it open
			port, _ := strconv.Atoi(f[1])
			dport, _ := strconv.Atoi(f[3])
			fd, local, err := connectTCP(port, netip.MustParseAddr(f[2]), dport)
			if err == nil {
				conns[local] = fd
				fmt.Printf("%s %d: <nil> from %d\n", f[0], port, local)
				break
			}
			fmt.Printf("%s %d: %v\n", f[0], port, err)
		case "reset-tcp": // reset-tcp <local port>: close a connection with a RST, so it leaves no TIME_WAIT
			port, _ := strconv.Atoi(f[1])
			fd, ok := conns[port]
			var err error = syscall.EBADF
			if ok {
				delete(conns, port)
				_ = syscall.SetsockoptLinger(fd, syscall.SOL_SOCKET, syscall.SO_LINGER, &syscall.Linger{Onoff: 1})
				err = syscall.Close(fd)
			}
			fmt.Printf("%s %d: %v\n", f[0], port, err)
		case "release-ping": // release-ping <identifier>: close a squatted one
			id, _ := strconv.Atoi(f[1])
			fd, ok := squatted[id]
			var err error = syscall.EBADF
			if ok {
				err = syscall.Close(fd)
				delete(squatted, id)
			}
			fmt.Printf("%s %d: %v\n", f[0], id, err)
		case "whoami":
			st, _ := os.ReadFile("/proc/self/status")
			var capEff string
			for _, l := range strings.Split(string(st), "\n") {
				if v, ok := strings.CutPrefix(l, "CapEff:"); ok {
					capEff = strings.TrimSpace(v)
				}
			}
			fmt.Printf("uid %d capeff %s\n", os.Getuid(), capEff)
		case "bind-ping", "squat-ping": // bind-ping|squat-ping <identifier>; squat keeps it
			id, _ := strconv.Atoi(f[1])
			fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_ICMP)
			if err == nil {
				err = syscall.Bind(fd, &syscall.SockaddrInet4{Port: id})
				if f[0] == "bind-ping" || err != nil {
					_ = syscall.Close(fd)
				} else {
					squatted[id] = fd
				}
			}
			fmt.Printf("%s %d: %v\n", f[0], id, err)
		case "bind-ping6": // bind-ping6 <identifier>: an IPv6 ping socket
			id, _ := strconv.Atoi(f[1])
			fd, err := syscall.Socket(syscall.AF_INET6, syscall.SOCK_DGRAM, syscall.IPPROTO_ICMPV6)
			if errors.Is(err, syscall.EAFNOSUPPORT) {
				fmt.Printf("%s %d: no IPv6\n", f[0], id)
				break
			}
			if err == nil {
				err = syscall.Bind(fd, &syscall.SockaddrInet6{Port: id})
				_ = syscall.Close(fd)
			}
			fmt.Printf("%s %d: %v\n", f[0], id, err)
		case "forge": // forge <src> <dst> <port> <udp checksum> <ttl>
			src, dst := netip.MustParseAddr(f[1]), netip.MustParseAddr(f[2])
			port, _ := strconv.Atoi(f[3])
			sum, _ := strconv.Atoi(f[4])
			ttl, _ := strconv.Atoi(f[5])
			var err error
			if udp < 0 {
				// The agent's source port: nothing stops a user from binding it.
				if udp, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0); err == nil {
					if err = syscall.Bind(udp, &syscall.SockaddrInet4{Port: port}); err == nil {
						err = syscall.Connect(udp, &syscall.SockaddrInet4{Port: port, Addr: dst.As4()})
					}
				}
			}
			if err == nil {
				err = syscall.SetsockoptInt(udp, syscall.IPPROTO_IP, syscall.IP_TTL, ttl)
			}
			// A payload whose UDP checksum is the probe id; corked, so the
			// kernel computes the checksum in software. (It cannot know
			// the probe's nonce either.)
			payload := buildUDP(src, dst, uint16(port), uint16(port), uint16(sum), 0)[8:]
			for range 5 { // an ICMP error queued on the socket fails one write
				if err != nil && !errors.Is(err, syscall.ECONNREFUSED) {
					break
				}
				_ = syscall.SetsockoptInt(udp, syscall.IPPROTO_UDP, 1 /* UDP_CORK */, 1)
				_, err = syscall.Write(udp, payload)
				_ = syscall.SetsockoptInt(udp, syscall.IPPROTO_UDP, 1, 0)
				if err == nil {
					break
				}
			}
			fmt.Printf("forge %d ttl %d: %v\n", sum, ttl, err)
		default:
			fmt.Println("unknown command")
		}
	}
}

// soReusePort is SO_REUSEPORT (asm-generic; not in package syscall).
const soReusePort = 15

// bindTCP binds a TCP socket to port on the wildcard address of one family
// with SO_REUSEADDR and SO_REUSEPORT set (the IPv6 one IPv6-only), and
// closes it.
func bindTCP(port int, v6 bool) error {
	family := syscall.AF_INET
	var sa syscall.Sockaddr = &syscall.SockaddrInet4{Port: port}
	if v6 {
		family, sa = syscall.AF_INET6, &syscall.SockaddrInet6{Port: port}
	}
	fd, err := syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	for _, o := range []int{syscall.SO_REUSEADDR, soReusePort} {
		if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, o, 1); err != nil {
			return err
		}
	}
	if v6 {
		if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, 1); err != nil {
			return err
		}
	}
	return syscall.Bind(fd, sa)
}

// connectTCP connects a TCP socket from local port port (0: the kernel
// picks one) to dst:dport, like `nc -p <port> <dst> <dport>`, and returns
// it with its local port.
func connectTCP(port int, dst netip.Addr, dport int) (int, int, error) {
	family := syscall.AF_INET
	var local, remote syscall.Sockaddr = &syscall.SockaddrInet4{Port: port}, &syscall.SockaddrInet4{Port: dport, Addr: dst.As4()}
	if dst.Is6() {
		family = syscall.AF_INET6
		local, remote = &syscall.SockaddrInet6{Port: port}, &syscall.SockaddrInet6{Port: dport, Addr: dst.As16()}
	}
	fd, err := syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, 0, err
	}
	if port != 0 {
		err = syscall.Bind(fd, local)
	}
	if err == nil {
		err = syscall.Connect(fd, remote)
	}
	var sa syscall.Sockaddr
	if err == nil {
		sa, err = syscall.Getsockname(fd)
	}
	if err != nil {
		_ = syscall.Close(fd)
		return -1, 0, err
	}
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		return fd, sa.Port, nil
	case *syscall.SockaddrInet6:
		return fd, sa.Port, nil
	}
	_ = syscall.Close(fd)
	return -1, 0, fmt.Errorf("local address %v", sa)
}

func TestInnerHoldFlows(t *testing.T) {
	innerOnly(t)
	r := newRawOrSkip(t)
	u := startUnprivileged(t)
	flows := []Flow{{Protocol: ICMP, ID: 4242}, {Protocol: UDP, ID: 4243}}
	if err := r.HoldFlows(flows); err != nil {
		t.Fatal(err)
	}
	if got := u.do("bind-ping 4242"); got != "bind-ping 4242: address already in use" {
		t.Errorf("unprivileged process on the held echo identifier: %q", got)
	}
	// The one (IPv4) socket holds the identifier for IPv6 ping sockets too:
	// the kernel keeps both families' identifiers in one table.
	switch got := u.do("bind-ping6 4242"); got {
	case "bind-ping6 4242: address already in use":
	case "bind-ping6 4242: no IPv6":
		t.Log("no IPv6 in this kernel; the IPv6 side is not checked")
	default:
		t.Errorf("unprivileged IPv6 ping socket on the held echo identifier: %q", got)
	}
	// Probes on the held identifier still get their echo replies (raw
	// sockets receive a copy before the ping socket does).
	dst := netip.MustParseAddr("10.99.3.2")
	res, err := r.Trace(t.Context(), TraceRequest{Target: dst, Protocol: ICMP, FlowID: 4242, Paris: true,
		MaxTTL: 4, Timeout: time.Second})
	if err != nil || res[3].Status != "reply" || res[3].From != dst {
		t.Errorf("trace on the held identifier: %v %+v", err, res)
	}
	if got := u.do("bind-ping 4244"); got != "bind-ping 4244: <nil>" {
		t.Errorf("unprivileged process on a free echo identifier: %q (ping sockets not permitted?)", got)
	}
	// A second agent (here another prober in this process) fails clearly,
	// but only for the ICMP flow: its UDP flow is probed.
	other := NewRaw(nil)
	t.Cleanup(func() { _ = other.Close() })
	if err := other.Check(); err != nil {
		t.Fatal(err)
	}
	err = other.HoldFlows(flows)
	if !errors.Is(err, ErrFlowInUse) || !strings.Contains(err.Error(), "ICMP echo identifier 4242") {
		t.Errorf("second holder: %v", err)
	}
	icmp := func(p *Raw, id uint16) error {
		res, err := p.Trace(t.Context(), TraceRequest{Target: dst, Protocol: ICMP, FlowID: id, Paris: true,
			MaxTTL: 4, Timeout: time.Second})
		if err == nil && (res[3].Status != "reply" || res[3].From != dst) {
			err = fmt.Errorf("trace %+v", res)
		}
		return err
	}
	if err := icmp(other, 4242); !errors.Is(err, ErrFlowInUse) {
		t.Errorf("ICMP trace on an identifier another process holds: %v", err)
	}
	res, err = other.Trace(t.Context(), TraceRequest{Target: dst, Protocol: UDP, Port: DefaultUDPPort, FlowID: 4243,
		Paris: true, MaxTTL: 4, Timeout: time.Second})
	if err != nil || res[3].Status != "reply" {
		t.Errorf("UDP trace beside the refused ICMP flow: %v %+v", err, res)
	}
	// An identifier another process took first stops its flow until that
	// process lets it go; then the next trace holds it and probes, without
	// another HoldFlows.
	if got := u.do("squat-ping 4245"); got != "squat-ping 4245: <nil>" {
		t.Fatalf("squat: %q", got)
	}
	err = other.HoldFlows([]Flow{{Protocol: ICMP, ID: 4245}})
	if !errors.Is(err, ErrFlowInUse) || !strings.Contains(err.Error(), "ICMP echo identifier 4245") {
		t.Errorf("identifier held by an unprivileged process: %v", err)
	}
	if err := icmp(other, 4245); !errors.Is(err, ErrFlowInUse) {
		t.Errorf("ICMP trace on an identifier an unprivileged process holds: %v", err)
	}
	if got := u.do("release-ping 4245"); got != "release-ping 4245: <nil>" {
		t.Fatalf("release: %q", got)
	}
	if err := icmp(other, 4245); err != nil {
		t.Errorf("ICMP trace once the identifier is free: %v", err)
	}
	if got := u.do("bind-ping 4245"); got != "bind-ping 4245: address already in use" {
		t.Errorf("identifier taken back by the retry is not held: %q", got)
	}
	// UDP source ports are deliberately not held (see HoldFlows).
	if c, err := net.ListenPacket("udp4", ":4243"); err != nil {
		t.Errorf("UDP port of a held flow: %v", err)
	} else {
		_ = c.Close()
	}
	// A reload holds the new configuration's flows besides the running
	// one's (the daemon's supervisor), so the running one's stay held
	// throughout the start, and only the next HoldFlows releases them.
	if err := r.HoldFlows(append(slices.Clone(flows), Flow{Protocol: ICMP, ID: 4246})); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{4242, 4246} {
		if got := u.do("bind-ping %d", id); got != fmt.Sprintf("bind-ping %d: address already in use", id) {
			t.Errorf("identifier %d while both configurations' flows are held: %q", id, got)
		}
	}
	if err := r.HoldFlows([]Flow{{Protocol: ICMP, ID: 4246}}); err != nil {
		t.Fatal(err)
	}
	if got := u.do("bind-ping 4242"); got != "bind-ping 4242: <nil>" {
		t.Errorf("the previous configuration's identifier once the new one runs: %q", got)
	}
	if got := u.do("bind-ping 4246"); got != "bind-ping 4246: address already in use" {
		t.Errorf("the new configuration's identifier: %q", got)
	}
	// Releasing a flow frees its identifier; Close frees the rest.
	if err := r.HoldFlows(nil); err != nil {
		t.Fatal(err)
	}
	if err := other.HoldFlows(flows); err != nil {
		t.Errorf("after release: %v", err)
	}
	_ = other.Close()
	if got := u.do("bind-ping 4242"); got != "bind-ping 4242: <nil>" {
		t.Errorf("after close: %q", got)
	}
}

// TestInnerForgeRefused is the round-two review's proof of concept turned
// around. An unprivileged process binds the agent's UDP source port and,
// told the exact probe id of the silent TTL 2 (more than it could learn),
// sends datagrams with that UDP checksum: at TTL 1, so r1's time exceeded
// would put r1 at TTL 2 (a fake loop), and at TTL 64, so the target's port
// unreachable would end the path at TTL 2. Both errors quote the kernel's IP
// ID, not the probe's, and are refused. The echo identifier it cannot bind.
func TestInnerForgeRefused(t *testing.T) {
	innerOnly(t)
	r := newRawOrSkip(t)
	u := startUnprivileged(t)
	src, dst := netip.MustParseAddr("10.99.1.2"), netip.MustParseAddr("10.99.3.2")

	var forgeTTL int
	var answers []string
	r.eng.io = tapIO{netIO: r, onSend: func(p *outPacket) {
		if p.ttl == 2 {
			id := binary.BigEndian.Uint16(p.l4[6:8])
			answers = append(answers, u.do("forge %s %s %d %d %d", src, dst, DefaultFlowID, id, forgeTTL))
		}
	}}
	for _, ttl := range []int{1, 64} {
		forgeTTL = ttl
		before := r.Stats().MismatchedReplies
		res, err := r.Trace(t.Context(), TraceRequest{Target: dst, Protocol: UDP, Port: DefaultUDPPort,
			FlowID: DefaultFlowID, Paris: true, MaxTTL: 4, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, h := range res {
			got = append(got, fmt.Sprintf("%s %v", h.Status, h.From))
		}
		want := []string{"ttl-expired 10.99.1.1", "no-reply invalid IP", "reply 10.99.3.2", "reply 10.99.3.2"}
		if strings.Join(got, ", ") != strings.Join(want, ", ") {
			t.Errorf("forged at ttl %d: got %v, want %v", ttl, got, want)
		}
		if r.Stats().MismatchedReplies == before {
			t.Errorf("forged at ttl %d: no mismatched reply counted (did the forgery reach the agent?)", ttl)
		}
		time.Sleep(1100 * time.Millisecond) // routers' ICMP rate limits
	}
	for _, a := range answers {
		if !strings.HasSuffix(a, ": <nil>") {
			t.Errorf("attacker could not send: %s", a)
		}
	}

	if err := r.HoldFlows([]Flow{{Protocol: ICMP, ID: DefaultFlowID}}); err != nil {
		t.Fatal(err)
	}
	if got := u.do("bind-ping %d", DefaultFlowID); !strings.HasSuffix(got, "address already in use") {
		t.Errorf("attacker on the agent's echo identifier: %q", got)
	}
}

// tapIO calls onSend before every probe goes out.
type tapIO struct {
	netIO
	onSend func(*outPacket)
}

func (t tapIO) send(p *outPacket) error {
	t.onSend(p)
	return t.netIO.send(p)
}
