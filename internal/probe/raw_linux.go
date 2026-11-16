//go:build linux

package probe

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/bpf"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// recvBufferBytes is the kernel receive buffer requested for the ICMP
// listeners; replies from many targets can arrive in bursts.
const recvBufferBytes = 1 << 20

// Raw is the raw-socket Prober. All targets share, per address family:
//
//   - IPv4: one IPPROTO_RAW socket (IP_HDRINCL, send-only, any protocol) so
//     the TTL and IP ID are set per packet without setsockopt races, and
//     one ICMP socket that receives every reply.
//   - IPv6: one ICMPv6 socket that sends echo requests and receives every
//     reply, plus a send-only raw UDP socket (with a drop-all BPF filter so
//     it never queues incoming UDP). The hop limit and source address are
//     passed per packet as ancillary data.
//   - TCP probes additionally need one raw TCP socket per family, because
//     the destination answers with a TCP SYN-ACK or RST rather than ICMP.
//     A BPF filter lets only those two through, so ordinary TCP traffic is
//     never copied to user space. On IPv6 the same socket sends the SYNs.
//
// Sockets are opened on first use, so a host without IPv6 works as long as
// it has no IPv6 targets. Call Check at startup to fail early when raw
// sockets are not permitted.
type Raw struct {
	eng *engine
	log *slog.Logger

	mu     sync.Mutex
	closed bool
	v4     *raw4
	v6     *raw6
	wg     sync.WaitGroup

	// kernelStamped counts replies whose receive time came from the kernel.
	kernelStamped atomic.Uint64
}

type raw4 struct {
	send *net.IPConn
	recv *net.IPConn
	tcp  *net.IPConn // receives SYN-ACK/RST; nil until the first TCP probe
}

type raw6 struct {
	icmpConn *net.IPConn
	icmp     *ipv6.PacketConn
	udp      *ipv6.PacketConn // nil until the first IPv6 UDP probe
	tcp      *ipv6.PacketConn // sends SYNs, receives SYN-ACK/RST; nil until the first IPv6 TCP probe
}

// NewRaw returns a raw-socket prober. It opens no sockets yet.
func NewRaw(logger *slog.Logger) *Raw {
	if logger == nil {
		logger = slog.Default()
	}
	r := &Raw{log: logger}
	r.eng = newEngine(r)
	return r
}

// Check opens the IPv4 sockets (and the IPv6 ones if the kernel supports
// IPv6) so that missing privileges surface at startup. The returned error
// wraps ErrPermission when CAP_NET_RAW is missing.
func (r *Raw) Check() error {
	if _, err := r.open4(); err != nil {
		return err
	}
	if _, err := r.open6(false); err != nil && !errors.Is(err, syscall.EAFNOSUPPORT) {
		return err
	}
	return nil
}

// Trace implements Prober.
func (r *Raw) Trace(ctx context.Context, req TraceRequest) ([]HopResult, error) {
	return r.eng.trace(ctx, req)
}

// Stats implements StatsProvider.
func (r *Raw) Stats() Stats { return r.eng.stats() }

// Close closes all sockets and waits for the receive loops to exit.
func (r *Raw) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	var errs []error
	if r.v4 != nil {
		errs = append(errs, r.v4.send.Close(), r.v4.recv.Close())
		if r.v4.tcp != nil {
			errs = append(errs, r.v4.tcp.Close())
		}
	}
	if r.v6 != nil {
		errs = append(errs, r.v6.icmp.Close())
		if r.v6.udp != nil {
			errs = append(errs, r.v6.udp.Close())
		}
		if r.v6.tcp != nil {
			errs = append(errs, r.v6.tcp.Close())
		}
	}
	r.mu.Unlock()
	r.wg.Wait()
	return errors.Join(errs...)
}

var errClosed = errors.New("probe: prober is closed")

func listen(network, addr string) (*net.IPConn, error) {
	var lc net.ListenConfig
	c, err := lc.ListenPacket(context.Background(), network, addr)
	if err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			return nil, fmt.Errorf("open %s socket: %w", network, ErrPermission)
		}
		return nil, fmt.Errorf("open %s socket: %w", network, err)
	}
	return c.(*net.IPConn), nil
}

func (r *Raw) open4() (*raw4, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errClosed
	}
	if r.v4 != nil {
		return r.v4, nil
	}
	recv, err := listen("ip4:icmp", "0.0.0.0")
	if err != nil {
		return nil, err
	}
	var f ipv4.ICMPFilter
	f.SetAll(true)
	f.Accept(ipv4.ICMPTypeEchoReply)
	f.Accept(ipv4.ICMPTypeDestinationUnreachable)
	f.Accept(ipv4.ICMPTypeTimeExceeded)
	if err := ipv4.NewPacketConn(recv).SetICMPFilter(&f); err != nil {
		r.log.Warn("cannot install ICMP filter; all ICMP will be parsed", "err", err)
	}
	if err := recv.SetReadBuffer(recvBufferBytes); err != nil {
		r.log.Debug("cannot enlarge ICMP receive buffer", "err", err)
	}
	// Protocol 255 (IPPROTO_RAW) implies IP_HDRINCL and never receives.
	send, err := listen("ip4:255", "0.0.0.0")
	if err != nil {
		_ = recv.Close()
		return nil, err
	}
	r.v4 = &raw4{send: send, recv: recv}
	r.wg.Add(1)
	go r.recvLoop(recv, true, r.eng.handle)
	return r.v4, nil
}

// TCP answers to SYN probes: ACK set and SYN or RST set. Everything else a
// raw TCP socket would receive (every inbound segment on the host) is
// dropped in the kernel.
var (
	// IPv4 raw sockets see the packet from the IP header.
	tcpFilter4 = []bpf.Instruction{
		bpf.LoadMemShift{Off: 0},           // X = IP header length
		bpf.LoadIndirect{Off: 13, Size: 1}, // A = TCP flags
		bpf.JumpIf{Cond: bpf.JumpBitsNotSet, Val: tcpACK, SkipTrue: 2},
		bpf.JumpIf{Cond: bpf.JumpBitsNotSet, Val: tcpSYN | tcpRST, SkipTrue: 1},
		bpf.RetConstant{Val: 0xffff},
		bpf.RetConstant{Val: 0},
	}
	// IPv6 raw sockets see the packet from the transport header.
	tcpFilter6 = []bpf.Instruction{
		bpf.LoadAbsolute{Off: 13, Size: 1},
		bpf.JumpIf{Cond: bpf.JumpBitsNotSet, Val: tcpACK, SkipTrue: 2},
		bpf.JumpIf{Cond: bpf.JumpBitsNotSet, Val: tcpSYN | tcpRST, SkipTrue: 1},
		bpf.RetConstant{Val: 0xffff},
		bpf.RetConstant{Val: 0},
	}
)

type bpfSetter interface {
	SetBPF([]bpf.RawInstruction) error
}

func setFilter(c bpfSetter, prog []bpf.Instruction) error {
	raw, err := bpf.Assemble(prog)
	if err != nil {
		return err
	}
	return c.SetBPF(raw)
}

// open4TCP opens the IPv4 TCP listener (the sockets for sending are shared).
func (r *Raw) open4TCP() error {
	if _, err := r.open4(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errClosed
	}
	if r.v4.tcp != nil {
		return nil
	}
	c, err := listen("ip4:tcp", "0.0.0.0")
	if err != nil {
		return err
	}
	if err := setFilter(ipv4.NewPacketConn(c), tcpFilter4); err != nil {
		_ = c.Close()
		return fmt.Errorf("tcp listener filter: %w", err)
	}
	r.v4.tcp = c
	r.wg.Add(1)
	go r.recvLoop(c, true, r.eng.handleTCP)
	return nil
}

func (r *Raw) open6(needUDP bool) (*raw6, error) {
	return r.open6For(needUDP, false)
}

func (r *Raw) open6For(needUDP, needTCP bool) (*raw6, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errClosed
	}
	if r.v6 == nil {
		c, err := listen("ip6:ipv6-icmp", "::")
		if err != nil {
			return nil, err
		}
		pc := ipv6.NewPacketConn(c)
		var f ipv6.ICMPFilter
		f.SetAll(true)
		f.Accept(ipv6.ICMPTypeEchoReply)
		f.Accept(ipv6.ICMPTypeDestinationUnreachable)
		f.Accept(ipv6.ICMPTypeTimeExceeded)
		if err := pc.SetICMPFilter(&f); err != nil {
			r.log.Warn("cannot install ICMPv6 filter; all ICMPv6 will be parsed", "err", err)
		}
		if err := c.SetReadBuffer(recvBufferBytes); err != nil {
			r.log.Debug("cannot enlarge ICMPv6 receive buffer", "err", err)
		}
		r.v6 = &raw6{icmpConn: c, icmp: pc}
		r.wg.Add(1)
		go r.recvLoop(c, false, r.eng.handle)
	}
	if needTCP && r.v6.tcp == nil {
		c, err := listen("ip6:tcp", "::")
		if err != nil {
			return nil, err
		}
		pc := ipv6.NewPacketConn(c)
		if err := setFilter(pc, tcpFilter6); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("tcp listener filter: %w", err)
		}
		r.v6.tcp = pc
		r.wg.Add(1)
		go r.recvLoop(c, false, r.eng.handleTCP)
	}
	if needUDP && r.v6.udp == nil {
		c, err := listen("ip6:udp", "::")
		if err != nil {
			return nil, err
		}
		pc := ipv6.NewPacketConn(c)
		drop, err := bpf.Assemble([]bpf.Instruction{bpf.RetConstant{Val: 0}})
		if err == nil {
			err = pc.SetBPF(drop)
		}
		if err != nil {
			r.log.Warn("cannot install drop filter on raw UDPv6 socket", "err", err)
		}
		r.v6.udp = pc
	}
	return r.v6, nil
}

// maxKernelLag bounds how much earlier than its user-space read a kernel
// receive timestamp may be. Anything outside 0..maxKernelLag means the wall
// clock stepped in between, and the user-space time is used instead.
const maxKernelLag = 100 * time.Millisecond

// recvLoop feeds every message received on c to handle. IPv4 raw sockets
// deliver the IP header (ReadMsgIP, unlike ReadFrom, does not strip it), so
// it is removed here; IPv6 raw sockets never include it.
//
// RTTs are measured on the monotonic clock. When the kernel timestamps
// packets on arrival (SO_TIMESTAMPNS), the receive time is moved back by the
// time the packet waited before this loop read it, so RTTs exclude Go
// scheduling delay. The kernel stamp is wall-clock time; only the lag between
// two wall-clock readings taken microseconds apart is used, never the stamp
// itself.
func (r *Raw) recvLoop(c *net.IPConn, ipv4Header bool, handle func(netip.Addr, []byte, time.Time)) {
	defer r.wg.Done()
	stamped := enableRxTimestamps(c)
	if !stamped {
		r.log.Debug("kernel receive timestamps unavailable; RTTs include scheduling delay")
	}
	buf := make([]byte, 1500) // ICMP errors are at most 576 (v4) / 1280 (v6) bytes
	oob := make([]byte, 128)
	for {
		n, oobn, _, from, err := c.ReadMsgIP(buf, oob)
		at := time.Now()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			r.log.Warn("raw socket read failed", "err", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		addr, ok := netip.AddrFromSlice(from.IP)
		if !ok {
			continue
		}
		if stamped {
			if k, ok := kernelRxTime(oob[:oobn]); ok {
				if lag := at.Sub(k); lag >= 0 && lag <= maxKernelLag {
					at = at.Add(-lag) // keeps at's monotonic reading
					r.kernelStamped.Add(1)
				}
			}
		}
		msg := buf[:n]
		if ipv4Header {
			msg = stripIPv4Header(msg)
		}
		handle(addr.WithZone(from.Zone), msg, at)
	}
}

func enableRxTimestamps(c *net.IPConn) bool {
	rc, err := c.SyscallConn()
	if err != nil {
		return false
	}
	var serr error
	if err := rc.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TIMESTAMPNS, 1)
	}); err != nil {
		return false
	}
	return serr == nil
}

// kernelRxTime extracts an SCM_TIMESTAMPNS timestamp from ancillary data.
func kernelRxTime(oob []byte) (time.Time, bool) {
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return time.Time{}, false
	}
	for _, m := range msgs {
		if m.Header.Level != syscall.SOL_SOCKET || m.Header.Type != syscall.SCM_TIMESTAMPNS {
			continue
		}
		// struct timespec: two longs.
		if strconv.IntSize == 64 && len(m.Data) >= 16 {
			sec := int64(binary.NativeEndian.Uint64(m.Data[0:]))  //nolint:gosec // kernel time_t
			nsec := int64(binary.NativeEndian.Uint64(m.Data[8:])) //nolint:gosec // 0..999999999
			return time.Unix(sec, nsec), true
		}
		if strconv.IntSize == 32 && len(m.Data) >= 8 {
			sec := int32(binary.NativeEndian.Uint32(m.Data[0:]))  //nolint:gosec // kernel time_t
			nsec := int32(binary.NativeEndian.Uint32(m.Data[4:])) //nolint:gosec // 0..999999999
			return time.Unix(int64(sec), int64(nsec)), true
		}
	}
	return time.Time{}, false
}

// stripIPv4Header returns the payload of an IPv4 packet, or b unchanged if
// it does not start with a plausible IPv4 header.
func stripIPv4Header(b []byte) []byte {
	if len(b) < ipv4HeaderLen || b[0]>>4 != 4 {
		return b
	}
	ihl := int(b[0]&0x0f) * 4
	if ihl < ipv4HeaderLen || ihl > len(b) {
		return b
	}
	return b[ihl:]
}

func isNoRoute(err error) bool {
	return errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.EADDRNOTAVAIL)
}

// source asks the kernel which local address it would use towards dst by
// connecting a UDP socket, which sends nothing.
func (r *Raw) source(ctx context.Context, dst netip.Addr) (netip.Addr, error) {
	network := "udp4"
	if dst.Is6() {
		network = "udp6"
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, network, netip.AddrPortFrom(dst, DefaultUDPPort).String())
	if err != nil {
		if isNoRoute(err) {
			return netip.Addr{}, fmt.Errorf("route to %s: %w: %w", dst, ErrNoRoute, err)
		}
		return netip.Addr{}, fmt.Errorf("route to %s: %w", dst, err)
	}
	defer c.Close()
	ua, ok := c.LocalAddr().(*net.UDPAddr)
	if !ok {
		return netip.Addr{}, fmt.Errorf("route to %s: unexpected local address %v", dst, c.LocalAddr())
	}
	return ua.AddrPort().Addr(), nil
}

func (r *Raw) send(p *outPacket) error {
	var err error
	if p.dst.Is4() {
		var s *raw4
		if s, err = r.open4(); err != nil {
			return err
		}
		if p.proto == protoTCP {
			if err = r.open4TCP(); err != nil {
				return err
			}
		}
		pkt := append(buildIPv4(p.src, p.dst, p.proto, p.ttl, p.ipID, len(p.l4)), p.l4...)
		_, err = s.send.WriteToIP(pkt, &net.IPAddr{IP: p.dst.AsSlice()})
	} else {
		var s *raw6
		if s, err = r.open6For(p.proto == protoUDP, p.proto == protoTCP); err != nil {
			return err
		}
		conn := s.icmp
		switch p.proto {
		case protoUDP:
			conn = s.udp
		case protoTCP:
			conn = s.tcp
		}
		cm := &ipv6.ControlMessage{HopLimit: int(p.ttl)}
		if !p.src.IsLinkLocalUnicast() {
			cm.Src = p.src.AsSlice()
		}
		_, err = conn.WriteTo(p.l4, cm, &net.IPAddr{IP: p.dst.AsSlice(), Zone: p.zone})
	}
	if err != nil {
		if isNoRoute(err) {
			return fmt.Errorf("send to %s: %w: %w", p.dst, ErrNoRoute, err)
		}
		return fmt.Errorf("send to %s: %w", p.dst, err)
	}
	return nil
}
