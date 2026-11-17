package probe

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/record"
)

// simNet is an in-memory network for the engine: it receives the real probe
// bytes, and answers with real ICMP bytes built the way routers build them
// (quoting the probe's IP header and its whole L4 packet, as Linux does, or
// only the first 8 L4 bytes), delivered asynchronously through engine.handle
// just like the raw receive loop does.
type simNet struct {
	eng *engine
	src map[bool]netip.Addr // keyed by is4

	mu      sync.Mutex
	paths   map[netip.Addr]*simPath
	sent    []outPacket
	sendErr error
	srcErr  error
	wg      sync.WaitGroup
}

type simPath struct {
	// routers[i] answers TTL i+1 with time exceeded; an invalid address is a
	// silent router. TTLs beyond len(routers) reach the destination.
	routers []netip.Addr
	delay   time.Duration
	// slow maps TTL -> reply delay overriding delay (to produce late replies).
	slow map[int]time.Duration
	// unreachTTL > 0 makes that router answer destination unreachable with
	// unreachCode for its TTL and all higher ones.
	unreachTTL  int
	unreachCode uint8
	// blackhole makes the destination silent.
	blackhole bool
	// dup sends every reply twice.
	dup bool
	// mpls maps TTL -> label stack the router at that TTL reports.
	mpls map[int][]record.MPLSLabel
	// otherIPID makes ICMPv4 errors quote another IP identification than
	// the probe carried, like those for another process's packet.
	otherIPID bool
	// shortQuote makes ICMP errors quote only the first 8 bytes of the
	// probe's L4 packet (RFC 792's minimum).
	shortQuote bool
	// forge, if set, is called for every probe sent; a message it returns
	// is delivered at once (from the returned address), before the genuine
	// reply: a spoofer who guessed the probe's key.
	forge func(p *outPacket) (netip.Addr, []byte)
}

// quoteLen is how much of p's L4 packet the path's ICMP errors quote.
func (p *simPath) quoteLen(o *outPacket) int {
	if p.shortQuote {
		return 8
	}
	return len(o.l4)
}

func newSim() *simNet {
	s := &simNet{
		src: map[bool]netip.Addr{
			true:  netip.MustParseAddr("192.0.2.1"),
			false: netip.MustParseAddr("2001:db8::1"),
		},
		paths: map[netip.Addr]*simPath{},
	}
	s.eng = newEngine(s)
	return s
}

func (s *simNet) setPath(dst netip.Addr, p *simPath) {
	s.mu.Lock()
	s.paths[dst] = p
	s.mu.Unlock()
}

func (s *simNet) source(_ context.Context, dst netip.Addr) (netip.Addr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srcErr != nil {
		return netip.Addr{}, s.srcErr
	}
	return s.src[dst.Is4()], nil
}

func (s *simNet) send(p *outPacket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	cp := *p
	cp.l4 = bytes.Clone(p.l4)
	s.sent = append(s.sent, cp)
	path, ok := s.paths[p.dst]
	if !ok {
		return nil
	}
	ttl := int(p.ttl)
	if path.forge != nil {
		// Synchronously, so it surely arrives first (the engine holds no
		// lock while it sends).
		if from, msg := path.forge(&cp); msg != nil {
			s.deliverer(msg)(from, msg, time.Now())
		}
	}
	from, msg := s.reply(path, &cp, ttl)
	if msg == nil {
		return nil
	}
	deliver := s.deliverer(msg)
	delay := path.delay
	if d, ok := path.slow[ttl]; ok {
		delay = d
	}
	n := 1
	if path.dup {
		n = 2
	}
	for range n {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			time.Sleep(delay)
			deliver(from, msg, time.Now())
		}()
	}
	return nil
}

// deliverer returns the engine's entry point for msg: for a simulated TCP
// segment handleTCP, after stripping the tcpMarker, otherwise handle.
func (s *simNet) deliverer(msg []byte) func(netip.Addr, []byte, time.Time) {
	if isTCPSegment(msg) {
		return func(from netip.Addr, seg []byte, at time.Time) { s.eng.handleTCP(from, seg[1:], at) }
	}
	return s.eng.handle
}

func (s *simNet) reply(path *simPath, p *outPacket, ttl int) (netip.Addr, []byte) {
	v4 := p.dst.Is4()
	switch {
	case path.unreachTTL > 0 && ttl >= path.unreachTTL:
		typ := uint8(icmp6Unreachable)
		if v4 {
			typ = icmp4Unreachable
		}
		return path.routers[path.unreachTTL-1], icmpError(typ, path.unreachCode, p, path.quoteLen(p))
	case ttl <= len(path.routers):
		r := path.routers[ttl-1]
		if !r.IsValid() {
			return r, nil
		}
		typ := uint8(icmp6TimeExceeded)
		if v4 {
			typ = icmp4TimeExceeded
		}
		msg := icmpError(typ, 0, p, path.quoteLen(p))
		if path.otherIPID && v4 {
			binary.BigEndian.PutUint16(msg[8+4:], p.ipID^0x5a5a)
		}
		if l, ok := path.mpls[ttl]; ok {
			msg = withMPLS(msg, !v4, l)
		}
		return r, msg
	case path.blackhole:
		return netip.Addr{}, nil
	case p.proto == protoTCP:
		return p.dst, synAck(p)
	case p.proto == protoUDP:
		if v4 {
			msg := icmpError(icmp4Unreachable, icmp4PortUnreach, p, path.quoteLen(p))
			if path.otherIPID {
				binary.BigEndian.PutUint16(msg[8+4:], p.ipID^0x5a5a)
			}
			return p.dst, msg
		}
		return p.dst, icmpError(icmp6Unreachable, icmp6PortUnreach, p, path.quoteLen(p))
	default: // echo reply: same message with the reply type
		msg := bytes.Clone(p.l4)
		if v4 {
			msg[0] = icmp4EchoReply
		} else {
			msg[0] = icmp6EchoReply
		}
		return p.dst, msg
	}
}

// tcpMarker prefixes simulated TCP segments so send knows to deliver them
// through handleTCP rather than handle.
const tcpMarker = 0xff

func isTCPSegment(msg []byte) bool { return len(msg) > 0 && msg[0] == tcpMarker }

// synAck is the destination's answer to a SYN probe (prefixed by tcpMarker).
func synAck(p *outPacket) []byte {
	seg := make([]byte, 21)
	seg[0] = tcpMarker
	b := seg[1:]
	copy(b[0:2], p.l4[2:4]) // their source port is our destination port
	copy(b[2:4], p.l4[0:2])
	binary.BigEndian.PutUint32(b[4:], 0x5eed)
	binary.BigEndian.PutUint32(b[8:], binary.BigEndian.Uint32(p.l4[4:8])+1)
	b[12] = 5 << 4
	b[13] = tcpSYN | tcpACK
	return seg
}

// icmpError builds an ICMP error quoting p as it left the host: its IP
// header and the first n bytes of its L4 packet.
func icmpError(typ, code uint8, p *outPacket, n int) []byte {
	var quoted []byte
	if p.dst.Is4() {
		quoted = buildIPv4(p.src, p.dst, p.proto, 1, p.ipID, len(p.l4))
	} else {
		h := make([]byte, ipv6HeaderLen)
		h[0] = 0x60
		binary.BigEndian.PutUint16(h[4:], uint16(len(p.l4)))
		h[6] = p.proto
		h[7] = 1
		s, d := p.src.As16(), p.dst.As16()
		copy(h[8:24], s[:])
		copy(h[24:40], d[:])
		quoted = h
	}
	msg := []byte{typ, code, 0, 0, 0, 0, 0, 0}
	msg = append(msg, quoted...)
	return append(msg, p.l4[:n]...)
}

func addrs(ss ...string) []netip.Addr {
	out := make([]netip.Addr, len(ss))
	for i, s := range ss {
		if s != "*" {
			out[i] = netip.MustParseAddr(s)
		}
	}
	return out
}

type wantHop struct {
	status record.Status
	from   string
	code   int
}

func checkResults(t *testing.T, got []HopResult, want []wantHop) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d results, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.TTL != i+1 {
			t.Errorf("result %d has TTL %d", i, g.TTL)
		}
		var from string
		if g.From.IsValid() {
			from = g.From.String()
		}
		if g.Status != w.status || from != w.from || g.Code != w.code {
			t.Errorf("ttl %d: got %s from %q code %d, want %s from %q code %d",
				i+1, g.Status, from, g.Code, w.status, w.from, w.code)
		}
		if g.Status.Responded() && g.RTT <= 0 {
			t.Errorf("ttl %d: responded without RTT", i+1)
		}
	}
}

func TestEngineTrace(t *testing.T) {
	type tc struct {
		name    string
		target  string
		proto   Protocol
		routers []string
	}
	cases := []tc{
		{"icmp4", "198.51.100.7", ICMP, []string{"10.0.0.1", "10.0.0.2", "*", "10.0.0.4"}},
		{"udp4", "198.51.100.7", UDP, []string{"10.0.0.1", "10.0.0.2", "*", "10.0.0.4"}},
		{"icmp6", "2001:db8:1::7", ICMP, []string{"2001:db8:a::1", "2001:db8:a::2", "*", "2001:db8:a::4"}},
		{"udp6", "2001:db8:1::7", UDP, []string{"2001:db8:a::1", "2001:db8:a::2", "*", "2001:db8:a::4"}},
		{"tcp4", "198.51.100.7", TCP, []string{"10.0.0.1", "10.0.0.2", "*", "10.0.0.4"}},
		{"tcp6", "2001:db8:1::7", TCP, []string{"2001:db8:a::1", "2001:db8:a::2", "*", "2001:db8:a::4"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newSim()
			dst := netip.MustParseAddr(c.target)
			s.setPath(dst, &simPath{routers: addrs(c.routers...), delay: time.Millisecond})
			res, err := s.eng.trace(t.Context(), TraceRequest{
				Target: dst, Protocol: c.proto, Port: 33434, FlowID: 4321, Paris: true,
				MaxTTL: 7, Timeout: 200 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			want := []wantHop{
				{record.StatusTTLExpired, c.routers[0], 0},
				{record.StatusTTLExpired, c.routers[1], 0},
				{record.StatusNoReply, "", 0},
				{record.StatusTTLExpired, c.routers[3], 0},
				{record.StatusReply, c.target, 0},
				{record.StatusReply, c.target, 0},
				{record.StatusReply, c.target, 0},
			}
			checkResults(t, res, want)
			s.wg.Wait()
			if n := s.eng.pendingLen(); n != 0 {
				t.Errorf("%d probes left pending", n)
			}
		})
	}
}

// flowFields returns the header bytes ECMP routers hash on (beyond addresses
// and protocol): ICMP type, code, checksum and identifier; UDP ports.
func flowFields(p outPacket) string {
	if p.proto == protoUDP || p.proto == protoTCP {
		return fmt.Sprintf("%x", p.l4[0:4])
	}
	return fmt.Sprintf("%x", p.l4[0:6])
}

// probeID returns the bytes that carry the probe id: ICMP sequence, UDP
// checksum, or the top of the TCP sequence number.
func probeID(p outPacket) uint16 {
	if p.proto == protoTCP {
		return binary.BigEndian.Uint16(p.l4[4:6])
	}
	return binary.BigEndian.Uint16(p.l4[6:8])
}

func TestParisKeepsFlowConstant(t *testing.T) {
	for _, proto := range []Protocol{ICMP, UDP, TCP} {
		for _, target := range []string{"198.51.100.7", "2001:db8:1::7"} {
			for _, paris := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/paris=%v", proto, target, paris), func(t *testing.T) {
					s := newSim()
					dst := netip.MustParseAddr(target)
					s.setPath(dst, &simPath{routers: addrs("*", "*", "*"), blackhole: true})
					for range 2 { // two cycles: flow must also be stable across cycles
						_, err := s.eng.trace(t.Context(), TraceRequest{
							Target: dst, Protocol: proto, Port: 33434, FlowID: 4321, Paris: paris,
							MaxTTL: 8, Timeout: 5 * time.Millisecond,
						})
						if err != nil {
							t.Fatal(err)
						}
					}
					flows := map[string]bool{}
					// Ids are unique among probes with the same key fields
					// (ICMP identifier, or ports: classic mode varies a port).
					ids := map[string]bool{}
					for _, p := range s.sent {
						flows[flowFields(p)] = true
						keyFields := p.l4[0:4]
						if proto == ICMP {
							keyFields = p.l4[4:6]
						}
						ids[fmt.Sprintf("%x/%d", keyFields, probeID(p))] = true
					}
					if paris && len(flows) != 1 {
						t.Errorf("paris probes span %d flows: %v", len(flows), flows)
					}
					if !paris && len(flows) < 8 {
						t.Errorf("classic probes span only %d flows", len(flows))
					}
					if len(ids) != len(s.sent) {
						t.Errorf("%d probes but %d distinct probe ids in their flows", len(s.sent), len(ids))
					}
				})
			}
		}
	}
}

func TestEngineLateReply(t *testing.T) {
	s := newSim()
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{
		routers: addrs("10.0.0.1", "10.0.0.2", "10.0.0.3"),
		delay:   time.Millisecond,
		slow:    map[int]time.Duration{2: 150 * time.Millisecond},
	})
	start := time.Now()
	res, err := s.eng.trace(t.Context(), TraceRequest{
		Target: dst, Protocol: ICMP, FlowID: 1, Paris: true, MaxTTL: 4, Timeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 140*time.Millisecond {
		t.Errorf("trace took %v, should end at timeout+grace", d)
	}
	checkResults(t, res, []wantHop{
		{record.StatusTTLExpired, "10.0.0.1", 0},
		{record.StatusNoReply, "", 0},
		{record.StatusTTLExpired, "10.0.0.3", 0},
		{record.StatusReply, "198.51.100.7", 0},
	})
	s.wg.Wait()
	if got := s.eng.stats().LateReplies; got != 1 {
		t.Errorf("late replies = %d, want 1", got)
	}
}

func TestEngineEndsEarlyWhenAllAnswered(t *testing.T) {
	s := newSim()
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{routers: addrs("10.0.0.1"), delay: time.Millisecond})
	start := time.Now()
	_, err := s.eng.trace(t.Context(), TraceRequest{
		Target: dst, Protocol: ICMP, FlowID: 1, Paris: true, MaxTTL: 30, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("trace took %v although every probe was answered", d)
	}
}

func TestEngineUnreachable(t *testing.T) {
	s := newSim()
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{routers: addrs("10.0.0.1", "10.0.0.2"), unreachTTL: 2, unreachCode: 1})
	res, err := s.eng.trace(t.Context(), TraceRequest{
		Target: dst, Protocol: UDP, Port: 33434, FlowID: 1, Paris: true, MaxTTL: 3, Timeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkResults(t, res, []wantHop{
		{record.StatusTTLExpired, "10.0.0.1", 0},
		{record.StatusUnreachable, "10.0.0.2", 1},
		{record.StatusUnreachable, "10.0.0.2", 1},
	})
}

// A port unreachable from a router (not the target) is not a destination
// reply.
func TestEnginePortUnreachableFromRouter(t *testing.T) {
	s := newSim()
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{routers: addrs("10.0.0.1", "10.0.0.2"), unreachTTL: 2, unreachCode: icmp4PortUnreach})
	res, err := s.eng.trace(t.Context(), TraceRequest{
		Target: dst, Protocol: UDP, Port: 33434, FlowID: 1, Paris: true, MaxTTL: 2, Timeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkResults(t, res, []wantHop{
		{record.StatusTTLExpired, "10.0.0.1", 0},
		{record.StatusUnreachable, "10.0.0.2", icmp4PortUnreach},
	})
}

func TestEngineDuplicatesAndForeignTraffic(t *testing.T) {
	s := newSim()
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{routers: addrs("10.0.0.1"), dup: true})
	// Someone else's ping reply and garbage arrive while we trace.
	stop := make(chan struct{})
	var noise sync.WaitGroup
	noise.Go(func() {
		other := unhex(t, "0000 0000 1111 2222 00")
		for {
			select {
			case <-stop:
				return
			default:
				s.eng.handle(dst, other, time.Now())
				s.eng.handle(dst, []byte{0x0b}, time.Now())
				runtime.Gosched()
			}
		}
	})
	res, err := s.eng.trace(t.Context(), TraceRequest{
		Target: dst, Protocol: ICMP, FlowID: 1, Paris: true, MaxTTL: 3, Timeout: 100 * time.Millisecond,
	})
	close(stop)
	noise.Wait()
	if err != nil {
		t.Fatal(err)
	}
	checkResults(t, res, []wantHop{
		{record.StatusTTLExpired, "10.0.0.1", 0},
		{record.StatusReply, "198.51.100.7", 0},
		{record.StatusReply, "198.51.100.7", 0},
	})
	s.wg.Wait()
	if got := s.eng.stats().LateReplies; got != 0 {
		t.Errorf("late replies = %d; duplicates of answered probes and foreign traffic must not count", got)
	}
}

func TestEngineConcurrentTargets(t *testing.T) {
	s := newSim()
	const n = 64
	targets := make([]netip.Addr, n)
	for i := range targets {
		targets[i] = netip.AddrFrom4([4]byte{198, 51, 100, byte(i + 1)})
		routers := []netip.Addr{
			netip.MustParseAddr("10.0.0.1"),
			netip.AddrFrom4([4]byte{10, 1, byte(i), 2}),
		}
		s.setPath(targets[i], &simPath{routers: routers, delay: time.Duration(i%5) * time.Millisecond})
	}
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i, dst := range targets {
		proto := ICMP
		if i%2 == 1 {
			proto = UDP
		}
		wg.Go(func() {
			res, err := s.eng.trace(t.Context(), TraceRequest{
				Target: dst, Protocol: proto, Port: 33434, FlowID: DefaultFlowID, Paris: true,
				MaxTTL: 4, Timeout: 500 * time.Millisecond,
			})
			if err != nil {
				errs <- err
				return
			}
			want := netip.AddrFrom4([4]byte{10, 1, byte(i), 2})
			if res[1].From != want || res[2].Status != record.StatusReply || res[2].From != dst {
				errs <- fmt.Errorf("target %s got %+v", dst, res)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if n := s.eng.pendingLen(); n != 0 {
		t.Errorf("%d probes left pending", n)
	}
}

func TestEngineCancel(t *testing.T) {
	s := newSim()
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{routers: addrs("*"), blackhole: true})
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(20*time.Millisecond, cancel)
	start := time.Now()
	_, err := s.eng.trace(ctx, TraceRequest{Target: dst, Protocol: ICMP, FlowID: 1, Paris: true, MaxTTL: 5, Timeout: 5 * time.Second})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("cancel took %v", d)
	}
	if n := s.eng.pendingLen(); n != 0 {
		t.Errorf("%d probes left pending", n)
	}
}

func TestEngineNoRoute(t *testing.T) {
	dst := netip.MustParseAddr("198.51.100.7")
	req := TraceRequest{Target: dst, Protocol: ICMP, FlowID: 1, Paris: true, MaxTTL: 3, Timeout: time.Second}

	s := newSim()
	s.srcErr = fmt.Errorf("route lookup: %w", ErrNoRoute)
	if _, err := s.eng.trace(t.Context(), req); !errors.Is(err, ErrNoRoute) {
		t.Errorf("source failure: err = %v, want ErrNoRoute", err)
	}

	s = newSim()
	s.sendErr = fmt.Errorf("sendto: %w: %w", ErrNoRoute, syscall.ENETUNREACH)
	if _, err := s.eng.trace(t.Context(), req); !errors.Is(err, ErrNoRoute) {
		t.Errorf("send failure: err = %v, want ErrNoRoute", err)
	}
	if n := s.eng.pendingLen(); n != 0 {
		t.Errorf("%d probes left pending", n)
	}

	s = newSim()
	s.sendErr = syscall.ENOBUFS
	if _, err := s.eng.trace(t.Context(), req); err == nil || errors.Is(err, ErrNoRoute) {
		t.Errorf("ENOBUFS: err = %v, want a non-ErrNoRoute error", err)
	}
}

func TestEngineInvalidRequest(t *testing.T) {
	s := newSim()
	bad := []TraceRequest{
		{Protocol: ICMP, MaxTTL: 3, Timeout: time.Second},
		{Target: dst4, Protocol: "tcp", MaxTTL: 3, Timeout: time.Second},
		{Target: dst4, Protocol: ICMP, MaxTTL: 0, Timeout: time.Second},
		{Target: dst4, Protocol: ICMP, MaxTTL: 256, Timeout: time.Second},
		{Target: dst4, Protocol: ICMP, MaxTTL: 3},
		{Target: dst4, Protocol: UDP, FlowID: 1, MaxTTL: 3, Timeout: time.Second},
		{Target: dst4, Protocol: UDP, Port: 1, MaxTTL: 3, Timeout: time.Second},
	}
	for _, req := range bad {
		if _, err := s.eng.trace(t.Context(), req); err == nil {
			t.Errorf("request %+v accepted", req)
		}
	}
}

// TestEngineSoak runs many short cycles and checks that nothing accumulates:
// no pending probes, no goroutines, and tombstones are pruned.
func TestEngineSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test")
	}
	s := newSim()
	s.eng.tombstoneTTL = 50 * time.Millisecond
	s.eng.grace = time.Millisecond
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{routers: addrs("10.0.0.1", "*", "10.0.0.3"), slow: map[int]time.Duration{3: 20 * time.Millisecond}})
	before := runtime.NumGoroutine()
	for range 300 {
		_, err := s.eng.trace(t.Context(), TraceRequest{
			Target: dst, Protocol: ICMP, FlowID: 1, Paris: true, MaxTTL: 6, Timeout: 5 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	s.wg.Wait()
	if n := s.eng.pendingLen(); n != 0 {
		t.Errorf("%d probes left pending", n)
	}
	s.eng.mu.Lock()
	tombs := len(s.eng.tombs)
	s.eng.mu.Unlock()
	// Only tombstones from roughly the last tombstoneTTL may remain.
	if tombs > 6*60 {
		t.Errorf("%d tombstones retained; pruning is not working", tombs)
	}
	if late := s.eng.stats().LateReplies; late == 0 {
		t.Error("expected late replies from the slow hop")
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Errorf("goroutines grew from %d to %d", before, after)
	}
}

func TestEngineMinTTL(t *testing.T) {
	s := newSim()
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{routers: addrs("10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4")})
	res, err := s.eng.trace(t.Context(), TraceRequest{Target: dst, Protocol: UDP, Port: 33434, FlowID: 1,
		MinTTL: 3, MaxTTL: 6, Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 4 || res[0].TTL != 3 || res[3].TTL != 6 {
		t.Fatalf("results %+v", res)
	}
	if res[0].From.String() != "10.0.0.3" || res[1].From.String() != "10.0.0.4" || res[2].Status != record.StatusReply {
		t.Errorf("results %+v", res)
	}
	// Classic mode keeps each TTL's destination port independent of MinTTL.
	if got := binary.BigEndian.Uint16(s.sent[0].l4[2:4]); got != 33434+2 {
		t.Errorf("classic dport for ttl 3 = %d", got)
	}
	if _, err := s.eng.trace(t.Context(), TraceRequest{Target: dst, Protocol: ICMP, MinTTL: 7, MaxTTL: 6, Timeout: time.Second}); err == nil {
		t.Error("MinTTL > MaxTTL accepted")
	}
}

// A TCP probe rejected by the destination's own firewall with ICMP port
// unreachable still reached the destination.
func TestEngineTCPRejectedByDestination(t *testing.T) {
	s := newSim()
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{routers: addrs("10.0.0.1", "198.51.100.7"), unreachTTL: 2, unreachCode: icmp4PortUnreach})
	res, err := s.eng.trace(t.Context(), TraceRequest{Target: dst, Protocol: TCP, Port: 443, FlowID: 1, Paris: true,
		MaxTTL: 2, Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	checkResults(t, res, []wantHop{
		{record.StatusTTLExpired, "10.0.0.1", 0},
		{record.StatusReply, "198.51.100.7", 0},
	})
}

// withMPLS turns an ICMP error into an RFC 4884 multi-part message: the
// quoted datagram padded to 128 bytes, its length in the header, and an
// extension carrying labels.
func withMPLS(msg []byte, v6 bool, labels []record.MPLSLabel) []byte {
	quoted := make([]byte, 128)
	copy(quoted, msg[8:])
	out := append(append([]byte{}, msg[:8]...), quoted...)
	if v6 {
		out[4] = 128 / 8
	} else {
		out[5] = 128 / 4
	}
	obj := []byte{0, byte(4 + 4*len(labels)), extClassMPLS, extCTypeLabelStack}
	for _, l := range labels {
		v := l.Label<<12 | uint32(l.TC)<<9 | uint32(l.TTL)
		if l.S {
			v |= 0x100
		}
		obj = binary.BigEndian.AppendUint32(obj, v)
	}
	ext := append([]byte{extVersion << 4, 0, 0, 0}, obj...)
	binary.BigEndian.PutUint16(ext[2:], checksum(0, ext))
	return append(out, ext...)
}

func TestEngineMPLSLabels(t *testing.T) {
	labels := []record.MPLSLabel{{Label: 24005, TTL: 1}, {Label: 16, S: true, TTL: 1}}
	for _, target := range []string{"198.51.100.7", "2001:db8:1::7"} {
		t.Run(target, func(t *testing.T) {
			s := newSim()
			dst := netip.MustParseAddr(target)
			router := netip.MustParseAddr("10.0.0.2")
			if dst.Is6() {
				router = netip.MustParseAddr("2001:db8:a::2")
			}
			s.setPath(dst, &simPath{routers: []netip.Addr{router}, mpls: map[int][]record.MPLSLabel{1: labels}})
			res, err := s.eng.trace(t.Context(), TraceRequest{Target: dst, Protocol: ICMP, FlowID: 1, Paris: true,
				MaxTTL: 2, Timeout: 200 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			if res[0].Status != record.StatusTTLExpired || !reflect.DeepEqual(res[0].MPLS, labels) {
				t.Errorf("hop 1: %+v", res[0])
			}
			if res[1].MPLS != nil {
				t.Errorf("destination reply carries labels: %+v", res[1])
			}
		})
	}
}

// An ICMPv4 error must quote the IP identification the probe was sent with.
// An unprivileged process can send packets with the probe's ports, UDP
// checksum or echo sequence (and so draw errors that quote its key), but
// cannot choose the IP ID.
func TestEngineQuotedIPIDMustMatch(t *testing.T) {
	for _, proto := range []Protocol{ICMP, UDP, TCP} {
		t.Run(string(proto), func(t *testing.T) {
			s := newSim()
			dst := netip.MustParseAddr("198.51.100.7")
			s.setPath(dst, &simPath{routers: addrs("10.0.0.1", "10.0.0.2"), otherIPID: true})
			res, err := s.eng.trace(t.Context(), TraceRequest{Target: dst, Protocol: proto, Port: 33434, FlowID: 1,
				Paris: true, MaxTTL: 3, Timeout: 50 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			want := []wantHop{{record.StatusNoReply, "", 0}, {record.StatusNoReply, "", 0}, {record.StatusReply, "198.51.100.7", 0}}
			if proto == UDP { // the destination's answer is an ICMP error too
				want[2] = wantHop{record.StatusNoReply, "", 0}
			}
			checkResults(t, res, want)
			s.wg.Wait()
			wantMismatched := uint64(2)
			if proto == UDP {
				wantMismatched = 3
			}
			if got := s.eng.stats().MismatchedReplies; got != wantMismatched {
				t.Errorf("mismatched replies = %d, want %d", got, wantMismatched)
			}
		})
	}
}

// The IP ID is drawn on its own, so it adds 16 bits a spoofer must guess
// besides the probe id (until round three it was the probe id).
func TestEngineIPIDIndependentOfProbeID(t *testing.T) {
	s := newSim()
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{routers: addrs("*", "*"), blackhole: true})
	for range 10 {
		if _, err := s.eng.trace(t.Context(), TraceRequest{Target: dst, Protocol: UDP, Port: 33434, FlowID: 1,
			Paris: true, MaxTTL: 30, Timeout: time.Millisecond}); err != nil {
			t.Fatal(err)
		}
	}
	same, ipIDs := 0, map[uint16]bool{}
	for _, p := range s.sent {
		if p.ipID == 0 {
			t.Fatal("IP ID 0 (the kernel would choose one)")
		}
		if p.ipID == probeID(p) {
			same++
		}
		ipIDs[p.ipID] = true
	}
	// 300 independent draws: ~0.005 equal by chance, ~0.7 repeated IP IDs.
	if same > 1 || len(ipIDs) < len(s.sent)-5 {
		t.Errorf("%d of %d probes carry their id as IP ID; %d distinct IP IDs", same, len(s.sent), len(ipIDs))
	}
}

// forgery is a reply an off-path spoofer sends for a probe, having guessed
// its key (the 16-bit probe id), built from the probe as sent.
type forgery struct {
	name   string
	target string
	proto  Protocol
	// forge returns the forged message, or nil if it would by chance be
	// the genuine reply.
	forge func(p *outPacket) []byte
	// fromTarget forges as the destination (an echo reply, a SYN-ACK);
	// otherwise as a router that is not on the path.
	fromTarget bool
}

// quoting returns an ICMP time exceeded quoting p after change modified a
// copy of it, or nil if change reports the copy would be p itself.
func quoting(p *outPacket, change func(q *outPacket) bool) []byte {
	q := *p
	q.l4 = bytes.Clone(p.l4)
	if !change(&q) {
		return nil
	}
	typ := uint8(icmp4TimeExceeded)
	if p.dst.Is6() {
		typ = icmp6TimeExceeded
	}
	return icmpError(typ, 0, &q, len(q.l4))
}

// otherNonce replaces the nonce in an ICMP or UDP probe.
func otherNonce(q *outPacket) bool {
	binary.BigEndian.PutUint32(q.l4[10:], ^binary.BigEndian.Uint32(q.l4[10:]))
	return true
}

func forgeries() []forgery {
	ipIDIsProbeID := func(p *outPacket) []byte { // the round-two layout, which the review's spoofer used
		return quoting(p, func(q *outPacket) bool { q.ipID = probeID(*q); return q.ipID != p.ipID })
	}
	nonce := func(p *outPacket) []byte { return quoting(p, otherNonce) }
	shortQuote := func(p *outPacket) []byte {
		msg := quoting(p, func(*outPacket) bool { return true })
		return msg[:len(msg)-len(p.l4)+8]
	}
	otherSeq := func(p *outPacket) []byte {
		return quoting(p, func(q *outPacket) bool {
			binary.BigEndian.PutUint16(q.l4[6:], ^binary.BigEndian.Uint16(q.l4[6:])) // the low 16 bits
			return true
		})
	}
	// An echo reply carrying the data probes had before round three:
	// ^seq, then the magic, both known to anyone.
	oldEchoData := func(p *outPacket) []byte {
		msg := append(bytes.Clone(p.l4[:8]), 0, 0)
		binary.BigEndian.PutUint16(msg[8:], ^binary.BigEndian.Uint16(p.l4[6:]))
		msg = append(msg, payloadMagic...)
		msg[0] = icmp4EchoReply
		if p.dst.Is6() {
			msg[0] = icmp6EchoReply
		}
		return msg
	}
	// A SYN-ACK acknowledging only the probe id, as round two checked.
	ackOfID := func(p *outPacket) []byte {
		seg := synAck(p)
		ack := binary.BigEndian.Uint32(p.l4[4:])&0xffff0000 + 1
		if ack == binary.BigEndian.Uint32(p.l4[4:])+1 {
			return nil
		}
		binary.BigEndian.PutUint32(seg[1+8:], ack)
		return seg
	}
	return []forgery{
		{"icmp4 time exceeded with the probe id as IP ID", "198.51.100.7", ICMP, ipIDIsProbeID, false},
		{"udp4 time exceeded with the probe id as IP ID", "198.51.100.7", UDP, ipIDIsProbeID, false},
		{"tcp4 time exceeded with the probe id as IP ID", "198.51.100.7", TCP, ipIDIsProbeID, false},
		{"icmp4 time exceeded quoting another nonce", "198.51.100.7", ICMP, nonce, false},
		{"udp4 time exceeded quoting another nonce", "198.51.100.7", UDP, nonce, false},
		{"icmp6 time exceeded quoting another nonce", "2001:db8:1::7", ICMP, nonce, false},
		{"udp6 time exceeded quoting another nonce", "2001:db8:1::7", UDP, nonce, false},
		{"icmp6 time exceeded quoting 8 bytes", "2001:db8:1::7", ICMP, shortQuote, false},
		{"udp6 time exceeded quoting 8 bytes", "2001:db8:1::7", UDP, shortQuote, false},
		{"tcp4 time exceeded quoting another sequence number", "198.51.100.7", TCP, otherSeq, false},
		{"tcp6 time exceeded quoting another sequence number", "2001:db8:1::7", TCP, otherSeq, false},
		{"icmp4 echo reply with the old data", "198.51.100.7", ICMP, oldEchoData, true},
		{"icmp6 echo reply with the old data", "2001:db8:1::7", ICMP, oldEchoData, true},
		{"tcp4 syn-ack acknowledging the probe id only", "198.51.100.7", TCP, ackOfID, true},
		{"tcp6 syn-ack acknowledging the probe id only", "2001:db8:1::7", TCP, ackOfID, true},
	}
}

// A forged reply that carries a probe's key (a spoofer guessed its id) but
// not what else the probe was sent with is refused and counted, and the
// probe stays pending: the genuine reply, arriving later, is the one
// recorded.
func TestEngineForgedRepliesRefused(t *testing.T) {
	const delay = 20 * time.Millisecond
	for _, f := range forgeries() {
		t.Run(f.name, func(t *testing.T) {
			s := newSim()
			dst := netip.MustParseAddr(f.target)
			routers := addrs("10.0.0.1", "10.0.0.2")
			fake := netip.MustParseAddr("203.0.113.66")
			if dst.Is6() {
				routers, fake = addrs("2001:db8:a::1", "2001:db8:a::2"), netip.MustParseAddr("2001:db8:66::66")
			}
			if f.fromTarget {
				fake = dst
			}
			var forged atomic.Int64
			s.setPath(dst, &simPath{routers: routers, delay: delay, forge: func(p *outPacket) (netip.Addr, []byte) {
				msg := f.forge(p)
				if msg != nil {
					forged.Add(1)
				}
				return fake, msg
			}})
			res, err := s.eng.trace(t.Context(), TraceRequest{Target: dst, Protocol: f.proto, Port: 33434, FlowID: 4321,
				Paris: true, MaxTTL: 4, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			checkResults(t, res, []wantHop{
				{record.StatusTTLExpired, routers[0].String(), 0},
				{record.StatusTTLExpired, routers[1].String(), 0},
				{record.StatusReply, f.target, 0},
				{record.StatusReply, f.target, 0},
			})
			for _, h := range res {
				if h.RTT < delay {
					t.Errorf("ttl %d answered after %v: the forgery was taken", h.TTL, h.RTT)
				}
			}
			s.wg.Wait()
			if got, want := s.eng.stats().MismatchedReplies, uint64(forged.Load()); got != want || want < 3 {
				t.Errorf("mismatched replies = %d, want %d (forged)", got, want)
			}
		})
	}
}

// Routers may quote only the first 8 L4 bytes (RFC 792). On IPv4 that
// still answers (the IP ID matched); on IPv6, where RFC 4443 requires the
// whole probe, it does not: a spoofer would otherwise skip the nonce by
// quoting less.
func TestEngineShortQuotes(t *testing.T) {
	for _, c := range []struct {
		target  string
		routers []string
		answers bool
	}{
		{"198.51.100.7", []string{"10.0.0.1", "10.0.0.2"}, true},
		{"2001:db8:1::7", []string{"2001:db8:a::1", "2001:db8:a::2"}, false},
	} {
		for _, proto := range []Protocol{ICMP, UDP, TCP} {
			t.Run(fmt.Sprintf("%s/%s", c.target, proto), func(t *testing.T) {
				s := newSim()
				dst := netip.MustParseAddr(c.target)
				s.setPath(dst, &simPath{routers: addrs(c.routers...), shortQuote: true})
				res, err := s.eng.trace(t.Context(), TraceRequest{Target: dst, Protocol: proto, Port: 33434, FlowID: 1,
					Paris: true, MaxTTL: 3, Timeout: 100 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				want := []wantHop{{record.StatusTTLExpired, c.routers[0], 0}, {record.StatusTTLExpired, c.routers[1], 0},
					{record.StatusReply, c.target, 0}}
				answers := c.answers || proto == TCP // TCP's sequence number is in the first 8 bytes
				if !answers {
					want[0], want[1] = wantHop{record.StatusNoReply, "", 0}, wantHop{record.StatusNoReply, "", 0}
					if proto == UDP { // the destination's port unreachable quotes 8 bytes too
						want[2] = wantHop{record.StatusNoReply, "", 0}
					}
				}
				checkResults(t, res, want)
			})
		}
	}
}

// Probe ids are random, so knowing the ids of some probes of a cycle (from
// the destination's answers) does not reveal the others.
func TestEngineRandomIDs(t *testing.T) {
	s := newSim()
	dst := netip.MustParseAddr("198.51.100.7")
	s.setPath(dst, &simPath{routers: addrs("*", "*"), blackhole: true})
	var ids []uint16
	for range 40 {
		if _, err := s.eng.trace(t.Context(), TraceRequest{Target: dst, Protocol: UDP, Port: 33434, FlowID: 1,
			Paris: true, MaxTTL: 30, Timeout: time.Millisecond}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[uint16]bool{}
	consecutive := 0
	for i, p := range s.sent {
		id := probeID(p)
		ids = append(ids, id)
		if id == 0 || id == 0xffff {
			t.Fatalf("probe id %d", id)
		}
		if i > 0 && id == ids[i-1]+1 {
			consecutive++
		}
		seen[id] = true
	}
	// Every earlier probe is still tombstoned (nothing answers), so no id
	// may repeat; 1200 random ids hold ~0.02 consecutive pairs.
	if len(seen) != len(ids) {
		t.Errorf("%d probes but %d distinct ids although all were tombstoned", len(ids), len(seen))
	}
	if consecutive > 3 {
		t.Errorf("%d of %d ids follow their predecessor", consecutive, len(ids))
	}
}

func TestAllocIDAvoidsInFlightAndTombstonedKeys(t *testing.T) {
	seed := [32]byte{1, 2, 3}
	draws := rand.New(rand.NewChaCha8(seed))
	next := func() uint16 { return uint16(1 + draws.IntN(0xfffe)) }
	first, second, third := next(), next(), next()

	e := newEngine(nil)
	e.ids = rand.New(rand.NewChaCha8(seed))
	keyOf := func(id uint16) probeKey { return probeKey{dst: dst4, proto: protoUDP, a: 1, b: 2, c: id} }
	e.pending[keyOf(first)] = &pendingProbe{}
	e.tombs[keyOf(second)] = time.Now().Add(time.Minute)
	id, k, err := e.allocIDLocked(keyOf)
	if err != nil || id != third || k != keyOf(third) {
		t.Fatalf("alloc = %d %+v %v, want %d (draws %d and %d are taken)", id, k, err, third, first, second)
	}

	// With every id but one in flight, the free one is found.
	e = newEngine(nil)
	for id := uint16(1); id < 0xffff; id++ {
		e.pending[keyOf(id)] = &pendingProbe{}
	}
	delete(e.pending, keyOf(4242))
	if id, _, err := e.allocIDLocked(keyOf); err != nil || id != 4242 {
		t.Errorf("alloc = %d %v, want 4242", id, err)
	}
	// With every id in flight, allocation fails rather than steal one.
	e.pending[keyOf(4242)] = &pendingProbe{}
	if _, _, err := e.allocIDLocked(keyOf); err == nil {
		t.Error("allocated an in-flight id")
	}
	// A tombstoned id is reused only when nothing else is free.
	e = newEngine(nil)
	for id := uint16(1); id < 0xffff; id++ {
		e.tombs[keyOf(id)] = time.Now().Add(time.Minute)
	}
	e.pending[keyOf(7)] = &pendingProbe{}
	if id, _, err := e.allocIDLocked(keyOf); err != nil || id == 7 {
		t.Errorf("alloc = %d %v, want a tombstoned id other than 7", id, err)
	}
}

// A reply the kernel received before the probe was sent (its timestamp is
// moved back to the kernel's) cannot answer it.
func TestEngineReplyBeforeSendIgnored(t *testing.T) {
	e := newEngine(nil)
	tr := &traceState{req: TraceRequest{Protocol: ICMP, Timeout: time.Second}, first: 1,
		target: dst4, results: NoRepliesRange(1, 2), remaining: 2, done: make(chan struct{})}
	sent := time.Now()
	k := probeKey{dst: dst4, proto: protoICMP, a: 33434, c: 7}
	reply := buildEcho(src4, dst4, 33434, 7, 0x01020304, true)
	reply[0] = icmp4EchoReply
	e.pending[k] = &pendingProbe{tr: tr, ttl: 2, data: reply[8:], sent: sent}
	e.resolve(dst4, icmpReply{kind: kindEchoReply, key: k}, sent.Add(-50*time.Millisecond), reply)
	if got := tr.results[1]; got.Status != record.StatusNoReply || e.pending[k] == nil {
		t.Fatalf("early reply accepted: %+v", got)
	}
	e.resolve(dst4, icmpReply{kind: kindEchoReply, key: k}, sent.Add(3*time.Millisecond), reply)
	if got := tr.results[1]; got.Status != record.StatusReply || got.RTT != 3*time.Millisecond {
		t.Errorf("the real reply was not taken: %+v", got)
	}
}

// An echo reply must carry the probe's data: another process can send echo
// requests with the probe's identifier and sequence number (a ping with a
// chosen identifier), but its replies echo its own data.
func TestEngineEchoReplyMustCarryProbeData(t *testing.T) {
	for _, target := range []string{"198.51.100.7", "2001:db8:1::7"} {
		t.Run(target, func(t *testing.T) {
			e := newEngine(nil)
			dst := netip.MustParseAddr(target)
			tr := &traceState{req: TraceRequest{Protocol: ICMP, Timeout: time.Second}, first: 1,
				target: dst, results: NoRepliesRange(1, 1), remaining: 1, done: make(chan struct{})}
			src, proto, typ := src4, uint8(protoICMP), uint8(icmp4EchoReply)
			if dst.Is6() {
				src, proto, typ = src6, protoICMPv6, icmp6EchoReply
			}
			probe := buildEcho(src, dst, 33434, 9, 0x01020304, true)
			k := probeKey{dst: dst, proto: proto, a: 33434, c: 9}
			e.pending[k] = &pendingProbe{tr: tr, ttl: 1, data: probe[8:], sent: time.Now()}

			other := []byte{typ, 0, 0, 0, 0x82, 0x9a, 0, 9} // same id and seq, a ping's data
			other = append(other, "0123456789abcdef"...)
			e.handle(dst, other, time.Now())
			short := append([]byte{typ, 0, 0, 0, 0x82, 0x9a, 0, 9}, probe[8:12]...) // truncated data
			e.handle(dst, short, time.Now())
			if tr.results[0].Status != record.StatusNoReply || e.stats().MismatchedReplies != 2 {
				t.Fatalf("foreign echo reply taken: %+v, mismatched %d", tr.results[0], e.stats().MismatchedReplies)
			}
			ours := append([]byte{typ, 0, 0, 0}, probe[4:]...)
			e.handle(dst, ours, time.Now())
			if tr.results[0].Status != record.StatusReply {
				t.Errorf("the probe's own echo reply was not taken: %+v", tr.results[0])
			}
		})
	}
}
