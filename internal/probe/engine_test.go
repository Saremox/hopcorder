package probe

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/record"
)

// simNet is an in-memory network for the engine: it receives the real probe
// bytes, and answers with real ICMP bytes built the way routers build them
// (quoting the probe's IP header and first 8 L4 bytes), delivered
// asynchronously through engine.handle just like the raw receive loop does.
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
	from, msg := s.reply(path, &cp, ttl)
	if msg == nil {
		return nil
	}
	deliver := s.eng.handle
	if isTCPSegment(msg) {
		deliver, msg = s.eng.handleTCP, msg[1:]
	}
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

func (s *simNet) reply(path *simPath, p *outPacket, ttl int) (netip.Addr, []byte) {
	v4 := p.dst.Is4()
	switch {
	case path.unreachTTL > 0 && ttl >= path.unreachTTL:
		typ := uint8(icmp6Unreachable)
		if v4 {
			typ = icmp4Unreachable
		}
		return path.routers[path.unreachTTL-1], icmpError(typ, path.unreachCode, p)
	case ttl <= len(path.routers):
		r := path.routers[ttl-1]
		if !r.IsValid() {
			return r, nil
		}
		typ := uint8(icmp6TimeExceeded)
		if v4 {
			typ = icmp4TimeExceeded
		}
		return r, icmpError(typ, 0, p)
	case path.blackhole:
		return netip.Addr{}, nil
	case p.proto == protoTCP:
		return p.dst, synAck(p)
	case p.proto == protoUDP:
		if v4 {
			return p.dst, icmpError(icmp4Unreachable, icmp4PortUnreach, p)
		}
		return p.dst, icmpError(icmp6Unreachable, icmp6PortUnreach, p)
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

// icmpError builds an ICMP error quoting p as it left the host.
func icmpError(typ, code uint8, p *outPacket) []byte {
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
	return append(msg, p.l4[:8]...)
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
					ids := map[uint16]bool{}
					for _, p := range s.sent {
						flows[flowFields(p)] = true
						ids[probeID(p)] = true
					}
					if paris && len(flows) != 1 {
						t.Errorf("paris probes span %d flows: %v", len(flows), flows)
					}
					if !paris && len(flows) < 8 {
						t.Errorf("classic probes span only %d flows", len(flows))
					}
					if len(ids) != len(s.sent) {
						t.Errorf("%d probes but %d distinct probe ids", len(s.sent), len(ids))
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
