package probe

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saremox/hopcorder/internal/record"
)

// Defaults for the engine's timing.
const (
	// defaultGrace is added to the per-probe timeout before a cycle is
	// closed, to absorb the time spent sending and scheduling.
	defaultGrace = 20 * time.Millisecond
	// defaultTombstoneTTL is how long a timed-out probe's key is remembered
	// so that a reply arriving after the timeout is counted as late rather
	// than ignored as someone else's traffic.
	defaultTombstoneTTL = 30 * time.Second
)

// netIO is the socket layer under the engine. The Linux raw-socket code and
// the tests' network simulator implement it; replies flow back through
// engine.handle.
type netIO interface {
	// source returns the local address the kernel would use to reach dst,
	// or an error wrapping ErrNoRoute.
	source(ctx context.Context, dst netip.Addr) (netip.Addr, error)
	// send transmits one probe.
	send(p *outPacket) error
}

// outPacket is one probe ready to send. For IPv4 the socket layer prepends
// the IP header (it knows the TTL and IP ID); for IPv6 the hop limit and
// source go into ancillary data.
type outPacket struct {
	proto    uint8
	src, dst netip.Addr
	zone     string // IPv6 scope zone of dst, for link-local targets
	ttl      uint8
	ipID     uint16
	l4       []byte
}

// engine owns the pending-probe table shared by every concurrent trace. No
// goroutine is started per probe or per trace: a trace registers its probes,
// sends them, and waits on one channel and one timer; the socket layer's
// receive loops resolve probes through handle.
type engine struct {
	io           netIO
	grace        time.Duration
	tombstoneTTL time.Duration
	now          func() time.Time

	mu        sync.Mutex
	pending   map[probeKey]*pendingProbe
	tombs     map[probeKey]time.Time // key -> expiry
	lastPrune time.Time
	nextID    map[flowKey]uint16

	sent     atomic.Uint64
	sendErrs atomic.Uint64
	late     atomic.Uint64
}

// flowKey identifies a probe flow for id allocation: probes of different
// flows can never be confused, so each flow counts on its own.
type flowKey struct {
	dst   netip.Addr
	proto uint8
	flow  uint16
}

type pendingProbe struct {
	tr   *traceState
	ttl  int
	sent time.Time // zero until the probe has been handed to the socket
}

type traceState struct {
	req       TraceRequest
	first     int        // lowest TTL probed; results[0] is for this TTL
	target    netip.Addr // req.Target, unmapped and without zone
	results   []HopResult
	remaining int
	done      chan struct{}
}

func newEngine(io netIO) *engine {
	return &engine{
		io:           io,
		grace:        defaultGrace,
		tombstoneTTL: defaultTombstoneTTL,
		now:          time.Now,
		pending:      make(map[probeKey]*pendingProbe),
		tombs:        make(map[probeKey]time.Time),
		nextID:       make(map[flowKey]uint16),
	}
}

func (e *engine) stats() Stats {
	return Stats{ProbesSent: e.sent.Load(), SendErrors: e.sendErrs.Load(), LateReplies: e.late.Load()}
}

// pendingLen reports the number of in-flight probes, for leak tests.
func (e *engine) pendingLen() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pending)
}

// allocIDLocked returns the next probe identifier of a flow, in 1..0xfffe.
// The range suits both ICMP sequence numbers and UDP checksums (where 0 and
// 0xffff are reserved). Each flow counts separately from a random start, so
// an id is reused only after 65534 newer probes of the same flow (hours at
// any sane interval), long after the old probe's tombstone has expired.
func (e *engine) allocIDLocked(fk flowKey) uint16 {
	id, ok := e.nextID[fk]
	if !ok {
		id = uint16(rand.N(0xfffe)) //nolint:gosec // not security relevant
	}
	id++
	if id == 0 || id == 0xffff {
		id = 1
	}
	e.nextID[fk] = id
	return id
}

func (e *engine) trace(ctx context.Context, req TraceRequest) ([]HopResult, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	target := req.Target.Unmap().WithZone("")
	src, err := e.io.source(ctx, req.Target.Unmap())
	if err != nil {
		return nil, err
	}
	src = src.Unmap().WithZone("")
	if src.Is4() != target.Is4() {
		return nil, fmt.Errorf("source %s and target %s differ in address family", src, target)
	}

	first, n := req.FirstTTL(), req.MaxTTL-req.FirstTTL()+1
	tr := &traceState{
		req:       req,
		first:     first,
		target:    target,
		results:   NoRepliesRange(first, req.MaxTTL),
		remaining: n,
		done:      make(chan struct{}),
	}
	pkts := make([]outPacket, n)
	keys := make([]probeKey, n)
	probes := make([]*pendingProbe, n)

	fk := flowKey{dst: target, proto: protoUDP, flow: req.FlowID}
	switch req.Protocol {
	case ICMP:
		fk.proto = protoICMP
	case TCP:
		fk.proto = protoTCP
	}
	e.mu.Lock()
	for i := range pkts {
		ttl := first + i
		id := e.allocIDLocked(fk)
		p := outPacket{src: src, dst: target, zone: req.Target.Zone(), ttl: uint8(ttl), ipID: id} //nolint:gosec // ttl <= MaxTTL (255), checked by Validate
		var k probeKey
		switch req.Protocol {
		case ICMP:
			p.proto = protoICMP
			if target.Is6() {
				p.proto = protoICMPv6
			}
			p.l4 = buildEcho(src, target, req.FlowID, id, req.Paris)
			k = probeKey{dst: target, proto: p.proto, a: req.FlowID, c: id}
		case UDP:
			dport := req.Port
			if !req.Paris {
				dport += uint16(ttl - 1) //nolint:gosec // classic traceroute port walk; wraps like it does
			}
			p.proto = protoUDP
			p.l4 = buildUDP(src, target, req.FlowID, dport, id)
			k = probeKey{dst: target, proto: protoUDP, a: req.FlowID, b: dport, c: id}
		case TCP:
			// The destination port is the service being reached, so
			// classic mode walks the source port instead.
			sport := req.FlowID
			if !req.Paris {
				sport += uint16(ttl - 1) //nolint:gosec // wraps like classic traceroute
			}
			p.proto = protoTCP
			p.l4 = buildTCPSyn(src, target, sport, req.Port, tcpSeq(id))
			k = probeKey{dst: target, proto: protoTCP, a: sport, b: req.Port, c: id}
		}
		pkts[i], keys[i] = p, k
		probes[i] = &pendingProbe{tr: tr, ttl: ttl}
		e.pending[k] = probes[i]
		delete(e.tombs, k)
	}
	e.mu.Unlock()

	var (
		lastSent  time.Time
		failed    int
		noRoute   int
		firstFail error
	)
	for i := range pkts {
		e.mu.Lock()
		lastSent = e.now()
		probes[i].sent = lastSent
		e.mu.Unlock()
		if err := e.io.send(&pkts[i]); err != nil {
			e.sendErrs.Add(1)
			failed++
			if errors.Is(err, ErrNoRoute) {
				noRoute++
			}
			if firstFail == nil {
				firstFail = err
			}
			e.mu.Lock()
			if e.pending[keys[i]] == probes[i] {
				delete(e.pending, keys[i])
				e.resolveLocked(tr)
			}
			e.mu.Unlock()
			continue
		}
		e.sent.Add(1)
	}
	if failed == len(pkts) {
		e.finish(tr, keys, probes, false)
		if noRoute == len(pkts) {
			return nil, fmt.Errorf("trace %s: %w", target, ErrNoRoute)
		}
		return nil, fmt.Errorf("trace %s: every probe failed to send: %w", target, firstFail)
	}

	wait := time.Until(lastSent.Add(req.Timeout + e.grace))
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-tr.done:
	case <-timer.C:
	case <-ctx.Done():
		e.finish(tr, keys, probes, false)
		return nil, ctx.Err()
	}
	return e.finish(tr, keys, probes, true), nil
}

// finish removes the trace's unanswered probes from the pending table,
// leaving tombstones so later replies count as late, and returns a copy of
// the results.
func (e *engine) finish(tr *traceState, keys []probeKey, probes []*pendingProbe, tomb bool) []HopResult {
	now := e.now()
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, k := range keys {
		if e.pending[k] != probes[i] {
			continue
		}
		delete(e.pending, k)
		if tomb {
			e.tombs[k] = now.Add(e.tombstoneTTL)
		}
	}
	if now.Sub(e.lastPrune) > e.tombstoneTTL/2 {
		for k, exp := range e.tombs {
			if now.After(exp) {
				delete(e.tombs, k)
			}
		}
		e.lastPrune = now
	}
	out := make([]HopResult, len(tr.results))
	copy(out, tr.results)
	return out
}

func (e *engine) resolveLocked(tr *traceState) {
	tr.remaining--
	if tr.remaining == 0 {
		close(tr.done)
	}
}

// handle processes one received ICMP message (without IP header). It is
// called by the socket layer's receive loops; msg is not retained.
func (e *engine) handle(from netip.Addr, msg []byte, at time.Time) {
	from = from.Unmap().WithZone("")
	var (
		r   icmpReply
		err error
	)
	if from.Is4() {
		r, err = parseICMPv4(from, msg)
	} else {
		r, err = parseICMPv6(from, msg)
	}
	if err == nil {
		e.resolve(from, r, at, msg)
	}
}

// handleTCP processes one received TCP segment (without IP header): the
// destination's SYN-ACK or RST answering a TCP probe.
func (e *engine) handleTCP(from netip.Addr, seg []byte, at time.Time) {
	from = from.Unmap().WithZone("")
	if r, err := parseTCP(from, seg); err == nil {
		e.resolve(from, r, at, nil)
	}
}

// resolve records reply r for its pending probe. msg is the ICMP message,
// for extensions; nil for TCP replies.
func (e *engine) resolve(from netip.Addr, r icmpReply, at time.Time, msg []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.pending[r.key]
	if !ok {
		if exp, ok := e.tombs[r.key]; ok && !at.After(exp) {
			e.late.Add(1)
		}
		return
	}
	if p.sent.IsZero() {
		return // cannot be a reply to a probe that has not been sent
	}
	tr := p.tr
	delete(e.pending, r.key)
	rtt := at.Sub(p.sent)
	if rtt > tr.req.Timeout {
		e.late.Add(1)
		e.tombs[r.key] = at.Add(e.tombstoneTTL)
		e.resolveLocked(tr)
		return
	}
	res := &tr.results[p.ttl-tr.first]
	res.From, res.RTT = from, rtt
	switch r.kind {
	case kindEchoReply, kindTCPReply:
		res.Status = record.StatusReply
	case kindTimeExceeded:
		res.Status = record.StatusTTLExpired
		res.MPLS = parseMPLS(msg, from.Is6())
	case kindUnreachable:
		res.MPLS = parseMPLS(msg, from.Is6())
		portUnreach := (from.Is4() && r.code == icmp4PortUnreach) || (from.Is6() && r.code == icmp6PortUnreach)
		// For UDP this is the normal answer; for TCP it is a REJECTing
		// firewall on the destination itself. Either way it answered.
		if tr.req.Protocol != ICMP && from == tr.target && portUnreach {
			res.Status = record.StatusReply
		} else {
			res.Status, res.Code = record.StatusUnreachable, int(r.code)
		}
	}
	e.resolveLocked(tr)
}
