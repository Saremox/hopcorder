package probe

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
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
	ids       *rand.Rand // probe ids; guarded by mu

	sent       atomic.Uint64
	sendErrs   atomic.Uint64
	late       atomic.Uint64
	mismatched atomic.Uint64
}

// pendingProbe is one probe in flight. Its key holds 16 random bits (the
// probe id); the fields below hold more, drawn independently, which a reply
// must also match so that an off-path sender guessing ids cannot pass for
// one: ICMPv4 errors quote the IPv4 IP ID, echo replies echo the data (with
// its nonce) and ICMP errors may quote it, TCP replies acknowledge the full
// sequence number.
type pendingProbe struct {
	tr   *traceState
	ttl  int
	ipID uint16    // the IPv4 header's identification field, random
	data []byte    // ICMP and UDP: the payload after the 8-byte header, with the nonce
	seq  uint32    // TCP: the SYN's sequence number
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
	var seed [32]byte
	_, _ = cryptorand.Read(seed[:]) // never fails
	return &engine{
		io:           io,
		grace:        defaultGrace,
		tombstoneTTL: defaultTombstoneTTL,
		now:          time.Now,
		pending:      make(map[probeKey]*pendingProbe),
		tombs:        make(map[probeKey]time.Time),
		ids:          rand.New(rand.NewChaCha8(seed)),
	}
}

func (e *engine) stats() Stats {
	return Stats{ProbesSent: e.sent.Load(), SendErrors: e.sendErrs.Load(), LateReplies: e.late.Load(),
		MismatchedReplies: e.mismatched.Load()}
}

// pendingLen reports the number of in-flight probes, for leak tests.
func (e *engine) pendingLen() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pending)
}

// maxIDTries bounds the random draws for a free probe id before
// allocIDLocked falls back to scanning.
const maxIDTries = 16

// allocIDLocked returns a probe identifier in 1..0xfffe (the range suits
// both ICMP sequence numbers and UDP checksums, where 0 and 0xffff are
// reserved) and the probe's key, built by keyOf. Ids are drawn at random
// from a cryptographically strong generator, so the ids of one probe tell
// nothing about the others: a local process that learns the ids of the
// probes reaching the destination (from their answers) cannot predict those
// of the silent hops before them. An id whose key is in flight or
// tombstoned is never chosen while a free one exists; a tombstoned key is
// reused only if every id of the flow is taken, and an in-flight one never.
func (e *engine) allocIDLocked(keyOf func(id uint16) probeKey) (uint16, probeKey, error) {
	free := func(k probeKey, tombs bool) bool {
		if _, busy := e.pending[k]; busy {
			return false
		}
		_, tomb := e.tombs[k]
		return !tombs || !tomb
	}
	id := uint16(1 + e.ids.IntN(0xfffe)) //nolint:gosec // 1..0xfffe
	for range maxIDTries {
		if k := keyOf(id); free(k, true) {
			return id, k, nil
		}
		id = uint16(1 + e.ids.IntN(0xfffe)) //nolint:gosec // 1..0xfffe
	}
	for _, tombs := range []bool{true, false} {
		for range 0xfffe {
			if k := keyOf(id); free(k, tombs) {
				return id, k, nil
			}
			if id++; id == 0xffff {
				id = 1
			}
		}
	}
	return 0, probeKey{}, errors.New("probe: every probe id of the flow is in flight")
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

	e.mu.Lock()
	for i := range pkts {
		ttl := first + i
		p := outPacket{src: src, dst: target, zone: req.Target.Zone(), ttl: uint8(ttl)} //nolint:gosec // ttl <= MaxTTL (255), checked by Validate
		// key fields other than the probe id
		var a, b uint16
		switch req.Protocol {
		case ICMP:
			p.proto = protoICMP
			if target.Is6() {
				p.proto = protoICMPv6
			}
			a = req.FlowID
		case UDP:
			p.proto = protoUDP
			a, b = req.FlowID, req.Port
			if !req.Paris {
				b += uint16(ttl - 1) //nolint:gosec // classic traceroute port walk; wraps like it does
			}
		case TCP:
			p.proto = protoTCP
			a, b = tcpSourcePort(req.FlowID, req.Paris, ttl), req.Port
		}
		id, k, err := e.allocIDLocked(func(id uint16) probeKey {
			return probeKey{dst: target, proto: p.proto, a: a, b: b, c: id}
		})
		if err != nil {
			e.mu.Unlock()
			e.finish(tr, keys[:i], probes[:i], false)
			return nil, fmt.Errorf("trace %s: %w", target, err)
		}
		// Drawn independently of the id (the key), so a reply must match
		// 16 more random bits on IPv4 and, wherever it carries the
		// nonce or the full TCP sequence number, 32 more. 0 is not an
		// IP ID: Linux would fill in one of its own.
		p.ipID = uint16(1 + e.ids.IntN(0xffff)) //nolint:gosec // 1..0xffff
		nonce := e.ids.Uint32()
		probes[i] = &pendingProbe{tr: tr, ttl: ttl, ipID: p.ipID}
		switch req.Protocol {
		case ICMP:
			p.l4 = buildEcho(src, target, req.FlowID, id, nonce, req.Paris)
			probes[i].data = p.l4[8:]
		case UDP:
			p.l4 = buildUDP(src, target, a, b, id, nonce)
			probes[i].data = p.l4[8:]
		case TCP:
			probes[i].seq = tcpSeq(id, uint16(nonce)) //nolint:gosec // the low 16 random bits
			p.l4 = buildTCPSyn(src, target, a, b, probes[i].seq)
		}
		pkts[i], keys[i] = p, k
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

// matches reports whether reply r (parsed from msg), which carries p's key,
// also carries what else p was sent with. Otherwise it answers another
// packet with the same key: one an off-path sender forged by guessing the
// 16-bit probe id, another process's (an unprivileged one can choose ports,
// UDP checksum and echo sequence, but not the IP ID, the nonce or the
// sequence number the kernel gives its connections), or one a middlebox
// rewrote (IP IDs, say).
func (p *pendingProbe) matches(r icmpReply, msg []byte) bool {
	if r.quotedIPID && r.ipID != p.ipID {
		return false
	}
	switch {
	case r.key.proto == protoTCP:
		// A SYN-ACK or RST acknowledges exactly seq+1; an ICMP error
		// quotes the SYN's whole sequence number.
		return r.seq == p.seq
	case r.kind == kindEchoReply:
		// The destination echoes the request's data.
		return len(msg) >= 8 && bytes.HasPrefix(msg[8:], p.data)
	default:
		// An ICMP error quoting an echo request or a UDP probe: what it
		// quotes of the payload must be the probe's.
		return quoteMatches(msg[r.l4Off:r.l4Off+r.l4Len], p.data, p.tr.target.Is6())
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
	rtt := at.Sub(p.sent)
	if rtt < 0 {
		// Received (kernel timestamp) before the probe left: not an answer
		// to it, though it was read afterwards. The probe stays pending.
		return
	}
	if !p.matches(r, msg) {
		e.mismatched.Add(1)
		return
	}
	tr := p.tr
	delete(e.pending, r.key)
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
