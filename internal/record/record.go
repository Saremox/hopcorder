// Package record defines the wire record the agent emits once per (target,
// cycle) and the values derived from a cycle's hops: path trimming, path_hash
// and loop detection. See docs/HANDOFF.md §3.
package record

import (
	"crypto/sha1" //nolint:gosec // path_hash is an identifier, not a security boundary; SHA-1 is fixed by the schema.
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"
)

// SchemaVersion is the value of the "schema" field of every record this
// package produces. Bump it on any incompatible change to the wire format.
const SchemaVersion = 1

// Status is the outcome of a single probe.
type Status string

// Probe outcomes. See docs/HANDOFF.md §3.1.
const (
	// StatusReply means the destination itself answered (ICMP echo reply,
	// or ICMP port unreachable from the destination for UDP probes).
	StatusReply Status = "reply"
	// StatusTTLExpired means an intermediate router sent ICMP time exceeded.
	StatusTTLExpired Status = "ttl-expired"
	// StatusUnreachable means ICMP destination unreachable (other than the
	// destination's own port-unreachable for UDP). Hop.Code holds the code.
	StatusUnreachable Status = "unreachable"
	// StatusNoReply means nothing came back within the probe timeout.
	StatusNoReply Status = "no-reply"
)

// Valid reports whether s is one of the defined statuses.
func (s Status) Valid() bool {
	switch s {
	case StatusReply, StatusTTLExpired, StatusUnreachable, StatusNoReply:
		return true
	}
	return false
}

// Responded reports whether a probe with this status got an answer, i.e.
// whether the hop has an IP address.
func (s Status) Responded() bool {
	return s == StatusReply || s == StatusTTLExpired || s == StatusUnreachable
}

// Hop is the result for one TTL of one cycle.
type Hop struct {
	TTL    int        `json:"ttl"`
	Status Status     `json:"status"`
	IP     netip.Addr `json:"ip,omitzero"`
	// RTTMicros is the round-trip time in microseconds, omitted on no-reply.
	// Responding hops always carry at least 1.
	RTTMicros int64 `json:"rtt_us,omitempty"`
	// Code is the ICMP code, set only for StatusUnreachable. It is a pointer
	// because code 0 (net unreachable / no route) is meaningful. Codes are
	// raw ICMPv4 or ICMPv6 codes; interpret them according to Record.AF.
	Code *int `json:"code,omitempty"`
	// MPLS is the label stack the router reported in its ICMP error
	// (RFC 4884/4950 extensions), outermost label first; absent when the
	// router sent none.
	MPLS []MPLSLabel `json:"mpls,omitempty"`
}

// MPLSLabel is one entry of an MPLS label stack as quoted by a router.
type MPLSLabel struct {
	Label uint32 `json:"label"` // 20-bit label value
	TC    uint8  `json:"tc"`    // traffic class (formerly EXP)
	S     bool   `json:"s"`     // bottom of stack
	TTL   uint8  `json:"ttl"`   // the label's TTL when the packet arrived
}

// Loop is an IP address that answered at more than one TTL in one cycle.
type Loop struct {
	IP   netip.Addr `json:"ip"`
	TTLs []int      `json:"ttls"`
}

// Record is one path snapshot: one cycle against one target. A cycle is
// atomic; it is shipped whole or not at all.
type Record struct {
	Schema   int        `json:"schema"`
	AgentID  string     `json:"agent_id"`
	Seq      uint64     `json:"seq"`
	TS       time.Time  `json:"ts"`
	Target   string     `json:"target"`
	TargetIP netip.Addr `json:"target_ip"`
	AF       int        `json:"af"`
	Protocol string     `json:"protocol"`
	FlowID   int        `json:"flow_id"`
	Reached  bool       `json:"reached"`
	PathLen  int        `json:"path_len"`
	PathHash string     `json:"path_hash"`
	Loops    []Loop     `json:"loops"`
	Hops     []Hop      `json:"hops"`
}

// Finalize trims r.Hops according to the path-end rules and fills in the
// derived fields Reached, PathLen, PathHash and Loops. r.Hops must hold one
// entry per probed TTL; they are sorted by TTL first.
func (r *Record) Finalize(probePast int) {
	r.Hops = Trim(r.Hops, probePast)
	r.Reached = Reached(r.Hops)
	r.PathLen = len(r.Hops)
	r.PathHash = PathHash(r.Hops)
	r.Loops = DetectLoops(r.Hops)
	if r.Hops == nil {
		r.Hops = []Hop{}
	}
}

// Trim cuts a cycle's hops down to the meaningful path:
//
//   - The path ends at the lowest TTL that got StatusReply (the destination
//     answered) or StatusUnreachable (the network said it cannot go further).
//     That hop is kept, everything above it is dropped.
//   - Otherwise probePast TTLs beyond the last responding hop are kept and the
//     rest dropped, so a black-holed target does not produce max_ttl no-reply
//     hops per cycle.
//
// hops are sorted by TTL in place. The returned slice aliases hops.
func Trim(hops []Hop, probePast int) []Hop {
	if probePast < 0 {
		probePast = 0
	}
	slices.SortFunc(hops, func(a, b Hop) int { return a.TTL - b.TTL })
	lastResponding := -1
	for i, h := range hops {
		if h.Status == StatusReply || h.Status == StatusUnreachable {
			return hops[:i+1]
		}
		if h.Status.Responded() {
			lastResponding = i
		}
	}
	return hops[:min(len(hops), lastResponding+1+probePast)]
}

// Reached reports whether the destination answered in this cycle.
func Reached(hops []Hop) bool {
	for _, h := range hops {
		if h.Status == StatusReply {
			return true
		}
	}
	return false
}

// PathHash returns the first 16 hex characters of the SHA-1 over the ordered
// hop IPs joined with "|", using "*" for hops that did not respond. Because
// silent hops hash as "*" regardless of which router is behind them, a
// rate-limited router flickering in and out does not change the hash; a
// change in PathHash is a route change.
func PathHash(hops []Hop) string {
	parts := make([]string, len(hops))
	for i, h := range hops {
		if h.Status.Responded() && h.IP.IsValid() {
			parts[i] = h.IP.String()
		} else {
			parts[i] = "*"
		}
	}
	sum := sha1.Sum([]byte(strings.Join(parts, "|"))) //nolint:gosec // see import comment
	return hex.EncodeToString(sum[:])[:16]
}

// DetectLoops returns every IP that answered with time exceeded at more than
// one TTL, ordered by the first TTL it was seen at. Only time-exceeded
// answers count: they prove the router handled the packet in transit, so
// seeing one at two TTLs means the packet came back to it. A router that
// expires TTL k and then rejects TTL k+1 with an unreachable is a filtering
// edge, not a loop. A rate-limited looping router answers only some of its
// probes, so loops found this way are a floor, not a count. The result is
// never nil.
func DetectLoops(hops []Hop) []Loop {
	seen := make(map[netip.Addr][]int)
	var order []netip.Addr
	for _, h := range hops {
		if h.Status != StatusTTLExpired || !h.IP.IsValid() {
			continue
		}
		if _, ok := seen[h.IP]; !ok {
			order = append(order, h.IP)
		}
		seen[h.IP] = append(seen[h.IP], h.TTL)
	}
	loops := []Loop{}
	for _, ip := range order {
		if ttls := seen[ip]; len(ttls) > 1 {
			loops = append(loops, Loop{IP: ip, TTLs: ttls})
		}
	}
	return loops
}

// Limits of a valid record. They match the storage schema (TTLs and path
// lengths are UInt8, RTTs UInt32, ICMP codes 0-255) and keep one record
// small, since the server buffers and parses records from untrusted agents.
const (
	MaxHops        = 255
	MaxMPLSLabels  = 16
	MaxAgentIDLen  = 128
	MaxTargetLen   = 255
	maxPathHashLen = 64
	// MaxZoneLen bounds an IPv6 zone (an interface name or index; Linux
	// interface names have at most 15 characters).
	MaxZoneLen = 15
)

// The time range a record's ts must fall into (ClickHouse DateTime64 covers
// 1900-2299; Go's UnixNano 1678-2262).
var (
	minTS = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	maxTS = time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)
)

// ValidAgentID reports whether s may be an agent_id: 1 to MaxAgentIDLen
// characters from [A-Za-z0-9._:-]. The narrow character set keeps the value
// safe wherever it is shown or quoted (dashboards, logs, URLs).
func ValidAgentID(s string) bool {
	return len(s) > 0 && len(s) <= MaxAgentIDLen && !strings.ContainsFunc(s, func(c rune) bool {
		return !isAlnum(c) && c != '.' && c != '_' && c != ':' && c != '-'
	})
}

// ValidTarget reports whether s may be a target: a host name or an IP
// address (with an IPv6 zone, see ValidZone), 1 to MaxTargetLen characters
// from [A-Za-z0-9._:%-].
func ValidTarget(s string) bool {
	if len(s) == 0 || len(s) > MaxTargetLen || strings.ContainsFunc(s, func(c rune) bool {
		return !isAlnum(c) && c != '.' && c != '_' && c != ':' && c != '%' && c != '-'
	}) {
		return false
	}
	if ip, err := netip.ParseAddr(s); err == nil && ip.Zone() != "" {
		return ValidZone(ip.Zone())
	}
	return true
}

// ValidZone reports whether z may be the zone of a target address: 1 to
// MaxZoneLen characters from [A-Za-z0-9._-]. netip accepts any text as a
// zone, so without this an address could carry a megabyte of anything.
func ValidZone(z string) bool {
	return len(z) > 0 && len(z) <= MaxZoneLen && !strings.ContainsFunc(z, func(c rune) bool {
		return !isAlnum(c) && c != '.' && c != '_' && c != '-'
	})
}

// addrOf checks that ip, a hop or loop address, belongs to address family
// af and has no zone (routers' addresses never have one).
func addrOf(ip netip.Addr, af int) error {
	switch {
	case ip.Zone() != "":
		return errors.New("ip has a zone")
	case af == 4 && !ip.Is4(), af == 6 && (!ip.Is6() || ip.Is4In6()):
		return fmt.Errorf("ip is not an IPv%d address", af)
	}
	return nil
}

func isAlnum(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// Validate checks that r is a well-formed schema-1 record, finalized: its
// derived fields are those Finalize gives its hops.
func (r *Record) Validate() error {
	var errs []error
	if r.Schema != SchemaVersion {
		errs = append(errs, fmt.Errorf("schema %d, want %d", r.Schema, SchemaVersion))
	}
	if !ValidAgentID(r.AgentID) {
		errs = append(errs, fmt.Errorf("agent_id: want 1 to %d characters from [A-Za-z0-9._:-]", MaxAgentIDLen))
	}
	if r.TS.Before(minTS) || !r.TS.Before(maxTS) {
		errs = append(errs, errors.New("ts out of range"))
	}
	if !ValidTarget(r.Target) {
		errs = append(errs, fmt.Errorf("target: want a host name or address of 1 to %d characters", MaxTargetLen))
	}
	// Error messages never quote a value: they end up in responses and logs.
	switch {
	case !r.TargetIP.IsValid():
		errs = append(errs, errors.New("target_ip is missing"))
	case r.TargetIP.Zone() != "" && !ValidZone(r.TargetIP.Zone()):
		errs = append(errs, fmt.Errorf("target_ip: zone must be 1 to %d characters from [A-Za-z0-9._-]", MaxZoneLen))
	case r.AF == 4 && !r.TargetIP.Is4(), r.AF == 6 && !r.TargetIP.Is6():
		errs = append(errs, fmt.Errorf("target_ip does not match af %d", r.AF))
	case r.AF != 4 && r.AF != 6:
		errs = append(errs, fmt.Errorf("af %d, want 4 or 6", r.AF))
	}
	switch r.Protocol {
	case "icmp", "udp", "tcp":
	default:
		errs = append(errs, errors.New("protocol: want icmp, udp or tcp"))
	}
	if len(r.PathHash) > maxPathHashLen || strings.ContainsFunc(r.PathHash, func(c rune) bool { return c < '!' || c > '~' }) {
		errs = append(errs, errors.New("path_hash: want at most 64 printable characters"))
	}
	if r.FlowID < 0 || r.FlowID > 0xffff {
		errs = append(errs, fmt.Errorf("flow_id %d out of range", r.FlowID))
	}
	if len(r.Hops) > MaxHops {
		// Stop here: the per-hop checks below would only repeat it.
		return errors.Join(append(errs, fmt.Errorf("%d hops, at most %d", len(r.Hops), MaxHops))...)
	}
	if r.PathLen != len(r.Hops) {
		errs = append(errs, fmt.Errorf("path_len %d but %d hops", r.PathLen, len(r.Hops)))
	}
	if r.Loops == nil {
		errs = append(errs, errors.New("loops is null"))
	}
	if err := validateLoops(r.Loops, len(r.Hops), r.AF); err != nil {
		errs = append(errs, err)
	}
	for i, h := range r.Hops {
		if err := h.validate(r.AF); err != nil {
			errs = append(errs, fmt.Errorf("hops[%d]: %w", i, err))
		}
		if h.TTL != i+1 {
			errs = append(errs, fmt.Errorf("hops[%d]: ttl %d, want %d", i, h.TTL, i+1))
		}
	}
	return errors.Join(append(errs, r.validateDerived()...)...)
}

// validateDerived checks what Finalize derives from the hops, which every
// agent sends as Finalize made it (security review, round four): the path
// ends at its first reply or unreachable (Trim), reached says whether the
// destination answered, and path_hash and loops are the hops'. Otherwise a
// record could claim to reach its destination without a reply, and records
// flipping reached at no cost made the server derive an outage from each.
func (r *Record) validateDerived() []error {
	var errs []error
	for i, h := range r.Hops[:max(len(r.Hops)-1, 0)] {
		if h.Status == StatusReply || h.Status == StatusUnreachable {
			errs = append(errs, fmt.Errorf("hops[%d]: status %s before the end of the path", i, h.Status))
			break
		}
	}
	if r.Reached != Reached(r.Hops) {
		errs = append(errs, errors.New("reached does not match the hops (a reply from the destination)"))
	}
	if r.PathHash != PathHash(r.Hops) {
		errs = append(errs, errors.New("path_hash does not match the hops"))
	}
	if !slices.EqualFunc(r.Loops, DetectLoops(r.Hops), func(x, y Loop) bool {
		return x.IP == y.IP && slices.Equal(x.TTLs, y.TTLs)
	}) {
		errs = append(errs, errors.New("loops do not match the hops (addresses that expired the TTL at more than one TTL)"))
	}
	return errs
}

// validateLoops checks that loops could have come from hops hops of address
// family af: at most one entry per hop, each a valid address seen at 2 or
// more distinct, increasing TTLs within the path.
func validateLoops(loops []Loop, hops, af int) error {
	if len(loops) > hops {
		return fmt.Errorf("%d loops for %d hops", len(loops), hops)
	}
	for i, l := range loops {
		if !l.IP.IsValid() || len(l.TTLs) < 2 || len(l.TTLs) > hops {
			return fmt.Errorf("loops[%d]: want an address seen at 2 to %d TTLs", i, hops)
		}
		if err := addrOf(l.IP, af); err != nil {
			return fmt.Errorf("loops[%d]: %w", i, err)
		}
		for j, ttl := range l.TTLs {
			if ttl < 1 || ttl > hops || (j > 0 && ttl <= l.TTLs[j-1]) {
				return fmt.Errorf("loops[%d]: ttls must increase within 1..%d", i, hops)
			}
		}
	}
	return nil
}

func (h Hop) validate(af int) error {
	if !h.Status.Valid() {
		return errors.New("invalid status")
	}
	if h.Status.Responded() {
		if !h.IP.IsValid() {
			return fmt.Errorf("status %s without ip", h.Status)
		}
		if err := addrOf(h.IP, af); err != nil {
			return err
		}
		if h.RTTMicros <= 0 {
			return fmt.Errorf("status %s without rtt_us", h.Status)
		}
		if h.RTTMicros > 1<<32-1 {
			return errors.New("rtt_us out of range")
		}
	} else if h.IP.IsValid() || h.RTTMicros != 0 {
		return errors.New("no-reply hop carries ip or rtt_us")
	}
	if (h.Status == StatusUnreachable) != (h.Code != nil) {
		return errors.New("code must be set exactly for unreachable hops")
	}
	if h.Code != nil && (*h.Code < 0 || *h.Code > 255) {
		return errors.New("code out of range")
	}
	if len(h.MPLS) > MaxMPLSLabels {
		return fmt.Errorf("%d MPLS labels, at most %d", len(h.MPLS), MaxMPLSLabels)
	}
	if len(h.MPLS) > 0 && h.Status != StatusTTLExpired && h.Status != StatusUnreachable {
		return fmt.Errorf("status %s carries an MPLS label stack", h.Status)
	}
	for _, l := range h.MPLS {
		if l.Label > 0xfffff || l.TC > 7 {
			return errors.New("invalid MPLS label")
		}
	}
	return nil
}

// IntPtr returns a pointer to c, for Hop.Code.
func IntPtr(c int) *int { return &c }

// PathTracker decides whether a target's route changed from one cycle to
// the next. Silent hops are wildcards, so a rate-limited router going quiet
// and coming back is not a change (path_hash alone cannot tell those apart:
// the router's address and "*" hash differently). The tracker remembers the
// last address seen at each TTL, so a router replaced while it was silent is
// still caught when the new one first answers. A change is any of:
//
//   - a different target address (the name resolved elsewhere);
//   - a TTL answering from a different address than last seen there;
//   - a reached destination closer than its distance;
//   - a reached destination further away, where a TTL from its distance on
//     answered as a router.
//
// A destination further away behind TTLs that are all silent is not a
// change, and its distance stays: the destination's reply at its distance
// was lost or late while the next TTL's arrived (the destination answers
// every TTL from its distance on), and one ordinary loss made two route
// changes, there and back (review, round six). A path that really grew by
// silent hops is caught as any router replaced by a silent one is: when
// one of those TTLs answers.
//
// Gaining or losing reachability alone is not a path change; it is visible
// in Reached. The zero value is ready to use; it is not safe for concurrent
// use.
type PathTracker struct {
	started bool
	target  netip.Addr
	ips     []netip.Addr // last address seen per TTL-1; invalid = unknown
	dist    int          // the destination's distance, 0 = unknown
}

// PathChange describes why PathTracker considered a cycle a route change.
type PathChange struct {
	Reason ChangeReason
	// TTLs, From and To list, for ReasonRouter, each TTL that answered from
	// a different address than last seen there.
	TTLs     []int
	From, To []netip.Addr
	// PrevDistance and Distance are the destination's distances for
	// ReasonDistance.
	PrevDistance, Distance int
}

// ChangeReason is the kind of route change.
type ChangeReason string

// Route change reasons, in the order they are checked.
const (
	ReasonTarget   ChangeReason = "target"   // the target name resolved to another address
	ReasonRouter   ChangeReason = "router"   // a TTL answered from a different address
	ReasonDistance ChangeReason = "distance" // the destination answered at a different TTL
)

// Observe records rec and reports whether its route differs from what the
// tracker knew. The first observation is never a change. After a change the
// tracker forgets everything rec does not confirm.
func (p *PathTracker) Observe(rec *Record) bool {
	_, changed := p.ObserveChange(rec)
	return changed
}

// ObserveChange is Observe that also describes the change.
func (p *PathTracker) ObserveChange(rec *Record) (PathChange, bool) {
	var ch PathChange
	changed := false
	if p.started {
		ch, changed = p.differs(rec)
	}
	if !p.started || changed {
		p.started, p.target, p.ips, p.dist = true, rec.TargetIP, nil, 0
	}
	for _, h := range rec.Hops {
		if h.Status.Responded() && h.IP.IsValid() && h.TTL >= 1 {
			for len(p.ips) < h.TTL {
				p.ips = append(p.ips, netip.Addr{})
			}
			p.ips[h.TTL-1] = h.IP
		}
	}
	// A cycle that is no change reached the destination at its distance or
	// further away behind silent TTLs: the distance stays.
	if rec.Reached && p.dist == 0 {
		p.dist = rec.PathLen
	}
	return ch, changed
}

func (p *PathTracker) differs(rec *Record) (PathChange, bool) {
	if rec.TargetIP != p.target {
		return PathChange{Reason: ReasonTarget}, true
	}
	var ch PathChange
	for _, h := range rec.Hops {
		if !h.Status.Responded() || h.TTL < 1 || h.TTL > len(p.ips) {
			continue
		}
		if known := p.ips[h.TTL-1]; known.IsValid() && known != h.IP {
			ch.TTLs = append(ch.TTLs, h.TTL)
			ch.From = append(ch.From, known)
			ch.To = append(ch.To, h.IP)
		}
	}
	if len(ch.TTLs) > 0 {
		ch.Reason = ReasonRouter
		return ch, true
	}
	if rec.Reached && p.dist != 0 && (rec.PathLen < p.dist || p.grew(rec)) {
		return PathChange{Reason: ReasonDistance, PrevDistance: p.dist, Distance: rec.PathLen}, true
	}
	return PathChange{}, false
}

// grew reports whether rec, reached further away than p.dist, shows that
// the path grew: a TTL from p.dist on answered, so the destination is no
// longer there.
func (p *PathTracker) grew(rec *Record) bool {
	if rec.PathLen <= p.dist {
		return false
	}
	for _, h := range rec.Hops {
		if h.TTL >= p.dist && h.TTL < rec.PathLen && h.Status.Responded() {
			return true
		}
	}
	return false
}
