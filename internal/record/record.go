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

// DetectLoops returns every IP that responded at more than one TTL, ordered
// by the first TTL it was seen at. A rate-limited looping router answers only
// some of its probes, so loops found this way are a floor, not a count. The
// result is never nil.
func DetectLoops(hops []Hop) []Loop {
	seen := make(map[netip.Addr][]int)
	var order []netip.Addr
	for _, h := range hops {
		if !h.Status.Responded() || !h.IP.IsValid() {
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

// Validate checks that r is a well-formed schema-1 record.
func (r *Record) Validate() error {
	var errs []error
	if r.Schema != SchemaVersion {
		errs = append(errs, fmt.Errorf("schema %d, want %d", r.Schema, SchemaVersion))
	}
	if r.AgentID == "" {
		errs = append(errs, errors.New("agent_id is empty"))
	}
	if r.TS.IsZero() {
		errs = append(errs, errors.New("ts is zero"))
	}
	if r.Target == "" {
		errs = append(errs, errors.New("target is empty"))
	}
	switch {
	case !r.TargetIP.IsValid():
		errs = append(errs, errors.New("target_ip is missing"))
	case r.AF == 4 && !r.TargetIP.Is4(), r.AF == 6 && !r.TargetIP.Is6():
		errs = append(errs, fmt.Errorf("target_ip %s does not match af %d", r.TargetIP, r.AF))
	case r.AF != 4 && r.AF != 6:
		errs = append(errs, fmt.Errorf("af %d, want 4 or 6", r.AF))
	}
	if r.Protocol == "" {
		errs = append(errs, errors.New("protocol is empty"))
	}
	if r.FlowID < 0 || r.FlowID > 0xffff {
		errs = append(errs, fmt.Errorf("flow_id %d out of range", r.FlowID))
	}
	if r.PathLen != len(r.Hops) {
		errs = append(errs, fmt.Errorf("path_len %d but %d hops", r.PathLen, len(r.Hops)))
	}
	if r.Loops == nil {
		errs = append(errs, errors.New("loops is null"))
	}
	for i, h := range r.Hops {
		if err := h.validate(); err != nil {
			errs = append(errs, fmt.Errorf("hops[%d]: %w", i, err))
		}
		if h.TTL != i+1 {
			errs = append(errs, fmt.Errorf("hops[%d]: ttl %d, want %d", i, h.TTL, i+1))
		}
	}
	return errors.Join(errs...)
}

func (h Hop) validate() error {
	if !h.Status.Valid() {
		return fmt.Errorf("invalid status %q", h.Status)
	}
	if h.Status.Responded() {
		if !h.IP.IsValid() {
			return fmt.Errorf("status %s without ip", h.Status)
		}
		if h.RTTMicros <= 0 {
			return fmt.Errorf("status %s without rtt_us", h.Status)
		}
	} else if h.IP.IsValid() || h.RTTMicros != 0 {
		return errors.New("no-reply hop carries ip or rtt_us")
	}
	if (h.Status == StatusUnreachable) != (h.Code != nil) {
		return errors.New("code must be set exactly for unreachable hops")
	}
	return nil
}

// IntPtr returns a pointer to c, for Hop.Code.
func IntPtr(c int) *int { return &c }
