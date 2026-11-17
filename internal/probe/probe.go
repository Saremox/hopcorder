// Package probe sends traceroute probes and collects one result per TTL.
//
// Everything above this package talks to the Prober interface, so it can be
// tested against Fake without raw sockets. The raw-socket implementation
// (Raw, Linux only) shares one ICMP listener per address family across all
// targets and demultiplexes replies by parsing the original packet quoted in
// ICMP errors. See docs/HANDOFF.md §4.
package probe

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/saremox/hopcorder/internal/record"
)

// Protocol is the probe protocol.
type Protocol string

// Supported probe protocols.
const (
	ICMP Protocol = "icmp"
	UDP  Protocol = "udp"
	TCP  Protocol = "tcp"
)

// DefaultFlowID is the ICMP identifier / UDP source port used when a target
// does not configure one. Every probe of a target uses the same flow ID, so
// in paris mode ECMP routers keep the whole cycle (and every later cycle) on
// one path.
const DefaultFlowID = 33434

// DefaultUDPPort is the UDP destination port when none is configured.
const DefaultUDPPort = 33434

// DefaultTCPPort is the TCP destination port when none is configured: the
// port most likely to be let through on the way to a server.
const DefaultTCPPort = 443

// MaxTTL is the largest TTL a request may ask for.
const MaxTTL = 255

// TraceRequest describes one cycle against one target.
type TraceRequest struct {
	Target   netip.Addr
	Protocol Protocol
	// Port is the UDP or TCP destination port. Ignored for ICMP.
	Port uint16
	// FlowID is the ICMP identifier (icmp) or the source port (udp, tcp).
	FlowID uint16
	// Paris keeps every header field that ECMP routers hash on constant
	// across the TTLs of a cycle. With Paris false, probes vary the ICMP
	// checksum, the UDP destination port or the TCP source port per TTL
	// like classic traceroute.
	Paris bool
	// MinTTL and MaxTTL bound the TTLs probed; MinTTL..MaxTTL are sent at
	// once. MinTTL 0 means 1. A cycle normally probes from 1; a higher
	// MinTTL lets a caller extend a cycle that stopped short.
	MinTTL int
	MaxTTL int
	// Timeout is the per-probe timeout. Replies arriving later are dropped
	// and counted as late.
	Timeout time.Duration
}

// Validate checks the request.
func (r TraceRequest) Validate() error {
	if !r.Target.IsValid() {
		return errors.New("probe: invalid target address")
	}
	switch r.Protocol {
	case ICMP:
	case UDP, TCP:
		if r.Port == 0 {
			return fmt.Errorf("probe: %s requires a destination port", r.Protocol)
		}
		if r.FlowID == 0 {
			return fmt.Errorf("probe: %s requires a non-zero source port (flow id)", r.Protocol)
		}
	default:
		return fmt.Errorf("probe: unsupported protocol %q", r.Protocol)
	}
	if r.MaxTTL < 1 || r.MaxTTL > MaxTTL {
		return fmt.Errorf("probe: max ttl %d out of range 1..%d", r.MaxTTL, MaxTTL)
	}
	if r.MinTTL < 0 || r.MinTTL > r.MaxTTL {
		return fmt.Errorf("probe: min ttl %d out of range 0..%d", r.MinTTL, r.MaxTTL)
	}
	if r.Timeout <= 0 {
		return errors.New("probe: timeout must be positive")
	}
	return nil
}

// HopResult is the outcome of the probe sent with one TTL.
type HopResult struct {
	TTL    int
	Status record.Status
	// From is the address that answered; invalid for StatusNoReply.
	From netip.Addr
	// RTT is the round-trip time; zero for StatusNoReply.
	RTT time.Duration
	// Code is the ICMP code for StatusUnreachable.
	Code int
	// MPLS is the label stack the router reported (RFC 4950), if any.
	MPLS []record.MPLSLabel
}

// Prober runs traceroute cycles.
type Prober interface {
	// Trace probes TTLs req.MinTTL..req.MaxTTL in parallel and returns one
	// result per TTL, ordered by TTL. It returns an error only if the cycle could not
	// be run at all; see ErrNoRoute.
	Trace(ctx context.Context, req TraceRequest) ([]HopResult, error)
}

// Stats are cumulative counters a prober exposes for metrics.
type Stats struct {
	ProbesSent  uint64
	SendErrors  uint64
	LateReplies uint64
	// MismatchedReplies counts replies that matched an in-flight probe's
	// addresses, ports and 16-bit id but not the rest of it: an ICMPv4
	// error quoting another IP identification, an echo reply with other
	// data, an ICMP error quoting another payload (or, on IPv6, too little
	// of it to show the probe's nonce), a TCP reply acknowledging another
	// sequence number. They answer another process's packets, or were
	// forged by someone guessing probe ids, or a middlebox on the path
	// rewrote IP IDs (hops beyond it then stay silent on IPv4).
	MismatchedReplies uint64
}

// StatsProvider is implemented by probers that expose Stats.
type StatsProvider interface {
	Stats() Stats
}

// ErrNoRoute is returned (wrapped) by Trace when the host has no route to the
// target, for example because the uplink is down. The cycle did run in the
// sense that nothing could have answered; callers should record it as a cycle
// in which no hop responded rather than as a gap.
var ErrNoRoute = errors.New("no route to target")

// Flow is the local identifier a target's probes are sent with: the ICMP
// echo identifier, or the UDP or TCP source port (the target's flow_id).
type Flow struct {
	Protocol Protocol
	ID       uint16
}

// FlowHolder is implemented by probers that hold their flows' local
// identifiers with ordinary sockets so that no other process on the host
// can claim them. An unprivileged process holding one could otherwise see
// the answers to the probes and send packets that pass for probes. Raw
// holds ICMP echo identifiers (see its HoldFlows for why not ports).
type FlowHolder interface {
	// HoldFlows makes flows the set held, releasing identifiers of flows
	// no longer listed. If an identifier cannot be held, nothing changes
	// and the error wraps ErrFlowInUse when another process holds it.
	HoldFlows(flows []Flow) error
}

// ErrFlowInUse is returned (wrapped) by HoldFlows when another process holds
// a flow's identifier, for example another hopcorder probing with the same
// flow_id.
var ErrFlowInUse = errors.New("in use by another process")

// ErrPermission is returned (wrapped) when raw sockets cannot be opened.
var ErrPermission = errors.New("raw sockets need CAP_NET_RAW: run `sudo setcap cap_net_raw+ep $(which hopcorder)` " +
	"or, under systemd, set AmbientCapabilities=CAP_NET_RAW (running as root is not required)")

// FirstTTL returns the lowest TTL the request probes.
func (r TraceRequest) FirstTTL() int { return max(r.MinTTL, 1) }

// NoReplies returns results for TTLs 1..maxTTL in which nothing answered.
func NoReplies(maxTTL int) []HopResult { return NoRepliesRange(1, maxTTL) }

// NoRepliesRange returns results for TTLs minTTL..maxTTL in which nothing
// answered.
func NoRepliesRange(minTTL, maxTTL int) []HopResult {
	out := make([]HopResult, 0, max(0, maxTTL-minTTL+1))
	for ttl := minTTL; ttl <= maxTTL; ttl++ {
		out = append(out, HopResult{TTL: ttl, Status: record.StatusNoReply})
	}
	return out
}
