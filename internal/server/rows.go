// Package server is the hopcorder ingest server (milestone 2): it accepts
// zstd-compressed NDJSON batches from agents, deduplicates them on
// (agent_id, seq), enriches every hop with its AS and reverse DNS name,
// stores them in ClickHouse, derives path-change and outage events, and
// serves a read API. See docs/HANDOFF.md §7 (M2) and §10.
package server

import (
	"encoding/json"
	"math"
	"net/netip"
	"strconv"
	"time"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// Enricher annotates hop addresses. Lookups must not block on the network.
type Enricher interface {
	ASN(ip netip.Addr) (asn uint32, name string)
	Name(ip netip.Addr) string
}

// noEnrich is used when enrichment is disabled.
type noEnrich struct{}

func (noEnrich) ASN(netip.Addr) (uint32, string) { return 0, "" }
func (noEnrich) Name(netip.Addr) string          { return "" }

// cycleRow is one row of the cycles table (JSONEachRow; Nested columns are
// flattened into "hops.x" / "loops.x" keys).
type cycleRow struct {
	AgentID   string               `json:"agent_id"`
	Seq       uint64               `json:"seq"`
	TS        string               `json:"ts"`
	Target    string               `json:"target"`
	TargetIP  string               `json:"target_ip"`
	AF        int                  `json:"af"`
	Protocol  string               `json:"protocol"`
	FlowID    int                  `json:"flow_id"`
	Reached   bool                 `json:"reached"`
	PathLen   int                  `json:"path_len"`
	PathHash  string               `json:"path_hash"`
	LoopIP    []string             `json:"loops.ip"`
	LoopTTLs  [][]int              `json:"loops.ttls"`
	HopTTL    []int                `json:"hops.ttl"`
	HopStatus []string             `json:"hops.status"`
	HopIP     []string             `json:"hops.ip"`
	HopRTT    []uint32             `json:"hops.rtt_us"`
	HopCode   []int                `json:"hops.code"`
	HopASN    []uint32             `json:"hops.asn"`
	HopASName []string             `json:"hops.as_name"`
	HopName   []string             `json:"hops.name"`
	HopMPLS   [][]record.MPLSLabel `json:"hops.mpls"`
	// HopLoopTTLs are, for each hop, the TTLs of the first loop with its
	// address (empty if none), which hops_mv writes on the hop's row as
	// is_loop and loop_ttls. The view used to look them up itself, which
	// made ClickHouse copy the cycle's loops onto every hop row first.
	HopLoopTTLs [][]int `json:"hops.loop_ttls"`
}

var noTTLs = []int{}

// buildCycleRow converts a validated record into a cycles row. Every slice
// is non-nil: ClickHouse rejects null for array columns. cost is what the
// row counts towards maxInsertBytes: its JSON, and for each hop row
// hops_mv makes of it hopRowBytes plus its loop_ttls text.
func buildCycleRow(rec *record.Record, en Enricher) (b []byte, cost int, err error) {
	n := len(rec.Hops)
	row := cycleRow{
		AgentID: rec.AgentID, Seq: rec.Seq, TS: clickhouse.Time(rec.TS), Target: rec.Target,
		TargetIP: rec.TargetIP.String(), AF: rec.AF, Protocol: rec.Protocol, FlowID: rec.FlowID,
		Reached: rec.Reached, PathLen: rec.PathLen, PathHash: rec.PathHash,
		LoopIP: make([]string, 0, len(rec.Loops)), LoopTTLs: make([][]int, 0, len(rec.Loops)),
		HopTTL: make([]int, n), HopStatus: make([]string, n), HopIP: make([]string, n),
		HopRTT: make([]uint32, n), HopCode: make([]int, n), HopASN: make([]uint32, n),
		HopASName: make([]string, n), HopName: make([]string, n), HopMPLS: make([][]record.MPLSLabel, n),
		HopLoopTTLs: make([][]int, n),
	}
	// The first loop of each address, with the length of its TTLs as text
	// ("2,4"), as the view used to pick them (indexOf(loops.ip, hops.ip)).
	type loop struct {
		ttls    []int
		textLen int
	}
	var loopOf map[netip.Addr]loop
	for _, l := range rec.Loops {
		row.LoopIP = append(row.LoopIP, l.IP.String())
		row.LoopTTLs = append(row.LoopTTLs, l.TTLs)
		if _, ok := loopOf[l.IP]; ok {
			continue
		}
		if loopOf == nil {
			loopOf = map[netip.Addr]loop{}
		}
		textLen := len(l.TTLs) - 1
		for _, ttl := range l.TTLs {
			textLen += len(strconv.Itoa(ttl))
		}
		loopOf[l.IP] = loop{l.TTLs, textLen}
	}
	cost = n * hopRowBytes
	for i, h := range rec.Hops {
		row.HopTTL[i] = h.TTL
		row.HopStatus[i] = string(h.Status)
		row.HopCode[i] = -1
		row.HopMPLS[i] = h.MPLS
		if row.HopMPLS[i] == nil {
			row.HopMPLS[i] = []record.MPLSLabel{}
		}
		row.HopLoopTTLs[i] = noTTLs
		if h.Code != nil {
			row.HopCode[i] = *h.Code
		}
		if !h.IP.IsValid() {
			continue
		}
		if l, ok := loopOf[h.IP]; ok {
			row.HopLoopTTLs[i] = l.ttls
			cost += l.textLen
		}
		row.HopIP[i] = h.IP.String()
		row.HopRTT[i] = uint32(min(h.RTTMicros, math.MaxUint32)) //nolint:gosec // clamped, validated non-negative
		row.HopASN[i], row.HopASName[i] = en.ASN(h.IP)
		row.HopName[i] = en.Name(h.IP)
	}
	b, err = json.Marshal(row)
	return b, cost + len(b), err
}

// pathChangeRow is one row of the path_changes table.
type pathChangeRow struct {
	TS           string   `json:"ts"`
	AgentID      string   `json:"agent_id"`
	Target       string   `json:"target"`
	Protocol     string   `json:"protocol"`
	FlowID       int      `json:"flow_id"`
	Seq          uint64   `json:"seq"`
	Reason       string   `json:"reason"`
	TTLs         []int    `json:"ttls"`
	FromIPs      []string `json:"from_ips"`
	ToIPs        []string `json:"to_ips"`
	PrevSeq      uint64   `json:"prev_seq"`
	PrevPathHash string   `json:"prev_path_hash"`
	PathHash     string   `json:"path_hash"`
	PrevPathLen  int      `json:"prev_path_len"`
	PathLen      int      `json:"path_len"`
}

// outageRow is one row of the outages table.
type outageRow struct {
	AgentID    string  `json:"agent_id"`
	Target     string  `json:"target"`
	Protocol   string  `json:"protocol"`
	FlowID     int     `json:"flow_id"`
	StartSeq   uint64  `json:"start_seq"`
	StartTS    string  `json:"start_ts"`
	EndSeq     uint64  `json:"end_seq"`
	EndTS      *string `json:"end_ts"`
	Cycles     int     `json:"cycles"`
	LastHopTTL int     `json:"last_hop_ttl"`
	LastHopIP  string  `json:"last_hop_ip"`
	Version    uint64  `json:"version"`
	// pend is what storeRecords decided when this close row was first
	// lost; it is kept with the row while the row is pending.
	pend pendKind
}

// pendKind classifies a close row that could not be stored.
type pendKind uint8

const (
	pendNew pendKind = iota // not lost yet
	// pendOwn: the outage opened and closed within the part whose insert
	// failed, which wrote no other row of it: storage has nothing of it.
	pendOwn
	// pendKeep: the outage started before that part, or storage holds it
	// open (an earlier derivation of the part, before a crash or a failed
	// cycles insert, wrote its open row: heldOpen), so the close row must
	// not be dropped.
	pendKeep
)

func addrStrings(a []netip.Addr) []string {
	out := make([]string, len(a))
	for i, ip := range a {
		out[i] = ip.String()
	}
	return out
}

// lastResponding returns the highest TTL that answered and its address.
func lastResponding(hops []record.Hop) (int, string) {
	for i := len(hops) - 1; i >= 0; i-- {
		if hops[i].Status.Responded() && hops[i].IP.IsValid() {
			return hops[i].TTL, hops[i].IP.String()
		}
	}
	return 0, ""
}

func tsPtr(t time.Time) *string {
	s := clickhouse.Time(t)
	return &s
}
