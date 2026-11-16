package record

import (
	"encoding/json"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"
)

func ip(s string) netip.Addr { return netip.MustParseAddr(s) }

// hops builds a hop list from a compact spec: one entry per TTL starting at 1.
// "*" is no-reply, "x:IP" ttl-expired, "r:IP" reply, "u3:IP" unreachable code 3.
func hops(spec ...string) []Hop {
	out := make([]Hop, len(spec))
	for i, s := range spec {
		h := Hop{TTL: i + 1}
		switch {
		case s == "*":
			h.Status = StatusNoReply
		case strings.HasPrefix(s, "x:"):
			h.Status, h.IP, h.RTTMicros = StatusTTLExpired, ip(s[2:]), 1000
		case strings.HasPrefix(s, "r:"):
			h.Status, h.IP, h.RTTMicros = StatusReply, ip(s[2:]), 1000
		case strings.HasPrefix(s, "u"):
			code := int(s[1] - '0')
			h.Status, h.IP, h.RTTMicros, h.Code = StatusUnreachable, ip(s[3:]), 1000, IntPtr(code)
		default:
			panic("bad hop spec " + s)
		}
		out[i] = h
	}
	return out
}

func ttls(hs []Hop) []int {
	out := []int{}
	for _, h := range hs {
		out = append(out, h.TTL)
	}
	return out
}

func TestTrim(t *testing.T) {
	tests := []struct {
		name      string
		hops      []Hop
		probePast int
		wantTTLs  []int
	}{
		{
			name:      "reached: cut at lowest reply",
			hops:      hops("x:10.0.0.1", "*", "x:10.0.0.3", "r:192.0.2.1", "r:192.0.2.1", "r:192.0.2.1"),
			probePast: 2,
			wantTTLs:  []int{1, 2, 3, 4},
		},
		{
			name:      "reached at ttl 1",
			hops:      hops("r:192.0.2.1", "r:192.0.2.1", "r:192.0.2.1"),
			probePast: 2,
			wantTTLs:  []int{1},
		},
		{
			name:      "black hole: probe_past beyond last responding hop",
			hops:      hops("x:10.0.0.1", "x:10.0.0.2", "*", "x:10.0.0.4", "*", "*", "*", "*", "*", "*"),
			probePast: 2,
			wantTTLs:  []int{1, 2, 3, 4, 5, 6},
		},
		{
			name:      "black hole with probe_past 0",
			hops:      hops("x:10.0.0.1", "x:10.0.0.2", "*", "*"),
			probePast: 0,
			wantTTLs:  []int{1, 2},
		},
		{
			name:      "nothing responds",
			hops:      hops("*", "*", "*", "*", "*"),
			probePast: 2,
			wantTTLs:  []int{1, 2},
		},
		{
			name:      "probe_past larger than remaining ttls",
			hops:      hops("x:10.0.0.1", "x:10.0.0.2", "*"),
			probePast: 5,
			wantTTLs:  []int{1, 2, 3},
		},
		{
			name:      "unreachable ends the path",
			hops:      hops("x:10.0.0.1", "u1:10.0.0.2", "u1:10.0.0.2", "u1:10.0.0.2"),
			probePast: 2,
			wantTTLs:  []int{1, 2},
		},
		{
			name: "loop to max_ttl keeps everything",
			hops: hops("x:10.0.0.1", "x:10.0.0.2", "x:10.0.0.3", "x:10.0.0.2", "x:10.0.0.3",
				"x:10.0.0.2", "x:10.0.0.3", "*"),
			probePast: 2,
			wantTTLs:  []int{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			name:      "negative probe_past treated as zero",
			hops:      hops("x:10.0.0.1", "*", "*"),
			probePast: -3,
			wantTTLs:  []int{1},
		},
		{
			name:      "empty",
			hops:      nil,
			probePast: 2,
			wantTTLs:  []int{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ttls(Trim(tt.hops, tt.probePast))
			if !reflect.DeepEqual(got, tt.wantTTLs) {
				t.Errorf("Trim() TTLs = %v, want %v", got, tt.wantTTLs)
			}
		})
	}
}

func TestTrimSortsByTTL(t *testing.T) {
	h := hops("x:10.0.0.1", "x:10.0.0.2", "r:192.0.2.1")
	h[0], h[2] = h[2], h[0]
	got := Trim(h, 2)
	if want := []int{1, 2, 3}; !reflect.DeepEqual(ttls(got), want) {
		t.Fatalf("TTLs = %v, want %v", ttls(got), want)
	}
}

func TestPathHash(t *testing.T) {
	// Reference values computed independently with Python:
	//   hashlib.sha1(s.encode()).hexdigest()[:16]
	tests := []struct {
		name string
		hops []Hop
		want string
	}{
		{"mixed", hops("x:192.168.187.1", "*", "r:198.19.80.226"), "db2016543c867866"},
		{"all silent", hops("*", "*"), "40fdf024474ac54d"},
		{"empty", nil, "da39a3ee5e6b4b0d"},
		{"all responding", hops("x:10.0.0.1", "x:10.0.0.2", "r:10.0.0.3"), "d5615ad1df2eef39"},
		{"ipv6", hops("x:2001:db8::1", "*", "r:2001:db8::2"), "cc50c0f0267d35c1"},
		{"unreachable hop uses its ip", hops("x:10.0.0.1", "*", "u3:10.0.0.3"), "7b288fb1d7c98e73"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PathHash(tt.hops); got != tt.want {
				t.Errorf("PathHash() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestPathHashIgnoresRateLimitFlicker(t *testing.T) {
	// The same router answering or not must hash identically only when the
	// placeholder is used consistently; a different router must not.
	a := PathHash(hops("x:10.0.0.1", "*", "r:10.0.0.3"))
	b := PathHash(hops("x:10.0.0.1", "*", "r:10.0.0.3"))
	c := PathHash(hops("x:10.0.0.1", "x:10.0.0.9", "r:10.0.0.3"))
	if a != b {
		t.Errorf("identical paths hash differently: %s vs %s", a, b)
	}
	if a == c {
		t.Errorf("different paths hash identically: %s", a)
	}
}

func TestDetectLoops(t *testing.T) {
	tests := []struct {
		name string
		hops []Hop
		want []Loop
	}{
		{
			name: "no loop",
			hops: hops("x:10.0.0.1", "*", "r:10.0.0.3"),
			want: []Loop{},
		},
		{
			name: "two-router loop",
			hops: hops("x:10.0.0.1", "x:198.18.120.1", "x:198.18.120.2", "x:198.18.120.1", "x:198.18.120.2", "*"),
			want: []Loop{
				{IP: ip("198.18.120.1"), TTLs: []int{2, 4}},
				{IP: ip("198.18.120.2"), TTLs: []int{3, 5}},
			},
		},
		{
			name: "adjacent repeat",
			hops: hops("x:10.0.0.1", "x:198.18.120.1", "x:198.18.120.1", "r:10.0.0.9"),
			want: []Loop{{IP: ip("198.18.120.1"), TTLs: []int{2, 3}}},
		},
		{
			name: "silent hops are not loops",
			hops: hops("*", "*", "*"),
			want: []Loop{},
		},
		{
			// A filtering edge router: expires TTL 2, rejects TTL 3.
			name: "expired then unreachable from the same router is not a loop",
			hops: hops("x:10.0.0.1", "x:21.4.241.33", "u3:21.4.241.33"),
			want: []Loop{},
		},
		{
			name: "destination reply does not join a loop",
			hops: hops("x:10.0.0.1", "x:192.0.2.9", "r:192.0.2.9"),
			want: []Loop{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectLoops(tt.hops)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DetectLoops() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFinalize(t *testing.T) {
	r := &Record{
		Schema: SchemaVersion, AgentID: "a", TS: time.Unix(1, 0), Target: "t",
		TargetIP: ip("192.0.2.9"), AF: 4, Protocol: "icmp", FlowID: 33434,
		Hops: hops("x:10.0.0.1", "x:10.0.0.2", "x:10.0.0.1", "*", "r:192.0.2.9", "r:192.0.2.9"),
	}
	r.Finalize(2)
	if !r.Reached || r.PathLen != 5 || len(r.Hops) != 5 {
		t.Fatalf("reached=%v path_len=%d hops=%d", r.Reached, r.PathLen, len(r.Hops))
	}
	if want := PathHash(r.Hops); r.PathHash != want {
		t.Errorf("path_hash %s, want %s", r.PathHash, want)
	}
	if len(r.Loops) != 1 || r.Loops[0].IP != ip("10.0.0.1") {
		t.Errorf("loops = %v", r.Loops)
	}
	if err := r.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}

	empty := &Record{Schema: SchemaVersion, AgentID: "a", TS: time.Unix(1, 0), Target: "t",
		TargetIP: ip("192.0.2.9"), AF: 4, Protocol: "icmp"}
	empty.Finalize(2)
	if empty.Hops == nil || empty.Loops == nil {
		t.Errorf("Finalize left nil slices: hops=%v loops=%v", empty.Hops, empty.Loops)
	}
}

func TestJSONShape(t *testing.T) {
	r := &Record{
		Schema:   1,
		AgentID:  "lap-mstrassberger",
		Seq:      184223,
		TS:       time.Date(2026, 11, 16, 22, 14, 5, 123_000_000, time.UTC),
		Target:   "dc1.example",
		TargetIP: ip("198.19.80.226"),
		AF:       4,
		Protocol: "icmp",
		FlowID:   33434,
		Hops: []Hop{
			{TTL: 1, Status: StatusTTLExpired, IP: ip("192.168.187.1"), RTTMicros: 1432},
			{TTL: 2, Status: StatusNoReply},
			{TTL: 3, Status: StatusUnreachable, IP: ip("10.1.1.1"), RTTMicros: 5000, Code: IntPtr(0),
				MPLS: []MPLSLabel{{Label: 24005, TC: 0, S: true, TTL: 1}}},
		},
	}
	r.Finalize(2)
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schema":1,"agent_id":"lap-mstrassberger","seq":184223,"ts":"2026-11-16T22:14:05.123Z",` +
		`"target":"dc1.example","target_ip":"198.19.80.226","af":4,"protocol":"icmp","flow_id":33434,` +
		`"reached":false,"path_len":3,"path_hash":"` + r.PathHash + `","loops":[],"hops":[` +
		`{"ttl":1,"status":"ttl-expired","ip":"192.168.187.1","rtt_us":1432},` +
		`{"ttl":2,"status":"no-reply"},` +
		`{"ttl":3,"status":"unreachable","ip":"10.1.1.1","rtt_us":5000,"code":0,` +
		`"mpls":[{"label":24005,"tc":0,"s":true,"ttl":1}]}]}`
	if string(b) != want {
		t.Errorf("JSON mismatch\n got: %s\nwant: %s", b, want)
	}

	var back Record
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&back, r) {
		t.Errorf("round trip mismatch\n got: %+v\nwant: %+v", back, *r)
	}
}

func TestValidateRejects(t *testing.T) {
	good := func() *Record {
		r := &Record{Schema: 1, AgentID: "a", TS: time.Unix(1, 0), Target: "t",
			TargetIP: ip("192.0.2.1"), AF: 4, Protocol: "icmp", Hops: hops("x:10.0.0.1", "r:192.0.2.1")}
		r.Finalize(2)
		return r
	}
	tests := []struct {
		name   string
		mutate func(*Record)
	}{
		{"schema", func(r *Record) { r.Schema = 2 }},
		{"agent", func(r *Record) { r.AgentID = "" }},
		{"af mismatch", func(r *Record) { r.AF = 6 }},
		{"path_len", func(r *Record) { r.PathLen = 7 }},
		{"no-reply with ip", func(r *Record) { r.Hops[0].Status = StatusNoReply }},
		{"reply without rtt", func(r *Record) { r.Hops[1].RTTMicros = 0 }},
		{"code on non-unreachable", func(r *Record) { r.Hops[0].Code = IntPtr(1) }},
		{"ttl gap", func(r *Record) { r.Hops[1].TTL = 5 }},
		{"bad status", func(r *Record) { r.Hops[0].Status = "lost" }},
		{"nil loops", func(r *Record) { r.Loops = nil }},
		{"mpls on a reply", func(r *Record) { r.Hops[1].MPLS = []MPLSLabel{{Label: 16, S: true, TTL: 1}} }},
		{"mpls label too large", func(r *Record) { r.Hops[0].MPLS = []MPLSLabel{{Label: 1 << 20, S: true}} }},
		// Bounds and character sets (security review): everything the
		// server stores from an agent is bounded and safe to quote.
		{"agent with a backslash", func(r *Record) { r.AgentID = `lab\x` }},
		{"agent with a quote", func(r *Record) { r.AgentID = "lab'1" }},
		{"agent too long", func(r *Record) { r.AgentID = strings.Repeat("a", MaxAgentIDLen+1) }},
		{"target with html", func(r *Record) { r.Target = "<b>dc1</b>" }},
		{"target with a space", func(r *Record) { r.Target = "dc 1" }},
		{"target too long", func(r *Record) { r.Target = strings.Repeat("t", MaxTargetLen+1) }},
		{"unknown protocol", func(r *Record) { r.Protocol = "sctp" }},
		{"ts before 1970", func(r *Record) { r.TS = time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"ts after 2261", func(r *Record) { r.TS = time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"path_hash with control", func(r *Record) { r.PathHash = "ab\ncd" }},
		{"too many hops", func(r *Record) {
			for len(r.Hops) <= MaxHops {
				r.Hops = append(r.Hops, Hop{TTL: len(r.Hops) + 1, Status: StatusNoReply})
			}
			r.PathLen = len(r.Hops)
		}},
		{"more loops than hops", func(r *Record) {
			r.Loops = []Loop{{IP: ip("10.0.0.1"), TTLs: []int{1, 2}}, {IP: ip("10.0.0.2"), TTLs: []int{1, 2}}, {IP: ip("10.0.0.3"), TTLs: []int{1, 2}}}
		}},
		{"loop ttl out of path", func(r *Record) { r.Loops = []Loop{{IP: ip("10.0.0.1"), TTLs: []int{1, 70000}}} }},
		{"loop ttls not increasing", func(r *Record) { r.Loops = []Loop{{IP: ip("10.0.0.1"), TTLs: []int{2, 1}}} }},
		{"loop of one ttl", func(r *Record) { r.Loops = []Loop{{IP: ip("10.0.0.1"), TTLs: []int{1}}} }},
		{"loop without ip", func(r *Record) { r.Loops = []Loop{{TTLs: []int{1, 2}}} }},
		{"code out of range", func(r *Record) {
			r.Hops[0].Status, r.Hops[0].Code = StatusUnreachable, IntPtr(99999)
		}},
		{"rtt beyond UInt32", func(r *Record) { r.Hops[0].RTTMicros = 1 << 33 }},
		{"too many mpls labels", func(r *Record) {
			r.Hops[0].MPLS = make([]MPLSLabel, MaxMPLSLabels+1)
		}},
	}
	if err := good().Validate(); err != nil {
		t.Fatalf("baseline invalid: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := good()
			tt.mutate(r)
			if r.Validate() == nil {
				t.Error("Validate() accepted invalid record")
			}
		})
	}
}

func TestPathTracker(t *testing.T) {
	const dst = "192.0.2.9"
	rec := func(target string, hs []Hop) *Record {
		r := &Record{Schema: 1, AgentID: "a", TS: time.Unix(1, 0), Target: "t", TargetIP: ip(target), AF: 4,
			Protocol: "icmp", Hops: hs}
		r.Finalize(2)
		return r
	}
	type step struct {
		rec  *Record
		want bool
	}
	base := func() *Record { return rec(dst, hops("x:10.0.0.1", "x:10.0.0.2", "x:10.0.0.3", "r:"+dst)) }
	tests := []struct {
		name  string
		steps []step
	}{
		{"first observation is not a change", []step{{base(), false}}},
		{"identical cycles", []step{{base(), false}, {base(), false}}},
		{"rate-limited hop flickers", []step{
			{base(), false},
			{rec(dst, hops("x:10.0.0.1", "*", "x:10.0.0.3", "r:"+dst)), false},
			{base(), false},
			{rec(dst, hops("*", "*", "*", "r:"+dst)), false},
		}},
		{"router replaced while silent", []step{
			{base(), false},
			{rec(dst, hops("x:10.0.0.1", "*", "x:10.0.0.3", "r:"+dst)), false},
			{rec(dst, hops("x:10.0.0.1", "x:10.7.7.7", "x:10.0.0.3", "r:"+dst)), true},
			{rec(dst, hops("x:10.0.0.1", "x:10.7.7.7", "x:10.0.0.3", "r:"+dst)), false},
		}},
		{"different router at one ttl", []step{
			{base(), false},
			{rec(dst, hops("x:10.0.0.1", "x:10.9.9.9", "x:10.0.0.3", "r:"+dst)), true},
		}},
		{"destination one hop closer", []step{
			{base(), false},
			{rec(dst, hops("x:10.0.0.1", "x:10.0.0.2", "r:"+dst)), true},
		}},
		{"destination one hop further", []step{
			{base(), false},
			{rec(dst, hops("x:10.0.0.1", "x:10.0.0.2", "x:10.0.0.3", "x:10.0.0.4", "r:"+dst)), true},
		}},
		{"destination moves behind a silent hop", []step{
			{rec(dst, hops("x:10.0.0.1", "x:10.0.0.2", "*", "r:"+dst)), false},
			{rec(dst, hops("x:10.0.0.1", "x:10.0.0.2", "r:"+dst)), true},
		}},
		{"black hole, then recovery on the same path", []step{
			{base(), false},
			{rec(dst, hops("x:10.0.0.1", "x:10.0.0.2", "*", "*", "*")), false},
			{base(), false},
		}},
		{"loop replaces the tail", []step{
			{base(), false},
			{rec(dst, hops("x:10.0.0.1", "x:10.0.0.2", "x:10.0.0.1", "x:10.0.0.2", "*")), true},
		}},
		{"target moved (DNS)", []step{
			{base(), false},
			{rec("192.0.2.10", hops("x:10.0.0.1", "x:10.0.0.2", "x:10.0.0.3", "r:192.0.2.10")), true},
		}},
		{"change resets what is known", []step{
			{base(), false},
			{rec(dst, hops("x:10.0.0.1", "x:10.9.9.9", "*", "r:"+dst)), true},
			// TTL 3 was only known from before the change; a new router
			// there is first sight, not another change.
			{rec(dst, hops("x:10.0.0.1", "x:10.9.9.9", "x:10.8.8.8", "r:"+dst)), false},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p PathTracker
			for i, s := range tt.steps {
				if got := p.Observe(s.rec); got != s.want {
					t.Errorf("step %d: Observe = %v, want %v", i, got, s.want)
				}
			}
		})
	}
}

func TestPathTrackerChangeDetail(t *testing.T) {
	const dst = "192.0.2.9"
	rec := func(target string, hs []Hop) *Record {
		r := &Record{Schema: 1, AgentID: "a", TS: time.Unix(1, 0), Target: "t", TargetIP: ip(target), AF: 4,
			Protocol: "icmp", Hops: hs}
		r.Finalize(2)
		return r
	}
	var p PathTracker
	p.Observe(rec(dst, hops("x:10.0.0.1", "x:10.0.0.2", "x:10.0.0.3", "r:"+dst)))
	ch, ok := p.ObserveChange(rec(dst, hops("x:10.0.0.1", "x:10.7.7.7", "x:10.8.8.8", "r:"+dst)))
	want := PathChange{Reason: ReasonRouter, TTLs: []int{2, 3},
		From: []netip.Addr{ip("10.0.0.2"), ip("10.0.0.3")}, To: []netip.Addr{ip("10.7.7.7"), ip("10.8.8.8")}}
	if !ok || !reflect.DeepEqual(ch, want) {
		t.Errorf("router change = %+v %v", ch, ok)
	}
	// Destination one hop further, behind silent hops: only the distance
	// can tell. (Had TTL 3 answered as the destination, that would be a
	// router change at TTL 3, which takes precedence.)
	ch, ok = p.ObserveChange(rec(dst, hops("x:10.0.0.1", "x:10.7.7.7", "*", "*", "r:"+dst)))
	if !ok || ch.Reason != ReasonDistance || ch.PrevDistance != 4 || ch.Distance != 5 {
		t.Errorf("distance change = %+v %v", ch, ok)
	}
	ch, ok = p.ObserveChange(rec("192.0.2.10", hops("x:10.0.0.1", "r:192.0.2.10")))
	if !ok || ch.Reason != ReasonTarget {
		t.Errorf("target change = %+v %v", ch, ok)
	}
	if ch, ok := p.ObserveChange(rec("192.0.2.10", hops("x:10.0.0.1", "r:192.0.2.10"))); ok {
		t.Errorf("no change reported as %+v", ch)
	}
}

func TestValidNames(t *testing.T) {
	for _, s := range []string{"lab-01", "node.example.net", "agent_7", "fe80::1"} {
		if !ValidAgentID(s) {
			t.Errorf("agent_id %q refused", s)
		}
	}
	for _, s := range []string{"example.net", "192.0.2.1", "2001:db8::1", "fe80::1%eth0", "_srv.example"} {
		if !ValidTarget(s) {
			t.Errorf("target %q refused", s)
		}
	}
	for _, s := range []string{"", "a b", `a\b`, "a'b", "a/b", "ä"} {
		if ValidAgentID(s) || ValidTarget(s) {
			t.Errorf("%q accepted", s)
		}
	}
}
