package output

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/agent"
	"github.com/saremox/hopcorder/internal/probe"
	"github.com/saremox/hopcorder/internal/record"
	"github.com/saremox/hopcorder/internal/spool"
)

type fakeSpoolStats struct{}

func (fakeSpoolStats) Stats() spool.Stats {
	return spool.Stats{Bytes: 4096, Records: 7, Dropped: 3, LastSeq: 99}
}
func (fakeSpoolStats) Pending(string) uint64 { return 5 }

type fakeProbeStats struct{}

func (fakeProbeStats) Stats() probe.Stats {
	return probe.Stats{ProbesSent: 120, LateReplies: 2, MismatchedReplies: 3}
}

func mkRecord(hops ...string) *record.Record {
	r := &record.Record{Schema: 1, AgentID: "a", TS: time.Now(), Target: "t", TargetIP: netip.MustParseAddr("192.0.2.9"),
		AF: 4, Protocol: "icmp"}
	for i, h := range hops {
		hop := record.Hop{TTL: i + 1, Status: record.StatusNoReply}
		switch {
		case strings.HasPrefix(h, "="):
			hop.Status, hop.IP, hop.RTTMicros = record.StatusReply, netip.MustParseAddr(h[1:]), 25000
		case h != "*":
			hop.Status, hop.IP, hop.RTTMicros = record.StatusTTLExpired, netip.MustParseAddr(h), 1000
		}
		r.Hops = append(r.Hops, hop)
	}
	r.Finalize(2)
	return r
}

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func TestMetrics(t *testing.T) {
	targets := []agent.Target{{Host: "dc1.example", Protocol: probe.ICMP}, {Host: "dc2.example", Protocol: probe.UDP}}
	m := NewMetrics(MetricsOptions{AgentID: "lab", Version: "v0", Targets: targets, Outputs: []string{"http"},
		Spool: fakeSpoolStats{}, Prober: fakeProbeStats{}})
	t1 := &targets[0]
	m.CycleDone(t1, mkRecord("10.0.0.1", "10.0.0.2", "=192.0.2.9"))
	m.CycleDone(t1, mkRecord("10.0.0.1", "*", "=192.0.2.9"))                        // rate-limited flicker: not a change
	m.CycleDone(t1, mkRecord("10.0.0.1", "10.7.7.7", "=192.0.2.9"))                 // path change
	m.CycleDone(t1, mkRecord("10.0.0.1", "198.18.1.1", "198.18.1.2", "198.18.1.1")) // loop, unreached, change
	m.CycleOverrun(t1)
	m.CycleFailed(t1, errors.New("trace: no route"))
	m.CycleFailed(&targets[1], fmt.Errorf("trace: %w", probe.ErrFlowInUse))
	m.ResolveFailed(&targets[1])
	m.OutputError("http")(nil)

	out := scrape(t, m.Handler())
	for _, want := range []string{
		`hopcorder_cycles_total{agent_id="lab",protocol="icmp",result="reached",target="dc1.example"} 3`,
		`hopcorder_cycles_total{agent_id="lab",protocol="icmp",result="unreached",target="dc1.example"} 1`,
		`hopcorder_cycles_total{agent_id="lab",protocol="udp",result="reached",target="dc2.example"} 0`,
		`hopcorder_path_changes_total{agent_id="lab",protocol="icmp",target="dc1.example"} 2`,
		`hopcorder_loops_detected_total{agent_id="lab",protocol="icmp",target="dc1.example"} 1`,
		`hopcorder_path_length{agent_id="lab",protocol="icmp",target="dc1.example"} 4`,
		`hopcorder_e2e_rtt_seconds_count{agent_id="lab",protocol="icmp",target="dc1.example"} 3`,
		`hopcorder_e2e_rtt_seconds_bucket{agent_id="lab",protocol="icmp",target="dc1.example",le="0.025"} 3`,
		`hopcorder_cycle_overruns_total{agent_id="lab",protocol="icmp",target="dc1.example"} 1`,
		`hopcorder_cycle_errors_total{agent_id="lab",protocol="icmp",target="dc1.example"} 1`,
		`hopcorder_cycle_errors_total{agent_id="lab",protocol="udp",target="dc2.example"} 1`,
		`hopcorder_flow_in_use{agent_id="lab",protocol="icmp",target="dc1.example"} 0`,
		`hopcorder_flow_in_use{agent_id="lab",protocol="udp",target="dc2.example"} 1`,
		`hopcorder_resolve_errors_total{agent_id="lab",protocol="udp",target="dc2.example"} 1`,
		`hopcorder_output_errors_total{agent_id="lab",output="http"} 1`,
		`hopcorder_output_pending_records{agent_id="lab",output="http"} 5`,
		`hopcorder_spool_bytes{agent_id="lab"} 4096`,
		`hopcorder_spool_records{agent_id="lab"} 7`,
		`hopcorder_spool_records_dropped_total{agent_id="lab"} 3`,
		`hopcorder_probe_late_replies_total{agent_id="lab"} 2`,
		`hopcorder_probes_sent_total{agent_id="lab"} 120`,
		`hopcorder_probe_mismatched_replies_total{agent_id="lab"} 3`,
		`hopcorder_build_info{agent_id="lab",goversion=`,
		`go_goroutines `,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics lack %q", want)
		}
	}
	// Bounded labels: no hop address may ever appear in the exposition.
	for _, ip := range []string{"10.0.0.1", "10.0.0.2", "10.7.7.7", "198.18.1.1", "192.0.2.9"} {
		if strings.Contains(out, ip) {
			t.Errorf("hop address %s leaked into metrics", ip)
		}
	}
}

func TestMetricsServer(t *testing.T) {
	// Find a free port, then serve on it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	quiet := slog.New(slog.DiscardHandler)
	s, err := ListenMetrics(t.Context(), addr, quiet)
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string) (int, string) {
		t.Helper()
		resp, err := http.Get("http://" + addr + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode, string(body)
	}
	if code, _ := get("/metrics"); code != http.StatusServiceUnavailable {
		t.Errorf("without metrics: status %d, want 503", code)
	}
	// What is served changes; the socket stays.
	s.Serve("/metrics", NewMetrics(MetricsOptions{AgentID: "one"}))
	if code, body := get("/metrics"); code != http.StatusOK || !strings.Contains(body, `hopcorder_build_info{agent_id="one"`) {
		t.Errorf("status %d body %.200s", code, body)
	}
	s.Serve("/m2", NewMetrics(MetricsOptions{AgentID: "two"}))
	if code, body := get("/m2"); code != http.StatusOK || !strings.Contains(body, `hopcorder_build_info{agent_id="two"`) {
		t.Errorf("after the swap: status %d body %.200s", code, body)
	}
	if code, _ := get("/metrics"); code != http.StatusNotFound {
		t.Errorf("old path after the swap: status %d", code)
	}
	if s.Addr() != addr {
		t.Errorf("Addr() = %s", s.Addr())
	}
	if _, err := ListenMetrics(t.Context(), addr, quiet); err == nil {
		t.Error("second listener on the same address succeeded")
	}
	s.Close()
	// After Close the address is free again immediately.
	s2, err := ListenMetrics(t.Context(), addr, quiet)
	if err != nil {
		t.Fatalf("rebind after Close: %v", err)
	}
	s2.Close()
}
