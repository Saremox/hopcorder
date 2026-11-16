package probe

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/record"
)

func summary(res []HopResult) string {
	parts := make([]string, len(res))
	for i, h := range res {
		switch h.Status {
		case record.StatusNoReply:
			parts[i] = "*"
		case record.StatusUnreachable:
			parts[i] = fmt.Sprintf("!%d:%s", h.Code, h.From)
		case record.StatusReply:
			parts[i] = "=" + h.From.String()
		default:
			parts[i] = h.From.String()
		}
	}
	return strings.Join(parts, " ")
}

func fakeHop(ips ...string) FakeHop {
	return FakeHop{IPs: addrs(ips...)}
}

func TestFakeScenarios(t *testing.T) {
	target := netip.MustParseAddr("198.51.100.7")
	base := time.Date(2026, 11, 16, 23, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		scenarios []Scenario
		req       TraceRequest
		cycles    []string // expected summary per consecutive cycle
		late      uint64
	}{
		{
			name:      "reachable path",
			scenarios: []Scenario{{Hops: []FakeHop{fakeHop("10.0.0.1"), fakeHop("10.0.0.2")}, Reachable: true}},
			cycles:    []string{"10.0.0.1 10.0.0.2 =198.51.100.7 =198.51.100.7"},
		},
		{
			name: "rate limited hop answers every other cycle",
			scenarios: []Scenario{{Hops: []FakeHop{fakeHop("10.0.0.1"),
				{IPs: addrs("10.0.0.2"), ReplyEvery: 2}}, Reachable: true}},
			cycles: []string{
				"10.0.0.1 10.0.0.2 =198.51.100.7 =198.51.100.7",
				"10.0.0.1 * =198.51.100.7 =198.51.100.7",
				"10.0.0.1 10.0.0.2 =198.51.100.7 =198.51.100.7",
			},
		},
		{
			name:      "ecmp stable in paris mode",
			scenarios: []Scenario{{Hops: []FakeHop{fakeHop("10.0.0.1", "10.0.0.2"), fakeHop("10.0.0.3", "10.0.0.4")}, Reachable: true}},
			req:       TraceRequest{Paris: true, FlowID: 1},
			cycles: []string{
				"10.0.0.2 10.0.0.4 =198.51.100.7 =198.51.100.7",
				"10.0.0.2 10.0.0.4 =198.51.100.7 =198.51.100.7",
			},
		},
		{
			name:      "ecmp flaps in classic mode",
			scenarios: []Scenario{{Hops: []FakeHop{fakeHop("10.0.0.1", "10.0.0.2"), fakeHop("10.0.0.3", "10.0.0.4")}, Reachable: true}},
			req:       TraceRequest{Paris: false, FlowID: 1},
			cycles: []string{
				"10.0.0.2 10.0.0.3 =198.51.100.7 =198.51.100.7",
				"10.0.0.1 10.0.0.4 =198.51.100.7 =198.51.100.7",
			},
		},
		{
			name:      "loop",
			scenarios: []Scenario{{Hops: []FakeHop{fakeHop("10.0.0.1")}, Loop: addrs("198.18.120.1", "198.18.120.2")}},
			cycles:    []string{"10.0.0.1 198.18.120.1 198.18.120.2 198.18.120.1"},
		},
		{
			name:      "black hole",
			scenarios: []Scenario{{Hops: []FakeHop{fakeHop("10.0.0.1"), {Silent: true}}}},
			cycles:    []string{"10.0.0.1 * * *"},
		},
		{
			name:      "late reply",
			scenarios: []Scenario{{Hops: []FakeHop{fakeHop("10.0.0.1"), {IPs: addrs("10.0.0.2"), Late: true}}, Reachable: true}},
			cycles:    []string{"10.0.0.1 * =198.51.100.7 =198.51.100.7"},
			late:      1,
		},
		{
			name: "unreachable",
			scenarios: []Scenario{{Hops: []FakeHop{fakeHop("10.0.0.1"),
				{IPs: addrs("10.0.0.2"), Unreachable: record.IntPtr(1)}}, Reachable: true}},
			cycles: []string{"10.0.0.1 !1:10.0.0.2 !1:10.0.0.2 !1:10.0.0.2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFake()
			f.Now = func() time.Time { return base }
			f.Set(target, tt.scenarios...)
			req := tt.req
			req.Target, req.Protocol, req.MaxTTL, req.Timeout = target, ICMP, 4, time.Second
			for i, want := range tt.cycles {
				res, err := f.Trace(t.Context(), req)
				if err != nil {
					t.Fatal(err)
				}
				if got := summary(res); got != want {
					t.Errorf("cycle %d:\n got %s\nwant %s", i, got, want)
				}
			}
			if got := f.Stats().LateReplies; got != tt.late {
				t.Errorf("late replies = %d, want %d", got, tt.late)
			}
		})
	}
}

func TestFakePathChangeAtT(t *testing.T) {
	target := netip.MustParseAddr("198.51.100.7")
	t0 := time.Date(2026, 11, 16, 23, 0, 0, 0, time.UTC)
	now := t0
	f := NewFake()
	f.Now = func() time.Time { return now }
	f.Set(target,
		Scenario{From: t0.Add(10 * time.Minute), Hops: []FakeHop{fakeHop("10.0.0.1"), fakeHop("10.9.9.9")}, Reachable: true},
		Scenario{Hops: []FakeHop{fakeHop("10.0.0.1"), fakeHop("10.0.0.2")}, Reachable: true},
	)
	req := TraceRequest{Target: target, Protocol: ICMP, MaxTTL: 3, Timeout: time.Second}
	res, _ := f.Trace(t.Context(), req)
	if got := summary(res); got != "10.0.0.1 10.0.0.2 =198.51.100.7" {
		t.Errorf("before T: %s", got)
	}
	now = t0.Add(10 * time.Minute)
	res, _ = f.Trace(t.Context(), req)
	if got := summary(res); got != "10.0.0.1 10.9.9.9 =198.51.100.7" {
		t.Errorf("at T: %s", got)
	}
}

func TestFakeErrorsAndDuration(t *testing.T) {
	target := netip.MustParseAddr("198.51.100.7")
	f := NewFake()
	req := TraceRequest{Target: target, Protocol: ICMP, MaxTTL: 3, Timeout: time.Second}
	if _, err := f.Trace(t.Context(), req); err == nil {
		t.Error("trace without scenario succeeded")
	}
	f.Set(target, Scenario{Err: fmt.Errorf("x: %w", ErrNoRoute)})
	if _, err := f.Trace(t.Context(), req); !errors.Is(err, ErrNoRoute) {
		t.Errorf("err = %v, want ErrNoRoute", err)
	}
	f.Set(target, Scenario{Duration: time.Hour})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := f.Trace(ctx, req); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want deadline exceeded", err)
	}
	if got := len(f.Requests()); got != 3 {
		t.Errorf("recorded %d requests, want 3", got)
	}
}
