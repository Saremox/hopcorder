// Package agent runs the probing schedule: one goroutine per target, start
// offsets staggered across the interval, periodic DNS re-resolution, and
// assembly of each cycle's probe results into a wire record. See
// docs/HANDOFF.md §5.1.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/saremox/hopcorder/internal/probe"
	"github.com/saremox/hopcorder/internal/record"
)

// Target is one destination with its effective (defaulted) settings.
type Target struct {
	Host      string
	Protocol  probe.Protocol
	Port      uint16
	FlowID    uint16
	Paris     bool
	AF        int // 4, 6, or 0 to prefer IPv4 when a name has both
	Interval  time.Duration
	Timeout   time.Duration
	MaxTTL    int
	ProbePast int
	// AdaptiveTTL probes only a few TTLs past the destination's last known
	// distance instead of up to MaxTTL; see runner.cycle.
	AdaptiveTTL bool
}

// adaptiveMargin is how many TTLs beyond the destination's last distance an
// adaptive cycle probes.
const adaptiveMargin = 3

// Sink receives every finished record. Publish must make the record durable
// (the spool assigns rec.Seq) before returning.
type Sink interface {
	Publish(ctx context.Context, rec *record.Record) error
}

// Observer is notified about cycles, for metrics. Implementations must be
// safe for concurrent use.
type Observer interface {
	CycleDone(t *Target, rec *record.Record)
	CycleFailed(t *Target)
	CycleOverrun(t *Target)
	ResolveFailed(t *Target)
}

// Resolver looks up target addresses; *net.Resolver implements it.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Options configures an Agent.
type Options struct {
	AgentID  string
	Targets  []Target
	DNSTTL   time.Duration
	Prober   probe.Prober
	Sink     Sink
	Observer Observer
	Resolver Resolver
	Logger   *slog.Logger
}

// Agent schedules cycles for all targets.
type Agent struct {
	opts Options
	log  *slog.Logger
}

// New validates opts and returns an Agent.
func New(opts Options) (*Agent, error) {
	if opts.AgentID == "" {
		return nil, errors.New("agent: empty agent id")
	}
	if opts.Prober == nil {
		return nil, errors.New("agent: no prober")
	}
	if len(opts.Targets) == 0 {
		return nil, errors.New("agent: no targets")
	}
	for i, t := range opts.Targets {
		if t.Interval <= 0 || t.Timeout <= 0 || t.MaxTTL <= 0 {
			return nil, fmt.Errorf("agent: target %d (%s): interval, timeout and max ttl must be positive", i, t.Host)
		}
	}
	if opts.DNSTTL <= 0 {
		opts.DNSTTL = 5 * time.Minute
	}
	if opts.Resolver == nil {
		opts.Resolver = net.DefaultResolver
	}
	if opts.Observer == nil {
		opts.Observer = nopObserver{}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Agent{opts: opts, log: opts.Logger}, nil
}

// Run probes every target on its interval until ctx is cancelled. In-flight
// cycles are abandoned on cancellation. Run requires a Sink.
func (a *Agent) Run(ctx context.Context) error {
	if a.opts.Sink == nil {
		return errors.New("agent: Run needs a sink")
	}
	var wg sync.WaitGroup
	n := len(a.opts.Targets)
	for i := range a.opts.Targets {
		t := &a.opts.Targets[i]
		// Spread start times evenly across each target's interval so the
		// probes of all targets do not burst out at the same instant.
		offset := t.Interval * time.Duration(i) / time.Duration(n)
		r := newRunner(a, t)
		wg.Go(func() { r.loop(ctx, offset) })
	}
	wg.Wait()
	return nil
}

// RunOnce runs one cycle for every target concurrently and returns the
// records in target order. Records are not published to the sink and keep
// seq 0. Targets that failed are missing from the result and reported in
// the joined error.
func (a *Agent) RunOnce(ctx context.Context) ([]*record.Record, error) {
	recs := make([]*record.Record, len(a.opts.Targets))
	errs := make([]error, len(a.opts.Targets))
	var wg sync.WaitGroup
	for i := range a.opts.Targets {
		t := &a.opts.Targets[i]
		wg.Go(func() {
			r := newRunner(a, t)
			recs[i], errs[i] = r.cycle(ctx, time.Now())
			if errs[i] != nil {
				errs[i] = fmt.Errorf("%s: %w", t.Host, errs[i])
			}
		})
	}
	wg.Wait()
	out := recs[:0]
	for _, r := range recs {
		if r != nil {
			out = append(out, r)
		}
	}
	return out, errors.Join(errs...)
}

// runner owns the schedule and resolved address of one target.
type runner struct {
	a   *Agent
	t   *Target
	log *slog.Logger

	// limit is the highest TTL the next adaptive cycle probes; 0 = MaxTTL.
	// Only the runner's own loop touches it.
	limit int

	mu         sync.Mutex
	ip         netip.Addr
	resolvedAt time.Time
	resolving  bool
	bg         sync.WaitGroup
}

func newRunner(a *Agent, t *Target) *runner {
	return &runner{a: a, t: t, log: a.log.With("target", t.Host, "protocol", string(t.Protocol))}
}

func (r *runner) loop(ctx context.Context, offset time.Duration) {
	defer r.bg.Wait()
	next := time.Now().Add(offset)
	timer := time.NewTimer(time.Until(next))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		start := time.Now()
		rec, err := r.cycle(ctx, start)
		if ctx.Err() != nil {
			return
		}
		switch {
		case err != nil:
			r.a.opts.Observer.CycleFailed(r.t)
			r.log.Warn("cycle failed", "err", err)
		default:
			if err := r.a.opts.Sink.Publish(ctx, rec); err != nil {
				if ctx.Err() != nil {
					return
				}
				r.log.Error("cannot persist record", "err", err)
			}
			r.a.opts.Observer.CycleDone(r.t, rec)
		}

		// The interval is a minimum period: an overrunning cycle is
		// followed immediately by the next, and cycles never queue up.
		next = next.Add(r.t.Interval)
		if now := time.Now(); !next.After(now) {
			r.a.opts.Observer.CycleOverrun(r.t)
			r.log.Debug("cycle overran interval", "took", now.Sub(start), "interval", r.t.Interval)
			next = now
		}
		timer.Reset(time.Until(next))
	}
}

// cycle resolves the target if needed, runs one trace and assembles the
// record (without seq).
func (r *runner) cycle(ctx context.Context, start time.Time) (*record.Record, error) {
	ip, err := r.address(ctx, start)
	if err != nil {
		r.a.opts.Observer.ResolveFailed(r.t)
		return nil, err
	}
	// An adaptive cycle stops a few TTLs past where the destination last
	// answered. If it then neither reaches the destination nor hits an
	// unreachable, the path may have grown, so the same cycle probes the
	// remaining TTLs: the record is always what a full-depth cycle would
	// have recorded, only TTLs beyond the destination are saved.
	limit := r.t.MaxTTL
	if r.t.AdaptiveTTL && r.limit > 0 {
		limit = min(r.limit, r.t.MaxTTL)
	}
	res, err := r.trace(ctx, ip, 1, limit)
	if err != nil {
		return nil, err
	}
	if limit < r.t.MaxTTL && !pathEnded(res) {
		more, err := r.trace(ctx, ip, limit+1, r.t.MaxTTL)
		if err != nil {
			return nil, err
		}
		res = append(res, more...)
	}
	rec := buildRecord(r.a.opts.AgentID, r.t, ip, start, res)
	r.limit = 0
	if rec.Reached {
		r.limit = rec.PathLen + adaptiveMargin
	}
	return rec, nil
}

// trace probes TTLs minTTL..maxTTL. No route (e.g. the uplink is down) is
// an observation, not a gap: it yields silent hops rather than an error.
func (r *runner) trace(ctx context.Context, ip netip.Addr, minTTL, maxTTL int) ([]probe.HopResult, error) {
	res, err := r.a.opts.Prober.Trace(ctx, probe.TraceRequest{
		Target:   ip,
		Protocol: r.t.Protocol,
		Port:     r.t.Port,
		FlowID:   r.t.FlowID,
		Paris:    r.t.Paris,
		MinTTL:   minTTL,
		MaxTTL:   maxTTL,
		Timeout:  r.t.Timeout,
	})
	if err != nil {
		if ctx.Err() != nil || !errors.Is(err, probe.ErrNoRoute) {
			return nil, fmt.Errorf("trace %s: %w", ip, err)
		}
		r.log.Debug("no route to target; recording a silent cycle", "ip", ip, "err", err)
		return probe.NoRepliesRange(minTTL, maxTTL), nil
	}
	return res, nil
}

// pathEnded reports whether some TTL got a destination reply or an
// unreachable, beyond which nothing is recorded.
func pathEnded(res []probe.HopResult) bool {
	for _, h := range res {
		if h.Status == record.StatusReply || h.Status == record.StatusUnreachable {
			return true
		}
	}
	return false
}

// address returns the target address, resolving it on first use and
// re-resolving in the background once it is older than the DNS TTL, so a
// slow or failing resolver never delays a cycle once an address is known.
func (r *runner) address(ctx context.Context, now time.Time) (netip.Addr, error) {
	if ip, err := netip.ParseAddr(r.t.Host); err == nil {
		return ip.Unmap(), nil
	}
	r.mu.Lock()
	ip, stale := r.ip, now.Sub(r.resolvedAt) >= r.a.opts.DNSTTL
	if ip.IsValid() {
		if stale && !r.resolving {
			r.resolving = true
			r.bg.Go(func() {
				// Runs beside the cycles, bounded by its own timeout, and
				// abandoned at shutdown so a dead resolver cannot delay it.
				rctx, cancel := context.WithTimeout(ctx, r.resolveTimeout())
				defer cancel()
				_, _ = r.resolve(rctx)
			})
		}
		r.mu.Unlock()
		return ip, nil
	}
	r.mu.Unlock()
	rctx, cancel := context.WithTimeout(ctx, r.resolveTimeout())
	defer cancel()
	return r.resolve(rctx)
}

func (r *runner) resolveTimeout() time.Duration {
	return min(5*time.Second, max(r.t.Interval/2, time.Second))
}

func (r *runner) resolve(ctx context.Context) (netip.Addr, error) {
	network := "ip"
	switch r.t.AF {
	case 4:
		network = "ip4"
	case 6:
		network = "ip6"
	}
	addrs, err := r.a.opts.Resolver.LookupNetIP(ctx, network, r.t.Host)
	var ip netip.Addr
	if err == nil {
		ip = pickAddr(addrs)
		if !ip.IsValid() {
			err = fmt.Errorf("no usable address for %s", r.t.Host)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolving = false
	if err != nil {
		if r.ip.IsValid() && ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
			return r.ip, nil // shutting down; not a resolver failure
		}
		if r.ip.IsValid() {
			r.a.opts.Observer.ResolveFailed(r.t)
			r.log.Warn("re-resolving target failed; keeping previous address", "ip", r.ip, "err", err)
			return r.ip, nil
		}
		return netip.Addr{}, fmt.Errorf("resolve %s: %w", r.t.Host, err)
	}
	if r.ip.IsValid() && r.ip != ip {
		r.log.Info("target address changed", "old", r.ip, "new", ip)
	}
	r.ip, r.resolvedAt = ip, time.Now()
	return ip, nil
}

// pickAddr prefers the first IPv4 address, then the first IPv6 address.
func pickAddr(addrs []netip.Addr) netip.Addr {
	var v6 netip.Addr
	for _, a := range addrs {
		a = a.Unmap()
		if a.Is4() {
			return a
		}
		if !v6.IsValid() && a.Is6() {
			v6 = a
		}
	}
	return v6
}

// buildRecord turns one cycle's probe results into a finalized record.
func buildRecord(agentID string, t *Target, ip netip.Addr, start time.Time, res []probe.HopResult) *record.Record {
	hops := make([]record.Hop, len(res))
	for i, h := range res {
		hop := record.Hop{TTL: h.TTL, Status: h.Status}
		if h.Status.Responded() {
			hop.IP = h.From
			hop.RTTMicros = max(1, h.RTT.Microseconds())
		}
		if h.Status == record.StatusUnreachable {
			hop.Code = record.IntPtr(h.Code)
		}
		hops[i] = hop
	}
	af := 4
	if ip.Is6() {
		af = 6
	}
	rec := &record.Record{
		Schema:   record.SchemaVersion,
		AgentID:  agentID,
		TS:       start.UTC().Truncate(time.Millisecond),
		Target:   t.Host,
		TargetIP: ip,
		AF:       af,
		Protocol: string(t.Protocol),
		FlowID:   int(t.FlowID),
		Hops:     hops,
	}
	rec.Finalize(t.ProbePast)
	return rec
}

type nopObserver struct{}

func (nopObserver) CycleDone(*Target, *record.Record) {}
func (nopObserver) CycleFailed(*Target)               {}
func (nopObserver) CycleOverrun(*Target)              {}
func (nopObserver) ResolveFailed(*Target)             {}
