package output

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/netutil"

	"github.com/saremox/hopcorder/internal/agent"
	"github.com/saremox/hopcorder/internal/probe"
	"github.com/saremox/hopcorder/internal/record"
	"github.com/saremox/hopcorder/internal/spool"
)

// SpoolStats is what the metrics output reads from the spool.
type SpoolStats interface {
	Stats() spool.Stats
	Pending(consumer string) uint64
}

// Metrics is the Prometheus output. It implements agent.Observer.
//
// Labels are bounded by configuration: agent_id (constant), target (the
// configured host), protocol, result and output. No hop address or other
// per-probe value is ever a label; that detail lives in the records. So two
// targets with the same host and protocol (another port or address family)
// share every per-target series: counters count both, and gauges are kept
// meaningful for both where that matters (hopcorder_flow_in_use).
type Metrics struct {
	reg *prometheus.Registry

	cycles        *prometheus.CounterVec
	rtt           *prometheus.HistogramVec
	pathLen       *prometheus.GaugeVec
	pathChanges   *prometheus.CounterVec
	loops         *prometheus.CounterVec
	overruns      *prometheus.CounterVec
	cycleErrors   *prometheus.CounterVec
	flowInUse     *prometheus.GaugeVec
	resolveErrors *prometheus.CounterVec
	outputErrors  *prometheus.CounterVec

	mu    sync.Mutex
	paths map[*agent.Target]*record.PathTracker
	inUse map[series]map[*agent.Target]bool // targets refused for their flow, by series
}

// series names the per-target series a target's metrics go to.
type series struct{ target, protocol string }

// MetricsOptions wires the metrics output to the rest of the agent.
type MetricsOptions struct {
	AgentID string
	Version string
	Targets []agent.Target
	Outputs []string
	Spool   SpoolStats          // may be nil
	Prober  probe.StatsProvider // may be nil
}

// NewMetrics registers all metrics in a private registry.
func NewMetrics(o MetricsOptions) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	r := prometheus.WrapRegistererWith(prometheus.Labels{"agent_id": o.AgentID}, reg)
	tp := []string{"target", "protocol"}
	m := &Metrics{
		reg: reg,
		cycles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_cycles_total", Help: "Completed probe cycles by whether the destination answered.",
		}, []string{"target", "protocol", "result"}),
		rtt: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "hopcorder_e2e_rtt_seconds", Help: "Round-trip time to the destination in reached cycles.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, tp),
		pathLen: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hopcorder_path_length", Help: "Hops in the last cycle's trimmed path.",
		}, tp),
		pathChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_path_changes_total", Help: "Cycles whose route differed from the previous cycle's (silent hops match anything).",
		}, tp),
		loops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_loops_detected_total", Help: "Cycles in which at least one routing loop was detected.",
		}, tp),
		overruns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_cycle_overruns_total", Help: "Cycles that took longer than the interval.",
		}, tp),
		cycleErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_cycle_errors_total", Help: "Cycles that could not be run.",
		}, tp),
		flowInUse: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hopcorder_flow_in_use",
			Help: "Targets of this host and protocol not probed because another process holds their flow's identifier (ICMP echo identifier or TCP source port = flow_id): 1 for a single target.",
		}, tp),
		resolveErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_resolve_errors_total", Help: "Failed DNS resolutions of a target.",
		}, tp),
		outputErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_output_errors_total", Help: "Failed deliveries by output.",
		}, []string{"output"}),
		paths: map[*agent.Target]*record.PathTracker{},
		inUse: map[series]map[*agent.Target]bool{},
	}
	r.MustRegister(m.cycles, m.rtt, m.pathLen, m.pathChanges, m.loops, m.overruns,
		m.cycleErrors, m.flowInUse, m.resolveErrors, m.outputErrors)

	build := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hopcorder_build_info", Help: "Build information.",
		ConstLabels: prometheus.Labels{"version": o.Version, "goversion": runtime.Version()},
	})
	build.Set(1)
	r.MustRegister(build)

	if sp := o.Spool; sp != nil {
		r.MustRegister(
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "hopcorder_spool_bytes", Help: "Compressed record bytes held in the spool.",
			}, func() float64 { return float64(sp.Stats().Bytes) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "hopcorder_spool_records", Help: "Records held in the spool.",
			}, func() float64 { return float64(sp.Stats().Records) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "hopcorder_spool_records_dropped_total", Help: "Undelivered records dropped by the spool's age or size bound.",
			}, func() float64 { return float64(sp.Stats().Dropped) }),
		)
		for _, name := range o.Outputs {
			r.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "hopcorder_output_pending_records", Help: "Spooled records not yet delivered by an output.",
				ConstLabels: prometheus.Labels{"output": name},
			}, func() float64 { return float64(sp.Pending(name)) }))
			m.outputErrors.WithLabelValues(name)
		}
	}
	for _, name := range o.Outputs {
		c := abandonedCount(name)
		r.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "hopcorder_output_abandoned_total",
			Help:        "Deliveries still blocked (a write that never returned) when a configuration stopped for a reload or shutdown, left behind after the grace period; their records stay spooled.",
			ConstLabels: prometheus.Labels{"output": name},
		}, func() float64 { return float64(c.Load()) }))
	}
	if pr := o.Prober; pr != nil {
		r.MustRegister(
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "hopcorder_probe_late_replies_total", Help: "Replies that arrived after the probe timeout and were dropped.",
			}, func() float64 { return float64(pr.Stats().LateReplies) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "hopcorder_probes_sent_total", Help: "Probe packets sent.",
			}, func() float64 { return float64(pr.Stats().ProbesSent) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "hopcorder_probe_send_errors_total", Help: "Probe packets the kernel refused to send.",
			}, func() float64 { return float64(pr.Stats().SendErrors) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "hopcorder_probe_mismatched_replies_total",
				Help: "Replies that matched an in-flight probe's id but not its IPv4 ID, nonce or TCP sequence number (another process's packets, forgeries, or IP IDs rewritten on the path); dropped.",
			}, func() float64 { return float64(pr.Stats().MismatchedReplies) }),
		)
	}
	// Create every per-target series up front so rate() works from the
	// first scrape.
	for i := range o.Targets {
		t := &o.Targets[i]
		host, proto := t.Host, string(t.Protocol)
		m.cycles.WithLabelValues(host, proto, "reached")
		m.cycles.WithLabelValues(host, proto, "unreached")
		for _, v := range []*prometheus.CounterVec{m.pathChanges, m.loops, m.overruns, m.cycleErrors, m.resolveErrors} {
			v.WithLabelValues(host, proto)
		}
		m.flowInUse.WithLabelValues(host, proto)
	}
	return m
}

// Handler serves the metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// maxMetricsConns caps concurrent connections to the metrics endpoint.
const maxMetricsConns = 64

// MetricsServer is the metrics endpoint's HTTP server. Its listener
// outlives configurations: a reload swaps what it serves (Serve), never the
// socket, so the port is never free for another local process to take
// (which would make the new configuration, and going back to the old one,
// fail to bind it, and could serve made-up metrics).
type MetricsServer struct {
	addr   string
	srv    *http.Server
	done   chan struct{}
	served atomic.Pointer[served] // nil: no configuration is running
}

// served is what a MetricsServer serves: h at exactly path.
type served struct {
	path string
	h    http.Handler
}

// ListenMetrics binds addr and serves on it. Until Serve installs metrics
// it answers 503.
func ListenMetrics(ctx context.Context, addr string, log *slog.Logger) (*MetricsServer, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listen %s: %w", addr, err)
	}
	// Bounded in every phase and in connection count: idle or slow
	// clients cannot pin goroutines and file descriptors the agent needs
	// for probing.
	ln = netutil.LimitListener(ln, maxMetricsConns)
	s := &MetricsServer{addr: addr, done: make(chan struct{})}
	s.srv = &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10}
	log.Info("serving metrics", "addr", ln.Addr().String())
	go func() {
		defer close(s.done)
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
		}
	}()
	return s, nil
}

// Addr returns the address the server was asked to listen on.
func (s *MetricsServer) Addr() string { return s.addr }

// Serve makes the server serve m's metrics at path, and 404 at any other.
// With m nil it answers 503 instead, while no configuration is running.
//
// The path is compared with the request's (decoded) path as it is, never
// parsed: as an http.ServeMux pattern, a path ServeMux could not parse
// panicked here and killed the agent at start or on a reload.
func (s *MetricsServer) Serve(path string, m *Metrics) {
	if m == nil {
		s.served.Store(nil)
		return
	}
	s.served.Store(&served{path: path, h: m.Handler()})
}

// ServeHTTP implements http.Handler.
func (s *MetricsServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cur := s.served.Load()
	switch {
	case cur == nil:
		http.Error(w, "the agent is (re)starting", http.StatusServiceUnavailable)
	case r.URL.Path != cur.path:
		http.NotFound(w, r)
	default:
		cur.h.ServeHTTP(w, r)
	}
}

// Close shuts the server down and returns once the listener is closed.
func (s *MetricsServer) Close() {
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(sctx)
	<-s.done
}

// OutputError counts a failed delivery; pass it as DrainOptions.OnError.
func (m *Metrics) OutputError(output string) func(error) {
	c := m.outputErrors.WithLabelValues(output)
	return func(error) { c.Inc() }
}

// CycleDone implements agent.Observer.
func (m *Metrics) CycleDone(t *agent.Target, rec *record.Record) {
	host, proto := t.Host, string(t.Protocol)
	result := "unreached"
	if rec.Reached {
		result = "reached"
		if n := len(rec.Hops); n > 0 {
			m.rtt.WithLabelValues(host, proto).Observe(float64(rec.Hops[n-1].RTTMicros) / 1e6)
		}
	}
	m.cycles.WithLabelValues(host, proto, result).Inc()
	m.setInUse(t, false)
	m.pathLen.WithLabelValues(host, proto).Set(float64(rec.PathLen))
	if len(rec.Loops) > 0 {
		m.loops.WithLabelValues(host, proto).Inc()
	}
	m.mu.Lock()
	p := m.paths[t]
	if p == nil {
		p = &record.PathTracker{}
		m.paths[t] = p
	}
	changed := p.Observe(rec)
	m.mu.Unlock()
	if changed {
		m.pathChanges.WithLabelValues(host, proto).Inc()
	}
}

// CycleFailed implements agent.Observer.
func (m *Metrics) CycleFailed(t *agent.Target, err error) {
	m.cycleErrors.WithLabelValues(t.Host, string(t.Protocol)).Inc()
	if errors.Is(err, probe.ErrFlowInUse) {
		m.setInUse(t, true)
	}
}

// setInUse records whether t is refused for its flow, and sets its series'
// gauge to how many of the series' targets are. (Set to 0 or 1 by each
// target's cycles, a series shared by two targets flapped while only one
// was refused.)
func (m *Metrics) setInUse(t *agent.Target, in bool) {
	k := series{t.Host, string(t.Protocol)}
	m.mu.Lock()
	defer m.mu.Unlock()
	refused := m.inUse[k]
	if in {
		if refused == nil {
			refused = map[*agent.Target]bool{}
			m.inUse[k] = refused
		}
		refused[t] = true
	} else {
		delete(refused, t)
	}
	m.flowInUse.WithLabelValues(k.target, k.protocol).Set(float64(len(refused)))
}

// CycleOverrun implements agent.Observer.
func (m *Metrics) CycleOverrun(t *agent.Target) {
	m.overruns.WithLabelValues(t.Host, string(t.Protocol)).Inc()
}

// ResolveFailed implements agent.Observer.
func (m *Metrics) ResolveFailed(t *agent.Target) {
	m.resolveErrors.WithLabelValues(t.Host, string(t.Protocol)).Inc()
}
