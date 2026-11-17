package server

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// serverMetrics are the server's Prometheus metrics. Labels are bounded:
// HTTP status code, table, result, event. Never an agent, target or hop.
type serverMetrics struct {
	requests    *prometheus.CounterVec
	recordsVec  *prometheus.CounterVec
	insertErrs  *prometheus.CounterVec
	insertSecs  prometheus.Histogram
	changes     prometheus.Counter
	outagesVec  *prometheus.CounterVec
	queryErrors prometheus.Counter
	hostRefused prometheus.Counter
	unrestored  prometheus.Counter
	evictedVec  *prometheus.CounterVec
}

// newServerMetrics creates the metrics and registers them with reg (if not
// nil).
func newServerMetrics(reg prometheus.Registerer) *serverMetrics {
	m := &serverMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_server_ingest_requests_total", Help: "Ingest requests by HTTP status code.",
		}, []string{"code"}),
		recordsVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_server_records_total", Help: "Ingested records by result.",
		}, []string{"result"}),
		insertErrs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_server_insert_errors_total", Help: "Failed ClickHouse inserts by table.",
		}, []string{"table"}),
		insertSecs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "hopcorder_server_insert_seconds", Help: "Duration of cycles inserts.",
			Buckets: prometheus.ExponentialBuckets(0.002, 2, 12),
		}),
		changes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hopcorder_server_path_changes_total", Help: "Route changes derived at ingest.",
		}),
		outagesVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_server_outages_total", Help: "Reachability-loss windows opened and closed.",
		}, []string{"event"}),
		queryErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hopcorder_server_query_errors_total", Help: "Failed read-API queries.",
		}),
		hostRefused: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hopcorder_server_host_refused_total",
			Help: "Requests refused because their Host header is not in allowed_hosts.",
		}),
		unrestored: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hopcorder_server_unrestored_cycles_total",
			Help: "Stored cycles that derived no events because their stream's state was not restored in time " +
				"(the stream is restored with its next batch) or there was no room for it " +
				"(ingest.max_streams, max_streams_per_agent).",
		}),
		evictedVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopcorder_server_state_evictions_total",
			Help: "Agent and stream states dropped at ingest.max_agents and ingest.max_streams (not idle ones).",
		}, []string{"kind"}),
	}
	// recovered: accepted records whose cycles a failed or interrupted
	// insert had stored after all (found when the batch came again).
	for _, r := range []string{"accepted", "duplicate", "rejected", "recovered"} {
		m.recordsVec.WithLabelValues(r)
	}
	for _, k := range []string{"agent", "stream"} {
		m.evictedVec.WithLabelValues(k)
	}
	if reg != nil {
		reg.MustRegister(m.requests, m.recordsVec, m.insertErrs, m.insertSecs, m.changes, m.outagesVec, m.queryErrors,
			m.hostRefused, m.unrestored, m.evictedVec)
	}
	return m
}

func (m *serverMetrics) request(code int) { m.requests.WithLabelValues(strconv.Itoa(code)).Inc() }
func (m *serverMetrics) records(accepted, dups int) {
	m.recordsVec.WithLabelValues("accepted").Add(float64(accepted))
	m.recordsVec.WithLabelValues("duplicate").Add(float64(dups))
}
func (m *serverMetrics) rejected(n int)  { m.recordsVec.WithLabelValues("rejected").Add(float64(n)) }
func (m *serverMetrics) recovered(n int) { m.recordsVec.WithLabelValues("recovered").Add(float64(n)) }
func (m *serverMetrics) insertError(table string) {
	m.insertErrs.WithLabelValues(table).Inc()
}
func (m *serverMetrics) insertDuration(d time.Duration) { m.insertSecs.Observe(d.Seconds()) }
func (m *serverMetrics) pathChanges(n int)              { m.changes.Add(float64(n)) }
func (m *serverMetrics) outage(event string)            { m.outagesVec.WithLabelValues(event).Inc() }
func (m *serverMetrics) unrestoredCycles(n int)         { m.unrestored.Add(float64(n)) }
func (m *serverMetrics) evicted(kind string, n int) {
	m.evictedVec.WithLabelValues(kind).Add(float64(n))
}
