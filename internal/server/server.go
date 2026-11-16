package server

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// Defaults for request limits.
const (
	DefaultMaxBodyBytes    = 64 << 20  // compressed request body
	DefaultMaxDecodedBytes = 512 << 20 // after decompression
	maxLineBytes           = 4 << 20   // one record
	maxReportedErrors      = 5
)

// Pinger checks that storage is reachable.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Options configures a Server.
type Options struct {
	Store    Store
	Pinger   Pinger
	Enricher Enricher
	Logger   *slog.Logger
	// Registry receives the server's metrics and is served on /metrics;
	// nil disables /metrics.
	Registry *prometheus.Registry
	// MaxBodyBytes and MaxDecodedBytes bound one ingest request.
	MaxBodyBytes, MaxDecodedBytes int64
	// UI serves the web UI under /ui/ (internal/ui); nil serves none.
	UI http.Handler
}

// Server serves ingest, the read API and health endpoints.
//
// Paths are split by prefix so a reverse proxy can secure them differently
// (HANDOFF.md §2): /api/v1/ingest for agents, /api/v1/query/ and /ui/ for
// readers, /healthz, /readyz and /metrics for operators. The server itself
// does no authentication.
type Server struct {
	opts     Options
	log      *slog.Logger
	ingester *Ingester
	metrics  *serverMetrics
	ready    atomic.Bool
}

// New returns a server. Call SetReady once migrations have been applied.
func New(o Options) *Server {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if o.MaxDecodedBytes <= 0 {
		o.MaxDecodedBytes = DefaultMaxDecodedBytes
	}
	var reg prometheus.Registerer
	if o.Registry != nil {
		reg = o.Registry
	}
	m := newServerMetrics(reg)
	return &Server{opts: o, log: o.Logger, metrics: m, ingester: NewIngester(o.Store, o.Enricher, o.Logger, m)}
}

// SetReady marks the server ready (storage migrated).
func (s *Server) SetReady() { s.ready.Store(true) }

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/ingest", s.handleIngest)
	mux.HandleFunc("GET /api/v1/query/targets", s.handleTargets)
	mux.HandleFunc("GET /api/v1/query/cycles", s.handleCycles)
	mux.HandleFunc("GET /api/v1/query/path-changes", s.handlePathChanges)
	mux.HandleFunc("GET /api/v1/query/loops", s.handleLoops)
	mux.HandleFunc("GET /api/v1/query/outages", s.handleOutages)
	mux.HandleFunc("GET /api/v1/query/topology", s.handleTopology)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /readyz", s.handleReady)
	if s.opts.Registry != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(s.opts.Registry, promhttp.HandlerOpts{}))
	}
	if s.opts.UI != nil {
		mux.Handle("GET /ui/", s.opts.UI)
		// Relative redirects to ui/, so they stay under a proxy's path prefix.
		redirect := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", "ui/")
			w.WriteHeader(http.StatusFound)
		}
		mux.HandleFunc("GET /ui", redirect)
		mux.HandleFunc("GET /{$}", redirect)
	}
	return mux
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		http.Error(w, "migrations not applied yet", http.StatusServiceUnavailable)
		return
	}
	if s.opts.Pinger != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.opts.Pinger.Ping(ctx); err != nil {
			http.Error(w, "clickhouse unreachable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	_, _ = io.WriteString(w, "ready\n")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

type errorBody struct {
	Error string `json:"error"`
}

// handleIngest accepts NDJSON (zstd, gzip or uncompressed), one wire record
// per line. Records that fail validation are rejected for good (a retry
// cannot fix them) and reported back; the rest are stored. A storage
// failure returns 503 so the agent retries the whole batch, which the
// deduplication makes safe.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	code := http.StatusOK
	defer func() { s.metrics.request(code) }()
	fail := func(c int, msg string) {
		code = c
		writeJSON(w, c, errorBody{Error: msg})
	}
	if !s.ready.Load() {
		fail(http.StatusServiceUnavailable, "server is starting")
		return
	}
	body := http.MaxBytesReader(w, r.Body, s.opts.MaxBodyBytes)
	var in io.Reader = body
	switch enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); enc {
	case "zstd":
		dec, err := zstd.NewReader(body, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(s.opts.MaxDecodedBytes))) //nolint:gosec // positive
		if err != nil {
			fail(http.StatusBadRequest, "bad zstd stream: "+err.Error())
			return
		}
		defer dec.Close()
		in = dec
	case "gzip":
		gz, err := gzip.NewReader(body)
		if err != nil {
			fail(http.StatusBadRequest, "bad gzip stream: "+err.Error())
			return
		}
		defer gz.Close()
		in = gz
	case "", "identity":
	default:
		fail(http.StatusUnsupportedMediaType, "unsupported Content-Encoding "+enc)
		return
	}
	limited := &io.LimitedReader{R: in, N: s.opts.MaxDecodedBytes + 1}

	var (
		recs []*record.Record
		res  IngestResult
	)
	sc := bufio.NewScanner(limited)
	sc.Buffer(make([]byte, 64<<10), maxLineBytes)
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(strings.TrimSpace(string(b))) == 0 {
			continue
		}
		rec, err := parseRecord(b)
		if err != nil {
			res.Rejected++
			if len(res.Errors) < maxReportedErrors {
				res.Errors = append(res.Errors, fmt.Sprintf("line %d: %v", line, err))
			}
			continue
		}
		recs = append(recs, rec)
	}
	if err := sc.Err(); err != nil {
		var tooBig *http.MaxBytesError
		switch {
		case errors.As(err, &tooBig):
			fail(http.StatusRequestEntityTooLarge, "request body too large")
		case errors.Is(err, bufio.ErrTooLong):
			fail(http.StatusRequestEntityTooLarge, "a line exceeds the record size limit")
		case errors.Is(err, zstd.ErrDecoderSizeExceeded), errors.Is(err, zstd.ErrWindowSizeExceeded):
			fail(http.StatusRequestEntityTooLarge, "decompressed body too large")
		default:
			fail(http.StatusBadRequest, "cannot read body: "+err.Error())
		}
		return
	}
	if limited.N <= 0 {
		fail(http.StatusRequestEntityTooLarge, "decompressed body too large")
		return
	}
	s.metrics.rejected(res.Rejected)
	if res.Rejected > 0 {
		s.log.Warn("rejected invalid records", "count", res.Rejected, "first", res.Errors[0])
	}

	ir, err := s.ingester.Ingest(r.Context(), recs)
	res.Accepted, res.Duplicates = ir.Accepted, ir.Duplicates
	if err != nil {
		s.log.Error("ingest failed; the agent will retry", "err", err)
		fail(http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	writeJSON(w, code, res)
}

func parseRecord(b []byte) (*record.Record, error) {
	var rec record.Record
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, fmt.Errorf("not a record: %w", err)
	}
	if rec.Schema != record.SchemaVersion {
		return nil, fmt.Errorf("unsupported schema %d", rec.Schema)
	}
	if rec.Seq == 0 {
		return nil, errors.New("seq 0 (records printed by --once are not spooled and cannot be ingested)")
	}
	if err := rec.Validate(); err != nil {
		return nil, err
	}
	return &rec, nil
}

// queryParams holds the common read-API parameters.
type queryParams struct {
	params   clickhouse.Params
	limit    int
	from, to time.Time
}

// parseQuery reads agent, target, protocol, from, to (RFC 3339 or Unix
// milliseconds; default the last 24 hours) and limit.
func parseQuery(r *http.Request, needTarget bool) (queryParams, error) {
	q := r.URL.Query()
	now := time.Now()
	to, err := parseTime(q.Get("to"), now)
	if err != nil {
		return queryParams{}, fmt.Errorf("to: %w", err)
	}
	from, err := parseTime(q.Get("from"), to.Add(-24*time.Hour))
	if err != nil {
		return queryParams{}, fmt.Errorf("from: %w", err)
	}
	if !from.Before(to) {
		return queryParams{}, errors.New("from must be before to")
	}
	limit := 1000
	if v := q.Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > 10000 {
			return queryParams{}, errors.New("limit must be 1..10000")
		}
	}
	if needTarget && (q.Get("agent") == "" || q.Get("target") == "") {
		return queryParams{}, errors.New("agent and target are required")
	}
	return queryParams{limit: limit, from: from, to: to, params: clickhouse.Params{
		"agent": q.Get("agent"), "target": q.Get("target"), "protocol": q.Get("protocol"),
		"from": clickhouse.Time(from), "to": clickhouse.Time(to), "limit": strconv.Itoa(limit),
	}}, nil
}

func parseTime(s string, def time.Time) (time.Time, error) {
	if s == "" {
		return def, nil
	}
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(ms), nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

const streamFilter = `({agent:String} = '' OR agent_id = {agent:String})
	AND ({target:String} = '' OR target = {target:String})
	AND ({protocol:String} = '' OR protocol = {protocol:String})`

// runQuery streams the rows of q as {"rows": [...]}.
func (s *Server) runQuery(w http.ResponseWriter, r *http.Request, q string, qp queryParams, reshape func(json.RawMessage) (any, error)) {
	var rows []any
	err := s.opts.Store.Query(r.Context(), q, qp.params, func(m json.RawMessage) error {
		if reshape == nil {
			rows = append(rows, m)
			return nil
		}
		v, err := reshape(m)
		if err != nil {
			return err
		}
		rows = append(rows, v)
		return nil
	})
	if err != nil {
		s.metrics.queryErrors.Inc()
		s.log.Error("query failed", "path", r.URL.Path, "err", err)
		writeJSON(w, http.StatusBadGateway, errorBody{Error: "query failed"})
		return
	}
	if rows == nil {
		rows = []any{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (s *Server) handleTargets(w http.ResponseWriter, r *http.Request) {
	qp, err := parseQuery(r, false)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	s.runQuery(w, r, `SELECT agent_id, target, protocol, flow_id, max(ts) AS last_ts, count() AS cycles,
		countIf(reached) / count() AS reach_ratio, argMax(reached, ts) AS last_reached,
		argMax(path_len, ts) AS last_path_len, argMax(target_ip, ts) AS last_target_ip,
		argMax(path_hash, ts) AS last_path_hash
		FROM cycles WHERE ts >= {from:DateTime64(3)} AND ts < {to:DateTime64(3)} AND `+streamFilter+`
		GROUP BY agent_id, target, protocol, flow_id ORDER BY agent_id, target, protocol, flow_id
		LIMIT {limit:UInt32}`, qp, nil)
}

// apiHop is a hop as the read API returns it: the wire hop plus enrichment.
type apiHop struct {
	TTL    int                `json:"ttl"`
	Status string             `json:"status"`
	IP     string             `json:"ip,omitempty"`
	RTTus  uint32             `json:"rtt_us,omitempty"`
	Code   *int               `json:"code,omitempty"`
	ASN    uint32             `json:"asn,omitempty"`
	ASName string             `json:"as_name,omitempty"`
	Name   string             `json:"name,omitempty"`
	MPLS   []record.MPLSLabel `json:"mpls,omitempty"`
}

// handleCycles returns full cycles of one stream, newest first, or oldest
// first with order=asc (the UI steps through cycles with limit=1 either way).
func (s *Server) handleCycles(w http.ResponseWriter, r *http.Request) {
	qp, err := parseQuery(r, true)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	order := "DESC"
	switch r.URL.Query().Get("order") {
	case "", "desc":
	case "asc":
		order = "ASC"
	default:
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "order must be asc or desc"})
		return
	}
	type row struct {
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
	}
	type loop struct {
		IP   string `json:"ip"`
		TTLs []int  `json:"ttls"`
	}
	s.runQuery(w, r, `SELECT agent_id, seq, ts, target, target_ip, af, protocol, flow_id, reached, path_len,
		path_hash, loops.ip, loops.ttls, hops.ttl, hops.status, hops.ip, hops.rtt_us, hops.code, hops.asn,
		hops.as_name, hops.name, hops.mpls
		FROM cycles WHERE ts >= {from:DateTime64(3)} AND ts < {to:DateTime64(3)} AND `+streamFilter+`
		ORDER BY ts `+order+` LIMIT {limit:UInt32}`, qp, func(m json.RawMessage) (any, error) {
		var c row
		if err := json.Unmarshal(m, &c); err != nil {
			return nil, err
		}
		hops := make([]apiHop, len(c.HopTTL))
		for i := range hops {
			h := apiHop{TTL: c.HopTTL[i], Status: c.HopStatus[i], IP: c.HopIP[i], RTTus: c.HopRTT[i],
				ASN: c.HopASN[i], ASName: c.HopASName[i], Name: c.HopName[i], MPLS: c.HopMPLS[i]}
			if code := c.HopCode[i]; code >= 0 {
				h.Code = &code
			}
			hops[i] = h
		}
		loops := make([]loop, len(c.LoopIP))
		for i := range loops {
			loops[i] = loop{IP: c.LoopIP[i], TTLs: c.LoopTTLs[i]}
		}
		return map[string]any{
			"agent_id": c.AgentID, "seq": c.Seq, "ts": c.TS, "target": c.Target, "target_ip": c.TargetIP,
			"af": c.AF, "protocol": c.Protocol, "flow_id": c.FlowID, "reached": c.Reached,
			"path_len": c.PathLen, "path_hash": c.PathHash, "loops": loops, "hops": hops,
		}, nil
	})
}

func (s *Server) handlePathChanges(w http.ResponseWriter, r *http.Request) {
	qp, err := parseQuery(r, false)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	s.runQuery(w, r, `SELECT ts, agent_id, target, protocol, flow_id, seq, reason, ttls, from_ips, to_ips,
		prev_seq, prev_path_hash, path_hash, prev_path_len, path_len
		FROM path_changes FINAL WHERE ts >= {from:DateTime64(3)} AND ts < {to:DateTime64(3)} AND `+streamFilter+`
		ORDER BY ts DESC LIMIT {limit:UInt32}`, qp, nil)
}

func (s *Server) handleLoops(w http.ResponseWriter, r *http.Request) {
	qp, err := parseQuery(r, false)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	s.runQuery(w, r, `SELECT ts, agent_id, target, protocol, flow_id, seq, loop_ip, ttls
		FROM loop_events WHERE ts >= {from:DateTime64(3)} AND ts < {to:DateTime64(3)} AND `+streamFilter+`
		ORDER BY ts DESC LIMIT {limit:UInt32}`, qp, nil)
}

// handleOutages returns reachability-loss windows overlapping [from, to);
// open ones have a null end_ts.
func (s *Server) handleOutages(w http.ResponseWriter, r *http.Request) {
	qp, err := parseQuery(r, false)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	s.runQuery(w, r, `SELECT agent_id, target, protocol, flow_id, start_seq, start_ts, end_seq, end_ts,
		if(end_ts IS NULL, NULL, dateDiff('millisecond', start_ts, assumeNotNull(end_ts))) AS duration_ms,
		cycles, last_hop_ttl, last_hop_ip
		FROM outages FINAL WHERE start_ts < {to:DateTime64(3)}
		AND (end_ts IS NULL OR end_ts >= {from:DateTime64(3)}) AND `+streamFilter+`
		ORDER BY start_ts DESC LIMIT {limit:UInt32}`, qp, nil)
}
