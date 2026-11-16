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
	"mime"
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
	DefaultMaxBodyBytes         = 8 << 20  // compressed request body
	DefaultMaxDecodedBytes      = 32 << 20 // after decompression
	DefaultMaxConcurrentIngest  = 4
	DefaultMaxConcurrentQueries = 8
	maxLineBytes                = 1 << 20 // one record
	maxReportedErrors           = 5
	// maxZstdWindow caps the window a zstd frame may declare, which the
	// decoder allocates before reading data (an agent's frames are no
	// larger than its batch, 1 MiB by default).
	maxZstdWindow = 32 << 20
	// An agent sends its own records; a request with more agents or
	// streams than this is not from an agent.
	maxAgentsPerRequest  = 16
	maxStreamsPerRequest = 4096
	// queueWait is how long a request waits for a free slot before the
	// server answers 503 (agents retry later).
	queueWait = 10 * time.Second
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
	// MaxConcurrentIngest and MaxConcurrentQueries bound the ingest and
	// read-API requests processed at once; more wait up to 10s, then get 503.
	MaxConcurrentIngest, MaxConcurrentQueries int
	// MaxResponseBytes bounds the rows of one read-API response (rows
	// beyond it are dropped and the response says "truncated": true);
	// MaxQueryMemory is ClickHouse's max_memory_usage for each read-API
	// query.
	MaxResponseBytes, MaxQueryMemory int64
	// AllowedHosts are the host names (without port) that requests may
	// name in their Host header, against DNS rebinding; empty allows any.
	// /healthz, /readyz and /metrics answer every host.
	AllowedHosts []string
	// MaxStreams and StreamIdleTTL bound the per-stream state kept in memory
	// (see Ingester).
	MaxStreams    int
	StreamIdleTTL time.Duration
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
	opts      Options
	log       *slog.Logger
	ingester  *Ingester
	metrics   *serverMetrics
	ready     atomic.Bool
	ingestSem chan struct{}
	querySem  chan struct{}
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
	if o.MaxConcurrentIngest <= 0 {
		o.MaxConcurrentIngest = DefaultMaxConcurrentIngest
	}
	if o.MaxConcurrentQueries <= 0 {
		o.MaxConcurrentQueries = DefaultMaxConcurrentQueries
	}
	if o.MaxResponseBytes <= 0 {
		o.MaxResponseBytes = DefaultMaxResponseBytes
	}
	if o.MaxQueryMemory <= 0 {
		o.MaxQueryMemory = DefaultMaxQueryMemory
	}
	var reg prometheus.Registerer
	if o.Registry != nil {
		reg = o.Registry
	}
	m := newServerMetrics(reg)
	in := NewIngester(o.Store, o.Enricher, o.Logger, m)
	if o.MaxStreams > 0 {
		in.maxStreams = o.MaxStreams
	}
	if o.StreamIdleTTL > 0 {
		in.idleTTL = o.StreamIdleTTL
	}
	return &Server{opts: o, log: o.Logger, metrics: m, ingester: in,
		ingestSem: make(chan struct{}, o.MaxConcurrentIngest), querySem: make(chan struct{}, o.MaxConcurrentQueries)}
}

// acquire takes a slot of sem, waiting up to queueWait.
func acquire(ctx context.Context, sem chan struct{}) bool {
	select {
	case sem <- struct{}{}:
		return true
	default:
	}
	t := time.NewTimer(queueWait)
	defer t.Stop()
	select {
	case sem <- struct{}{}:
		return true
	case <-t.C:
	case <-ctx.Done():
	}
	return false
}

// limitQueries bounds the read-API requests running at once, so readers
// cannot crowd out ingest in ClickHouse, and the memory and result size of
// each of their queries there. max_result_bytes is a backstop at twice the
// response budget (it counts ClickHouse's in-memory columns, not JSON, and
// stops only between blocks); the response budget is what truncates.
func (s *Server) limitQueries(h http.HandlerFunc) http.HandlerFunc {
	settings := clickhouse.Settings{
		"max_memory_usage":     strconv.FormatInt(s.opts.MaxQueryMemory, 10),
		"max_result_bytes":     strconv.FormatInt(2*s.opts.MaxResponseBytes, 10),
		"result_overflow_mode": "break",
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if !acquire(r.Context(), s.querySem) {
			writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "too many queries; retry later"})
			return
		}
		defer func() { <-s.querySem }()
		h(w, r.WithContext(clickhouse.WithSettings(r.Context(), settings)))
	}
}

// SetReady marks the server ready (storage migrated).
func (s *Server) SetReady() { s.ready.Store(true) }

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/ingest", s.handleIngest)
	mux.HandleFunc("GET /api/v1/query/targets", s.limitQueries(s.handleTargets))
	mux.HandleFunc("GET /api/v1/query/cycles", s.limitQueries(s.handleCycles))
	mux.HandleFunc("GET /api/v1/query/path-changes", s.limitQueries(s.handlePathChanges))
	mux.HandleFunc("GET /api/v1/query/loops", s.limitQueries(s.handleLoops))
	mux.HandleFunc("GET /api/v1/query/outages", s.limitQueries(s.handleOutages))
	mux.HandleFunc("GET /api/v1/query/topology", s.limitQueries(s.handleTopology))
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
	return newHostCheck(s, s.opts.AllowedHosts, mux)
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
			// Details stay in the log: they name internal hosts.
			s.log.Debug("readiness: clickhouse unreachable", "err", err)
			http.Error(w, "clickhouse unreachable", http.StatusServiceUnavailable)
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
	// Agents send application/x-ndjson. Refusing the content types a web
	// page can send without a CORS preflight keeps browsers from posting
	// records to a server they can reach.
	switch mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt {
	case "application/x-ndjson", "application/jsonl", "application/json":
	default:
		fail(http.StatusUnsupportedMediaType, "Content-Type must be application/x-ndjson")
		return
	}
	if !acquire(r.Context(), s.ingestSem) {
		fail(http.StatusServiceUnavailable, "server busy; retry later")
		return
	}
	defer func() { <-s.ingestSem }()
	body := http.MaxBytesReader(w, r.Body, s.opts.MaxBodyBytes)
	var in io.Reader = body
	switch enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); enc {
	case "zstd":
		dec, err := zstd.NewReader(body, zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(uint64(s.opts.MaxDecodedBytes)),                     //nolint:gosec // positive
			zstd.WithDecoderMaxWindow(uint64(min(s.opts.MaxDecodedBytes, maxZstdWindow)))) //nolint:gosec // positive
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
	agents, streams := map[string]bool{}, map[streamKey]bool{}
	for _, rec := range recs {
		agents[rec.AgentID] = true
		streams[keyOf(rec)] = true
	}
	if len(agents) > maxAgentsPerRequest || len(streams) > maxStreamsPerRequest {
		fail(http.StatusBadRequest, fmt.Sprintf("a request may carry at most %d agents and %d streams",
			maxAgentsPerRequest, maxStreamsPerRequest))
		return
	}
	s.metrics.rejected(res.Rejected)
	if res.Rejected > 0 {
		s.log.Warn("rejected invalid records", "count", res.Rejected, "first", res.Errors[0])
	}

	ir, err := s.ingester.Ingest(r.Context(), recs)
	res.Accepted, res.Duplicates = ir.Accepted, ir.Duplicates
	switch {
	case errors.Is(err, ErrStaleBatch):
		fail(http.StatusConflict, err.Error())
		return
	case err != nil:
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

// runQuery streams the rows of q as {"rows": [...], "truncated": false},
// row by row, within MaxResponseBytes ("truncated": true when rows were
// left out).
func (s *Server) runQuery(w http.ResponseWriter, r *http.Request, q string, qp queryParams, reshape func(json.RawMessage) (any, error)) {
	js := newJSONStream(w, s.opts.MaxResponseBytes, streamFlushBytes)
	js.beginArray("rows")
	err := s.opts.Store.Query(r.Context(), q, qp.params, func(m json.RawMessage) error {
		b := []byte(m)
		if reshape != nil {
			v, err := reshape(m)
			if err != nil {
				return err
			}
			if b, err = json.Marshal(v); err != nil {
				return err
			}
		}
		return js.row(b)
	})
	if err != nil && !errors.Is(err, errResponseFull) {
		s.queryFailed(r, js, "", err)
		return
	}
	js.endArray()
	js.finish()
}

// queryFailed logs and answers a read query that failed (or whose client
// went away).
func (s *Server) queryFailed(r *http.Request, js *jsonStream, part string, err error) {
	if js.err != nil || r.Context().Err() != nil {
		s.log.Debug("query abandoned by the client", "path", r.URL.Path, "err", err)
		return
	}
	s.metrics.queryErrors.Inc()
	if part != "" {
		s.log.Error("query failed", "path", r.URL.Path, "part", part, "err", err)
	} else {
		s.log.Error("query failed", "path", r.URL.Path, "err", err)
	}
	js.fail()
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

// maxCycles bounds limit for cycles, the widest rows: a cycle can be
// 230 KB of JSON (255 hops with 16 MPLS labels each), and ClickHouse formats
// a result like this as one block, so 1000 of them took about 650 MiB in
// ClickHouse and 10000 failed the default query.max_memory_bytes.
const maxCycles = 1000

// handleCycles returns full cycles of one stream, newest first, or oldest
// first with order=asc (the UI steps through cycles with limit=1 either way).
func (s *Server) handleCycles(w http.ResponseWriter, r *http.Request) {
	qp, err := parseQuery(r, true)
	if err == nil && qp.limit > maxCycles {
		err = fmt.Errorf("limit must be 1..%d for cycles", maxCycles)
	}
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
