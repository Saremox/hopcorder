package server

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"slices"
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
	// maxErrorLen bounds an error reported to the agent and logged.
	maxErrorLen = 200
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
	// Request bodies are read before a request takes an ingest slot, so a
	// slow client holds no slot: at most maxReading at once, each within
	// bodyReadTimeout (agents give a request 30s), and together buffering
	// at most the larger of 32 MiB and twice max_body_bytes.
	maxReading      = 64
	bodyReadTimeout = 20 * time.Second
	minReadBuffer   = 32 << 20
	readChunk       = 64 << 10
	// maxFutureSkew is how far ahead of the server's clock a record's ts
	// may be.
	maxFutureSkew = 24 * time.Hour
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
	// MaxStreams, MaxStreamsPerAgent, MaxAgents and StreamIdleTTL bound
	// the per-stream and per-agent state kept in memory (see Ingester);
	// MaxStreamsPerAgent defaults to 4096 or half of MaxStreams if less.
	MaxStreams         int
	MaxStreamsPerAgent int
	MaxAgents          int
	StreamIdleTTL      time.Duration
	// MaxRecordAge refuses records older than this (default 30 days);
	// records more than a day ahead of the server's clock are refused too.
	MaxRecordAge time.Duration
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
	// readSem bounds the requests whose bodies are being read; readBuf
	// hands out the readChunk-byte chunks their bodies are buffered in.
	readSem     chan struct{}
	readBuf     *chunkPool
	bodyTimeout time.Duration
	// writeProgress, if set (tests), replaces the constant: how long a
	// read-API client has for each part of its response.
	writeProgress time.Duration
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
	if o.MaxStreamsPerAgent > 0 {
		in.maxStreamsPerAgent = o.MaxStreamsPerAgent
	}
	if o.MaxAgents > 0 {
		in.maxAgents = o.MaxAgents
	}
	if o.StreamIdleTTL > 0 {
		in.idleTTL = o.StreamIdleTTL
	}
	if o.MaxRecordAge > 0 {
		in.maxRecordAge = o.MaxRecordAge
	}
	readBuf := max(minReadBuffer, 2*o.MaxBodyBytes)
	return &Server{opts: o, log: o.Logger, metrics: m, ingester: in,
		ingestSem: make(chan struct{}, o.MaxConcurrentIngest), querySem: make(chan struct{}, o.MaxConcurrentQueries),
		readSem: make(chan struct{}, maxReading), readBuf: newChunkPool(chunksFor(readBuf)),
		bodyTimeout: bodyReadTimeout}
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
// stops only between blocks, hence the small blocks); the response budget
// is what truncates. The other settings keep a query's memory to what it
// needs rather than what the machine has (see readQuerySettings).
//
// A request keeps its slot until its response is sent, and each part of
// the response must reach the client within writeProgress, so a client
// that stops reading gives its slot up then (see writeProgress).
func (s *Server) limitQueries(h http.HandlerFunc) http.HandlerFunc {
	settings := clickhouse.Settings{
		"max_memory_usage":     strconv.FormatInt(s.opts.MaxQueryMemory, 10),
		"max_result_bytes":     strconv.FormatInt(2*s.opts.MaxResponseBytes, 10),
		"result_overflow_mode": "break",
		// A sort with LIMIT keeps only its top rows once it holds this
		// much (ClickHouse waits for 1 GB by default).
		"max_bytes_before_remerge_sort": strconv.FormatInt(s.opts.MaxQueryMemory/4, 10),
	}
	for k, v := range readQuerySettings {
		settings[k] = v
	}
	progress := s.writeProgress
	if progress <= 0 {
		progress = writeProgress
	}
	return func(w http.ResponseWriter, r *http.Request) {
		pw := newProgressWriter(w, progress, WriteTimeout)
		defer pw.done()
		// Responses hold stored data: never cached, never sniffed.
		pw.Header().Set("Cache-Control", "no-store")
		pw.Header().Set("X-Content-Type-Options", "nosniff")
		if !acquire(r.Context(), s.querySem) {
			writeJSON(pw, http.StatusServiceUnavailable, errorBody{Error: "too many queries; retry later"})
			return
		}
		defer func() { <-s.querySem }()
		h(pw, r.WithContext(clickhouse.WithSettings(r.Context(), settings)))
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
//
// The body is read first, under its own bounds (readBody); only then does
// the request take one of the max_concurrent ingest slots, so the slots are
// spent on decoding and storing, never on waiting for a client.
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
	enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	switch enc {
	case "zstd", "gzip", "", "identity":
	default:
		fail(http.StatusUnsupportedMediaType, truncate("unsupported Content-Encoding "+enc))
		return
	}
	if r.ContentLength > s.opts.MaxBodyBytes {
		fail(http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	raw, release, c, err := s.readBody(w, r)
	if err != nil {
		fail(c, err.Error())
		return
	}
	defer release() // on the way out before decoding; release is idempotent
	if !acquire(r.Context(), s.ingestSem) {
		fail(http.StatusServiceUnavailable, "server busy; retry later")
		return
	}
	defer func() { <-s.ingestSem }()

	recs, res, c, msg := s.decode(enc, raw)
	// The body is garbage now: its chunks go back to the read buffer
	// rather than staying reserved while the records are stored.
	release()
	if c != 0 {
		fail(c, msg)
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

// readBody reads the compressed request body into memory, at most
// max_body_bytes: with at most maxReading requests reading at once, each
// within bodyReadTimeout, and all of them holding at most the read
// buffer's chunks of readChunk bytes, so slow or stalled clients cost
// neither ingest slots nor unbounded memory. A body first reserves all the
// chunks it may need (by its Content-Length, or max_body_bytes without
// one), and keeps those it filled until release returns them, which the
// handler calls once the body is decoded. On an error, code is the status
// to answer.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) (body io.Reader, release func(), code int, err error) {
	if !acquire(r.Context(), s.readSem) {
		return nil, nil, http.StatusServiceUnavailable, errors.New("server busy; retry later")
	}
	defer func() { <-s.readSem }()
	deadline := time.Now().Add(s.bodyTimeout)
	// Not supported by every ResponseWriter (tests); the server's own
	// ReadTimeout still applies then.
	_ = http.NewResponseController(w).SetReadDeadline(deadline)
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	need := chunksFor(s.opts.MaxBodyBytes)
	if r.ContentLength >= 0 { // the handler refused larger ones
		need = chunksFor(r.ContentLength)
	}
	if err := s.readBuf.acquire(ctx, need); err != nil {
		if r.Context().Err() == nil {
			return nil, nil, http.StatusServiceUnavailable, errors.New("server busy; retry later")
		}
		return nil, nil, http.StatusRequestTimeout, errors.New("request body not received in time")
	}
	held := need
	release = func() {
		s.readBuf.release(held)
		held = 0
	}
	fail := func(code int, msg string) (io.Reader, func(), int, error) {
		release()
		return nil, nil, code, errors.New(msg)
	}
	in := http.MaxBytesReader(w, r.Body, s.opts.MaxBodyBytes)
	var chunks []io.Reader
	var total int64
	eof := false
	for !eof && len(chunks) < need {
		size := int64(readChunk)
		if r.ContentLength >= 0 {
			size = min(size, r.ContentLength-total)
		}
		c := make([]byte, size)
		n, err := io.ReadFull(in, c)
		chunks, total = append(chunks, bytes.NewReader(c[:n])), total+int64(n)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return fail(readError(ctx, err))
			}
			eof = true
		}
	}
	if !eof {
		// Every reserved chunk is full, so the body has to end here (with
		// a Content-Length it does: net/http reads no further).
		var b [1]byte
		switch _, err := io.ReadFull(in, b[:]); {
		case errors.Is(err, io.EOF):
		case err == nil:
			return fail(http.StatusRequestEntityTooLarge, "request body too large")
		default:
			return fail(readError(ctx, err))
		}
	}
	// Chunks reserved for a body of unknown length that it did not fill.
	s.readBuf.release(held - len(chunks))
	held = len(chunks)
	return io.MultiReader(chunks...), release, 0, nil
}

// readError is the status and message for an error reading a body.
func readError(ctx context.Context, err error) (int, string) {
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		return http.StatusRequestEntityTooLarge, "request body too large"
	case errors.Is(err, os.ErrDeadlineExceeded) || ctx.Err() != nil:
		return http.StatusRequestTimeout, "request body not received in time"
	default:
		return http.StatusBadRequest, "cannot read body"
	}
}

// chunksFor is the number of read-buffer chunks n bytes take.
func chunksFor(n int64) int { return int((n + readChunk - 1) / readChunk) }

// decode decompresses and parses raw. A non-zero code is a failure of the
// whole request; invalid records are only counted and reported in res.
func (s *Server) decode(enc string, raw io.Reader) (recs []*record.Record, res IngestResult, code int, msg string) {
	in := raw
	switch enc {
	case "zstd":
		dec, err := zstd.NewReader(in, zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(uint64(s.opts.MaxDecodedBytes)),                     //nolint:gosec // positive
			zstd.WithDecoderMaxWindow(uint64(min(s.opts.MaxDecodedBytes, maxZstdWindow)))) //nolint:gosec // positive
		if err != nil {
			return nil, res, http.StatusBadRequest, truncate("bad zstd stream: " + err.Error())
		}
		// Closed before the records are stored: it holds the window.
		defer dec.Close()
		in = dec
	case "gzip":
		gz, err := gzip.NewReader(in)
		if err != nil {
			return nil, res, http.StatusBadRequest, truncate("bad gzip stream: " + err.Error())
		}
		defer gz.Close()
		in = gz
	}
	limited := &io.LimitedReader{R: in, N: s.opts.MaxDecodedBytes + 1}
	sc := bufio.NewScanner(limited)
	sc.Buffer(make([]byte, 64<<10), maxLineBytes)
	now := s.ingester.now()
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(bytes.TrimSpace(b)) == 0 {
			continue
		}
		rec, err := parseRecord(b, now, s.ingester.maxRecordAge)
		if err != nil {
			res.Rejected++
			if len(res.Errors) < maxReportedErrors {
				res.Errors = append(res.Errors, truncate(fmt.Sprintf("line %d: %v", line, err)))
			}
			continue
		}
		recs = append(recs, rec)
	}
	if err := sc.Err(); err != nil {
		switch {
		case errors.Is(err, bufio.ErrTooLong):
			return nil, res, http.StatusRequestEntityTooLarge, "a line exceeds the record size limit"
		case errors.Is(err, zstd.ErrDecoderSizeExceeded), errors.Is(err, zstd.ErrWindowSizeExceeded):
			return nil, res, http.StatusRequestEntityTooLarge, "decompressed body too large"
		default:
			return nil, res, http.StatusBadRequest, truncate("cannot read body: " + err.Error())
		}
	}
	if limited.N <= 0 {
		return nil, res, http.StatusRequestEntityTooLarge, "decompressed body too large"
	}
	return recs, res, 0, ""
}

// truncate cuts an error message reported to agents and logged to
// maxErrorLen bytes (of valid UTF-8).
func truncate(s string) string {
	if len(s) <= maxErrorLen {
		return s
	}
	return strings.ToValidUTF8(s[:maxErrorLen-3], "") + "..."
}

// parseRecord decodes and validates one line. Its errors name fields, not
// what they held: they are reported to the agent and logged.
func parseRecord(b []byte, now time.Time, maxAge time.Duration) (*record.Record, error) {
	var rec record.Record
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, jsonError(err)
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
	// Bounding ts also bounds the monthly partitions a batch can touch.
	switch {
	case rec.TS.Before(now.Add(-maxAge)):
		return nil, fmt.Errorf("ts more than %s old (ingest.max_record_age)", maxAge)
	case rec.TS.After(now.Add(maxFutureSkew)):
		return nil, fmt.Errorf("ts more than %s ahead of the server's clock", maxFutureSkew)
	}
	compact(&rec)
	return &rec, nil
}

// compact gives a decoded record's slices the length they hold: the decoder
// grows them by doubling. A hop is 88 bytes in memory for as little as 30
// of JSON (a silent one), so the records of a request take up to about
// three times its decoded size; with the slack it was 3.6 times (records of
// 9 silent hops).
func compact(rec *record.Record) {
	if cap(rec.Hops) > len(rec.Hops) {
		rec.Hops = slices.Clone(rec.Hops)
	}
	for i := range rec.Hops {
		if h := &rec.Hops[i]; cap(h.MPLS) > len(h.MPLS) {
			h.MPLS = slices.Clone(h.MPLS)
		}
	}
	if cap(rec.Loops) > len(rec.Loops) {
		rec.Loops = slices.Clone(rec.Loops)
	}
	for i := range rec.Loops {
		if l := &rec.Loops[i]; cap(l.TTLs) > len(l.TTLs) {
			l.TTLs = slices.Clone(l.TTLs)
		}
	}
}

// jsonError describes a decoding error without the value that caused it
// (encoding/json quotes values of addresses and times in its messages).
func jsonError(err error) error {
	var syn *json.SyntaxError
	var typ *json.UnmarshalTypeError
	switch {
	case errors.As(err, &syn):
		return fmt.Errorf("not a record: invalid JSON at byte %d", syn.Offset)
	case errors.As(err, &typ) && typ.Field != "":
		return fmt.Errorf("not a record: invalid value for %s", truncate(typ.Field))
	case errors.As(err, &typ):
		return errors.New("not a record: want a JSON object")
	default:
		return errors.New("not a record: invalid address or ts")
	}
}

// queryParams holds the common read-API parameters.
type queryParams struct {
	params   clickhouse.Params
	limit    int
	from, to time.Time
}

// The time range a read query may ask for: the range of a record's ts
// (record.Validate), and at most maxQuerySpan of it.
var (
	minQueryTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	maxQueryTime = time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)
)

const maxQuerySpan = topologyMaxBuckets * 24 * time.Hour

// parseQuery reads agent, target, protocol, from, to (RFC 3339 or Unix
// milliseconds; default the last 24 hours, at most 400 days, within
// 1970-2262) and limit.
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
	// Times outside what records may hold failed in ClickHouse (502, an
	// error logged and counted); a longer span cannot be drawn either (the
	// timeline's largest buckets are a day, at most 400 of them).
	if from.Before(minQueryTime) || to.After(maxQueryTime) {
		return queryParams{}, fmt.Errorf("from and to must lie within %s and %s",
			minQueryTime.Format(time.DateOnly), maxQueryTime.Format(time.DateOnly))
	}
	if to.Sub(from) > maxQuerySpan {
		return queryParams{}, fmt.Errorf("from and to may be at most %d days apart", maxQuerySpan/(24*time.Hour))
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
	// A stream is a group of about 800 bytes in each of ClickHouse's
	// threads: 660,000 planted streams took 494 MiB. So at most as many as
	// the ingester tracks (ingest.max_streams) are aggregated, or limit if
	// that is more; beyond, the rows are among the streams ClickHouse came
	// across first, and there are limit of them, as when there are more
	// streams than limit.
	groups := max(s.ingester.maxStreams, qp.limit+1)
	s.runQuery(w, r, `SELECT agent_id, target, protocol, flow_id, max(ts) AS last_ts, count() AS cycles,
		countIf(reached) / count() AS reach_ratio, argMax(reached, ts) AS last_reached,
		argMax(path_len, ts) AS last_path_len, argMax(target_ip, ts) AS last_target_ip,
		argMax(path_hash, ts) AS last_path_hash
		FROM cycles WHERE ts >= {from:DateTime64(3, 'UTC')} AND ts < {to:DateTime64(3, 'UTC')} AND `+streamFilter+`
		GROUP BY agent_id, target, protocol, flow_id ORDER BY agent_id, target, protocol, flow_id
		LIMIT {limit:UInt32}
		SETTINGS max_rows_to_group_by = `+strconv.Itoa(groups)+`, group_by_overflow_mode = 'any'`, qp, nil)
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
// ClickHouse and 10000 failed the default query.max_memory_bytes (most of it
// for formatting the block in parallel, which read queries no longer do).
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
	// With the stream given as equalities (agent and target are required),
	// ClickHouse reads the cycles in the order of the sort key (agent_id,
	// target, protocol, ts) and stops after limit, rather than sorting every
	// cycle of the window, whole (streamFilter's form defeats that). Without
	// a protocol, streams of several protocols are merged, which needs a
	// sort: a first query finds the times of the cycles to return, so that
	// the sort gets only those. 1000 of 10,000 maximal cycles took 765 MiB
	// (round four); now 52 MiB, 82 MiB without a protocol.
	const window = `FROM cycles WHERE ts >= {from:DateTime64(3, 'UTC')} AND ts < {to:DateTime64(3, 'UTC')}
		AND agent_id = {agent:String} AND target = {target:String}`
	stream, cut := ` AND protocol = {protocol:String}`, ""
	if qp.params["protocol"] == "" {
		n, last, err := s.rowsThatFit(r.Context(), `SELECT ts, 0 AS bytes `+window+`
			ORDER BY ts `+order+` LIMIT {limit:UInt32}`, qp.params)
		if err != nil {
			s.queryFailed(r, newJSONStream(w, 0, 0), "", err)
			return
		}
		qp.params["limit"], qp.params["last"] = strconv.Itoa(n), last
		stream, cut = "", ` AND ts >= {last:DateTime64(3, 'UTC')}`
		if order == "ASC" {
			cut = ` AND ts <= {last:DateTime64(3, 'UTC')}`
		}
	}
	s.runQuery(w, r, `SELECT agent_id, seq, ts, target, target_ip, af, protocol, flow_id, reached, path_len,
		path_hash, loops.ip, loops.ttls, hops.ttl, hops.status, hops.ip, hops.rtt_us, hops.code, hops.asn,
		hops.as_name, hops.name, hops.mpls
		`+window+stream+cut+`
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

// pathChangeBytes is a lower bound of the JSON of a path_changes row as the
// read API sends it (ClickHouse's, HTML-escaped): its strings and arrays
// with their quotes and separators, and less than the rest (226 bytes with
// every string and array empty).
const pathChangeBytes = `200 + length(agent_id) + length(target) + length(protocol) + length(prev_path_hash)
	+ length(path_hash) + 2 * length(ttls) + arraySum(arrayMap(x -> length(x) + 2, from_ips))
	+ arraySum(arrayMap(x -> length(x) + 2, to_ips))`

// handlePathChanges returns route changes, newest first. A change lists
// every TTL that changed, so a row can be several KB (255 TTLs: 9 KB), and
// ClickHouse sorted whole rows: 10,000 of them (the UI's limit) took
// 537 MiB, for a response cut at 32 MiB. So the query is split: the first
// part sorts only the rows' times and sizes, which tell how many of the
// newest rows can fit the response; the second reads just those whole.
func (s *Server) handlePathChanges(w http.ResponseWriter, r *http.Request) {
	qp, err := parseQuery(r, false)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	const from = `FROM path_changes FINAL WHERE ts >= {from:DateTime64(3, 'UTC')} AND ts < {to:DateTime64(3, 'UTC')} AND ` + streamFilter
	n, oldest, err := s.rowsThatFit(r.Context(), `SELECT ts, `+pathChangeBytes+` AS bytes `+from+`
		ORDER BY ts DESC LIMIT {limit:UInt32}
		SETTINGS query_plan_execute_functions_after_sorting = 0`, qp.params)
	if err != nil {
		s.queryFailed(r, newJSONStream(w, 0, 0), "", err)
		return
	}
	qp.params["limit"], qp.params["oldest"] = strconv.Itoa(n), oldest
	s.runQuery(w, r, `SELECT ts, agent_id, target, protocol, flow_id, seq, reason, ttls, from_ips, to_ips,
		prev_seq, prev_path_hash, path_hash, prev_path_len, path_len
		`+from+` AND ts >= {oldest:DateTime64(3, 'UTC')}
		ORDER BY ts DESC LIMIT {limit:UInt32}`, qp, nil)
}

// rowsThatFit runs q, which returns the ts and a lower bound of the JSON
// size (bytes) of the rows a query would return, in its order, and returns
// how many of those rows to read and the ts of the last: the rows up to
// and including the first one past the response budget (which cuts the
// response there at the latest, since it counts at least as many bytes).
// The query that then reads them whole sorts only the rows from that ts on.
// Without rows, last is the start of the window.
func (s *Server) rowsThatFit(ctx context.Context, q string, params clickhouse.Params) (n int, last string, err error) {
	var total int64
	var ts time.Time
	err = s.opts.Store.Query(ctx, q, params, func(m json.RawMessage) error {
		var row struct {
			TS    time.Time `json:"ts"`
			Bytes int64     `json:"bytes"`
		}
		if err := json.Unmarshal(m, &row); err != nil {
			return fmt.Errorf("decode row: %w", err)
		}
		n, ts, total = n+1, row.TS, total+row.Bytes+1
		if total > s.opts.MaxResponseBytes {
			return errResponseFull
		}
		return nil
	})
	if err != nil && !errors.Is(err, errResponseFull) {
		return 0, "", err
	}
	if n == 0 {
		return 0, params["from"], nil
	}
	return n, clickhouse.Time(ts), nil
}

func (s *Server) handleLoops(w http.ResponseWriter, r *http.Request) {
	qp, err := parseQuery(r, false)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	s.runQuery(w, r, `SELECT ts, agent_id, target, protocol, flow_id, seq, loop_ip, ttls
		FROM loop_events WHERE ts >= {from:DateTime64(3, 'UTC')} AND ts < {to:DateTime64(3, 'UTC')} AND `+streamFilter+`
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
		FROM outages FINAL WHERE start_ts < {to:DateTime64(3, 'UTC')}
		AND (end_ts IS NULL OR end_ts >= {from:DateTime64(3, 'UTC')}) AND `+streamFilter+`
		ORDER BY start_ts DESC LIMIT {limit:UInt32}`, qp, nil)
}
