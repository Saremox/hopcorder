// Package config loads, defaults and validates the agent configuration: a
// YAML file plus command-line overrides. See docs/HANDOFF.md §5.4 and the
// README for the reference.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Defaults.
const (
	DefaultInterval     = 5 * time.Second
	DefaultTimeout      = time.Second
	DefaultMaxTTL       = 30
	DefaultProbePast    = 2
	DefaultProtocol     = "icmp"
	DefaultPort         = 33434
	DefaultFlowID       = 33434
	DefaultDNSTTL       = 5 * time.Minute
	DefaultSpoolMaxAge  = 7 * 24 * time.Hour
	DefaultSpoolMaxSize = ByteSize(1 << 30)

	DefaultHTTPTimeout       = 30 * time.Second
	DefaultHTTPBatchRecords  = 500
	DefaultHTTPBatchBytes    = ByteSize(1 << 20)
	DefaultHTTPFlushInterval = 10 * time.Second
	DefaultHTTPRetryMin      = time.Second
	DefaultHTTPRetryMax      = time.Minute
	DefaultMetricsPath       = "/metrics"

	minInterval     = 100 * time.Millisecond
	minSpoolMaxSize = ByteSize(1 << 20)
)

// Config is the agent configuration.
type Config struct {
	AgentID   string   `yaml:"agent_id"`
	Interval  Duration `yaml:"interval"`
	Timeout   Duration `yaml:"timeout"`
	MaxTTL    int      `yaml:"max_ttl"`
	ProbePast *int     `yaml:"probe_past"`
	Protocol  string   `yaml:"protocol"`
	Paris     *bool    `yaml:"paris"`
	// AdaptiveMaxTTL probes only a few TTLs past the destination's last
	// distance; cycles that fall short are extended in the same cycle.
	AdaptiveMaxTTL *bool    `yaml:"adaptive_max_ttl"`
	DNSTTL         Duration `yaml:"dns_ttl"`
	Log            Log      `yaml:"log"`
	Spool          Spool    `yaml:"spool"`
	Targets        []Target `yaml:"targets"`
	Outputs        Outputs  `yaml:"outputs"`
}

// Log configures logging.
type Log struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // text, json
}

// Spool configures the durable on-disk queue.
type Spool struct {
	Dir      string   `yaml:"dir"`
	MaxAge   Duration `yaml:"max_age"`
	MaxBytes ByteSize `yaml:"max_bytes"`
}

// Target is one probed destination. Zero fields inherit the global value.
type Target struct {
	Host     string `yaml:"host"`
	Protocol string `yaml:"protocol"`
	Port     int    `yaml:"port"`
	AF       int    `yaml:"af"` // 4 or 6; 0 prefers IPv4
	FlowID   int    `yaml:"flow_id"`
	Paris    *bool  `yaml:"paris"`
	// AdaptiveMaxTTL overrides the global setting.
	AdaptiveMaxTTL *bool    `yaml:"adaptive_max_ttl"`
	Interval       Duration `yaml:"interval"`
	Timeout        Duration `yaml:"timeout"`
	MaxTTL         int      `yaml:"max_ttl"`
	ProbePast      *int     `yaml:"probe_past"`
}

// Outputs selects where records go. A nil output is disabled.
type Outputs struct {
	JSONL   *JSONL   `yaml:"jsonl"`
	HTTP    *HTTP    `yaml:"http"`
	Metrics *Metrics `yaml:"metrics"`
}

// JSONL appends one JSON record per line to a file, or stdout for "-".
type JSONL struct {
	Path string `yaml:"path"`
}

// HTTP pushes zstd-compressed NDJSON batches to <URL>/api/v1/ingest.
type HTTP struct {
	URL             string            `yaml:"url"`
	Headers         map[string]string `yaml:"headers"`
	BearerToken     string            `yaml:"bearer_token"`
	BearerTokenFile string            `yaml:"bearer_token_file"`
	BasicAuth       *BasicAuth        `yaml:"basic_auth"`
	TLS             TLS               `yaml:"tls"`
	Timeout         Duration          `yaml:"timeout"`
	BatchRecords    int               `yaml:"batch_max_records"`
	BatchBytes      ByteSize          `yaml:"batch_max_bytes"`
	FlushInterval   Duration          `yaml:"flush_interval"`
	RetryMin        Duration          `yaml:"retry_min"`
	RetryMax        Duration          `yaml:"retry_max"`
}

// BasicAuth holds HTTP basic-auth credentials.
type BasicAuth struct {
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	PasswordFile string `yaml:"password_file"`
}

// TLS configures the HTTP client's TLS.
type TLS struct {
	CAFile     string `yaml:"ca_file"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	ServerName string `yaml:"server_name"`
}

// Metrics serves Prometheus metrics.
type Metrics struct {
	Listen string `yaml:"listen"`
	Path   string `yaml:"path"`
}

// Load reads and strictly decodes a YAML config file; unknown keys are
// errors. It does not apply defaults; call Finalize after any overrides.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	c, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return c, nil
}

// Parse strictly decodes a YAML config.
func Parse(b []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return &c, nil
}

// FinalizeOptions adjusts validation to the run mode.
type FinalizeOptions struct {
	// Once relaxes the spool and output requirements: --once prints to
	// stdout and touches neither.
	Once bool
	// Hostname returns the default agent_id; it defaults to os.Hostname.
	Hostname func() (string, error)
	// Stat checks that referenced files exist; it defaults to os.Stat.
	Stat func(string) (os.FileInfo, error)
}

// Finalize applies defaults (global values to targets, then built-in
// defaults) and validates the whole configuration, reporting every problem
// at once.
func (c *Config) Finalize(o FinalizeOptions) error {
	if o.Hostname == nil {
		o.Hostname = os.Hostname
	}
	if o.Stat == nil {
		o.Stat = os.Stat
	}
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.AgentID == "" {
		h, err := o.Hostname()
		if err != nil {
			fail("agent_id: not set and hostname unavailable: %v", err)
		}
		c.AgentID = h
	}
	setDur(&c.Interval, DefaultInterval)
	setDur(&c.Timeout, DefaultTimeout)
	setInt(&c.MaxTTL, DefaultMaxTTL)
	if c.ProbePast == nil {
		c.ProbePast = ptr(DefaultProbePast)
	}
	if c.Protocol == "" {
		c.Protocol = DefaultProtocol
	}
	if c.Paris == nil {
		c.Paris = ptr(true)
	}
	if c.AdaptiveMaxTTL == nil {
		c.AdaptiveMaxTTL = ptr(true)
	}
	setDur(&c.DNSTTL, DefaultDNSTTL)
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "text"
	}
	setDur(&c.Spool.MaxAge, DefaultSpoolMaxAge)
	if c.Spool.MaxBytes == 0 {
		c.Spool.MaxBytes = DefaultSpoolMaxSize
	}

	if c.AgentID != "" && (len(c.AgentID) > 128 || strings.ContainsFunc(c.AgentID, isSpaceOrControl)) {
		fail("agent_id %q: must be at most 128 characters without whitespace", c.AgentID)
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		fail("log.level %q: want debug, info, warn or error", c.Log.Level)
	}
	switch c.Log.Format {
	case "text", "json":
	default:
		fail("log.format %q: want text or json", c.Log.Format)
	}
	if c.DNSTTL.D() < time.Second {
		fail("dns_ttl %s: must be at least 1s", c.DNSTTL.D())
	}

	if len(c.Targets) == 0 {
		fail("targets: at least one target is required")
	}
	seen := map[string]int{}
	for i := range c.Targets {
		t := &c.Targets[i]
		c.defaultTarget(t)
		for _, err := range validateTarget(t) {
			fail("targets[%d] (%s): %w", i, t.Host, err)
		}
		key := fmt.Sprintf("%s|%s|%d|%d", t.Host, t.Protocol, t.Port, t.AF)
		if j, dup := seen[key]; dup {
			fail("targets[%d] (%s): duplicates targets[%d]", i, t.Host, j)
		}
		seen[key] = i
	}

	if !o.Once {
		if c.Spool.Dir == "" {
			fail("spool.dir: required")
		}
		if c.Spool.MaxAge.D() <= 0 {
			fail("spool.max_age: must be positive")
		}
		if c.Spool.MaxBytes < minSpoolMaxSize {
			fail("spool.max_bytes %d: must be at least 1MiB", c.Spool.MaxBytes)
		}
		if c.Outputs.JSONL == nil && c.Outputs.HTTP == nil && c.Outputs.Metrics == nil {
			fail("outputs: configure at least one of jsonl, http, metrics")
		}
	}
	if j := c.Outputs.JSONL; j != nil && j.Path == "" {
		fail("outputs.jsonl.path: required (use \"-\" for stdout)")
	}
	if h := c.Outputs.HTTP; h != nil {
		for _, err := range h.finalize(o.Stat) {
			fail("outputs.http: %w", err)
		}
	}
	if m := c.Outputs.Metrics; m != nil {
		if m.Path == "" {
			m.Path = DefaultMetricsPath
		}
		if _, _, err := net.SplitHostPort(m.Listen); err != nil {
			fail("outputs.metrics.listen %q: want host:port: %v", m.Listen, err)
		}
		if !strings.HasPrefix(m.Path, "/") {
			fail("outputs.metrics.path %q: must start with /", m.Path)
		}
	}
	return errors.Join(errs...)
}

func (c *Config) defaultTarget(t *Target) {
	t.Host = strings.TrimSpace(t.Host)
	if t.Protocol == "" {
		t.Protocol = c.Protocol
	}
	if t.Protocol == "udp" && t.Port == 0 {
		t.Port = DefaultPort
	}
	if t.FlowID == 0 {
		t.FlowID = DefaultFlowID
	}
	if t.Paris == nil {
		t.Paris = c.Paris
	}
	if t.AdaptiveMaxTTL == nil {
		t.AdaptiveMaxTTL = c.AdaptiveMaxTTL
	}
	setDur(&t.Interval, c.Interval.D())
	setDur(&t.Timeout, c.Timeout.D())
	setInt(&t.MaxTTL, c.MaxTTL)
	if t.ProbePast == nil {
		t.ProbePast = c.ProbePast
	}
	if t.AF == 0 {
		if ip, err := netip.ParseAddr(t.Host); err == nil {
			t.AF = 4
			if ip.Unmap().Is6() {
				t.AF = 6
			}
		}
	}
}

func validateTarget(t *Target) []error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if t.Host == "" {
		fail("host: required")
	}
	switch t.Protocol {
	case "icmp":
		if t.Port != 0 {
			fail("port: only valid for udp")
		}
	case "udp":
		if t.Port < 1 || t.Port > 65535 {
			fail("port %d: out of range 1..65535", t.Port)
		}
	case "tcp":
		fail("protocol tcp: not implemented yet (planned for milestone 2)")
	default:
		fail("protocol %q: want icmp or udp", t.Protocol)
	}
	switch t.AF {
	case 0, 4, 6:
	default:
		fail("af %d: want 4 or 6", t.AF)
	}
	if ip, err := netip.ParseAddr(t.Host); err == nil {
		if is6 := ip.Unmap().Is6(); (is6 && t.AF == 4) || (!is6 && t.AF == 6) {
			fail("af %d does not match address %s", t.AF, ip)
		}
	}
	if t.FlowID < 1 || t.FlowID > 65535 {
		fail("flow_id %d: out of range 1..65535", t.FlowID)
	}
	if t.Interval.D() < minInterval {
		fail("interval %s: must be at least %s", t.Interval.D(), minInterval)
	}
	if t.Timeout.D() <= 0 {
		fail("timeout %s: must be positive", t.Timeout.D())
	} else if t.Timeout.D() >= t.Interval.D() {
		fail("timeout %s: must be shorter than the interval %s", t.Timeout.D(), t.Interval.D())
	}
	if t.MaxTTL < 1 || t.MaxTTL > 255 {
		fail("max_ttl %d: out of range 1..255", t.MaxTTL)
	}
	if *t.ProbePast < 0 || *t.ProbePast > 255 {
		fail("probe_past %d: out of range 0..255", *t.ProbePast)
	}
	return errs
}

func (h *HTTP) finalize(stat func(string) (os.FileInfo, error)) []error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	setDur(&h.Timeout, DefaultHTTPTimeout)
	setInt(&h.BatchRecords, DefaultHTTPBatchRecords)
	if h.BatchBytes == 0 {
		h.BatchBytes = DefaultHTTPBatchBytes
	}
	setDur(&h.FlushInterval, DefaultHTTPFlushInterval)
	setDur(&h.RetryMin, DefaultHTTPRetryMin)
	setDur(&h.RetryMax, DefaultHTTPRetryMax)

	u, err := url.Parse(h.URL)
	switch {
	case h.URL == "":
		fail("url: required")
	case err != nil:
		fail("url %q: %v", h.URL, err)
	case u.Scheme != "http" && u.Scheme != "https":
		fail("url %q: scheme must be http or https", h.URL)
	case u.Host == "":
		fail("url %q: missing host", h.URL)
	case u.User != nil:
		fail("url: put credentials in basic_auth, not the URL")
	}
	if h.BearerToken != "" && h.BearerTokenFile != "" {
		fail("bearer_token and bearer_token_file are mutually exclusive")
	}
	if (h.BearerToken != "" || h.BearerTokenFile != "") && h.BasicAuth != nil {
		fail("bearer token and basic_auth are mutually exclusive")
	}
	checkFile := func(field, path string) {
		if path == "" {
			return
		}
		if _, err := stat(path); err != nil {
			fail("%s: %v", field, err)
		}
	}
	checkFile("bearer_token_file", h.BearerTokenFile)
	if b := h.BasicAuth; b != nil {
		if b.Username == "" {
			fail("basic_auth.username: required")
		}
		if b.Password != "" && b.PasswordFile != "" {
			fail("basic_auth: password and password_file are mutually exclusive")
		}
		checkFile("basic_auth.password_file", b.PasswordFile)
	}
	if (h.TLS.CertFile == "") != (h.TLS.KeyFile == "") {
		fail("tls: cert_file and key_file must be set together")
	}
	checkFile("tls.ca_file", h.TLS.CAFile)
	checkFile("tls.cert_file", h.TLS.CertFile)
	checkFile("tls.key_file", h.TLS.KeyFile)
	for k := range h.Headers {
		if k == "" || strings.ContainsFunc(k, func(r rune) bool { return r == ':' || isSpaceOrControl(r) }) {
			fail("headers: invalid header name %q", k)
		}
	}
	if h.Timeout.D() <= 0 {
		fail("timeout: must be positive")
	}
	if h.BatchRecords < 1 {
		fail("batch_max_records: must be positive")
	}
	if h.BatchBytes < 1024 {
		fail("batch_max_bytes: must be at least 1KiB")
	}
	if h.FlushInterval.D() <= 0 {
		fail("flush_interval: must be positive")
	}
	if h.RetryMin.D() <= 0 || h.RetryMax.D() < h.RetryMin.D() {
		fail("retry_min/retry_max: need 0 < retry_min <= retry_max")
	}
	return errs
}

func isSpaceOrControl(r rune) bool { return r <= ' ' || r == 0x7f }

func setDur(d *Duration, def time.Duration) {
	if *d == 0 {
		*d = Duration(def)
	}
}

func setInt(i *int, def int) {
	if *i == 0 {
		*i = def
	}
}

func ptr[T any](v T) *T { return &v }
