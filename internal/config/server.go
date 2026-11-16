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
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Server defaults.
const (
	DefaultServerListen      = "127.0.0.1:8080"
	DefaultClickHouseURL     = "http://127.0.0.1:8123"
	DefaultClickHouseDB      = "hopcorder"
	DefaultClickHouseUser    = "default"
	DefaultASNURL            = "https://iptoasn.com/data/ip2asn-combined.tsv.gz"
	DefaultMaxBodyBytes      = ByteSize(8 << 20)
	DefaultMaxDecodedBytes   = ByteSize(32 << 20)
	DefaultIngestConcurrency = 4
	DefaultQueryConcurrency  = 8
	DefaultMaxResponseBytes  = ByteSize(32 << 20)
	DefaultMaxQueryMemory    = ByteSize(1 << 30)
	DefaultMaxStreams        = 20000
	DefaultMaxAgents         = 10000
	DefaultStreamIdleTimeout = time.Hour
	DefaultMaxRecordAge      = 720 * time.Hour
	defaultClickHouseTimeout = 30 * time.Second
	DefaultRetentionDays     = 90
)

// ServerConfig configures `hopcorder server`.
type ServerConfig struct {
	// Listen is the HTTP address for ingest, the read API, health and
	// metrics. hopcorder does no authentication: put a reverse proxy in
	// front, which is why the default only listens on localhost.
	Listen     string           `yaml:"listen"`
	Log        Log              `yaml:"log"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	Ingest     IngestConfig     `yaml:"ingest"`
	Enrich     EnrichConfig     `yaml:"enrich"`
	Query      QueryConfig      `yaml:"query"`
	Metrics    *bool            `yaml:"metrics"` // serve /metrics; default true
	UI         *bool            `yaml:"ui"`      // serve the web UI under /ui/; default true

	// AllowedHosts are the host names that requests may carry in their
	// Host header (any port), against DNS rebinding: a web page whose own
	// name resolves to the server's address still sends that name. Empty:
	// localhost, 127.0.0.1, ::1 and the listen host when listening on a
	// loopback address, else no check (see HostAllowlist). "*" turns the
	// check off.
	AllowedHosts []string `yaml:"allowed_hosts"`
}

// ClickHouseConfig is the connection to ClickHouse's HTTP interface.
type ClickHouseConfig struct {
	URL          string   `yaml:"url"`
	Database     string   `yaml:"database"`
	User         string   `yaml:"user"`
	Password     string   `yaml:"password"`
	PasswordFile string   `yaml:"password_file"`
	Timeout      Duration `yaml:"timeout"`
	// RetentionDays bounds how long data is kept (like the prototype's 90
	// days); 0 keeps it forever. Applied on every start.
	RetentionDays *int `yaml:"retention_days"`
}

// IngestConfig bounds ingest requests and the state kept per stream.
type IngestConfig struct {
	MaxBodyBytes    ByteSize `yaml:"max_body_bytes"`
	MaxDecodedBytes ByteSize `yaml:"max_decoded_bytes"`
	// MaxConcurrent ingest requests are processed at once; more wait up to
	// 10s, then get 503 and the agent retries.
	MaxConcurrent int `yaml:"max_concurrent"`
	// MaxStreams streams (agent, target, protocol, flow) and MaxAgents
	// agents are tracked in memory; idle ones are dropped after
	// StreamIdleTimeout, the least recently used beyond the limits. A
	// dropped stream is restored from ClickHouse when it comes back.
	MaxStreams        int      `yaml:"max_streams"`
	MaxAgents         int      `yaml:"max_agents"`
	StreamIdleTimeout Duration `yaml:"stream_idle_timeout"`
	// MaxRecordAge refuses records older than this (and more than a day
	// in the future): it must exceed the agents' spool max_age.
	MaxRecordAge Duration `yaml:"max_record_age"`
}

// QueryConfig bounds the read API.
type QueryConfig struct {
	MaxConcurrent int `yaml:"max_concurrent"` // read-API requests at once
	// MaxResponseBytes bounds one response's rows; beyond it rows are left
	// out and the response says "truncated": true.
	MaxResponseBytes ByteSize `yaml:"max_response_bytes"`
	// MaxMemoryBytes is ClickHouse's max_memory_usage for each read-API
	// query.
	MaxMemoryBytes ByteSize `yaml:"max_memory_bytes"`
}

// EnrichConfig configures hop enrichment.
type EnrichConfig struct {
	RDNS RDNSConfig `yaml:"rdns"`
	ASN  ASNConfig  `yaml:"asn"`
}

// RDNSConfig configures reverse DNS.
type RDNSConfig struct {
	Enabled     *bool    `yaml:"enabled"` // default true
	Workers     int      `yaml:"workers"`
	TTL         Duration `yaml:"ttl"`
	NegativeTTL Duration `yaml:"negative_ttl"`
	MaxEntries  int      `yaml:"max_entries"`
}

// ASNConfig configures the iptoasn database. With url set it is downloaded
// every refresh and cached in file; with only file set the operator keeps
// the file current.
type ASNConfig struct {
	Enabled *bool    `yaml:"enabled"` // default true
	URL     *string  `yaml:"url"`     // default iptoasn's combined file; "" for file only
	File    string   `yaml:"file"`
	Refresh Duration `yaml:"refresh"`
}

// ParseServer strictly decodes a server config.
func ParseServer(b []byte) (*ServerConfig, error) {
	var c ServerConfig
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return &c, nil
}

// LoadServer reads a server config file.
func LoadServer(path string) (*ServerConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	c, err := ParseServer(b)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return c, nil
}

// Finalize applies defaults and validates, reporting every problem.
func (c *ServerConfig) Finalize() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if c.Listen == "" {
		c.Listen = DefaultServerListen
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "text"
	}
	ch := &c.ClickHouse
	if ch.URL == "" {
		ch.URL = DefaultClickHouseURL
	}
	if ch.Database == "" {
		ch.Database = DefaultClickHouseDB
	}
	if ch.User == "" {
		ch.User = DefaultClickHouseUser
	}
	setDur(&ch.Timeout, defaultClickHouseTimeout)
	if ch.RetentionDays == nil {
		ch.RetentionDays = ptr(DefaultRetentionDays)
	}
	if c.Ingest.MaxBodyBytes == 0 {
		c.Ingest.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if c.Ingest.MaxDecodedBytes == 0 {
		c.Ingest.MaxDecodedBytes = DefaultMaxDecodedBytes
	}
	setInt(&c.Ingest.MaxConcurrent, DefaultIngestConcurrency)
	setInt(&c.Ingest.MaxStreams, DefaultMaxStreams)
	setInt(&c.Ingest.MaxAgents, DefaultMaxAgents)
	setDur(&c.Ingest.StreamIdleTimeout, DefaultStreamIdleTimeout)
	setDur(&c.Ingest.MaxRecordAge, DefaultMaxRecordAge)
	setInt(&c.Query.MaxConcurrent, DefaultQueryConcurrency)
	if c.Query.MaxResponseBytes == 0 {
		c.Query.MaxResponseBytes = DefaultMaxResponseBytes
	}
	if c.Query.MaxMemoryBytes == 0 {
		c.Query.MaxMemoryBytes = DefaultMaxQueryMemory
	}
	if c.Metrics == nil {
		c.Metrics = ptr(true)
	}
	if c.UI == nil {
		c.UI = ptr(true)
	}
	r := &c.Enrich.RDNS
	if r.Enabled == nil {
		r.Enabled = ptr(true)
	}
	setInt(&r.Workers, 4)
	setDur(&r.TTL, 24*time.Hour)
	setDur(&r.NegativeTTL, time.Hour)
	setInt(&r.MaxEntries, 100000)
	a := &c.Enrich.ASN
	if a.Enabled == nil {
		a.Enabled = ptr(true)
	}
	if a.URL == nil {
		a.URL = ptr(DefaultASNURL)
	}
	setDur(&a.Refresh, 24*time.Hour)

	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		fail("listen %q: want host:port: %v", c.Listen, err)
	}
	for _, h := range c.AllowedHosts {
		if !validAllowedHost(h) {
			fail("allowed_hosts %q: want a host name or IP address without port, or \"*\"", h)
		}
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
	if u, err := url.Parse(ch.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		fail("clickhouse.url: want http(s)://host:port")
	} else if u.User != nil {
		fail("clickhouse.url: put credentials in user and password (or password_file), not in the URL")
	}
	if strings.ContainsAny(ch.Database, "`;. ") {
		fail("clickhouse.database %q: invalid name", ch.Database)
	}
	if ch.Password != "" && ch.PasswordFile != "" {
		fail("clickhouse: password and password_file are mutually exclusive")
	}
	if ch.PasswordFile != "" {
		if _, err := os.Stat(ch.PasswordFile); err != nil {
			fail("clickhouse.password_file: %v", err)
		}
	}
	if ch.Timeout.D() <= 0 {
		fail("clickhouse.timeout: must be positive")
	}
	if *ch.RetentionDays < 0 || *ch.RetentionDays > 36500 {
		fail("clickhouse.retention_days %d: want 0 (keep forever) to 36500", *ch.RetentionDays)
	}
	if c.Ingest.MaxBodyBytes < 1<<20 || c.Ingest.MaxDecodedBytes < c.Ingest.MaxBodyBytes {
		fail("ingest: need max_body_bytes >= 1MiB and max_decoded_bytes >= max_body_bytes")
	}
	if c.Ingest.MaxConcurrent < 1 || c.Ingest.MaxStreams < 1 || c.Ingest.MaxAgents < 1 ||
		c.Ingest.StreamIdleTimeout.D() < time.Minute || c.Query.MaxConcurrent < 1 {
		fail("ingest/query: max_concurrent, max_streams and max_agents must be positive, stream_idle_timeout at least 1m")
	}
	if c.Ingest.MaxRecordAge.D() < time.Hour {
		fail("ingest.max_record_age: must be at least 1h (and longer than the agents' spool max_age)")
	}
	if c.Query.MaxResponseBytes < 1<<20 || c.Query.MaxMemoryBytes < 16<<20 {
		fail("query: max_response_bytes must be at least 1MiB, max_memory_bytes at least 16MiB")
	}
	if *r.Enabled && (r.Workers < 1 || r.MaxEntries < 1 || r.TTL.D() <= 0 || r.NegativeTTL.D() <= 0) {
		fail("enrich.rdns: workers, max_entries, ttl and negative_ttl must be positive")
	}
	if *a.Enabled {
		if *a.URL == "" && a.File == "" {
			fail("enrich.asn: set url or file (or enabled: false)")
		}
		if *a.URL != "" {
			if u, err := url.Parse(*a.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
				fail("enrich.asn.url %q: want an http(s) URL", *a.URL)
			}
		}
		if a.Refresh.D() < time.Minute {
			fail("enrich.asn.refresh: must be at least 1m")
		}
	}
	return errors.Join(errs...)
}

// HostAllowlist returns the host names the server answers (see
// AllowedHosts), or nil for no check. Without allowed_hosts, a server on a
// loopback address answers localhost, 127.0.0.1, ::1 and its listen host:
// only a browser on the same machine can reach it, and DNS rebinding is how
// a web page would. On other addresses it is reached under service names,
// pod IPs and proxy hosts that it cannot know, so there is no check;
// operators list their names to get one.
func (c *ServerConfig) HostAllowlist() []string {
	if len(c.AllowedHosts) > 0 {
		if slices.Contains(c.AllowedHosts, "*") {
			return nil
		}
		return slices.Clone(c.AllowedHosts)
	}
	host, _, err := net.SplitHostPort(c.Listen)
	if err != nil {
		return nil
	}
	ip, err := netip.ParseAddr(host)
	if !strings.EqualFold(host, "localhost") && (err != nil || !ip.IsLoopback()) {
		return nil
	}
	hosts := []string{"localhost", "127.0.0.1", "::1"}
	if !slices.Contains(hosts, strings.ToLower(host)) {
		hosts = append(hosts, host)
	}
	return hosts
}

// validAllowedHost accepts "*", an IP address (IPv6 with or without
// brackets) or a host name.
func validAllowedHost(h string) bool {
	if h == "*" {
		return true
	}
	if _, err := netip.ParseAddr(strings.TrimSuffix(strings.TrimPrefix(h, "["), "]")); err == nil {
		return true
	}
	return h != "" && len(h) <= 253 && !strings.ContainsFunc(h, func(c rune) bool {
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		return !alnum && c != '.' && c != '-' && c != '_'
	})
}

// ClickHousePassword returns the configured password, reading the file if
// one is set.
func (c *ServerConfig) ClickHousePassword() (string, error) {
	if c.ClickHouse.PasswordFile == "" {
		return c.ClickHouse.Password, nil
	}
	b, err := os.ReadFile(c.ClickHouse.PasswordFile)
	if err != nil {
		return "", fmt.Errorf("clickhouse.password_file: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}
