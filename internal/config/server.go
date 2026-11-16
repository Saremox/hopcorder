package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
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
	DefaultMaxBodyBytes      = ByteSize(64 << 20)
	DefaultMaxDecodedBytes   = ByteSize(512 << 20)
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
	Metrics    *bool            `yaml:"metrics"` // serve /metrics; default true
	UI         *bool            `yaml:"ui"`      // serve the web UI under /ui/; default true
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

// IngestConfig bounds ingest requests.
type IngestConfig struct {
	MaxBodyBytes    ByteSize `yaml:"max_body_bytes"`
	MaxDecodedBytes ByteSize `yaml:"max_decoded_bytes"`
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
		fail("clickhouse.url %q: want http(s)://host:port", ch.URL)
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
