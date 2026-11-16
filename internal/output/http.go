package output

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// IngestPath is appended to the configured server URL.
const IngestPath = "/api/v1/ingest"

// HTTPConfig configures the HTTP push output. Authentication is the
// operator's reverse proxy's job; the agent only presents credentials.
type HTTPConfig struct {
	URL     string // server base URL; IngestPath is appended
	Headers map[string]string
	// BearerToken, or BearerTokenFile which is re-read for every request so
	// tokens can be rotated without a restart.
	BearerToken     string
	BearerTokenFile string
	// Basic auth; PasswordFile is re-read for every request.
	BasicUser         string
	BasicPassword     string
	BasicPasswordFile string
	CAFile            string // replaces the system roots when set
	CertFile          string // client certificate for mTLS
	KeyFile           string
	ServerName        string
	Timeout           time.Duration
	UserAgent         string
}

// HTTP POSTs batches as zstd-compressed NDJSON.
type HTTP struct {
	cfg    HTTPConfig
	url    string
	client *http.Client
	enc    *zstd.Encoder
}

// NewHTTP builds the client, loading CA and client certificates up front so
// misconfiguration fails at startup.
func NewHTTP(cfg HTTPConfig) (*HTTP, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("http output: %w", err)
	}
	if !strings.HasSuffix(strings.TrimRight(u.Path, "/"), IngestPath) {
		u.Path = strings.TrimRight(u.Path, "/") + IngestPath
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("http output: ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("http output: ca_file %s: no PEM certificates", cfg.CAFile)
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("http output: client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	// Fail now rather than on the first delivery if a secret file is
	// unreadable.
	for _, f := range []string{cfg.BearerTokenFile, cfg.BasicPasswordFile} {
		if f != "" {
			if _, err := readSecret(f); err != nil {
				return nil, fmt.Errorf("http output: %w", err)
			}
		}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tlsCfg
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	return &HTTP{
		cfg:    cfg,
		url:    u.String(),
		client: &http.Client{Transport: tr, Timeout: cfg.Timeout},
		enc:    enc,
	}, nil
}

// Name implements Output.
func (h *HTTP) Name() string { return "http" }

// URL returns the ingest URL.
func (h *HTTP) URL() string { return h.url }

// Deliver implements Output. Any non-2xx response is an error and the batch
// will be retried; the server deduplicates on (agent_id, seq), so it should
// answer 2xx for records it already has.
func (h *HTTP) Deliver(ctx context.Context, lines [][]byte) error {
	n := 0
	for _, l := range lines {
		n += len(l) + 1
	}
	raw := make([]byte, 0, n)
	for _, l := range lines {
		raw = append(raw, l...)
		raw = append(raw, '\n')
	}
	body := h.enc.EncodeAll(raw, nil)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range h.cfg.Headers {
		if strings.EqualFold(k, "Host") {
			req.Host = v
			continue
		}
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "zstd")
	if h.cfg.UserAgent != "" {
		req.Header.Set("User-Agent", h.cfg.UserAgent)
	}
	if err := h.authorize(req); err != nil {
		return err
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", h.url, err)
	}
	defer resp.Body.Close()
	excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // allow connection reuse
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("post %s: %s: %s", h.url, resp.Status, strings.TrimSpace(string(excerpt)))
	}
	return nil
}

func (h *HTTP) authorize(req *http.Request) error {
	switch {
	case h.cfg.BearerToken != "":
		req.Header.Set("Authorization", "Bearer "+h.cfg.BearerToken)
	case h.cfg.BearerTokenFile != "":
		tok, err := readSecret(h.cfg.BearerTokenFile)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	case h.cfg.BasicUser != "":
		pw := h.cfg.BasicPassword
		if h.cfg.BasicPasswordFile != "" {
			var err error
			if pw, err = readSecret(h.cfg.BasicPasswordFile); err != nil {
				return err
			}
		}
		req.SetBasicAuth(h.cfg.BasicUser, pw)
	}
	return nil
}

func readSecret(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read secret: %w", err)
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "", errors.New("read secret: " + path + " is empty")
	}
	return s, nil
}

// Close releases idle connections.
func (h *HTTP) Close() error {
	h.client.CloseIdleConnections()
	h.enc.Close()
	return nil
}
