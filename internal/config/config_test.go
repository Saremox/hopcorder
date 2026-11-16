package config

import (
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"
)

// handoffExample is the example from docs/HANDOFF.md §5.4.
const handoffExample = `
agent_id: lap-mstrassberger      # default: hostname
interval: 5s
timeout: 1s
max_ttl: 30
probe_past: 2
spool:
  dir: /var/lib/hopcorder
  max_age: 168h
  max_bytes: 1GiB
targets:
  - host: mumble.saremox.dev
  - host: 79.99.80.226
    protocol: udp
    port: 33434
outputs:
  jsonl:
    path: /var/log/hopcorder.jsonl
  http:
    url: https://hopcorder.example.net
    bearer_token_file: /etc/hopcorder/token
  metrics:
    listen: 127.0.0.1:9469
`

func statOK(string) (os.FileInfo, error) { return nil, nil }

func finalize(t *testing.T, yml string, o FinalizeOptions) (*Config, error) {
	t.Helper()
	c, err := Parse([]byte(yml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.Stat == nil {
		o.Stat = statOK
	}
	if o.Hostname == nil {
		o.Hostname = func() (string, error) { return "testhost", nil }
	}
	return c, c.Finalize(o)
}

func TestHandoffExample(t *testing.T) {
	c, err := finalize(t, handoffExample, FinalizeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if c.AgentID != "lap-mstrassberger" || c.Spool.MaxBytes != 1<<30 || c.Spool.MaxAge.D() != 168*time.Hour {
		t.Errorf("unexpected globals: %+v", c)
	}
	t0, t1 := c.Targets[0], c.Targets[1]
	if t0.Protocol != "icmp" || t0.Port != 0 || t0.AF != 0 || t0.Interval.D() != 5*time.Second || t0.MaxTTL != 30 ||
		*t0.ProbePast != 2 || !*t0.Paris || t0.FlowID != DefaultFlowID {
		t.Errorf("target 0 defaults: %+v", t0)
	}
	c2, err := finalize(t, "targets: [{host: x.example, protocol: tcp}, {host: y.example, protocol: tcp, port: 22}]", FinalizeOptions{Once: true})
	if err != nil {
		t.Fatal(err)
	}
	if c2.Targets[0].Port != DefaultTCPPort || c2.Targets[1].Port != 22 {
		t.Errorf("tcp ports %d %d", c2.Targets[0].Port, c2.Targets[1].Port)
	}
	if !*t0.AdaptiveMaxTTL {
		t.Error("adaptive_max_ttl must default to true")
	}
	if t1.Protocol != "udp" || t1.Port != 33434 || t1.AF != 4 {
		t.Errorf("target 1: %+v", t1)
	}
	h := c.Outputs.HTTP
	if h.BatchRecords != 500 || h.BatchBytes != 1<<20 || h.FlushInterval.D() != 10*time.Second || h.Timeout.D() != 30*time.Second {
		t.Errorf("http defaults: %+v", h)
	}
	if c.Outputs.Metrics.Path != "/metrics" {
		t.Errorf("metrics path %q", c.Outputs.Metrics.Path)
	}
}

func TestDefaultsAndOverrides(t *testing.T) {
	c, err := finalize(t, `
interval: 10s
protocol: udp
paris: false
targets:
  - host: example.net
  - host: 2001:db8::1
    protocol: icmp
    interval: 30s
    max_ttl: 12
    probe_past: 0
    paris: true
    flow_id: 4242
    adaptive_max_ttl: false
spool: {dir: /tmp/x}
outputs: {jsonl: {path: "-"}}
`, FinalizeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if c.AgentID != "testhost" {
		t.Errorf("agent_id %q", c.AgentID)
	}
	a, b := c.Targets[0], c.Targets[1]
	if a.Protocol != "udp" || a.Port != DefaultPort || a.Interval.D() != 10*time.Second || *a.Paris || a.Timeout.D() != time.Second {
		t.Errorf("target a: %+v", a)
	}
	if b.Protocol != "icmp" || b.Interval.D() != 30*time.Second || b.MaxTTL != 12 || *b.ProbePast != 0 || !*b.Paris ||
		b.FlowID != 4242 || b.AF != 6 || *b.AdaptiveMaxTTL {
		t.Errorf("target b: %+v", b)
	}
}

func TestValidationReportsEverything(t *testing.T) {
	_, err := finalize(t, `
agent_id: "has space"
interval: 50ms
max_ttl: 300
log: {level: loud, format: xml}
targets:
  - host: ""
  - host: 192.0.2.1
    af: 6
  - host: a.example
    protocol: sctp
  - host: b.example
    protocol: icmp
    port: 80
  - host: c.example
    protocol: udp
    port: 70000
  - host: d.example
    timeout: 10s
  - host: e.example
  - host: e.example
spool: {max_bytes: 10KiB}
outputs:
  jsonl: {}
  http:
    url: ftp://x
    bearer_token: a
    bearer_token_file: /t
    basic_auth: {password: x, password_file: y}
    tls: {cert_file: c}
    headers: {"Bad Header": x}
  metrics: {listen: nope}
`, FinalizeOptions{})
	if err == nil {
		t.Fatal("invalid config accepted")
	}
	for _, want := range []string{
		"agent_id", "log.level", "log.format",
		"targets[0] (): host: required",
		"af 6 does not match",
		"protocol \"sctp\": want icmp, udp or tcp",
		"port: only valid for udp",
		"port 70000",
		"must be shorter than the interval",
		"interval 50ms",
		"max_ttl 300",
		"targets[7] (e.example): duplicates targets[6]",
		"spool.dir: required",
		"spool.max_bytes",
		"outputs.jsonl.path",
		"scheme must be http or https",
		"mutually exclusive",
		"basic_auth.username",
		"cert_file and key_file",
		"invalid header name",
		"outputs.metrics.listen",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n%v", want, err)
		}
	}
}

func TestOnceRelaxesSpoolAndOutputs(t *testing.T) {
	if _, err := finalize(t, "targets: [{host: example.net}]", FinalizeOptions{Once: true}); err != nil {
		t.Errorf("once: %v", err)
	}
	if _, err := finalize(t, "targets: [{host: example.net}]", FinalizeOptions{}); err == nil {
		t.Error("daemon mode without spool or outputs accepted")
	}
}

func TestMissingFiles(t *testing.T) {
	_, err := finalize(t, `
targets: [{host: example.net}]
spool: {dir: /tmp/x}
outputs:
  http:
    url: https://x.example
    bearer_token_file: /nonexistent/token
    tls: {ca_file: /nonexistent/ca.pem}
`, FinalizeOptions{Stat: func(string) (os.FileInfo, error) { return nil, fs.ErrNotExist }})
	if err == nil || !strings.Contains(err.Error(), "bearer_token_file") || !strings.Contains(err.Error(), "tls.ca_file") {
		t.Errorf("err = %v", err)
	}
}

func TestStrictDecoding(t *testing.T) {
	for _, yml := range []string{
		"intervall: 5s",
		"targets: [{hostname: x}]",
		"interval: 5",
		"interval: fast",
		"spool: {max_bytes: lots}",
	} {
		if _, err := Parse([]byte(yml)); err == nil {
			t.Errorf("%q accepted", yml)
		}
	}
	if c, err := Parse(nil); err != nil || c == nil {
		t.Errorf("empty config: %v", err)
	}
}

func TestParseByteSize(t *testing.T) {
	tests := map[string]ByteSize{
		"1GiB": 1 << 30, "512MiB": 512 << 20, "10 KiB": 10 << 10, "1GB": 1e9, "5MB": 5e6,
		"1048576": 1 << 20, "100B": 100, "0": 0,
	}
	for in, want := range tests {
		got, err := ParseByteSize(in)
		if err != nil || got != want {
			t.Errorf("ParseByteSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "GiB", "-1MiB", "1.5GiB", "1PiB", "99999999999TiB"} {
		if _, err := ParseByteSize(in); err == nil {
			t.Errorf("ParseByteSize(%q) accepted", in)
		}
	}
}

func TestLoad(t *testing.T) {
	if _, err := Load("/nonexistent/hopcorder.yaml"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("err = %v", err)
	}
	p := t.TempDir() + "/c.yaml"
	if err := os.WriteFile(p, []byte("bogus: 1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err == nil || !strings.Contains(err.Error(), p) {
		t.Errorf("err = %v, want it to name the file", err)
	}
}

func TestShippedExampleIsValid(t *testing.T) {
	c, err := Load("../../deploy/agent.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(FinalizeOptions{Stat: statOK}); err != nil {
		t.Fatal(err)
	}
}

// TLS settings on a plain-http URL would be silently unused; batches must
// fit the server's default request limit.
func TestHTTPOutputSafety(t *testing.T) {
	base := "targets: [{host: example.net}]\nspool: {dir: /tmp/x}\noutputs:\n  http:\n"
	for _, c := range []struct {
		yaml, want string
	}{
		{"    url: http://ingest.example\n    tls: {server_name: x}\n", "needs an https url"},
		{"    url: https://ingest.example\n    batch_max_bytes: 64MiB\n", "batch_max_bytes"},
	} {
		cfg, err := Parse([]byte(base + c.yaml))
		if err != nil {
			t.Fatal(err)
		}
		if err := cfg.Finalize(FinalizeOptions{}); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%q: %v", c.yaml, err)
		}
	}
	h := &HTTP{URL: "http://ingest.example", BearerToken: "t"}
	if !h.CleartextCredentials() {
		t.Error("bearer token over http not flagged")
	}
	h.URL = "https://ingest.example"
	if h.CleartextCredentials() {
		t.Error("https flagged")
	}
}
