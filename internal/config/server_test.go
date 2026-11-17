package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestServerDefaults(t *testing.T) {
	c, err := ParseServer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(); err != nil {
		t.Fatal(err)
	}
	if c.Listen != DefaultServerListen || c.ClickHouse.URL != DefaultClickHouseURL || c.ClickHouse.Database != "hopcorder" ||
		!*c.Metrics || !*c.Enrich.RDNS.Enabled || *c.Enrich.ASN.URL != DefaultASNURL ||
		c.Enrich.ASN.Refresh.D() != 24*time.Hour || c.Ingest.MaxBodyBytes != DefaultMaxBodyBytes ||
		*c.ClickHouse.RetentionDays != 90 || !*c.UI || c.Ingest.MaxConcurrent != 4 || c.Query.MaxConcurrent != 8 ||
		c.Ingest.MaxStreams != 20000 || c.Ingest.MaxAgents != 10000 || c.Ingest.StreamIdleTimeout.D() != time.Hour ||
		c.Ingest.MaxRecordAge.D() != 30*24*time.Hour {
		t.Errorf("defaults %+v", c)
	}
}

func TestServerShippedExampleIsValid(t *testing.T) {
	c, err := LoadServer("../../deploy/server.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(); err != nil {
		t.Fatal(err)
	}
}

func TestServerValidation(t *testing.T) {
	c, err := ParseServer([]byte(`
listen: nope
log: {level: loud}
clickhouse: {url: "ftp://x", database: "a;b", password: x, password_file: /nonexistent, retention_days: -1}
ingest: {max_body_bytes: 1KiB, max_agents: -1, max_record_age: 10m}
enrich:
  rdns: {workers: -1}
  asn: {url: "", refresh: 1s}
`))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Finalize()
	if err == nil {
		t.Fatal("invalid config accepted")
	}
	for _, want := range []string{"listen", "log.level", "clickhouse.url", "clickhouse.database", "mutually exclusive",
		"retention_days",
		"password_file", "ingest:", "max_agents", "ingest.max_record_age", "enrich.rdns", "enrich.asn: set url or file",
		"enrich.asn.refresh"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q:\n%v", want, err)
		}
	}
	if _, err := ParseServer([]byte("lisen: x")); err == nil {
		t.Error("unknown key accepted")
	}

	c, err = ParseServer([]byte(`clickhouse: {url: "http://default:s3cret@ch:8123"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(); err == nil || !strings.Contains(err.Error(), "not in the URL") || strings.Contains(err.Error(), "s3cret") {
		t.Errorf("credentials in the URL: %v", err)
	}
}

func TestClickHousePasswordFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(p, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := &ServerConfig{ClickHouse: ClickHouseConfig{PasswordFile: p}}
	if pw, err := c.ClickHousePassword(); err != nil || pw != "s3cret" {
		t.Errorf("password %q %v", pw, err)
	}
}

func TestServerQueryLimits(t *testing.T) {
	c := &ServerConfig{}
	if err := c.Finalize(); err != nil {
		t.Fatal(err)
	}
	if c.Query.MaxResponseBytes != 32<<20 || c.Query.MaxMemoryBytes != 1<<30 {
		t.Errorf("query defaults %+v", c.Query)
	}
	c, err := ParseServer([]byte(`query: {max_response_bytes: 64KiB, max_memory_bytes: 1MiB}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(); err == nil || !strings.Contains(err.Error(), "max_response_bytes") {
		t.Errorf("tiny query limits: %v", err)
	}
}

// Without allowed_hosts, a server on loopback answers the localhost names
// only (DNS rebinding); elsewhere it cannot know its names and checks none.
func TestHostAllowlist(t *testing.T) {
	local := []string{"localhost", "127.0.0.1", "::1"}
	for _, tc := range []struct {
		listen string
		hosts  []string
		want   []string
	}{
		{"127.0.0.1:8080", nil, local},
		{"localhost:8080", nil, local},
		{"[::1]:8080", nil, local},
		{"127.0.0.2:8080", nil, append(local, "127.0.0.2")},
		{"0.0.0.0:8080", nil, nil},
		{":8080", nil, nil},
		{"[::]:8080", nil, nil},
		{"192.0.2.10:8080", nil, nil},
		{"127.0.0.1:8080", []string{"*"}, nil},
		{"0.0.0.0:8080", []string{"localhost", "hopcorder.example"}, []string{"localhost", "hopcorder.example"}},
	} {
		c := &ServerConfig{Listen: tc.listen, AllowedHosts: tc.hosts}
		if err := c.Finalize(); err != nil {
			t.Fatalf("%+v: %v", tc, err)
		}
		if got := c.HostAllowlist(); !slices.Equal(got, tc.want) {
			t.Errorf("listen %s, allowed_hosts %v: %v, want %v", tc.listen, tc.hosts, got, tc.want)
		}
	}
	c, err := ParseServer([]byte(`allowed_hosts: ["hopcorder.example:8080", "http://x", "", "[::1]", "192.0.2.1", "*",
  "*.lab.example", "*.", "*.*.example", "a.*.example", "*example"]`))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Finalize()
	for _, bad := range []string{`"hopcorder.example:8080"`, `"http://x"`, `""`, `"*."`, `"*.*.example"`,
		`"a.*.example"`, `"*example"`} {
		if err == nil || !strings.Contains(err.Error(), "allowed_hosts "+bad) {
			t.Errorf("%s accepted: %v", bad, err)
		}
	}
	for _, good := range []string{`"[::1]"`, `"192.0.2.1"`, `"*.lab.example"`} {
		if err != nil && strings.Contains(err.Error(), good) {
			t.Errorf("%s refused: %v", good, err)
		}
	}
}

// listen takes host:port, a unix socket's absolute path or "systemd".
func TestListenAddress(t *testing.T) {
	long := "/" + strings.Repeat("s", maxUnixPath)
	for _, tc := range []struct {
		listen, network, addr string
		bad                   bool
	}{
		{listen: "127.0.0.1:8080", network: "tcp", addr: "127.0.0.1:8080"},
		{listen: ":8080", network: "tcp", addr: ":8080"},
		{listen: "unix:/run/hopcorder-server.sock", network: "unix", addr: "/run/hopcorder-server.sock"},
		{listen: "systemd", network: ListenSystemd},
		{listen: "unix:run/x.sock", bad: true},
		{listen: "unix:@hopcorder", bad: true},
		{listen: "unix:", bad: true},
		{listen: "unix:" + long, bad: true},
		{listen: "unix:8080", bad: true},
		{listen: "Systemd", bad: true},
		{listen: "/run/x.sock", bad: true},
	} {
		network, addr, err := ListenAddress(tc.listen)
		if tc.bad {
			if err == nil {
				t.Errorf("%q accepted as %s %q", tc.listen, network, addr)
			}
			continue
		}
		if err != nil || network != tc.network || addr != tc.addr {
			t.Errorf("%q: %s %q %v, want %s %q", tc.listen, network, addr, err, tc.network, tc.addr)
		}
		c := &ServerConfig{Listen: tc.listen}
		if err := c.Finalize(); err != nil {
			t.Errorf("%q refused by Finalize: %v", tc.listen, err)
		}
	}
	c := &ServerConfig{Listen: "unix:relative.sock"}
	if err := c.Finalize(); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("relative socket path: %v", err)
	}
}
