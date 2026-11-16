package config

import (
	"os"
	"path/filepath"
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
		c.Enrich.ASN.Refresh.D() != 24*time.Hour || c.Ingest.MaxBodyBytes != DefaultMaxBodyBytes {
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
clickhouse: {url: "ftp://x", database: "a;b", password: x, password_file: /nonexistent}
ingest: {max_body_bytes: 1KiB}
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
		"password_file", "ingest:", "enrich.rdns", "enrich.asn: set url or file", "enrich.asn.refresh"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q:\n%v", want, err)
		}
	}
	if _, err := ParseServer([]byte("lisen: x")); err == nil {
		t.Error("unknown key accepted")
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
