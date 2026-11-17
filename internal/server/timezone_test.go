package server

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// zonelessTime matches a query parameter of a date or time type without a
// time zone, e.g. {from:DateTime64(3)} or {day:DateTime}.
var zonelessTime = regexp.MustCompile(`\{\w+:\s*(?:DateTime64\(\s*\d+\s*\)|DateTime|Date|Date32)\s*\}`)

// Every date or time parameter of the server's queries names its zone:
// clickhouse.Time sends UTC wall-clock text, and ClickHouse reads a
// parameter without a zone in its own server time zone (the host's, on an
// installation from packages), which shifted every read-API window and the
// restore's windows by the zone's offset (review round five). The clickhouse
// CI job runs against a ClickHouse in America/New_York to catch the effect.
func TestTimeParametersNameTheirZone(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			if m := zonelessTime.FindString(line); m != "" {
				t.Errorf("%s:%d: %s: a time parameter without a zone; use DateTime64(3, 'UTC')", f, i+1, m)
			}
			checked += strings.Count(line, ":DateTime64(")
		}
	}
	if checked == 0 {
		t.Fatal("found no time parameters at all; is the test looking at the right files?")
	}
}

func TestZonelessTimePattern(t *testing.T) {
	for s, want := range map[string]bool{
		"{from:DateTime64(3)}":        true,
		"{since: DateTime64(3) }":     true,
		"{day:DateTime}":              true,
		"{d:Date}":                    true,
		"{from:DateTime64(3, 'UTC')}": false,
		"{n:UInt32}":                  false,
		"DateTime64(3)":               false,
	} {
		if got := zonelessTime.MatchString(s); got != want {
			t.Errorf("%q: matched %v, want %v", s, got, want)
		}
	}
}
