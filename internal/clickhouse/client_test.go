package clickhouse

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRequests(t *testing.T) {
	var got []*http.Request
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got, bodies = append(got, r), append(bodies, string(b))
		switch {
		case r.URL.Path == "/ping":
			_, _ = io.WriteString(w, "Ok.\n")
		case strings.Contains(string(b), "boom"):
			w.Header().Set("X-ClickHouse-Exception-Code", "62")
			http.Error(w, "Code: 62. DB::Exception: Syntax error", http.StatusBadRequest)
		case strings.HasPrefix(string(b), "SELECT"):
			_, _ = io.WriteString(w, "{\"n\":1,\"s\":\"a\"}\n\n{\"n\":2,\"s\":\"b\"}\n")
		}
	}))
	defer srv.Close()
	c, err := New(Config{URL: srv.URL, Database: "db1", User: "u", Password: "p", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(ctx, "CREATE TABLE x", Params{"a": "it's"}); err != nil {
		t.Fatal(err)
	}
	r := got[len(got)-1]
	if r.URL.Query().Get("database") != "db1" || r.URL.Query().Get("param_a") != "it's" ||
		r.Header.Get("X-ClickHouse-User") != "u" || r.Header.Get("X-ClickHouse-Key") != "p" {
		t.Errorf("exec request %v %v", r.URL, r.Header)
	}
	if err := c.ExecServer(ctx, "CREATE DATABASE db1"); err != nil {
		t.Fatal(err)
	}
	if got[len(got)-1].URL.Query().Has("database") {
		t.Error("ExecServer must not select a database")
	}
	if err := c.InsertJSON(ctx, "cy`cles", [][]byte{[]byte(`{"a":1}`), []byte(`{"a":2}`)}); err != nil {
		t.Fatal(err)
	}
	if q := got[len(got)-1].URL.Query().Get("query"); q != "INSERT INTO `cy``cles` FORMAT JSONEachRow" {
		t.Errorf("insert query %q", q)
	}
	if bodies[len(bodies)-1] != "{\"a\":1}\n{\"a\":2}\n" {
		t.Errorf("insert body %q", bodies[len(bodies)-1])
	}
	type row struct {
		N int    `json:"n"`
		S string `json:"s"`
	}
	rows, err := QueryInto[row](ctx, c, "SELECT n, s FROM t", nil)
	if err != nil || len(rows) != 2 || rows[1] != (row{2, "b"}) {
		t.Errorf("rows %+v err %v", rows, err)
	}
	if q := got[len(got)-1].URL.Query(); q.Get("wait_end_of_query") != "1" || q.Get("default_format") != "JSONEachRow" {
		t.Errorf("query settings %v", q)
	}
	err = c.Exec(ctx, "boom", nil)
	var chErr *Error
	if !errors.As(err, &chErr) || chErr.Code != "62" || !strings.Contains(chErr.Text, "Syntax error") {
		t.Errorf("err = %v", err)
	}
	if err := c.InsertJSON(ctx, "t", nil); err != nil {
		t.Errorf("empty insert: %v", err)
	}
	stop := errors.New("stop")
	if err := c.Query(ctx, "SELECT 1", nil, func(json.RawMessage) error { return stop }); !errors.Is(err, stop) {
		t.Errorf("row callback error not returned: %v", err)
	}
}

func TestNewValidates(t *testing.T) {
	for _, cfg := range []Config{{URL: "ftp://x", Database: "d"}, {URL: "http://x"}, {URL: "://", Database: "d"}} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%+v accepted", cfg)
		}
	}
}

func TestTime(t *testing.T) {
	ts := time.Date(2026, 11, 16, 23, 14, 5, 123_456_789, time.FixedZone("CET", 3600))
	if got := Time(ts); got != "2026-11-16 22:14:05.123" {
		t.Errorf("Time = %q", got)
	}
}
