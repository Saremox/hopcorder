package clickhouse

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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

func TestEscapeParam(t *testing.T) {
	for in, want := range map[string]string{
		`plain.value-1`:          `plain.value-1`,
		`corp\lab1`:              `corp\\lab1`,
		`ends\`:                  `ends\\`,
		"tab\tnew\nret\rnul\x00": `tab\tnew\nret\rnul\0`,
		`quote'stays`:            `quote'stays`, // typed parameters need no quoting
	} {
		if got := escapeParam(in); got != want {
			t.Errorf("escapeParam(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewRejectsCredentialsInURL(t *testing.T) {
	_, err := New(Config{URL: "http://default:s3cret@127.0.0.1:8123", Database: "d"})
	if err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Errorf("err = %v", err)
	}
	if _, err := New(Config{URL: "ftp://u:s3cret@x", Database: "d"}); err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Errorf("scheme error leaks the password: %v", err)
	}
}

// Rows handed to the callback stay valid after it returns: the scanner
// reuses its buffer, and callers keep rows (a result above the scanner's
// first 64 KiB came back with rows overwritten by later ones).
func TestQueryRowsMayBeKept(t *testing.T) {
	const n = 3000 // about 300 KB
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := range n {
			fmt.Fprintf(w, "{\"i\":%d,\"pad\":%q}\n", i, strings.Repeat(strconv.Itoa(i%10), 80))
		}
	}))
	defer srv.Close()
	c, err := New(Config{URL: srv.URL, Database: "d"})
	if err != nil {
		t.Fatal(err)
	}
	var rows []json.RawMessage
	if err := c.Query(t.Context(), "SELECT", nil, func(m json.RawMessage) error {
		rows = append(rows, m)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(rows) != n {
		t.Fatalf("%d rows", len(rows))
	}
	for i, m := range rows {
		var r struct {
			I   int    `json:"i"`
			Pad string `json:"pad"`
		}
		if err := json.Unmarshal(m, &r); err != nil || r.I != i || r.Pad != strings.Repeat(strconv.Itoa(i%10), 80) {
			t.Fatalf("row %d changed after the callback: %.60s (%v)", i, m, err)
		}
	}
}

// Queries are read-only (so ClickHouse cancels them when the connection
// closes) and carry the settings of their context; statements do not.
func TestQuerySettings(t *testing.T) {
	var got []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = append(got, r.URL.Query())
	}))
	defer srv.Close()
	c, err := New(Config{URL: srv.URL, Database: "d", Timeout: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithSettings(t.Context(), Settings{"max_result_bytes": "1000", "result_overflow_mode": "break"})
	if err := c.Query(ctx, "SELECT 1", nil, func(json.RawMessage) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(ctx, "CREATE TABLE x", nil); err != nil {
		t.Fatal(err)
	}
	q, e := got[0], got[1]
	if q.Get("readonly") != "2" || q.Get("cancel_http_readonly_queries_on_client_close") != "1" ||
		q.Get("max_execution_time") != "15" || q.Get("max_result_bytes") != "1000" || q.Get("result_overflow_mode") != "break" {
		t.Errorf("query settings %v", q)
	}
	if e.Has("readonly") || e.Has("max_result_bytes") {
		t.Errorf("statement settings %v", e)
	}
}

// slowRows answers queries with n rows of about size bytes each, flushing
// every burst rows and pausing for gap after each burst.
func slowRows(n, size, burst int, gap time.Duration) http.HandlerFunc {
	pad := strings.Repeat("x", size)
	return func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		for i := range n {
			fmt.Fprintf(w, "{\"i\":%d,\"pad\":%q}\n", i, pad)
			if (i+1)%burst == 0 {
				_ = rc.Flush()
				select {
				case <-time.After(gap):
				case <-r.Context().Done():
					return
				}
			}
		}
	}
}

// A query's result may take longer than the timeout to arrive, and to be
// consumed: the timeout bounds each wait for ClickHouse, not the whole
// result. http.Client.Timeout covered the body too, so a streamed read-API
// response was cut off 30 s after its query started, however fast its client
// read (and the cut-off last line was passed on as a row).
func TestQueryResultOutlastsTimeout(t *testing.T) {
	const timeout = 300 * time.Millisecond
	for _, tc := range []struct {
		name    string
		h       http.HandlerFunc
		n       int
		pause   time.Duration // the caller's pause every 10 rows
		minTook time.Duration
	}{
		// ClickHouse sends two rows every 100 ms for 1.5 s.
		{"slow server", slowRows(30, 100, 2, 100*time.Millisecond), 30, 0, time.Second},
		// ClickHouse sends 8 MB at once; the caller takes 1.6 s over it.
		{"slow caller", slowRows(8000, 1000, 8000, 0), 8000, 2 * time.Millisecond, time.Second},
	} {
		srv := httptest.NewServer(tc.h)
		c, err := New(Config{URL: srv.URL, Database: "d", Timeout: timeout})
		if err != nil {
			t.Fatal(err)
		}
		start, n := time.Now(), 0
		err = c.Query(t.Context(), "SELECT", nil, func(m json.RawMessage) error {
			var r struct {
				I int `json:"i"`
			}
			if err := json.Unmarshal(m, &r); err != nil || r.I != n {
				return fmt.Errorf("row %d: %.40s: %w", n, m, err)
			}
			if n++; tc.pause > 0 && n%10 == 0 {
				time.Sleep(tc.pause)
			}
			return nil
		})
		took := time.Since(start)
		srv.Close()
		if err != nil || n != tc.n {
			t.Errorf("%s: %d of %d rows after %s: %v", tc.name, n, tc.n, took.Round(time.Millisecond), err)
		}
		if took < tc.minTook {
			t.Errorf("%s: took %s, want over %s (the test is too fast to show anything)", tc.name, took, tc.minTook)
		}
	}
}

// A result that ends inside a row is an error, and the cut-off row is not
// passed on: bufio.Scanner hands a last line without its newline over as a
// token, even after a read error.
func TestQueryCutRowIsAnError(t *testing.T) {
	const timeout = 300 * time.Millisecond
	for _, tc := range []struct {
		name string
		h    http.HandlerFunc
		want string
	}{
		// The connection breaks inside the second row.
		{"broken", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "1000")
			_, _ = io.WriteString(w, "{\"i\":0}\n{\"i\":1,\"pad\":\"ab")
		}, "EOF"},
		// ClickHouse stops sending inside the second row.
		{"stalled", func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "{\"i\":0}\n{\"i\":1,\"pad\":\"ab")
			_ = http.NewResponseController(w).Flush()
			select {
			case <-time.After(5 * time.Second):
			case <-r.Context().Done():
			}
		}, "no more of the result within 300ms"},
		// A clean end, but not of a row.
		{"unterminated", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "{\"i\":0}\n{\"i\":1}")
		}, "ends inside a row"},
	} {
		srv := httptest.NewServer(tc.h)
		c, err := New(Config{URL: srv.URL, Database: "d", Timeout: timeout})
		if err != nil {
			t.Fatal(err)
		}
		var rows []string
		err = c.Query(t.Context(), "SELECT", nil, func(m json.RawMessage) error {
			rows = append(rows, string(m))
			return nil
		})
		srv.Close()
		if err == nil || !strings.Contains(err.Error(), tc.want) || len(rows) != 1 || rows[0] != `{"i":0}` {
			t.Errorf("%s: rows %q, err %v (want one row and %q)", tc.name, rows, err, tc.want)
		}
	}
}

// A query must still be answered within the timeout, and statements and
// inserts keep it for the whole exchange, body included.
func TestTimeouts(t *testing.T) {
	const timeout = 300 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		wait := 5 * time.Second // no answer
		if strings.HasPrefix(string(b), "SLOW BODY") {
			wait = 0 // the headers at once, then the body trickles for 1.5 s
		}
		select {
		case <-time.After(wait):
		case <-r.Context().Done():
			return
		}
		slowRows(15, 10, 1, 100*time.Millisecond)(w, r)
	}))
	defer srv.Close()
	c, err := New(Config{URL: srv.URL, Database: "d", Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		run  func() error
		fail bool
	}{
		{"query", func() error {
			return c.Query(t.Context(), "SELECT", nil, func(json.RawMessage) error { return nil })
		}, true},
		{"insert", func() error { return c.InsertJSON(t.Context(), "t", [][]byte{[]byte(`{}`)}) }, true},
		{"statement", func() error { return c.Exec(t.Context(), "CREATE", nil) }, true},
		// Answered with 200 in time: the rest of the body is not waited for
		// (as before), and does not count.
		{"statement with a slow body", func() error { return c.Exec(t.Context(), "SLOW BODY", nil) }, false},
	} {
		start := time.Now()
		err := tc.run()
		took := time.Since(start)
		if tc.fail != (err != nil) || (err != nil && !strings.Contains(err.Error(), "no answer within 300ms")) ||
			took > timeout+500*time.Millisecond {
			t.Errorf("%s: %v after %s", tc.name, err, took.Round(time.Millisecond))
		}
	}
}

// Inserts and statements carry the write settings of their context (the
// ingester names its writes with a query_id to stop them by); queries do
// not.
func TestWriteSettings(t *testing.T) {
	var got []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = append(got, r.URL.Query())
	}))
	defer srv.Close()
	c, err := New(Config{URL: srv.URL, Database: "d"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithWriteSettings(t.Context(), Settings{"query_id": "hopcorder/lab/1"})
	if err := c.InsertJSON(ctx, "cycles", [][]byte{[]byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(ctx, "INSERT INTO hops SELECT 1", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Query(ctx, "SELECT 1", nil, func(json.RawMessage) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Get("query_id") != "hopcorder/lab/1" || got[1].Get("query_id") != "hopcorder/lab/1" ||
		got[0].Get("query") != "INSERT INTO `cycles` FORMAT JSONEachRow" || got[2].Has("query_id") {
		t.Errorf("requests %v", got)
	}
}
