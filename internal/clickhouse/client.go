// Package clickhouse is a minimal ClickHouse client over the HTTP interface,
// using only the standard library. It covers what the hopcorder server needs:
// running statements, inserting JSONEachRow rows and reading JSONEachRow
// results, with typed query parameters ({name:Type} placeholders) so values
// are never interpolated into SQL.
package clickhouse

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config configures a Client.
type Config struct {
	// URL of the HTTP interface, e.g. http://127.0.0.1:8123.
	URL      string
	Database string
	User     string
	Password string
	// Timeout bounds each request; default 30s.
	Timeout time.Duration
	// MaxExecutionTime bounds each SELECT on the server (ClickHouse's
	// max_execution_time), so a query outlives neither its caller nor the
	// request timeout; default Timeout minus 5s, at least 1s.
	MaxExecutionTime time.Duration
	// HTTPClient overrides the transport (TLS, proxies); optional.
	HTTPClient *http.Client
}

// Client talks to one ClickHouse database.
type Client struct {
	base    url.URL
	db      string
	user    string
	pass    string
	hc      *http.Client
	maxExec string // seconds
}

// Error is an exception reported by ClickHouse.
type Error struct {
	Status int
	Code   string // ClickHouse exception code, if reported
	Text   string
}

func (e *Error) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("clickhouse: code %s: %s", e.Code, e.Text)
	}
	return fmt.Sprintf("clickhouse: HTTP %d: %s", e.Status, e.Text)
}

// New returns a client. It does not contact the server.
func New(cfg Config) (*Client, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("clickhouse url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("clickhouse url %q: scheme must be http or https", u.Redacted())
	}
	if u.User != nil {
		return nil, errors.New("clickhouse url: put credentials in user and password, not in the URL")
	}
	if cfg.Database == "" {
		return nil, errors.New("clickhouse: database is required")
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{}
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if hc.Timeout == 0 {
		c := *hc
		c.Timeout = cfg.Timeout
		hc = &c
	}
	maxExec := cfg.MaxExecutionTime
	if maxExec <= 0 {
		maxExec = max(cfg.Timeout-5*time.Second, time.Second)
	}
	return &Client{base: *u, db: cfg.Database, user: cfg.User, pass: cfg.Password, hc: hc,
		maxExec: strconv.Itoa(max(int(maxExec/time.Second), 1))}, nil
}

// Database returns the configured database name.
func (c *Client) Database() string { return c.db }

// Ping checks that the server answers.
func (c *Client) Ping(ctx context.Context) error {
	u := c.base
	u.Path = strings.TrimRight(u.Path, "/") + "/ping"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return &Error{Status: resp.StatusCode, Text: "ping failed"}
	}
	return nil
}

// Params are values for {name:Type} placeholders.
type Params map[string]string

// Exec runs a statement that returns no rows, in the configured database.
func (c *Client) Exec(ctx context.Context, query string, params Params) error {
	return c.do(ctx, true, nil, params, strings.NewReader(query), nil)
}

// ExecServer runs a statement outside any database, e.g. CREATE DATABASE.
func (c *Client) ExecServer(ctx context.Context, query string) error {
	return c.do(ctx, false, nil, nil, strings.NewReader(query), nil)
}

// InsertJSON inserts rows (one JSON object each) into table with the
// JSONEachRow format, as one INSERT, i.e. one atomic block for batches of
// sane size.
func (c *Client) InsertJSON(ctx context.Context, table string, rows [][]byte) error {
	if len(rows) == 0 {
		return nil
	}
	var body bytes.Buffer
	for _, r := range rows {
		body.Write(r)
		body.WriteByte('\n')
	}
	q := url.Values{}
	q.Set("query", "INSERT INTO "+quoteIdent(table)+" FORMAT JSONEachRow")
	q.Set("input_format_skip_unknown_fields", "0")
	q.Set("date_time_input_format", "best_effort")
	return c.do(ctx, true, q, nil, &body, nil)
}

// Settings are ClickHouse settings for the queries of one caller, sent as
// URL parameters (e.g. max_memory_usage).
type Settings map[string]string

type settingsKey struct{}

// WithSettings returns a context whose Query calls also send s (after the
// client's own settings, so s may override them). The server uses it to
// bound the read API's queries without changing what the ingester's
// queries may use.
func WithSettings(ctx context.Context, s Settings) context.Context {
	return context.WithValue(ctx, settingsKey{}, s)
}

// SettingsFrom returns the settings WithSettings put into ctx, if any.
func SettingsFrom(ctx context.Context) Settings {
	s, _ := ctx.Value(settingsKey{}).(Settings)
	return s
}

// Query runs a SELECT and calls row for every result row (JSONEachRow,
// 64-bit integers as numbers, date-times as ISO 8601 in UTC). It waits for
// the end of the query before streaming, so a failure part-way is reported
// as an error rather than as truncated output. row gets its own copy of
// each row and may keep it. Queries run with readonly = 2: they cannot
// write, and only read-only queries are cancelled when the connection
// closes.
func (c *Client) Query(ctx context.Context, query string, params Params, row func(json.RawMessage) error) error {
	q := url.Values{}
	q.Set("wait_end_of_query", "1")
	q.Set("output_format_json_quote_64bit_integers", "0")
	q.Set("default_format", "JSONEachRow")
	q.Set("date_time_output_format", "iso") // 2026-11-16T22:14:05.123Z
	// A query must not keep running after its caller gave up: bound its
	// time, and cancel it when the connection closes. ClickHouse applies
	// the cancel setting only to queries with readonly > 0, which a POST
	// does not get by default; readonly = 2 still lets the URL set the
	// other settings.
	q.Set("readonly", "2")
	q.Set("max_execution_time", c.maxExec)
	q.Set("cancel_http_readonly_queries_on_client_close", "1")
	for k, v := range SettingsFrom(ctx) {
		q.Set(k, v)
	}
	return c.do(ctx, true, q, params, strings.NewReader(query), func(r io.Reader) error {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 64<<10), 64<<20)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			// The scanner reuses its buffer for the next lines; callers
			// keep rows (the read API did), so each gets its own copy.
			if err := row(json.RawMessage(bytes.Clone(line))); err != nil {
				return err
			}
		}
		return sc.Err()
	})
}

// QueryInto decodes every row into a new T and returns them.
func QueryInto[T any](ctx context.Context, c *Client, query string, params Params) ([]T, error) {
	var out []T
	err := c.Query(ctx, query, params, func(m json.RawMessage) error {
		var v T
		if err := json.Unmarshal(m, &v); err != nil {
			return fmt.Errorf("decode row: %w", err)
		}
		out = append(out, v)
		return nil
	})
	return out, err
}

func (c *Client) do(ctx context.Context, useDB bool, q url.Values, params Params, body io.Reader, read func(io.Reader) error) error {
	if q == nil {
		q = url.Values{}
	}
	if useDB {
		q.Set("database", c.db)
	}
	for k, v := range params {
		q.Set("param_"+k, escapeParam(v))
	}
	u := c.base
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return err
	}
	if c.user != "" {
		req.Header.Set("X-ClickHouse-User", c.user)
	}
	if c.pass != "" {
		req.Header.Set("X-ClickHouse-Key", c.pass)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("clickhouse: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		text, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return &Error{Status: resp.StatusCode, Code: resp.Header.Get("X-ClickHouse-Exception-Code"),
			Text: strings.TrimSpace(string(text))}
	}
	if read == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return read(resp.Body)
}

// QuoteIdent backquotes an identifier.
func QuoteIdent(s string) string { return quoteIdent(s) }

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// Time formats t for DateTime64(3) columns and parameters.
func Time(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000") }

// paramEscaper escapes a query parameter value for ClickHouse, which parses
// parameters in its escaped (TSV) format: a backslash starts an escape
// sequence, so an unescaped `a\tb` would arrive with a tab in it and a value
// ending in a backslash would fail the query.
var paramEscaper = strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`, "\x00", `\0`)

func escapeParam(v string) string { return paramEscaper.Replace(v) }
