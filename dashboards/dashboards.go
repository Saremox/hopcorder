// Package dashboards holds hopcorder's Grafana dashboards (the JSON files
// next to this one, provisioned by deploy/grafana) and reads the queries out
// of them, so tests can run every panel against ClickHouse or Grafana.
//
// The dashboards use the Grafana ClickHouse data source plugin
// (grafana-clickhouse-datasource) with raw SQL against the server's schema
// (internal/server/migrations).
package dashboards

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"
	"time"
)

//go:embed *.json
var files embed.FS

// Files returns the dashboard JSON files.
func Files() fs.FS { return files }

// Dashboard is the part of a Grafana dashboard model the tests look at.
type Dashboard struct {
	UID        string  `json:"uid"`
	Title      string  `json:"title"`
	Panels     []Panel `json:"panels"`
	Templating struct {
		List []Variable `json:"list"`
	} `json:"templating"`
	Annotations struct {
		List []Annotation `json:"list"`
	} `json:"annotations"`
}

// Panel is a dashboard panel.
type Panel struct {
	ID         int      `json:"id"`
	Type       string   `json:"type"`
	Title      string   `json:"title"`
	Datasource *Ref     `json:"datasource"`
	GridPos    GridPos  `json:"gridPos"`
	Targets    []Target `json:"targets"`
	Panels     []Panel  `json:"panels"` // a collapsed row's panels
}

// GridPos places a panel on the 24-column grid.
type GridPos struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

// Ref names a data source.
type Ref struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

// Target is one query of the ClickHouse plugin.
type Target struct {
	RefID      string `json:"refId"`
	Datasource *Ref   `json:"datasource"`
	EditorType string `json:"editorType"`
	// Format is the plugin's result format: 0 time series, 1 table.
	Format    int    `json:"format"`
	QueryType string `json:"queryType"`
	RawSQL    string `json:"rawSql"`
}

// Plugin result formats (sqlds FormatQueryOption).
const (
	FormatTimeSeries = 0
	FormatTable      = 1
)

// Variable is a template variable.
type Variable struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Datasource *Ref            `json:"datasource"`
	Query      json.RawMessage `json:"query"`
	Current    struct {
		Value any `json:"value"`
	} `json:"current"`
}

// SQL returns a query variable's SQL: the plugin stores either a string or
// an object with rawSql.
func (v Variable) SQL() string {
	var s string
	if json.Unmarshal(v.Query, &s) == nil {
		return s
	}
	var q struct {
		RawSQL string `json:"rawSql"`
	}
	_ = json.Unmarshal(v.Query, &q)
	return q.RawSQL
}

// Annotation is an annotation query.
type Annotation struct {
	Name       string  `json:"name"`
	BuiltIn    int     `json:"builtIn"`
	Datasource *Ref    `json:"datasource"`
	Target     *Target `json:"target"`
}

// Query is one SQL query of a dashboard.
type Query struct {
	Where  string // "panel <title>", "variable <name>" or "annotation <name>"
	Format int
	SQL    string
}

// Load parses the named dashboard file.
func Load(name string) (*Dashboard, error) {
	b, err := files.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("read dashboard: %w", err)
	}
	var d Dashboard
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("dashboard %s: %w", name, err)
	}
	return &d, nil
}

// Names lists the embedded dashboard files.
func Names() []string {
	m, _ := fs.Glob(files, "*.json")
	sort.Strings(m)
	return m
}

// AllPanels returns the panels, including those inside collapsed rows.
func (d *Dashboard) AllPanels() []Panel {
	var out []Panel
	for _, p := range d.Panels {
		out = append(out, p)
		out = append(out, p.Panels...)
	}
	return out
}

// Queries returns every SQL query: panel targets, query variables and
// annotation queries.
func (d *Dashboard) Queries() []Query {
	var out []Query
	for _, p := range d.AllPanels() {
		for _, t := range p.Targets {
			out = append(out, Query{Where: "panel " + p.Title, Format: t.Format, SQL: t.RawSQL})
		}
	}
	for _, v := range d.Templating.List {
		if v.Type == "query" {
			out = append(out, Query{Where: "variable " + v.Name, Format: FormatTable, SQL: v.SQL()})
		}
	}
	for _, a := range d.Annotations.List {
		if a.Target != nil && a.Target.RawSQL != "" {
			out = append(out, Query{Where: "annotation " + a.Name, Format: FormatTable, SQL: a.Target.RawSQL})
		}
	}
	return out
}

var varRef = regexp.MustCompile(`\$\{(\w+)(?::(\w+))?\}|\$(\w+)`)

// Variables returns the names of the template variables sql refers to
// (Grafana's $name, ${name} and ${name:format}; not the plugin's $__ macros).
func Variables(sql string) []string {
	var out []string
	for _, m := range varRef.FindAllStringSubmatch(sql, -1) {
		name := m[1] + m[3]
		if strings.HasPrefix(name, "__") {
			continue
		}
		out = append(out, name)
	}
	return out
}

// Interpolate replaces template variables like Grafana does before a query
// reaches the data source. The formats are Grafana's:
//
//   - ${name:percentencode}: encodeURIComponent, then !'()* as well, so the
//     result only contains [A-Za-z0-9-_.~%]. The dashboards put every value
//     into SQL this way, as decodeURLComponent('${name:percentencode}'):
//     nothing a value contains can end the string literal.
//   - ${name:sqlstring}: ' doubled. Not safe for ClickHouse, which also
//     treats \' as an escaped quote; the dashboards do not use it (the
//     tests check that).
//   - any other form: the raw value.
//
// Unknown variables and the plugin's $__ macros are left alone.
func Interpolate(sql string, vars map[string]string) string {
	return varRef.ReplaceAllStringFunc(sql, func(ref string) string {
		m := varRef.FindStringSubmatch(ref)
		name, format := m[1]+m[3], m[2]
		v, ok := vars[name]
		if !ok || strings.HasPrefix(name, "__") {
			return ref
		}
		switch format {
		case "percentencode":
			return PercentEncode(v)
		case "sqlstring":
			return "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}
		return v
	})
}

// PercentEncode is Grafana's percentencode variable format: every byte
// outside [A-Za-z0-9-_.~] as %XX.
func PercentEncode(v string) string {
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

var macroRef = regexp.MustCompile(`\$__(timeFilter|timeInterval)\(([^)]*)\)|\$__(fromTime|toTime)\b`)

// Expand renders a query like Grafana and the ClickHouse plugin would, for
// running it straight against ClickHouse: template variables, then the
// plugin macros the dashboards use ($__timeFilter, $__timeInterval,
// $__fromTime, $__toTime), with the plugin's own expansions.
func Expand(sql string, vars map[string]string, from, to time.Time, interval time.Duration) string {
	sql = Interpolate(sql, vars)
	dt := func(t time.Time) string { return fmt.Sprintf("toDateTime(%d)", t.Unix()) }
	return macroRef.ReplaceAllStringFunc(sql, func(ref string) string {
		m := macroRef.FindStringSubmatch(ref)
		col := strings.TrimSpace(m[2])
		switch m[1] + m[3] {
		case "timeFilter":
			return fmt.Sprintf("%s >= %s AND %s <= %s", col, dt(from), col, dt(to))
		case "timeInterval":
			return fmt.Sprintf("toStartOfInterval(toDateTime(%s), INTERVAL %d second)", col, max(int(interval.Seconds()), 1))
		case "fromTime":
			return dt(from)
		default: // toTime
			return dt(to)
		}
	})
}
