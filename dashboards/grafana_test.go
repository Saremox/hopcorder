//go:build grafana

package dashboards

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/output"
	"github.com/saremox/hopcorder/internal/record"
)

// The docker-compose stack end to end: records go through the server's
// ingest API into ClickHouse, and every dashboard query comes back through
// Grafana and the real ClickHouse plugin with the provisioned data source.
//
//	docker compose -f deploy/docker-compose.yml up -d --build --wait
//	HOPCORDER_SERVER_URL=http://127.0.0.1:8080 HOPCORDER_GRAFANA_URL=http://127.0.0.1:3000 \
//	  go test -tags grafana ./dashboards/
//
// HOPCORDER_GRAFANA_USER and HOPCORDER_GRAFANA_PASSWORD default to admin/admin.

const datasourceUID = "hopcorder-clickhouse"

type grafana struct {
	url, user, password string
}

func (g grafana) do(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, g.url+path, r)
	if err != nil {
		return 0, nil, err
	}
	req.SetBasicAuth(g.user, g.password)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// waitOK polls url until it answers 200.
func waitOK(t *testing.T, url string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s not ready after %v: %v", url, d, err)
		}
		time.Sleep(time.Second)
	}
}

// seed sends cycles like the server's ClickHouse tests do, ending now: a
// stable path on dc1 and dc2, a loop on dc1, an outage and a route change on
// both.
func seed(t *testing.T, serverURL, agent string) {
	t.Helper()
	pathA := []string{"10.0.0.1", "62.115.120.1", "=192.0.2.9"}
	pathB := []string{"10.0.0.1", "62.115.120.7", "=192.0.2.9"}
	broken := []string{"10.0.0.1", "62.115.120.1", "*", "*"}
	loop := []string{"10.0.0.1", "62.115.120.1", "62.115.120.2", "62.115.120.1", "62.115.120.2", "*"}
	type cycle struct {
		target string
		spec   []string
	}
	var cycles []cycle
	for range 3 {
		cycles = append(cycles, cycle{"dc1.example", pathA}, cycle{"dc2.example", pathA})
	}
	cycles = append(cycles, cycle{"dc1.example", loop}, cycle{"dc1.example", broken}, cycle{"dc2.example", broken},
		cycle{"dc1.example", pathB}, cycle{"dc2.example", pathB})

	start := time.Now().Add(-time.Duration(len(cycles)) * 5 * time.Second).Truncate(time.Millisecond)
	var lines [][]byte
	for i, c := range cycles {
		r := &record.Record{Schema: record.SchemaVersion, AgentID: agent, Seq: uint64(i + 1),
			TS: start.Add(time.Duration(i) * 5 * time.Second), Target: c.target,
			TargetIP: netip.MustParseAddr("192.0.2.9"), AF: 4, Protocol: "icmp", FlowID: 33434}
		for ttl, s := range c.spec {
			h := record.Hop{TTL: ttl + 1, Status: record.StatusNoReply}
			switch {
			case s == "*":
			case s[0] == '=':
				h.Status, h.IP, h.RTTMicros = record.StatusReply, netip.MustParseAddr(s[1:]), 20000
			default:
				h.Status, h.IP, h.RTTMicros = record.StatusTTLExpired, netip.MustParseAddr(s), 1000
			}
			r.Hops = append(r.Hops, h)
		}
		r.Finalize(2)
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, b)
	}
	h, err := output.NewHTTP(output.HTTPConfig{URL: serverURL})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Close() }()
	if err := h.Deliver(t.Context(), lines); err != nil {
		t.Fatalf("ingest: %v", err)
	}
}

type queryResult struct {
	Results map[string]struct {
		Status int    `json:"status"`
		Error  string `json:"error"`
		Frames []struct {
			Schema struct {
				Fields []struct {
					Name string `json:"name"`
				} `json:"fields"`
			} `json:"schema"`
			Data struct {
				Values [][]any `json:"values"`
			} `json:"data"`
		} `json:"frames"`
	} `json:"results"`
}

func TestGrafanaDashboards(t *testing.T) {
	gURL, sURL := os.Getenv("HOPCORDER_GRAFANA_URL"), os.Getenv("HOPCORDER_SERVER_URL")
	if gURL == "" || sURL == "" {
		t.Skip("HOPCORDER_GRAFANA_URL and HOPCORDER_SERVER_URL not set")
	}
	g := grafana{gURL, env("HOPCORDER_GRAFANA_USER", "admin"), env("HOPCORDER_GRAFANA_PASSWORD", "admin")}
	waitOK(t, sURL+"/readyz", 3*time.Minute)
	waitOK(t, gURL+"/api/health", 3*time.Minute)

	b := make([]byte, 4)
	_, _ = rand.Read(b)
	agent := "grafana-smoke-" + hex.EncodeToString(b)
	seed(t, sURL, agent)

	// Provisioning: the data source works and the dashboards are loaded.
	code, body, err := g.do(t.Context(), http.MethodGet, "/api/datasources/uid/"+datasourceUID+"/health", nil)
	if err != nil || code != http.StatusOK {
		t.Fatalf("data source health: %d %v %s", code, err, body)
	}
	for _, name := range Names() {
		d, err := Load(name)
		if err != nil {
			t.Fatal(err)
		}
		code, body, err := g.do(t.Context(), http.MethodGet, "/api/dashboards/uid/"+d.UID, nil)
		if err != nil || code != http.StatusOK {
			t.Errorf("%s not provisioned: %d %v %s", name, code, err, body)
		}

		vars := map[string]string{"datasource": datasourceUID, "agent": agent, "target": "dc1.example",
			"protocol": "icmp", "target_b": "dc2.example"}
		from, to := time.Now().Add(-time.Hour), time.Now().Add(time.Minute)
		for _, q := range d.Queries() {
			queryType := "table"
			if q.Format == FormatTimeSeries {
				queryType = "timeseries"
			}
			req := map[string]any{
				"from": strconv.FormatInt(from.UnixMilli(), 10),
				"to":   strconv.FormatInt(to.UnixMilli(), 10),
				"queries": []map[string]any{{
					"refId":         "A",
					"datasource":    map[string]string{"type": pluginType, "uid": datasourceUID},
					"editorType":    "sql",
					"format":        q.Format,
					"queryType":     queryType,
					"rawSql":        Interpolate(q.SQL, vars),
					"intervalMs":    60000,
					"maxDataPoints": 500,
				}},
			}
			code, body, err := g.do(t.Context(), http.MethodPost, "/api/ds/query", req)
			if err != nil {
				t.Fatal(err)
			}
			var res queryResult
			_ = json.Unmarshal(body, &res)
			a := res.Results["A"]
			if code != http.StatusOK || a.Error != "" {
				t.Errorf("%s %s: %d %s", name, q.Where, code, firstN(body, 600))
				continue
			}
			rows, fields := 0, []string{}
			for _, f := range a.Frames {
				if len(f.Data.Values) > 0 {
					rows += len(f.Data.Values[0])
				}
				for _, fl := range f.Schema.Fields {
					fields = append(fields, fl.Name)
				}
			}
			if rows == 0 {
				t.Errorf("%s %s: no rows: %s", name, q.Where, firstN(body, 600))
				continue
			}
			t.Logf("%s: %d frames, %d rows, fields %v", q.Where, len(a.Frames), rows, fields)
		}
	}
}

func firstN(b []byte, n int) string {
	if len(b) > n {
		return fmt.Sprintf("%s…", b[:n])
	}
	return string(b)
}
