package dashboards

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

const pluginType = "grafana-clickhouse-datasource"

func loadAll(t *testing.T) map[string]*Dashboard {
	t.Helper()
	names := Names()
	if len(names) == 0 {
		t.Fatal("no dashboards embedded")
	}
	out := map[string]*Dashboard{}
	for _, n := range names {
		d, err := Load(n)
		if err != nil {
			t.Fatal(err)
		}
		out[n] = d
	}
	return out
}

func readYAML(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func TestDashboardsWellFormed(t *testing.T) {
	uids := map[string]bool{}
	for name, d := range loadAll(t) {
		if d.UID == "" || d.Title == "" || uids[d.UID] {
			t.Errorf("%s: missing or duplicate uid/title %q %q", name, d.UID, d.Title)
		}
		uids[d.UID] = true

		vars := map[string]bool{}
		for _, v := range d.Templating.List {
			vars[v.Name] = true
			if v.Type == "query" && (v.Datasource == nil || v.Datasource.UID != "${datasource}" || v.SQL() == "") {
				t.Errorf("%s: variable %s: want SQL on ${datasource}", name, v.Name)
			}
		}
		if !vars["datasource"] {
			t.Errorf("%s: no datasource variable", name)
		}

		ids := map[int]bool{}
		var placed []GridPos
		for _, p := range d.AllPanels() {
			if ids[p.ID] {
				t.Errorf("%s: duplicate panel id %d", name, p.ID)
			}
			ids[p.ID] = true
			g := p.GridPos
			if g.W <= 0 || g.H <= 0 || g.X < 0 || g.X+g.W > 24 {
				t.Errorf("%s: panel %q outside the grid: %+v", name, p.Title, g)
			}
			for _, o := range placed {
				if g.X < o.X+o.W && o.X < g.X+g.W && g.Y < o.Y+o.H && o.Y < g.Y+g.H {
					t.Errorf("%s: panel %q overlaps another at %+v", name, p.Title, o)
				}
			}
			placed = append(placed, g)
			if p.Type == "row" {
				continue
			}
			if p.Datasource == nil || p.Datasource.UID != "${datasource}" || p.Datasource.Type != pluginType {
				t.Errorf("%s: panel %q: datasource %+v, want ${datasource}", name, p.Title, p.Datasource)
			}
			if len(p.Targets) == 0 {
				t.Errorf("%s: panel %q has no query", name, p.Title)
			}
			for _, tg := range p.Targets {
				if tg.EditorType != "sql" || strings.TrimSpace(tg.RawSQL) == "" ||
					(tg.Format != FormatTimeSeries && tg.Format != FormatTable) {
					t.Errorf("%s: panel %q: target %+v", name, p.Title, tg)
				}
			}
		}

		for _, q := range d.Queries() {
			for _, v := range Variables(q.SQL) {
				if !vars[v] {
					t.Errorf("%s: %s uses undefined variable $%s", name, q.Where, v)
				}
			}
			// Every value must enter SQL percent-encoded and decoded by
			// ClickHouse: Grafana's sqlstring does not escape backslashes,
			// which ClickHouse honours inside string literals.
			if refs, safe := len(Variables(q.SQL)), len(safeVarRef.FindAllString(q.SQL, -1)); refs != safe {
				t.Errorf("%s: %s: %d of %d variables are not decodeURLComponent('${name:percentencode}')",
					name, q.Where, refs-safe, refs)
			}
		}
	}
}

// The dashboards' default data source is the provisioned one, and the
// compose stack mounts the dashboards where the provider looks.
var safeVarRef = regexp.MustCompile(`decodeURLComponent\('\$\{\w+:percentencode\}'\)`)

func TestProvisioningMatches(t *testing.T) {
	var ds struct {
		Datasources []struct {
			UID  string `yaml:"uid"`
			Type string `yaml:"type"`
		} `yaml:"datasources"`
	}
	readYAML(t, "../deploy/grafana/provisioning/datasources/hopcorder.yaml", &ds)
	if len(ds.Datasources) != 1 || ds.Datasources[0].Type != pluginType {
		t.Fatalf("provisioned data sources %+v", ds.Datasources)
	}
	for name, d := range loadAll(t) {
		for _, v := range d.Templating.List {
			if v.Name == "datasource" && v.Current.Value != ds.Datasources[0].UID {
				t.Errorf("%s: default data source %v, provisioned %q", name, v.Current.Value, ds.Datasources[0].UID)
			}
		}
	}

	var prov struct {
		Providers []struct {
			Options struct {
				Path string `yaml:"path"`
			} `yaml:"options"`
		} `yaml:"providers"`
	}
	readYAML(t, "../deploy/grafana/provisioning/dashboards/hopcorder.yaml", &prov)
	var compose struct {
		Services map[string]struct {
			Volumes     []string          `yaml:"volumes"`
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	readYAML(t, "../deploy/docker-compose.yml", &compose)
	g := compose.Services["grafana"]
	if len(prov.Providers) != 1 || !slices.Contains(g.Volumes, "../dashboards:"+prov.Providers[0].Options.Path+":ro") {
		t.Errorf("compose grafana volumes %v do not mount ../dashboards at the provider path %+v", g.Volumes, prov.Providers)
	}
	if home := g.Environment["GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH"]; !strings.HasPrefix(home, prov.Providers[0].Options.Path+"/") {
		t.Errorf("home dashboard %q is not under the provider path", home)
	}
	for _, v := range []string{"HOST", "PORT", "DATABASE", "USER", "PASSWORD"} {
		if _, ok := g.Environment["HOPCORDER_CLICKHOUSE_"+v]; !ok {
			t.Errorf("compose grafana lacks HOPCORDER_CLICKHOUSE_%s for the data source", v)
		}
	}
}

func TestInterpolateAndExpand(t *testing.T) {
	vars := map[string]string{"agent": "o'hara", "target": "dc1"}
	got := Interpolate("a = ${agent:sqlstring} AND t = '${target}' AND u = $target AND $__timeFilter(ts) AND ${nope}", vars)
	want := "a = 'o''hara' AND t = 'dc1' AND u = dc1 AND $__timeFilter(ts) AND ${nope}"
	if got != want {
		t.Errorf("Interpolate:\n got %s\nwant %s", got, want)
	}
	from, to := time.Unix(1000, 0), time.Unix(2000, 0)
	got = Expand("SELECT $__timeInterval(ts) AS time FROM x WHERE $__timeFilter( pc.ts ) AND a <= $__toTime AND b >= $__fromTime",
		nil, from, to, 90*time.Second)
	want = "SELECT toStartOfInterval(toDateTime(ts), INTERVAL 90 second) AS time FROM x WHERE " +
		"pc.ts >= toDateTime(1000) AND pc.ts <= toDateTime(2000) AND a <= toDateTime(2000) AND b >= toDateTime(1000)"
	if got != want {
		t.Errorf("Expand:\n got %s\nwant %s", got, want)
	}
	// Grafana's percentencode: nothing that could end a SQL string survives.
	if got := PercentEncode(`!\' OR 1=1 -- é~_.`); got != "%21%5C%27%20OR%201%3D1%20--%20%C3%A9~_." {
		t.Errorf("PercentEncode = %s", got)
	}
	if got := Interpolate("a = decodeURLComponent('${agent:percentencode}')", map[string]string{"agent": "x'y"}); got !=
		"a = decodeURLComponent('x%27y')" {
		t.Errorf("Interpolate percentencode = %s", got)
	}
	if v := Variables("x = ${agent:sqlstring} AND $__timeFilter(ts) AND $target"); !slices.Equal(v, []string{"agent", "target"}) {
		t.Errorf("Variables = %v", v)
	}
}
