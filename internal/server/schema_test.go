package server

import (
	"strings"
	"testing"
)

func TestLoadMigrations(t *testing.T) {
	migs, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) == 0 || migs[0].version != 1 {
		t.Fatalf("migrations %+v", migs)
	}
	for _, m := range migs {
		for _, s := range m.stmts {
			if strings.Contains(s, "--") || strings.HasSuffix(s, ";") || s == "" {
				t.Errorf("migration %d: badly split statement %q", m.version, s)
			}
		}
	}
	all := strings.Join(migs[0].stmts, "\n")
	for _, obj := range []string{"TABLE IF NOT EXISTS cycles", "TABLE IF NOT EXISTS hops", "VIEW IF NOT EXISTS hops_mv",
		"TABLE IF NOT EXISTS loop_events", "VIEW IF NOT EXISTS loop_events_mv", "TABLE IF NOT EXISTS agent_seq",
		"VIEW IF NOT EXISTS agent_seq_mv", "TABLE IF NOT EXISTS path_changes", "TABLE IF NOT EXISTS outages"} {
		if !strings.Contains(all, obj) {
			t.Errorf("initial migration does not create %s", obj)
		}
	}
}

func TestSplitStatements(t *testing.T) {
	got := splitStatements("-- header\nCREATE TABLE a (x UInt8); -- trailing\n\nSELECT 1;\n-- only comment;\n")
	if len(got) != 2 || got[0] != "CREATE TABLE a (x UInt8)" || got[1] != "SELECT 1" {
		t.Errorf("got %q", got)
	}
}
