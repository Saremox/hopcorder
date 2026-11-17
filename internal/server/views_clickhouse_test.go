//go:build clickhouse

package server

import (
	"strings"
	"testing"
)

// fillViews writes what hops_mv and loop_events_mv would have, and only
// what is missing: after both views failed for every row of a batch, one
// fill (and a second one) leaves the hop and loop rows as a clean insert
// writes them.
func TestFillMatchesViews(t *testing.T) {
	c := testClickHouse(t)
	ingestOK(t, clickhouseIngester(c), richRecords("ref", 1, 60))
	for _, tb := range []string{"hops", "loop_events"} {
		if err := c.Exec(t.Context(), "ALTER TABLE "+tb+" ADD CONSTRAINT r4 CHECK agent_id != 'v'", nil); err != nil {
			t.Fatal(err)
		}
	}
	in := clickhouseIngester(c)
	recs := richRecords("v", 1, 60)
	if _, err := in.Ingest(t.Context(), recs); err == nil {
		t.Fatal("the views did not fail")
	}
	if n := count(t, c, "SELECT count() AS n FROM hops WHERE agent_id = 'v'") +
		count(t, c, "SELECT count() AS n FROM loop_events WHERE agent_id = 'v'"); n != 0 {
		t.Fatalf("%d rows written by the failing views", n)
	}
	for _, tb := range []string{"hops", "loop_events"} {
		if err := c.Exec(t.Context(), "ALTER TABLE "+tb+" DROP CONSTRAINT r4", nil); err != nil {
			t.Fatal(err)
		}
	}
	found := map[uint64]bool{}
	for _, r := range recs {
		found[r.Seq] = true
	}
	for range 2 {
		if err := in.fillViews(t.Context(), "v", recs, found); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range agentDiff(t, c, "v", "ref") {
		if strings.HasPrefix(d, "hops") || strings.HasPrefix(d, "loop_events") {
			t.Error(d)
		}
	}
}

// A batch in several parts whose third part fails in a view: the parts
// before it are stored, marked and their events derived; the retry (after
// a restart, in other boundaries) skips them, fills in the failed part and
// stores the rest.
func TestPartFailureMidBatchClickHouse(t *testing.T) {
	c := testClickHouse(t)
	ingestOK(t, clickhouseIngester(c), richRecords("ref", 1, 120))
	in := clickhouseIngester(c)
	in.partBytes = 16 << 10 // a few records a part
	if err := c.Exec(t.Context(), "ALTER TABLE hops ADD CONSTRAINT r4 CHECK ttl < 200", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := in.Ingest(t.Context(), richRecords("v", 1, 100)); err == nil { // fails at seq 50
		t.Fatal("the insert did not fail")
	}
	if n := count(t, c, "SELECT max(max_seq) AS n FROM agent_seq WHERE agent_id = 'v'"); n < 30 || n >= 50 {
		t.Errorf("mark %d after the failed part, want the end of the part before it", n)
	}
	if err := c.Exec(t.Context(), "ALTER TABLE hops DROP CONSTRAINT r4", nil); err != nil {
		t.Fatal(err)
	}
	in = clickhouseIngester(c)
	in.partBytes = 16 << 10
	ingestOK(t, in, richRecords("v", 1, 70))
	ingestOK(t, in, richRecords("v", 60, 120))
	for _, d := range agentDiff(t, c, "v", "ref") {
		t.Error(d)
	}
}
