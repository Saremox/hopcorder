//go:build clickhouse

package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// richRecords are agent's cycles from seq from to to: loops (an outage
// too, every ten from 3), MPLS labels, an unreachable at 15, an outage from
// 20 to 29, a route change at 40, and 255 hops at 50 and 51.
func richRecords(agent string, from, to uint64) []*record.Record {
	var recs []*record.Record
	for seq := from; seq <= to; seq++ {
		var r *record.Record
		switch {
		case seq == 50 || seq == 51:
			spec := make([]string, record.MaxHops)
			for i := range spec {
				spec[i] = fmt.Sprintf("10.%d.%d.1", i/200, i%200)
			}
			spec[len(spec)-1] = "=192.0.2.9"
			r = mk(agent, seq, spec...)
		case seq%10 == 3:
			r = mk(agent, seq, "10.0.0.1", "198.18.120.1", "198.18.120.2", "198.18.120.1", "198.18.120.2", "*")
		case seq == 15:
			r = mk(agent, seq, pathA[:2]...)
			r.Hops = append(r.Hops, record.Hop{TTL: 3, Status: record.StatusUnreachable,
				IP: netip.MustParseAddr("198.18.120.9"), RTTMicros: 900, Code: record.IntPtr(1)})
			r.Finalize(2)
		case seq >= 20 && seq < 30:
			r = mk(agent, seq, brokenA...)
		case seq >= 40:
			r = mk(agent, seq, pathB...)
		default:
			r = mk(agent, seq, pathA...)
		}
		if seq%7 == 0 {
			r.Hops[1].MPLS = []record.MPLSLabel{{Label: 24000 + uint32(seq), S: true, TTL: 1}}
		}
		recs = append(recs, r)
	}
	return recs
}

// agentDiff compares what agent and ref have in every table the ingester
// writes (agent_id aside, and events as FINAL reads them): rows only one
// of them has, counted with their multiplicity, duplicate cycles, and the
// stored marks.
func agentDiff(t *testing.T, c *clickhouse.Client, agent, ref string) []string {
	t.Helper()
	var diffs []string
	for _, tb := range []struct{ name, final, except string }{
		{"cycles", "", "agent_id, received_at"},
		{"hops", "", "agent_id"},
		{"loop_events", "", "agent_id"},
		{"path_changes", "FINAL", "agent_id"},
		{"outages", "FINAL", "agent_id"},
	} {
		// Each row's hash, +1 for agent's and -1 for ref's: a sum other
		// than 0 is a row one of them has more often.
		side := func(a string, s int) string {
			return fmt.Sprintf("SELECT sipHash128(*) AS h, %d AS side FROM (SELECT * EXCEPT (%s) FROM %s %s WHERE agent_id = '%s')",
				s, tb.except, tb.name, tb.final, a)
		}
		type row struct {
			Extra   int `json:"extra"`
			Missing int `json:"missing"`
			Total   int `json:"total"`
		}
		rows, err := clickhouse.QueryInto[row](t.Context(), c, `SELECT sumIf(s, s > 0) AS extra, -sumIf(s, s < 0) AS missing,
			(SELECT count() FROM `+tb.name+` `+tb.final+` WHERE agent_id = '`+ref+`') AS total
			FROM (SELECT h, sum(side) AS s FROM (`+side(agent, 1)+` UNION ALL `+side(ref, -1)+`) GROUP BY h)`, nil)
		if err != nil {
			t.Fatal(err)
		}
		if r := rows[0]; r.Extra != 0 || r.Missing != 0 {
			diffs = append(diffs, fmt.Sprintf("%s: %d rows more than the reference (duplicates or rows it lacks), %d of its %d missing",
				tb.name, r.Extra, r.Missing, r.Total))
		}
	}
	if n := count(t, c, "SELECT count() - uniqExact(seq) AS n FROM cycles WHERE agent_id = '"+agent+"'"); n != 0 {
		diffs = append(diffs, fmt.Sprintf("%d cycles stored twice", n))
	}
	mark := func(a string) int {
		return count(t, c, "SELECT max(max_seq) AS n FROM agent_seq WHERE agent_id = '"+a+"'")
	}
	if m, r := mark(agent), mark(ref); m != r {
		diffs = append(diffs, fmt.Sprintf("mark %d, the reference's %d", m, r))
	}
	return diffs
}

func ingestOK(t *testing.T, in *Ingester, recs []*record.Record) {
	t.Helper()
	if _, err := in.Ingest(t.Context(), recs); err != nil {
		t.Fatal(err)
	}
}

func clickhouseIngester(st Store) *Ingester {
	in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
	in.now = func() time.Time { return t0.Add(time.Hour) }
	return in
}

// A view that fails after ClickHouse committed the cycles part (here a
// constraint that the 255-hop cycles 50 and 51, or every loop, violate; in
// production a memory limit, a full disk or a restart) left the cycles
// without hop or loop rows, and, depending on the order the views ran in,
// either agent_seq_mv had moved the mark (the retry was "duplicates": the
// rows and the batch's events lost) or it had not (the retry stored every
// cycle twice). Retried in other boundaries, with or without a restart,
// the agent's cycles must end up as if nothing had failed.
func TestViewFailureRetriedExactlyOnce(t *testing.T) {
	for _, tc := range []struct{ view, table, check string }{
		{"hops_mv", "hops", "ttl < 200"},
		{"loop_events_mv", "loop_events", "length(ttls) < 2"},
	} {
		for _, restart := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/restart=%v", tc.view, restart), func(t *testing.T) {
				c := testClickHouse(t)
				ingestOK(t, clickhouseIngester(c), richRecords("ref", 1, 120))
				in := clickhouseIngester(c)
				ingestOK(t, in, richRecords("v", 1, 40))
				if err := c.Exec(t.Context(), "ALTER TABLE "+tc.table+" ADD CONSTRAINT r4 CHECK "+tc.check, nil); err != nil {
					t.Fatal(err)
				}
				_, err := in.Ingest(t.Context(), richRecords("v", 41, 60))
				if err == nil || !strings.Contains(err.Error(), tc.view) {
					t.Fatalf("want the insert to fail in %s, got %v", tc.view, err)
				}
				if err := c.Exec(t.Context(), "ALTER TABLE "+tc.table+" DROP CONSTRAINT r4", nil); err != nil {
					t.Fatal(err)
				}
				if restart {
					in = clickhouseIngester(c)
				}
				ingestOK(t, in, richRecords("v", 41, 50))
				ingestOK(t, in, richRecords("v", 41, 90))
				ingestOK(t, in, richRecords("v", 91, 120))
				for _, d := range agentDiff(t, c, "v", "ref") {
					t.Error(d)
				}
			})
		}
	}
}

// An insert that outlives the client's timeout still commits, cycles part
// and views, while the server has answered 503: the retry used to find the
// mark moved (if agent_seq_mv had run by then) and answer "duplicates",
// and nothing ever derived those cycles' events (here an outage from 20 to
// 29 and a route change at 40); or it stored them a second time. The
// retry, right away (the insert still running) or once it committed, and
// with a restart in between, must leave the cycles as if nothing had gone
// wrong. Right away, the hop rows the insert writes late would come on top
// of those the retry filled in, unless the retry stops the insert first.
func TestLateCommitRetriedExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wait    bool
		restart bool
	}{{"retry while it runs", false, false}, {"retry after it committed", true, false},
		{"restart while it runs", false, true}, {"restart after it committed", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			c := testClickHouse(t)
			ingestOK(t, clickhouseIngester(c), richRecords("ref", 1, 120))
			in := clickhouseIngester(c)
			ingestOK(t, in, richRecords("v", 1, 10))
			// hops_mv's rows of the batch below take 2 s to write: the
			// cycles part is committed, the hop rows come late.
			if err := c.Exec(t.Context(), "ALTER TABLE hops ADD CONSTRAINT slow CHECK sleepEachRow(0.002) = 0", nil); err != nil {
				t.Fatal(err)
			}
			short, err := clickhouse.New(clickhouse.Config{URL: os.Getenv("HOPCORDER_CLICKHOUSE_URL"), Database: c.Database(),
				User: os.Getenv("HOPCORDER_CLICKHOUSE_USER"), Password: os.Getenv("HOPCORDER_CLICKHOUSE_PASSWORD"),
				Timeout: 700 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			in.store = short
			start := time.Now()
			if _, err := in.Ingest(t.Context(), richRecords("v", 11, 100)); err == nil {
				t.Fatal("the insert did not time out")
			}
			t.Logf("first attempt failed after %v", time.Since(start).Round(time.Millisecond))
			if tc.wait {
				time.Sleep(3 * time.Second)
			}
			in.store = c
			if tc.restart {
				in = clickhouseIngester(c)
			}
			ingestOK(t, in, richRecords("v", 11, 50))
			ingestOK(t, in, richRecords("v", 11, 120))
			for _, d := range agentDiff(t, c, "v", "ref") {
				t.Error(d)
			}
		})
	}
}

// holdsAll's query against ClickHouse: a batch far below the agent's mark
// that cycles holds cycle for cycle (its ts kept to the millisecond, from
// one with sub-millisecond digits) is answered as duplicates; one with a
// cycle at a held seq but another ts, or with a seq not held, is refused.
func TestStaleRedeliveryClickHouse(t *testing.T) {
	c := testClickHouse(t)
	in := clickhouseIngester(c)
	in.now = func() time.Time { return t0.Add((staleWindow + 100) * 5 * time.Second) }
	old := func() []*record.Record {
		recs := richRecords("v", 1, 30)
		for _, r := range recs {
			r.TS = r.TS.Add(123456789 * time.Nanosecond)
		}
		return recs
	}
	ingestOK(t, in, old())
	ingestOK(t, in, richRecords("v", staleWindow+50, staleWindow+50))
	if res, err := in.Ingest(t.Context(), old()); err != nil || res.Duplicates != 30 || res.Accepted != 0 {
		t.Errorf("delivered again: %+v, %v; want 30 duplicates", res, err)
	}
	other := old()
	other[29].TS = other[29].TS.Add(time.Millisecond)
	for name, recs := range map[string][]*record.Record{"another cycle": other, "not held": richRecords("v", 31, 40)} {
		if _, err := in.Ingest(t.Context(), recs); !errors.Is(err, ErrStaleBatch) {
			t.Errorf("%s: %v, want ErrStaleBatch", name, err)
		}
	}
	if n := count(t, c, "SELECT count() AS n FROM cycles WHERE agent_id = 'v'"); n != 31 {
		t.Errorf("%d cycles stored, want 31", n)
	}
}
