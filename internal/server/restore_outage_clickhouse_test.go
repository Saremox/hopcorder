//go:build clickhouse

package server

// A restored stream resumes only an outage that is still open (review,
// round eight). The restore's lookup selected `NULL AS end_ts` and filtered
// `end_ts IS NULL`; ClickHouse reads a SELECT alias in WHERE, so the filter
// was always true and the newest outage came back open even when it had
// closed. The stream then went on with it: its next rows (an open row after
// the part, then a close row) have higher versions than its close row, so
// FINAL showed a finished outage as ongoing since its old start, or ending
// at the new cycle, and the new outage was never recorded. The fake store
// models the intended filter, so only ClickHouse shows it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/saremox/hopcorder/internal/clickhouse"
	"github.com/saremox/hopcorder/internal/record"
)

// faultStore is the ClickHouse store with two faults to switch on: the
// mark insert fails (a crash right after a part's events were stored), and
// the restore's replay query fails (ClickHouse too slow for the restore
// budget).
type faultStore struct {
	*clickhouse.Client
	failMark, failReplay bool
}

func (f *faultStore) InsertJSON(ctx context.Context, table string, rows [][]byte) error {
	if f.failMark && table == "agent_seq" {
		return errors.New("crashed before the mark")
	}
	return f.Client.InsertJSON(ctx, table, rows)
}

func (f *faultStore) Query(ctx context.Context, q string, p clickhouse.Params, row func(json.RawMessage) error) error {
	if f.failReplay && strings.Contains(q, "ORDER BY seq DESC LIMIT {depth:UInt32}") {
		return errors.New("restore budget used up")
	}
	return f.Client.Query(ctx, q, p, row)
}

type storedOutage struct {
	StartSeq uint64  `json:"start_seq"`
	EndSeq   uint64  `json:"end_seq"`
	EndTS    *string `json:"end_ts"`
	Cycles   int     `json:"cycles"`
}

func (o storedOutage) String() string {
	end := "ongoing"
	if o.EndTS != nil {
		end = fmt.Sprint(o.EndSeq)
	}
	return fmt.Sprintf("[%d to %s: %d cycles]", o.StartSeq, end, o.Cycles)
}

// storedOutages is the outages table as every reader sees it (FINAL).
func storedOutages(t *testing.T, c *clickhouse.Client) string {
	t.Helper()
	rows, err := clickhouse.QueryInto[storedOutage](t.Context(), c,
		"SELECT start_seq, end_seq, end_ts, cycles FROM outages FINAL ORDER BY start_seq", nil)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprint(rows)
}

func cyclesFor(from, to uint64, ts func(uint64) time.Time, spec func(uint64) []string) []*record.Record {
	var recs []*record.Record
	for seq := from; seq <= to; seq++ {
		r := mk("lab", seq, spec(seq)...)
		r.TS = ts(seq)
		recs = append(recs, r)
	}
	return recs
}

// Normal operation: a stream that had an outage comes back after more than
// max_record_age (30 days) without cycles (the agent was off, or the target
// out of its config). Its state was dropped long ago, so its first new
// cycle restores it, with nothing to replay; the lookup, unbounded then,
// found the outage that closed a month ago and resumed it.
func TestRestoreAfterAMonthKeepsTheOldOutageClosed(t *testing.T) {
	for _, comeback := range []string{"reached", "unreached"} {
		t.Run(comeback, func(t *testing.T) {
			c := testClickHouse(t)
			in := NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
			old := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
			spec := func(seq uint64) []string {
				if seq > 50 && seq <= 60 {
					return brokenA
				}
				return pathA
			}
			for from := uint64(1); from <= 71; from += 10 { // 1 to 80: an outage 51 to 60, closed at 61
				if _, err := in.Ingest(t.Context(), cyclesFor(from, from+9, old, spec)); err != nil {
					t.Fatal(err)
				}
			}
			// 31 days later the stream is back; a new ingester stands for the
			// idle drop (either way the stream is restored).
			in = NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
			later := func(seq uint64) time.Time { return t0.Add(31*24*time.Hour + time.Duration(seq)*5*time.Second) }
			back := pathA
			if comeback == "unreached" {
				back = brokenA
			}
			if _, err := in.Ingest(t.Context(), cyclesFor(81, 90, later, func(uint64) []string { return back })); err != nil {
				t.Fatal(err)
			}
			want := "[[51 to 61: 10 cycles]]"
			if comeback == "unreached" {
				want = "[[51 to 61: 10 cycles] [81 to ongoing: 10 cycles]]"
			}
			if got := storedOutages(t, c); got != want {
				t.Errorf("after the comeback: %s, want %s", got, want)
			}
		})
	}
}

// A plausible fault: while restores fail (ClickHouse too slow for their
// budget), a restarted server's streams derive no events. A new outage that
// begins then and lasts past replayDepth cycles has no row; the restore
// that then works must open one at the first trailing unreached cycle it
// replays (HANDOFF), not resume the stream's previous, closed outage.
func TestRestoreAfterFailedRestoresOpensTheNewOutage(t *testing.T) {
	c := testClickHouse(t)
	st := &faultStore{Client: c}
	in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
	ts := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
	spec := func(seq uint64) []string {
		if seq > 50 && seq <= 60 || seq > 100 && seq <= 150 {
			return brokenA
		}
		return pathA
	}
	ingest := func(from, to uint64) {
		t.Helper()
		if _, err := in.Ingest(t.Context(), cyclesFor(from, to, ts, spec)); err != nil {
			t.Fatal(err)
		}
	}
	for from := uint64(1); from <= 91; from += 10 { // 1 to 100: an outage 51 to 60, closed at 61
		ingest(from, from+9)
	}
	in = NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil) // a restart
	st.failReplay = true
	for from := uint64(101); from <= 131; from += 10 { // 101 to 140 down, no events derived
		ingest(from, from+9)
	}
	st.failReplay = false
	ingest(141, 145) // still down; the restore works again
	if got, want := storedOutages(t, c), "[[51 to 61: 10 cycles] [111 to ongoing: 35 cycles]]"; got != want {
		t.Errorf("while down: %s, want %s", got, want)
	}
	ingest(146, 155) // down until 150, reached from 151
	if got, want := storedOutages(t, c), "[[51 to 61: 10 cycles] [111 to 151: 40 cycles]]"; got != want {
		t.Errorf("after: %s, want %s", got, want)
	}
}

// A crash right after a part's events were stored, before its mark: the
// agent's retry derives the part again after the restart. An outage that
// closed in that part, longer than replayDepth or not, must stay one
// outage: the restore resumes it although its close row is stored, since
// it closed at or after the part's first cycle.
func TestRederivedPartClosesItsOutageOnce(t *testing.T) {
	for _, down := range []int{20, 45} {
		t.Run(fmt.Sprintf("down%d", down), func(t *testing.T) {
			c := testClickHouse(t)
			st := &faultStore{Client: c}
			in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
			ts := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
			last := uint64(50 + down)
			spec := func(seq uint64) []string {
				if seq > 50 && seq <= last {
					return brokenA
				}
				return pathA
			}
			from := uint64(1)
			for ; from+9 <= last; from += 10 {
				if _, err := in.Ingest(t.Context(), cyclesFor(from, from+9, ts, spec)); err != nil {
					t.Fatal(err)
				}
			}
			closing := cyclesFor(from, from+9, ts, spec)
			st.failMark = true
			if _, err := in.Ingest(t.Context(), closing); err != nil {
				t.Fatal(err)
			}
			st.failMark = false
			in = NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
			if _, err := in.Ingest(t.Context(), closing); err != nil {
				t.Fatal(err)
			}
			if got, want := storedOutages(t, c), fmt.Sprintf("[[51 to %d: %d cycles]]", last+1, down); got != want {
				t.Errorf("outages %s, want %s", got, want)
			}
		})
	}
}
