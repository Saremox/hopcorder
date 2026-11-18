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
	// failOutages: how many outages inserts fail from now on.
	failOutages int
}

func (f *faultStore) InsertJSON(ctx context.Context, table string, rows [][]byte) error {
	if f.failMark && table == "agent_seq" {
		return errors.New("crashed before the mark")
	}
	if table == "outages" && f.failOutages > 0 {
		f.failOutages--
		return errors.New("ClickHouse went away after the cycles insert")
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

// An outage open in storage whose end falls in cycles that derive no events
// (review, round nine): after a restart or an idle drop the stream's next
// batch could not be restored (its restore failed, ran out of its budget,
// or was beyond the 256 a request does), so the cycle that reached again
// wrote no close row. The restore that then worked replayed a stream that
// reached its destination and looked up no outage: the open row stayed
// "ongoing" for good, in the read API, the UI and Grafana. Now the restore
// closes it at that cycle.
//
// The review's first scenario, with ClickHouse too slow for the restore
// budget. Two days later, after another restart, the stream is down for 40
// cycles while restores fail; the restore that then works finds the
// stream's newest outage closed, so the new one opens at the first
// unreached cycle it replays, as for any outage whose start derived no
// events (round two; TestRestoreAfterFailedRestoresOpensTheNewOutage).
// Before, that restore took up the stale open outage again, and the new
// outage was merged into it with the reached cycles between them.
func TestRestoreClosesAnOutageThatEndedInSkippedCycles(t *testing.T) {
	c := testClickHouse(t)
	st := &faultStore{Client: c}
	in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
	ts := func(seq uint64) time.Time {
		if seq > 80 {
			return t0.Add(48*time.Hour + time.Duration(seq)*5*time.Second)
		}
		return t0.Add(time.Duration(seq) * 5 * time.Second)
	}
	spec := func(seq uint64) []string {
		if seq > 50 && seq <= 60 || seq > 80 && seq <= 130 {
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
	for from := uint64(1); from <= 51; from += 10 { // 1 to 60: down from 51, open at 60
		ingest(from, from+9)
	}
	in = NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil) // a restart
	st.failReplay = true
	ingest(61, 70) // reached again from 61; no events derived
	st.failReplay = false
	ingest(71, 80)
	if got, want := storedOutages(t, c), "[[51 to 61: 10 cycles]]"; got != want {
		t.Errorf("after the restore: %s, want %s", got, want)
	}

	in = NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil) // two days later, another restart
	st.failReplay = true
	ingest(81, 120) // down, no events derived
	st.failReplay = false
	ingest(121, 130) // still down; the restore works
	ingest(131, 131)
	if got, want := storedOutages(t, c), "[[51 to 61: 10 cycles] [91 to 131: 40 cycles]]"; got != want {
		t.Errorf("after the second outage: %s, want %s", got, want)
	}
}

// The review's second scenario, with no fault at all: an agent with 300
// targets (streams), one cycle each per round, and a server restart (an
// upgrade). A request restores at most 256 streams, so the first batch
// after the restart (500 records, the agent's batch_max_records) derives
// no events for 44 of them, and t299's outage ends in that batch.
func TestRestartBeyondTheRestoreBudgetClosesTheOutage(t *testing.T) {
	c := testClickHouse(t)
	in := NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
	const streams = 300
	seqOf := func(round, i int) uint64 { return uint64((round-1)*streams + i + 1) }
	var all []*record.Record
	for round := 1; round <= 12; round++ {
		for i := range streams {
			spec := pathA
			if i == streams-1 && round >= 5 && round <= 8 {
				spec = brokenA
			}
			r := mk("lab", seqOf(round, i), spec...)
			r.Target, r.TS = fmt.Sprintf("t%03d.example", i), t0.Add(time.Duration(round)*5*time.Second)
			all = append(all, r)
		}
	}
	send := func(recs []*record.Record) {
		t.Helper()
		for len(recs) > 0 {
			n := min(500, len(recs))
			if _, err := in.Ingest(t.Context(), recs[:n]); err != nil {
				t.Fatal(err)
			}
			recs = recs[n:]
		}
	}
	send(all[:8*streams]) // rounds 1 to 8; t299 down from round 5
	in = NewIngester(c, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
	send(all[8*streams:])
	want := fmt.Sprintf("[[%d to %d: 4 cycles]]", seqOf(5, streams-1), seqOf(9, streams-1))
	if got := storedOutages(t, c); got != want {
		t.Errorf("outages %s, want %s", got, want)
	}
}

// The outage that ended in skipped cycles is closed once, at the first
// cycle that reached, wherever the stream is when its restore works:
// reached, down again after a reached cycle the restore replays, or down
// for the whole replay (the case in which the stale outage was taken up
// again and the new one merged into it). The new outage begins with the
// first unreached cycle after the last reached one, which can lie before
// the replay. With a crash after the restored part's events and before its
// mark, the part is derived again after a restart and comes to the same
// outages.
func TestRestoreClosesAStaleOutageOnce(t *testing.T) {
	for _, tc := range []struct {
		name          string
		skipped, part [2]uint64 // cycles that derive no events, then the part that restores the stream
		down          func(uint64) bool
		want, end     string // after the part; after seq 140, which reaches
	}{
		{"reached", [2]uint64{61, 70}, [2]uint64{71, 80}, func(uint64) bool { return false },
			"[[51 to 61: 10 cycles]]", "[[51 to 61: 10 cycles]]"},
		{"down again", [2]uint64{61, 75}, [2]uint64{76, 80}, func(seq uint64) bool { return seq > 70 && seq < 140 },
			"[[51 to 61: 10 cycles] [71 to ongoing: 10 cycles]]", "[[51 to 61: 10 cycles] [71 to 140: 69 cycles]]"},
		{"down for the replay", [2]uint64{61, 120}, [2]uint64{121, 130}, func(seq uint64) bool { return seq > 80 && seq < 140 },
			"[[51 to 61: 10 cycles] [81 to ongoing: 50 cycles]]", "[[51 to 61: 10 cycles] [81 to 140: 59 cycles]]"},
	} {
		for _, crash := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/crash=%v", tc.name, crash), func(t *testing.T) {
				c := testClickHouse(t)
				st := &faultStore{Client: c}
				in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
				ts := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
				spec := func(seq uint64) []string {
					if seq > 50 && seq <= 60 || tc.down(seq) {
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
				for from := uint64(1); from <= 51; from += 10 { // 1 to 60: down from 51, open at 60
					ingest(from, from+9)
				}
				in = NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil) // a restart
				st.failReplay = true
				ingest(tc.skipped[0], tc.skipped[1])
				st.failReplay = false
				if crash {
					st.failMark = true
					ingest(tc.part[0], tc.part[1])
					st.failMark = false
					in = NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
				}
				ingest(tc.part[0], tc.part[1])
				if got := storedOutages(t, c); got != tc.want {
					t.Errorf("after the restored part: %s, want %s", got, tc.want)
				}
				ingest(tc.part[1]+1, 140)
				if got := storedOutages(t, c); got != tc.end {
					t.Errorf("after seq 140: %s, want %s", got, tc.end)
				}
			})
		}
	}
}

// ClickHouse fails after a part's cycles were stored, and its outages insert
// with them (all three attempts): the close row of an outage that ended in
// the part is lost (review, round ten). The stream keeps it and writes it
// again with its next part (round twelve; restored from storage until
// then); dropped, the outage stayed "ongoing" until the next restart, and
// for good once the stream had another outage.
func TestLostCloseRowIsRepairedAtTheNextBatch(t *testing.T) {
	for _, second := range []bool{false, true} {
		t.Run(fmt.Sprintf("second=%v", second), func(t *testing.T) {
			c := testClickHouse(t)
			st := &faultStore{Client: c}
			in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
			ts := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
			spec := func(seq uint64) []string {
				if seq > 50 && seq <= 60 || second && seq > 90 && seq <= 95 {
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
			for from := uint64(1); from <= 51; from += 10 { // down from 51
				ingest(from, from+9)
			}
			st.failOutages = 3 // every attempt of the next batch's outages insert
			ingest(61, 70)     // reached again at 61: its close row is lost
			ingest(71, 80)
			if got, want := storedOutages(t, c), "[[51 to 61: 10 cycles]]"; got != want {
				t.Errorf("at the next batch: %s, want %s", got, want)
			}
			for from := uint64(81); from <= 101; from += 10 {
				ingest(from, from+9)
			}
			in = NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil) // a restart
			for from := uint64(111); from <= 131; from += 10 {
				ingest(from, from+9)
			}
			want := "[[51 to 61: 10 cycles]]"
			if second {
				want = "[[51 to 61: 10 cycles] [91 to 96: 5 cycles]]"
			}
			if got := storedOutages(t, c); got != want {
				t.Errorf("at the end: %s, want %s", got, want)
			}
		})
	}
}

// A lost open row needs no restore: the stream writes it again, from its
// state, after its next part (review, round eleven). Restored instead, it
// rebuilt the outage from storage, which never had it: a catch-up batch
// with more down cycles than the replay recorded it with a later start.
func TestLostOpenRowInACatchUpBatchKeepsItsStart(t *testing.T) {
	c := testClickHouse(t)
	st := &faultStore{Client: c}
	in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
	ts := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
	spec := func(seq uint64) []string {
		if seq > 60 && seq <= 110 {
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
	for from := uint64(1); from <= 41; from += 10 {
		ingest(from, from+9)
	}
	st.failOutages = 3
	ingest(51, 100) // down from 61, 40 cycles: its open row is lost
	ingest(101, 110)
	if got, want := storedOutages(t, c), "[[61 to ongoing: 50 cycles]]"; got != want {
		t.Errorf("while down: %s, want %s", got, want)
	}
	ingest(111, 120)
	if got, want := storedOutages(t, c), "[[61 to 111: 50 cycles]]"; got != want {
		t.Errorf("after: %s, want %s", got, want)
	}
}

// And with the stream's next restore failed or skipped (the request's
// budget used up) while the outage ends: restored, it was not recorded at
// all.
func TestLostOpenRowThenSkippedRestoreIsRecorded(t *testing.T) {
	for _, why := range []string{"failed", "budget"} {
		t.Run(why, func(t *testing.T) {
			c := testClickHouse(t)
			st := &faultStore{Client: c}
			in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
			ts := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
			spec := func(seq uint64) []string {
				if seq > 55 && seq <= 62 {
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
			for from := uint64(1); from <= 41; from += 10 {
				ingest(from, from+9)
			}
			st.failOutages = 3
			ingest(51, 60) // down from 56: the open row is lost
			if why == "failed" {
				st.failReplay = true
			} else {
				in.maxRestores = 0
			}
			ingest(61, 70) // reached again at 63
			st.failReplay, in.maxRestores = false, maxRestores
			ingest(71, 80)
			ingest(81, 90)
			if got, want := storedOutages(t, c), "[[56 to 63: 7 cycles]]"; got != want {
				t.Errorf("at the end: %s, want %s", got, want)
			}
		})
	}
}

// The part whose outages insert fails holds a short outage that opens and
// closes (60, reached again at 61) and then the start of a long one (from
// 70), in a catch-up batch. Until round twelve the lost close row had the
// stream restored, and the restore rebuilt the long outage from storage,
// which never had its open row: [121 to 161: 40 cycles], the short one lost.
// Now both are written with the next part.
func TestLostCloseRowKeepsTheNextOutagesStart(t *testing.T) {
	c := testClickHouse(t)
	st := &faultStore{Client: c}
	in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
	ts := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
	spec := func(seq uint64) []string {
		if seq == 60 || seq >= 70 && seq <= 160 {
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
	for from := uint64(1); from <= 41; from += 10 {
		ingest(from, from+9)
	}
	st.failOutages = 3
	ingest(51, 150) // [60 to 61] and the start of [70 to ...]: both rows lost
	ingest(151, 160)
	if got, want := storedOutages(t, c), "[[60 to 61: 1 cycles] [70 to ongoing: 91 cycles]]"; got != want {
		t.Errorf("while down: %s, want %s", got, want)
	}
	ingest(161, 170)
	if got, want := storedOutages(t, c), "[[60 to 61: 1 cycles] [70 to 161: 91 cycles]]"; got != want {
		t.Errorf("after: %s, want %s", got, want)
	}
}

// The same in an ordinary batch, with the stream's next restore failed or
// skipped (the request's budget used up) while the long outage ends: it was
// not recorded at all. No restore is needed now.
func TestLostCloseRowThenSkippedRestoreKeepsTheNextOutage(t *testing.T) {
	for _, why := range []string{"failed", "budget"} {
		t.Run(why, func(t *testing.T) {
			c := testClickHouse(t)
			st := &faultStore{Client: c}
			in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
			ts := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
			spec := func(seq uint64) []string {
				if seq == 53 || seq >= 57 && seq <= 62 {
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
			for from := uint64(1); from <= 41; from += 10 {
				ingest(from, from+9)
			}
			st.failOutages = 3
			ingest(51, 60) // [53 to 54] and the start of [57 to ...]: both rows lost
			if why == "failed" {
				st.failReplay = true
			} else {
				in.maxRestores = 0
			}
			ingest(61, 70) // reached again at 63
			st.failReplay, in.maxRestores = false, maxRestores
			ingest(71, 80)
			if got, want := storedOutages(t, c), "[[53 to 54: 1 cycles] [57 to 63: 6 cycles]]"; got != want {
				t.Errorf("at the end: %s, want %s", got, want)
			}
		})
	}
}

// And the batch after a lost close row derives its events although the
// stream's restore would fail or not fit: restored, the stream skipped that
// batch, and its route change was lost with it.
func TestLostCloseRowKeepsTheNextBatchsRouteChange(t *testing.T) {
	for _, why := range []string{"failed", "budget"} {
		t.Run(why, func(t *testing.T) {
			c := testClickHouse(t)
			st := &faultStore{Client: c}
			in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
			ts := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
			spec := func(seq uint64) []string {
				switch {
				case seq >= 51 && seq <= 55:
					return brokenA
				case seq >= 65:
					return pathB // a route change at 65
				}
				return pathA
			}
			ingest := func(from, to uint64) {
				t.Helper()
				if _, err := in.Ingest(t.Context(), cyclesFor(from, to, ts, spec)); err != nil {
					t.Fatal(err)
				}
			}
			for from := uint64(1); from <= 41; from += 10 {
				ingest(from, from+9)
			}
			ingest(51, 55) // down 51-55: the open row is stored
			st.failOutages = 3
			ingest(56, 60) // reached again at 56: the close row is lost
			if why == "failed" {
				st.failReplay = true
			} else {
				in.maxRestores = 0
			}
			ingest(61, 70) // the route changes at 65
			st.failReplay, in.maxRestores = false, maxRestores
			ingest(71, 80)
			type change struct {
				Seq     uint64 `json:"seq"`
				PrevSeq uint64 `json:"prev_seq"`
				Reason  string `json:"reason"`
			}
			rows, err := clickhouse.QueryInto[change](t.Context(), c,
				"SELECT seq, prev_seq, reason FROM path_changes FINAL ORDER BY seq", nil)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := fmt.Sprint(rows), "[{65 64 router}]"; got != want {
				t.Errorf("path changes %s, want %s", got, want)
			}
			if got, want := storedOutages(t, c), "[[51 to 56: 5 cycles]]"; got != want {
				t.Errorf("outages %s, want %s", got, want)
			}
		})
	}
}

// lateStore's outages insert reports a failure but is kept, and committed
// later: the client gave up (the batch's events budget, a reset
// connection) while ClickHouse went on.
type lateStore struct {
	*clickhouse.Client
	late  bool     // the next outages insert "fails" but is kept
	saved [][]byte // what it held
}

func (f *lateStore) InsertJSON(ctx context.Context, table string, rows [][]byte) error {
	if table == "outages" && f.late {
		if f.saved == nil {
			f.saved = append([][]byte(nil), rows...)
		}
		return errors.New("context deadline exceeded (the insert goes on in ClickHouse)")
	}
	return f.Client.InsertJSON(ctx, table, rows)
}

// An outages insert counted as failed that lands after the next part: the
// restore had opened the long outage again at a later start, and the
// original open row, landing next to it, stayed "ongoing" for good.
func TestLateCommitOfALostCloseRowLeavesNothingOpen(t *testing.T) {
	c := testClickHouse(t)
	st := &lateStore{Client: c}
	in := NewIngester(st, fakeEnrich{}, slog.New(slog.DiscardHandler), nil)
	ts := func(seq uint64) time.Time { return t0.Add(time.Duration(seq) * 5 * time.Second) }
	spec := func(seq uint64) []string {
		if seq == 60 || seq >= 70 && seq <= 160 {
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
	for from := uint64(1); from <= 41; from += 10 {
		ingest(from, from+9)
	}
	st.late = true
	ingest(51, 150) // [60 to 61] closed, [70 to ...] open: the insert "fails"
	ingest(151, 160)
	st.late = false
	if err := c.InsertJSON(t.Context(), "outages", st.saved); err != nil { // ClickHouse commits it now
		t.Fatal(err)
	}
	ingest(161, 170)
	if got, want := storedOutages(t, c), "[[60 to 61: 1 cycles] [70 to 161: 91 cycles]]"; got != want {
		t.Errorf("at the end: %s, want %s", got, want)
	}
}
