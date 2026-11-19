package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/saremox/hopcorder/internal/record"
)

// An outage close row that could not be stored stays on its stream and is
// written again with the stream's next part, and counted once (review,
// round twelve). Until then the stream was restored from storage, which
// lost the outages whose rows were all in the failed inserts and rebuilt
// the stream's open outage from what storage had. Past maxPending rows the
// newest outages whose rows were all in failed inserts are dropped, and the
// stream is not restored (round thirteen: it was, as before round twelve).
func TestLostCloseRowsAreWrittenAgain(t *testing.T) {
	for _, tc := range []struct {
		failed int // parts whose outages insert fails, each with a closed outage
		want   string
	}{
		{failed: 3, want: "map[15:end 16, 1 cycles 25:end 26, 1 cycles 35:end 36, 1 cycles 45:end 46, 1 cycles]"},
		// Past the bound the newest is dropped: lost with its insert
		// (accepted, round six).
		{failed: maxPending + 1, want: "map[15:end 16, 1 cycles 25:end 26, 1 cycles 35:end 36, 1 cycles " +
			"45:end 46, 1 cycles 55:end 56, 1 cycles 65:end 66, 1 cycles 75:end 76, 1 cycles 85:end 86, 1 cycles " +
			"105:end 106, 1 cycles]"},
	} {
		t.Run(fmt.Sprint(tc.failed), func(t *testing.T) {
			st := newHookStore()
			reg := prometheus.NewRegistry()
			in := NewIngester(st, nil, quietLogger(), newServerMetrics(reg))
			var failing atomic.Bool
			var restores atomic.Int32
			st.insert = func(table string, _ int) error {
				if failing.Load() && table == "outages" {
					return errors.New("clickhouse: connection refused")
				}
				return nil
			}
			st.query = func(q string) error {
				if isRestore(q) {
					restores.Add(1)
				}
				return nil
			}
			ingest := func(from, to uint64) {
				t.Helper()
				var recs []*record.Record
				for seq := from; seq <= to; seq++ {
					spec := pathA
					if seq > 10 && seq%10 == 5 { // a one-cycle outage in every part after the first
						spec = brokenA
					}
					recs = append(recs, mk("lab", seq, spec...))
				}
				if _, err := in.Ingest(t.Context(), recs); err != nil {
					t.Fatal(err)
				}
			}
			ingest(1, 10)
			restores.Store(0)
			failing.Store(true)
			for i := range tc.failed {
				from := uint64(11 + 10*i)
				ingest(from, from+9)
			}
			failing.Store(false)
			from := uint64(11 + 10*tc.failed)
			ingest(from, from+9)
			if got := fmt.Sprint(outagesOf(st.table("outages"))); got != tc.want {
				t.Errorf("outages %s, want %s", got, tc.want)
			}
			if got := restores.Load(); got != 0 {
				t.Errorf("%d restore queries, want none", got)
			}
			n := tc.failed + 1
			if got, want := outageEvents(t, reg), fmt.Sprintf("close %d open %d", n, n); got != want {
				t.Errorf("outages counted: %s, want %s", got, want)
			}
		})
	}
}

// One failed part in which the stream closes more outages than maxPending
// (a lossy destination: every lost reply is a one-cycle outage) and is down
// at its end. Until round thirteen the stream was restored past the bound,
// and the restore rebuilt the long outage from storage, which never had its
// open row: [91 to 200: 109 cycles] for [42 to 200: 158 cycles]. And the
// failed part's own outages crowd out none that began before it: here the
// long one, still down when the next part fails as well, and closed in it.
func TestManyLostClosesInAPartKeepTheStreamsOutage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		failed [][2]uint64 // parts whose outages insert fails
		want   string
	}{
		// 20 one-cycle outages at 12, 14, ..., 50, then down from 54.
		{"one part", [][2]uint64{{11, 120}}, "map[12:end 13, 1 cycles 14:end 15, 1 cycles 16:end 17, 1 cycles " +
			"18:end 19, 1 cycles 20:end 21, 1 cycles 22:end 23, 1 cycles 24:end 25, 1 cycles 26:end 27, 1 cycles " +
			"54:end 200, 146 cycles]"},
		// The long outage begins in the first failed part and ends in the
		// second (at 200): its close row is kept over the newest of the
		// first part's own.
		{"two parts", [][2]uint64{{11, 120}, {121, 210}}, "map[12:end 13, 1 cycles 14:end 15, 1 cycles " +
			"16:end 17, 1 cycles 18:end 19, 1 cycles 20:end 21, 1 cycles 22:end 23, 1 cycles 24:end 25, 1 cycles " +
			"54:end 200, 146 cycles]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newHookStore()
			in := NewIngester(st, nil, quietLogger(), nil)
			var failing atomic.Bool
			var restores atomic.Int32
			st.insert = func(table string, _ int) error {
				if failing.Load() && table == "outages" {
					return errors.New("clickhouse: connection refused")
				}
				return nil
			}
			st.query = func(q string) error {
				if isRestore(q) {
					restores.Add(1)
				}
				return nil
			}
			ingest := func(from, to uint64) {
				t.Helper()
				var recs []*record.Record
				for seq := from; seq <= to; seq++ {
					spec := pathA
					if seq >= 12 && seq <= 50 && seq%2 == 0 || seq >= 54 && seq < 200 {
						spec = brokenA
					}
					recs = append(recs, mk("lab", seq, spec...))
				}
				if _, err := in.Ingest(t.Context(), recs); err != nil {
					t.Fatal(err)
				}
			}
			ingest(1, 10)
			restores.Store(0)
			failing.Store(true)
			next := uint64(11)
			for _, p := range tc.failed {
				ingest(p[0], p[1])
				next = p[1] + 1
			}
			failing.Store(false)
			ingest(next, 220)
			if got := fmt.Sprint(outagesOf(st.table("outages"))); got != tc.want {
				t.Errorf("outages %s, want %s", got, tc.want)
			}
			if got := restores.Load(); got != 0 {
				t.Errorf("%d restore queries, want none", got)
			}
		})
	}
}

// keepPending keeps at most maxPending rows: past it the newest of the
// failed parts' own outages make way for one that storage may hold open,
// and a row that must be kept is dropped only when all are such rows.
func TestKeepPendingBound(t *testing.T) {
	row := func(start uint64, k pendKind) outageRow { return outageRow{StartSeq: start, pend: k} }
	starts := func(s *stream) []uint64 {
		var out []uint64
		for _, o := range s.pending {
			out = append(out, o.StartSeq)
		}
		return out
	}
	s := &stream{}
	for i := range uint64(maxPending) {
		s.keepPending(row(10+i, pendOwn))
	}
	s.keepPending(row(100, pendOwn)) // dropped
	s.keepPending(row(1, pendKeep))  // evicts 17
	if got, want := fmt.Sprint(starts(s)), "[10 11 12 13 14 15 16 1]"; got != want {
		t.Errorf("own rows past the bound: %s, want %s", got, want)
	}
	s = &stream{}
	for i := range uint64(maxPending) {
		s.keepPending(row(10+i, pendKeep))
	}
	s.keepPending(row(100, pendKeep)) // dropped: the oldest stay
	if got, want := fmt.Sprint(starts(s)), "[10 11 12 13 14 15 16 17]"; got != want {
		t.Errorf("kept rows past the bound: %s, want %s", got, want)
	}
}

// A part derived again after a crash before its mark (review, round
// fourteen): the agent's retry carries more than the killed attempt, and
// its outages insert fails. The killed attempt stored its events, among
// them the open row of the outage open at its end. #91 marked every lost
// close row of such a part as one to keep, so the eight one-cycle outages'
// closes (stored already, and harmless) filled the stream's pending rows
// and the long outage's close row, the one that mattered, was dropped as
// the newest: "ongoing" for good. Now only the outages storage holds open
// are kept so.
func TestRederivedPartKeepsTheCloseOfTheOutageItLeftOpen(t *testing.T) {
	st := newHookStore()
	in := NewIngester(st, nil, quietLogger(), nil)
	var failMark, failOutages atomic.Bool
	st.insert = func(table string, _ int) error {
		switch {
		case failMark.Load() && table == "agent_seq":
			return errors.New("killed before the mark")
		case failOutages.Load() && table == "outages":
			return errors.New("clickhouse: connection refused")
		}
		return nil
	}
	ingest := func(from, to uint64) {
		t.Helper()
		var recs []*record.Record
		for seq := from; seq <= to; seq++ {
			spec := pathA
			// 8 one-cycle outages at 12, 14, ..., 26, then down from 30 to 70.
			if seq >= 12 && seq <= 26 && seq%2 == 0 || seq >= 30 && seq <= 70 {
				spec = brokenA
			}
			recs = append(recs, mk("lab", seq, spec...))
		}
		if _, err := in.Ingest(t.Context(), recs); err != nil {
			t.Fatal(err)
		}
	}
	ingest(1, 10)
	failMark.Store(true)
	ingest(11, 50) // stored with its events, the long outage open: then killed
	failMark.Store(false)
	in = NewIngester(st, nil, quietLogger(), nil) // the server restarts
	failOutages.Store(true)
	ingest(11, 120) // the agent's retry, with what came in since: its outages insert fails
	failOutages.Store(false)
	ingest(121, 130)
	got := outagesOf(st.table("outages"))
	if got[30] != "end 71, 41 cycles" {
		t.Errorf("the long outage: %q, want end 71, 41 cycles (all: %v)", got[30], got)
	}
}

// slowStore makes the stream replay query take replay and each
// path_changes insert take insert, honouring ctx as the HTTP client does.
type slowStore struct {
	*hookStore
	replay, insert time.Duration
}

func (s *slowStore) InsertJSON(ctx context.Context, table string, rows [][]byte) error {
	if table == "path_changes" && s.insert > 0 {
		select {
		case <-time.After(s.insert):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.hookStore.InsertJSON(ctx, table, rows)
}

// A batch's events budget starts after its first part's derive, so the
// restores in it do not count against it (round two). Since round four
// ectx.get() was evaluated before derive in the same argument list, and a
// slow restore left a slow events insert too little: the route change was
// lost (review, round sixteen, L1). Here with a 1 s budget: a 0.4 s restore
// and a 0.7 s insert.
func TestRestoresDoNotCountAgainstTheEventsBudget(t *testing.T) {
	for _, tc := range []struct {
		replay, insert time.Duration
	}{
		{0, 700 * time.Millisecond},                      // a slow insert alone: within the budget
		{400 * time.Millisecond, 0},                      // a slow restore alone
		{400 * time.Millisecond, 700 * time.Millisecond}, // both: over it if the restore counts
	} {
		t.Run(tc.replay.String()+"+"+tc.insert.String(), func(t *testing.T) {
			hs := newHookStore()
			st := &slowStore{hookStore: hs}
			hs.query = func(q string) error {
				if st.replay > 0 && strings.Contains(q, "ORDER BY seq DESC LIMIT {depth:UInt32}") {
					time.Sleep(st.replay)
				}
				return nil
			}
			in := NewIngester(st, nil, quietLogger(), nil)
			if _, err := in.Ingest(t.Context(), lab(1, 5)); err != nil {
				t.Fatal(err)
			}
			in = NewIngester(st, nil, quietLogger(), nil) // a restart: the stream is restored
			in.eventsTimeout = time.Second
			st.replay, st.insert = tc.replay, tc.insert
			if _, err := in.Ingest(t.Context(), lab(6, 12)); err != nil {
				t.Fatal(err)
			}
			if n := len(hs.table("path_changes")); n != 1 {
				t.Errorf("%d path changes stored, want the one at 6", n)
			}
		})
	}
}
