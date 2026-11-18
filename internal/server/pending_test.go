package server

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/saremox/hopcorder/internal/record"
)

// An outage close row that could not be stored stays on its stream and is
// written again with the stream's next part, and counted once (review,
// round twelve). Until then the stream was restored from storage, which
// lost the outages whose rows were all in the failed inserts and rebuilt
// the stream's open outage from what storage had. Past maxPending lost rows
// the stream is restored, as then.
func TestLostCloseRowsAreWrittenAgain(t *testing.T) {
	for _, tc := range []struct {
		failed   int  // parts whose outages insert fails, each with a closed outage
		restored bool // the stream was restored from storage
		want     string
	}{
		{failed: 3, restored: false, want: "map[15:end 16, 1 cycles 25:end 26, 1 cycles 35:end 36, 1 cycles 45:end 46, 1 cycles]"},
		// Past the bound the stream is restored; the outages whose rows
		// were all in failed inserts are lost (accepted, round six).
		{failed: maxPending + 1, restored: true, want: fmt.Sprintf("map[%d:end %d, 1 cycles]", 10*(maxPending+2)+5, 10*(maxPending+2)+6)},
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
			if got := restores.Load() > 0; got != tc.restored {
				t.Errorf("restored %v, want %v", got, tc.restored)
			}
			n := tc.failed + 1
			if got, want := outageEvents(t, reg), fmt.Sprintf("close %d open %d", n, n); got != want {
				t.Errorf("outages counted: %s, want %s", got, want)
			}
		})
	}
}
