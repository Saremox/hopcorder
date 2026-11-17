package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

// Topology limits: one response stays small enough for a browser to draw.
const (
	topologyMaxNodes     = 2000
	topologyMaxEdges     = 5000
	topologyMaxBuckets   = 400
	topologyMinBucketSec = 5
)

// bucketSteps are the timeline bucket sizes, so bucket edges fall on round
// times.
var bucketSteps = []time.Duration{5 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute,
	5 * time.Minute, 10 * time.Minute, 30 * time.Minute, time.Hour, 3 * time.Hour, 6 * time.Hour,
	12 * time.Hour, 24 * time.Hour}

// timelineBucket picks the smallest step that keeps [from, to) within
// topologyMaxBuckets buckets.
func timelineBucket(from, to time.Time) time.Duration {
	span := to.Sub(from)
	for _, s := range bucketSteps {
		if span/s <= topologyMaxBuckets {
			return s
		}
	}
	return bucketSteps[len(bucketSteps)-1]
}

// handleTopology aggregates one stream over [from, to) for the UI's hop
// graph: every address that answered at each TTL (nodes), every pair of
// consecutive answering hops within a cycle (edges; silent TTLs in between
// are skipped, so an edge can span several TTLs), and reachability per
// time bucket (timeline; rtt_p50_us is 0 in a bucket without a reached
// cycle). agent and target are required; give protocol too,
// or streams of several protocols are merged. limit is ignored: nodes and
// edges are capped, and so are the bytes of the whole response
// (MaxResponseBytes; parts after the one that filled it stay empty);
// truncated: true says so. The bucket size keeps the timeline to at most
// topologyMaxBuckets points.
//
// The edges pair each answering hop with the next by zipping the hop
// arrays with themselves shifted by one, not with a lambda over the
// indexes: ClickHouse copies the arrays a lambda captures for every
// element, which made a cycle cost the square of its hops (255 answering
// hops: about 160 KB a cycle, 3.1 GiB for 20,000).
func (s *Server) handleTopology(w http.ResponseWriter, r *http.Request) {
	qp, err := parseQuery(r, true)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	bucket := timelineBucket(qp.from, qp.to)
	qp.params["bucket"] = strconv.Itoa(int(bucket / time.Second))
	qp.params["nodes"] = strconv.Itoa(topologyMaxNodes + 1)
	qp.params["edges"] = strconv.Itoa(topologyMaxEdges + 1)

	const window = `ts >= {from:DateTime64(3)} AND ts < {to:DateTime64(3)} AND ` + streamFilter
	queries := []struct {
		name, sql string
		max       int
	}{
		{"timeline", `SELECT toStartOfInterval(ts, INTERVAL {bucket:UInt32} second) AS t, count() AS cycles,
			countIf(reached) AS reached_cycles, countIf(length(loops.ip) > 0) AS loop_cycles, max(path_len) AS max_path_len,
			toUInt32(ifNotFinite(quantileIf(0.5)(arrayFirst((rtt, st) -> st = 'reply', hops.rtt_us, hops.status), reached), 0)) AS rtt_p50_us
			FROM cycles WHERE ` + window + `
			GROUP BY t ORDER BY t`, topologyMaxBuckets + 1},
		{"nodes", `SELECT ttl, hop_ip AS ip, anyIf(hop_name, hop_name != '') AS name,
			anyIf(hop_asn, hop_asn != 0) AS asn, anyIf(hop_as_name, hop_as_name != '') AS as_name,
			count() AS cycles, toUInt32(ifNotFinite(quantile(0.5)(rtt_us), 0)) AS rtt_p50_us,
			toBool(countIf(status = 'reply') > 0) AS destination, countIf(is_loop) AS loop_cycles,
			min(ts) AS first_ts, max(ts) AS last_ts
			FROM hops WHERE ` + window + ` AND status != 'no-reply'
			GROUP BY ttl, hop_ip ORDER BY ttl, cycles DESC, ip LIMIT {nodes:UInt32}`, topologyMaxNodes},
		{"edges", `SELECT e.1 AS from_ttl, e.2 AS from_ip, e.3 AS to_ttl, e.4 AS to_ip, count() AS cycles,
			max(ts) AS last_ts
			FROM
			(
				SELECT ts, arrayFilter((t, st) -> st != 'no-reply', hops.ttl, hops.status) AS rt,
					arrayFilter((i, st) -> st != 'no-reply', hops.ip, hops.status) AS ri
				FROM cycles WHERE ` + window + `
			)
			ARRAY JOIN arrayZip(arrayPopBack(rt), arrayPopBack(ri), arrayPopFront(rt), arrayPopFront(ri)) AS e
			GROUP BY from_ttl, from_ip, to_ttl, to_ip ORDER BY cycles DESC, from_ttl, from_ip, to_ip
			LIMIT {edges:UInt32}`, topologyMaxEdges},
	}
	// The timeline first: it is small and the UI needs it whole. The response
	// is kept until all three parts are in (at most the byte budget), so a
	// failure in any of them is still a clean 502.
	js := newJSONStream(w, s.opts.MaxResponseBytes, 0)
	js.number("bucket_ms", bucket.Milliseconds())
	for _, q := range queries {
		js.beginArray(q.name)
		n := 0
		if !js.full() {
			err := s.opts.Store.Query(r.Context(), q.sql, qp.params, func(m json.RawMessage) error {
				if n == q.max {
					js.truncated = true
					return errPartFull
				}
				n++
				return js.row(m)
			})
			if err != nil && !errors.Is(err, errPartFull) && !errors.Is(err, errResponseFull) {
				s.queryFailed(r, js, q.name, err)
				return
			}
		}
		js.endArray()
	}
	js.finish()
}

// errPartFull ends a topology query at its row cap.
var errPartFull = errors.New("row cap reached")
