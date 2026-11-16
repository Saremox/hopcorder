-- ============================================================================
-- pathwatch : ClickHouse schema + analysis queries
-- ============================================================================
-- Sizing: 1 target x 10 hops @ 5s = 172,800 rows/day. Twenty targets is ~3.5M
-- rows/day, which compresses to well under 100 MB/day with these types.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS netpath;

CREATE TABLE IF NOT EXISTS netpath.path_probes
(
    ts          DateTime64(3, 'UTC'),
    src         LowCardinality(String),
    target      LowCardinality(String),
    target_ip   LowCardinality(String),
    cycle       UInt64,
    ttl         UInt8,
    -- reply | ttl-expired | no-reply | no-route | network-down | ...
    status      LowCardinality(String),
    hop_ip      LowCardinality(String),   -- '' when the hop did not answer
    hop_name    LowCardinality(String),
    rtt_us      UInt32,
    reached     UInt8,                    -- did this cycle reach the target
    path_len    UInt8,
    path_hash   LowCardinality(String),   -- fingerprint of the ordered hop list
    is_loop     UInt8,
    loop_ttls   String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (target, ttl, ts)
TTL toDateTime(ts) + INTERVAL 90 DAY;


-- ============================================================================
-- Q1. WHEN does it break? End-to-end reachability, minute by minute.
--     ttl = 1 gives exactly one row per cycle, so this is a clean cycle count.
-- ============================================================================
SELECT
    toStartOfMinute(ts)                      AS t,
    count()                                  AS cycles,
    round(100 * (1 - avg(reached)), 1)       AS pct_unreachable
FROM netpath.path_probes
WHERE target = 'dc1.example'
  AND ttl = 1
  AND ts >= now() - INTERVAL 3 DAY
GROUP BY t
HAVING pct_unreachable > 0
ORDER BY t;


-- ============================================================================
-- Q2. WHERE does it break? For every cycle that failed to reach the target,
--     the last hop that answered at all. This is the money query: it tells you
--     whether the path dies inside the transit provider or past it.
-- ============================================================================
SELECT
    t,
    last_ttl,
    last_hop,
    count() AS failed_cycles
FROM
(
    SELECT
        toStartOfMinute(ts)                                       AS t,
        cycle,
        maxIf(ttl, status != 'no-reply')                          AS last_ttl,
        argMaxIf(if(hop_name != '', hop_name, hop_ip),
                 ttl, status != 'no-reply')                       AS last_hop
    FROM netpath.path_probes
    WHERE target = 'dc1.example'
      AND reached = 0
      AND ts >= now() - INTERVAL 3 DAY
    GROUP BY t, cycle
)
GROUP BY t, last_ttl, last_hop
ORDER BY t, last_ttl;


-- ============================================================================
-- Q3. DID THE ROUTE CHANGE? Every transition of the path fingerprint, with the
--     full hop list before and after. One row per actual route change.
-- ============================================================================
SELECT
    ts               AS changed_at,
    prev_path        AS before,
    path             AS after
FROM
(
    SELECT
        ts,
        path,
        path_hash,
        neighbor(path_hash, -1) AS prev_hash,
        neighbor(path, -1)      AS prev_path
    FROM
    (
        SELECT
            min(ts)                                                        AS ts,
            cycle,
            any(path_hash)                                                 AS path_hash,
            arrayStringConcat(
                groupArray(if(hop_ip = '', '*', hop_ip)), ' > ')           AS path
        FROM netpath.path_probes
        WHERE target = 'dc1.example'
          AND ts >= now() - INTERVAL 3 DAY
        GROUP BY cycle
        ORDER BY cycle
    )
)
WHERE prev_hash != '' AND prev_hash != path_hash
ORDER BY changed_at;


-- ============================================================================
-- Q4. LOOPS. Any cycle where one IP answered at more than one TTL.
--     Deduplicated to one row per loop occurrence.
-- ============================================================================
SELECT
    ts,
    hop_ip,
    hop_name,
    loop_ttls,
    reached
FROM netpath.path_probes
WHERE target = 'dc1.example'
  AND is_loop = 1
  AND ttl = toUInt8(splitByChar(',', loop_ttls)[1])   -- first TTL only
  AND ts >= now() - INTERVAL 3 DAY
ORDER BY ts;


-- ============================================================================
-- Q5. REAL LOSS vs ICMP RATE LIMITING.
--     A hop's own no-reply rate only means forwarding loss if the end-to-end
--     rate is comparable. own_loss high + e2e_loss low = the router is just
--     deprioritising TTL-exceeded generation. Ignore those hops.
-- ============================================================================
SELECT
    ttl,
    any(if(hop_name != '', hop_name, hop_ip))                 AS hop,
    round(100 * countIf(status = 'no-reply') / count(), 1)    AS own_loss_pct,
    round(100 * (1 - avg(reached)), 1)                        AS e2e_loss_pct,
    multiIf(own_loss_pct < 5,                    'clean',
            e2e_loss_pct > own_loss_pct * 0.5,   'REAL LOSS',
                                                 'rate-limited (ignore)') AS verdict
FROM netpath.path_probes
WHERE target = 'dc1.example'
  AND ts >= now() - INTERVAL 1 DAY
GROUP BY ttl
ORDER BY ttl;


-- ============================================================================
-- Q6. Per-hop latency and loss heatmap, for Grafana.
--     Use ttl as the series and t as the time field.
-- ============================================================================
SELECT
    toStartOfInterval(ts, INTERVAL 1 MINUTE)                  AS t,
    concat(toString(ttl), '. ',
           any(if(hop_name != '', hop_name, hop_ip)))         AS hop,
    round(quantile(0.5)(nullIf(rtt_us, 0)) / 1000, 2)         AS rtt_p50_ms,
    round(quantile(0.95)(nullIf(rtt_us, 0)) / 1000, 2)        AS rtt_p95_ms,
    round(100 * countIf(status = 'no-reply') / count(), 1)    AS loss_pct
FROM netpath.path_probes
WHERE target = 'dc1.example'
  AND $__timeFilter(ts)
GROUP BY t, ttl
ORDER BY t, ttl;


-- ============================================================================
-- Q7. Correlate two targets. Since both DCs go via the same transit, this
--     shows whether they break in the same minutes (transit fault) or not
--     (two independent problems that happen to share a schedule).
-- ============================================================================
SELECT
    toStartOfMinute(ts)                                            AS t,
    round(100 * (1 - avgIf(reached, target = 'target-a')), 1)      AS a_loss,
    round(100 * (1 - avgIf(reached, target = 'target-b')), 1)      AS b_loss
FROM netpath.path_probes
WHERE ttl = 1
  AND target IN ('target-a', 'target-b')
  AND ts >= now() - INTERVAL 3 DAY
GROUP BY t
HAVING a_loss > 0 OR b_loss > 0
ORDER BY t;
