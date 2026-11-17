-- One row per (agent, cycle): the wire record, with hops and loops as Nested
-- columns, plus the server's enrichment (ASN, AS name, rDNS) for each hop.
-- This is the only table the server inserts records into, so a cycle lands
-- here atomically; per-hop rows are derived by hops_mv, in the same insert
-- but not atomically with it (round four: a failing view leaves the cycle
-- without them, and the server fills them in; see Ingester).
CREATE TABLE IF NOT EXISTS cycles
(
    agent_id    LowCardinality(String),
    seq         UInt64,
    ts          DateTime64(3, 'UTC'),
    target      LowCardinality(String),
    target_ip   LowCardinality(String),
    af          UInt8,
    protocol    LowCardinality(String),
    flow_id     UInt16,
    reached     Bool,
    path_len    UInt8,
    path_hash   LowCardinality(String),
    loops Nested
    (
        ip   String,
        ttls Array(UInt8)
    ),
    hops Nested
    (
        ttl     UInt8,
        status  Enum8('reply' = 1, 'ttl-expired' = 2, 'unreachable' = 3, 'no-reply' = 4),
        ip      LowCardinality(String),
        rtt_us  UInt32,
        code    Int16,
        asn     UInt32,
        as_name LowCardinality(String),
        name    LowCardinality(String),
        mpls    Array(Tuple(label UInt32, tc UInt8, s Bool, ttl UInt8))
    ),
    received_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (agent_id, target, protocol, ts);

-- One row per hop per cycle (the prototype's per-hop layout, extended with
-- agent_id, seq, af, protocol, flow_id, hop_asn, hop_as_name and more).
CREATE TABLE IF NOT EXISTS hops
(
    ts          DateTime64(3, 'UTC'),
    agent_id    LowCardinality(String),
    seq         UInt64,
    target      LowCardinality(String),
    target_ip   LowCardinality(String),
    af          UInt8,
    protocol    LowCardinality(String),
    flow_id     UInt16,
    reached     Bool,
    path_len    UInt8,
    path_hash   LowCardinality(String),
    ttl         UInt8,
    status      Enum8('reply' = 1, 'ttl-expired' = 2, 'unreachable' = 3, 'no-reply' = 4),
    hop_ip      LowCardinality(String),
    rtt_us      UInt32,
    code        Int16,
    hop_asn     UInt32,
    hop_as_name LowCardinality(String),
    hop_name    LowCardinality(String),
    mpls        Array(Tuple(label UInt32, tc UInt8, s Bool, ttl UInt8))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (agent_id, target, protocol, ts, ttl);

CREATE MATERIALIZED VIEW IF NOT EXISTS hops_mv TO hops AS
SELECT
    ts, agent_id, seq, target, target_ip, af, protocol, flow_id, reached, path_len, path_hash,
    hops.ttl AS ttl, hops.status AS status, hops.ip AS hop_ip, hops.rtt_us AS rtt_us, hops.code AS code,
    hops.asn AS hop_asn, hops.as_name AS hop_as_name, hops.name AS hop_name, hops.mpls AS mpls
FROM cycles
ARRAY JOIN hops;

-- One row per looping address per cycle.
CREATE TABLE IF NOT EXISTS loop_events
(
    ts       DateTime64(3, 'UTC'),
    agent_id LowCardinality(String),
    seq      UInt64,
    target   LowCardinality(String),
    protocol LowCardinality(String),
    flow_id  UInt16,
    loop_ip  LowCardinality(String),
    ttls     Array(UInt8)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (agent_id, target, protocol, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS loop_events_mv TO loop_events AS
SELECT ts, agent_id, seq, target, protocol, flow_id, loops.ip AS loop_ip, loops.ttls AS ttls
FROM cycles
ARRAY JOIN loops;

-- Highest seq stored per agent, for deduplication on (agent_id, seq). Since
-- migration 0004 the server writes it, once a cycle's hop and loop rows are
-- stored too, and agent_seq_mv is gone.
CREATE TABLE IF NOT EXISTS agent_seq
(
    agent_id LowCardinality(String),
    max_seq  SimpleAggregateFunction(max, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY agent_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS agent_seq_mv TO agent_seq AS
SELECT agent_id, max(seq) AS max_seq FROM cycles GROUP BY agent_id;

-- Route changes, decided by the server with record.PathTracker (silent
-- hops are wildcards; see HANDOFF.md §10). Idempotent per (stream, seq).
CREATE TABLE IF NOT EXISTS path_changes
(
    ts             DateTime64(3, 'UTC'),
    agent_id       LowCardinality(String),
    target         LowCardinality(String),
    protocol       LowCardinality(String),
    flow_id        UInt16,
    seq            UInt64,
    reason         Enum8('router' = 1, 'distance' = 2, 'target' = 3),
    ttls           Array(UInt8),
    from_ips       Array(String),
    to_ips         Array(String),
    prev_seq       UInt64,
    prev_path_hash LowCardinality(String),
    path_hash      LowCardinality(String),
    prev_path_len  UInt8,
    path_len       UInt8
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (agent_id, target, protocol, flow_id, seq);

-- Reachability-loss windows: from the first cycle that did not reach the
-- destination to the first that did again. Written when a window opens
-- (end_ts NULL) and again when it closes; the higher version wins.
CREATE TABLE IF NOT EXISTS outages
(
    agent_id     LowCardinality(String),
    target       LowCardinality(String),
    protocol     LowCardinality(String),
    flow_id      UInt16,
    start_seq    UInt64,
    start_ts     DateTime64(3, 'UTC'),
    end_seq      UInt64,
    end_ts       Nullable(DateTime64(3, 'UTC')),
    cycles       UInt32,
    last_hop_ttl UInt8,
    last_hop_ip  LowCardinality(String),
    version      UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(start_ts)
ORDER BY (agent_id, target, protocol, flow_id, start_seq);
