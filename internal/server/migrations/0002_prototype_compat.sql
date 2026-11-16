-- Reconcile with docs/prototype/schema.sql (added after M2).
--
-- The prototype keeps loops on the per-hop rows: is_loop and loop_ttls (the
-- comma-separated TTLs at which that row's address answered). hops gains
-- both, filled by hops_mv from the cycle's loops. Rows stored before this
-- migration keep the defaults; loop_events has them all.
ALTER TABLE hops
    ADD COLUMN IF NOT EXISTS is_loop   Bool   DEFAULT false,
    ADD COLUMN IF NOT EXISTS loop_ttls String DEFAULT '';

-- Recreated with the new columns. The server applies migrations before it
-- accepts ingest, so no insert can slip through between the two statements.
DROP VIEW IF EXISTS hops_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS hops_mv TO hops AS
SELECT
    ts, agent_id, seq, target, target_ip, af, protocol, flow_id, reached, path_len, path_hash,
    hops.ttl AS ttl, hops.status AS status, hops.ip AS hop_ip, hops.rtt_us AS rtt_us, hops.code AS code,
    hops.asn AS hop_asn, hops.as_name AS hop_as_name, hops.name AS hop_name, hops.mpls AS mpls,
    has(loops.ip, hops.ip) AS is_loop,
    arrayStringConcat(arrayMap(x -> toString(x), arrayElement(loops.ttls, indexOf(loops.ip, hops.ip))), ',') AS loop_ttls
FROM cycles
ARRAY JOIN hops;

-- The prototype's netpath.path_probes, by its column names, so its analysis
-- queries run against hopcorder data with only the database name changed:
-- src = agent_id, cycle = seq. Extra columns are appended. Status values are
-- reply, ttl-expired, unreachable and no-reply; the prototype's no-route and
-- network-down become no-reply cycles here (HANDOFF.md §10).
CREATE VIEW IF NOT EXISTS path_probes AS
SELECT
    ts, agent_id AS src, target, target_ip, seq AS cycle, ttl, toString(status) AS status,
    hop_ip, hop_name, rtt_us, toUInt8(reached) AS reached, path_len, path_hash,
    toUInt8(is_loop) AS is_loop, loop_ttls,
    agent_id, protocol, flow_id, af, code, hop_asn, hop_as_name, mpls
FROM hops;
