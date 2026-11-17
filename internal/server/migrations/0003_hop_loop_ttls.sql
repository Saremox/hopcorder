-- Each hop's loop TTLs come with the cycle (security review, round three).
--
-- hops_mv computed is_loop and loop_ttls from the cycle's loops after
-- ARRAY JOIN hops, so ClickHouse copied the cycle's whole loops.ip and
-- loops.ttls arrays onto every hop row before looking the hop up: hops x
-- loops per cycle. One valid record (255 hops, 255 loops of 255 TTLs) took
-- about 48 MB to insert, and an 8 MiB insert of them over 1.6 GiB. The
-- server now sends each hop's loop TTLs (those of the first loop with the
-- hop's address, empty if none, as the view chose them), so they count
-- towards the size of an insert, and the view only formats them row by row.
--
-- The default (one empty list per hop) is for cycles stored before this
-- migration and for rows inserted by a server without the column during an
-- upgrade: their hop rows get no loop marks; loop_events has every loop.
ALTER TABLE cycles
    ADD COLUMN IF NOT EXISTS `hops.loop_ttls` Array(Array(UInt8))
        DEFAULT arrayResize(CAST([], 'Array(Array(UInt8))'), length(`hops.ttl`)) AFTER `hops.mpls`;

-- Recreated reading the new column. The server applies migrations before it
-- accepts ingest, so no insert can slip through between the two statements.
DROP VIEW IF EXISTS hops_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS hops_mv TO hops AS
SELECT
    ts, agent_id, seq, target, target_ip, af, protocol, flow_id, reached, path_len, path_hash,
    hops.ttl AS ttl, hops.status AS status, hops.ip AS hop_ip, hops.rtt_us AS rtt_us, hops.code AS code,
    hops.asn AS hop_asn, hops.as_name AS hop_as_name, hops.name AS hop_name, hops.mpls AS mpls,
    notEmpty(hops.loop_ttls) AS is_loop,
    arrayStringConcat(arrayMap(x -> toString(x), hops.loop_ttls), ',') AS loop_ttls
FROM cycles
ARRAY JOIN hops;
