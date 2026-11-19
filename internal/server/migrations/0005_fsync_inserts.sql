-- Inserted parts are fsynced before ClickHouse answers (security and
-- correctness review, round seventeen).
--
-- The server answers an agent 200 once ClickHouse has acknowledged the
-- batch's inserts, and the agent then deletes the records from its spool.
-- ClickHouse's MergeTree defaults (fsync_after_insert = 0,
-- fsync_part_directory = 0) leave an inserted part in the page cache until
-- Linux writes it back, up to about 35 s: a power loss, a kernel panic or a
-- hard reset of ClickHouse's host lost every acknowledged cycle of that
-- window (208 of 360 in a simulated power loss), with the mark in
-- agent_seq gone back with them so that nothing re-sent them, and with
-- many agents ClickHouse refused to attach the tables at all afterwards
-- ("Suspiciously many broken parts"). A crash of the ClickHouse process
-- itself lost nothing.
--
-- The cost is an fsync of each inserted part and its directory: an insert
-- into each of these tables per part of an agent's batch. Merged parts are
-- fsynced since 0006: left in the page cache, after a crash under heavy
-- ingest more than max_suspicious_broken_parts of them kept a table from
-- attaching (review, round eighteen).
ALTER TABLE cycles MODIFY SETTING fsync_after_insert = 1, fsync_part_directory = 1;
ALTER TABLE hops MODIFY SETTING fsync_after_insert = 1, fsync_part_directory = 1;
ALTER TABLE loop_events MODIFY SETTING fsync_after_insert = 1, fsync_part_directory = 1;
ALTER TABLE agent_seq MODIFY SETTING fsync_after_insert = 1, fsync_part_directory = 1;
ALTER TABLE path_changes MODIFY SETTING fsync_after_insert = 1, fsync_part_directory = 1;
ALTER TABLE outages MODIFY SETTING fsync_after_insert = 1, fsync_part_directory = 1;
