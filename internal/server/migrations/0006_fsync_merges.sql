-- Merged parts are fsynced too (security and correctness review, round
-- eighteen).
--
-- 0005 fsyncs inserted parts, so a crash of ClickHouse's host loses no
-- acknowledged cycle. Merged parts were left in the page cache: at a crash
-- their directory entries were already on disk (the inserts' fsyncs commit
-- the journal) but their files were empty, ClickHouse 25.8 counted each as
-- broken, and past max_suspicious_broken_parts (100) it refused to attach
-- the table. Under heavy ingest (300 agents, or 16 catching up with
-- 500-record batches) hops, path_changes, outages or loop_events stayed
-- unattached in 5 of 6 simulated power losses: no data was missing (the
-- merged parts' sources survive), but the server stayed unready and ingest
-- refused every agent until an operator intervened. With this setting the
-- same runs left no broken part, at no measurable cost in throughput.
ALTER TABLE cycles MODIFY SETTING min_compressed_bytes_to_fsync_after_merge = 1;
ALTER TABLE hops MODIFY SETTING min_compressed_bytes_to_fsync_after_merge = 1;
ALTER TABLE loop_events MODIFY SETTING min_compressed_bytes_to_fsync_after_merge = 1;
ALTER TABLE agent_seq MODIFY SETTING min_compressed_bytes_to_fsync_after_merge = 1;
ALTER TABLE path_changes MODIFY SETTING min_compressed_bytes_to_fsync_after_merge = 1;
ALTER TABLE outages MODIFY SETTING min_compressed_bytes_to_fsync_after_merge = 1;
