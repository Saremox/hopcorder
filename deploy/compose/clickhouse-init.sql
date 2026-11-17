-- Grafana's ClickHouse user in the docker-compose try-out: it may read the
-- hopcorder database and nothing else. No writes or DDL (readonly = 2 still
-- lets the plugin set query settings), and no table functions that reach
-- outside, such as url(), file() or remote(), which need privileges it does
-- not have. Anyone who can reach Grafana can send it SQL (anonymous viewers
-- included), so this user is the boundary. Use the same pattern for Grafana
-- in production.
CREATE USER IF NOT EXISTS grafana IDENTIFIED WITH sha256_password BY 'grafana' SETTINGS readonly = 2;
GRANT SELECT ON hopcorder.* TO grafana;

-- What its queries may cost. readonly = 2 lets a query set any setting, so
-- without these one query could run without a time or memory limit
-- (max_execution_time = 0) and keep running after Grafana gave up on it,
-- starving the server's inserts. The constraints need MIN 1: 0 means
-- "unlimited" and is below any MAX. A query may lower the limits, not raise
-- them; switching to another settings profile is not allowed either.
-- Grafana's dashboards need a fraction of this.
--
-- No temporary data on disk: with a spill setting (external sort or GROUP
-- BY, a grace_hash or partial_merge join) one query wrote 100 MB/s to
-- ClickHouse's data volume until it was full and the server's inserts
-- failed. max_temporary_data_on_disk_size_for_* did not stop that (25.8),
-- so the spill settings are constant: a query that does not fit
-- max_memory_usage fails instead. join_algorithm keeps ClickHouse's
-- default, the in-memory joins.
--
-- OR REPLACE, so re-running the script brings an existing profile up to
-- date.
CREATE SETTINGS PROFILE OR REPLACE grafana_limits SETTINGS
    max_execution_time = 60 MIN 1 MAX 300,
    max_memory_usage = 1000000000 MIN 1 MAX 1000000000,
    max_memory_usage_for_user = 2000000000 MIN 1 MAX 2000000000,
    max_concurrent_queries_for_user = 16 MIN 1 MAX 16,
    cancel_http_readonly_queries_on_client_close = 1 CONST,
    max_bytes_before_external_sort = 0 CONST,
    max_bytes_ratio_before_external_sort = 0 CONST,
    max_bytes_before_external_group_by = 0 CONST,
    max_bytes_ratio_before_external_group_by = 0 CONST,
    join_algorithm = 'direct,parallel_hash,hash' CONST
TO grafana;
