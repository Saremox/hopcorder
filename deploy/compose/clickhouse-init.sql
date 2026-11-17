-- Grafana's ClickHouse user in the docker-compose try-out: it may read the
-- hopcorder database and nothing else. No writes or DDL, and no table
-- functions that reach outside, such as url(), file() or remote(), which
-- need privileges it does not have. Anyone who can reach Grafana can send it
-- SQL (anonymous viewers included), so this user is the boundary. Use the
-- same pattern for Grafana in production.
--
-- OR REPLACE, so re-running the script on an existing volume brings the
-- user and its profile up to date: a user's own settings win over its
-- profile's, and the user of older versions had readonly = 2. The password
-- is the one in docker-compose.yml.
CREATE USER OR REPLACE grafana IDENTIFIED WITH sha256_password BY 'grafana';
GRANT SELECT ON hopcorder.* TO grafana;

-- What its queries may do and cost. readonly = 1: a query may change no
-- setting but those marked CHANGEABLE_IN_READONLY, which are what the
-- Grafana plugin sends. readonly = 2 with a list of forbidden settings was
-- no boundary: the settings off the list let one query switch off
-- ClickHouse's memory tracking and OOM-kill it (max_untracked_memory with
-- memory_profiler_step, or max_threads with use_concurrency_control = 0),
-- pin memory in the query cache for good (use_query_cache), make
-- ClickHouse write MB/s of its own logs (the profilers, log_*), or keep a
-- query out of query_log (log_queries).
--
-- The plugin (grafana-clickhouse-datasource 4.23.5, over clickhouse-go
-- 2.50.0) sends max_execution_time, its query timeout plus 5 s, so that
-- timeout must stay at 295 s or below. It also sends the data source's
-- custom settings and, with the row limit on, `limit`: the provisioned
-- data source has neither, and either would need CHANGEABLE_IN_READONLY
-- here. database, default_format, client_protocol_version and query_id are
-- request parameters, not settings. A setting may still be "set" to the
-- value it has.
--
-- Memory: 400 MB per query and 600 MB for all of Grafana's queries
-- together, the server chart's defaults (clickhouse.deploy.readOnlyUser):
-- under 40% of what its bundled ClickHouse of 2 GiB may use (80%), so the
-- rest stays for the hopcorder server's inserts and reads, ClickHouse's
-- caches and itself. The dashboards' largest query took 17 MiB on a week of
-- eight streams. On a larger host raise them here.
--
-- cancel_http_readonly_queries_on_client_close needs readonly > 0: a query
-- stops when Grafana gives up on it.
--
-- No temporary data on disk: with a spill setting (external sort or GROUP
-- BY, a grace_hash or partial_merge join) one query wrote 100 MB/s to
-- ClickHouse's data volume until it was full and the server's inserts
-- failed. readonly = 1 refuses them; the constraints stay, for the profile
-- with readonly = 2. A query that does not fit max_memory_usage fails
-- instead. join_algorithm keeps ClickHouse's default, the in-memory joins.
-- The limits keep their MIN 1 for the same reason (0 means "unlimited" and
-- is below any MAX).
CREATE SETTINGS PROFILE OR REPLACE grafana_limits SETTINGS
    readonly = 1 CONST,
    max_execution_time = 60 MIN 1 MAX 300 CHANGEABLE_IN_READONLY,
    max_memory_usage = 400000000 MIN 1 MAX 400000000,
    max_memory_usage_for_user = 600000000 MIN 1 MAX 600000000,
    max_concurrent_queries_for_user = 16 MIN 1 MAX 16,
    cancel_http_readonly_queries_on_client_close = 1 CONST,
    max_bytes_before_external_sort = 0 CONST,
    max_bytes_ratio_before_external_sort = 0 CONST,
    max_bytes_before_external_group_by = 0 CONST,
    max_bytes_ratio_before_external_group_by = 0 CONST,
    join_algorithm = 'direct,parallel_hash,hash' CONST
TO grafana;
