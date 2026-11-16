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
CREATE SETTINGS PROFILE IF NOT EXISTS grafana_limits SETTINGS
    max_execution_time = 60 MIN 1 MAX 300,
    max_memory_usage = 1000000000 MIN 1 MAX 1000000000,
    max_memory_usage_for_user = 2000000000 MIN 1 MAX 2000000000,
    max_concurrent_queries_for_user = 16 MIN 1 MAX 16,
    cancel_http_readonly_queries_on_client_close = 1 CONST
TO grafana;
