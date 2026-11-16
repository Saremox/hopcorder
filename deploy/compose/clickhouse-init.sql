-- Grafana's ClickHouse user in the docker-compose try-out: it may read the
-- hopcorder database and nothing else. No writes or DDL (readonly = 2 still
-- lets the plugin set query settings), and no table functions such as url()
-- or file(), which need privileges it does not have. Anyone who can reach
-- Grafana can send it SQL (anonymous viewers included), so this user is the
-- boundary. Use the same pattern for Grafana in production.
CREATE USER IF NOT EXISTS grafana IDENTIFIED WITH sha256_password BY 'grafana' SETTINGS readonly = 2;
GRANT SELECT ON hopcorder.* TO grafana;
