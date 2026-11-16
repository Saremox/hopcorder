# Changelog

The section for a version is the text of its GitHub release.

## v0.1.0

The first release: the agent, the ingest server, Grafana dashboards and the
built-in UI, with container images and Helm charts.

**Install**

- Binaries (Linux, amd64 and arm64): the archives below; the agent needs
  `CAP_NET_RAW` (`sudo setcap cap_net_raw+ep hopcorder`), not root.
- Images: `ghcr.io/saremox/hopcorder-agent:0.1.0` and
  `ghcr.io/saremox/hopcorder-server:0.1.0`.
- Helm: `helm install hopcorder-server oci://ghcr.io/saremox/charts/hopcorder-server --version 0.1.0`
  and `oci://ghcr.io/saremox/charts/hopcorder-agent` (the chart packages are
  also attached below).
- A five-minute try-out with Docker: `docker compose -f deploy/docker-compose.yml up -d --build`
  from a checkout.

**Agent** (`hopcorder agent`)

- One probe per TTL per cycle, all TTLs in parallel, so ICMP rate limiting on
  routers does not turn into fake loss; one record per cycle with the
  responding address at every TTL.
- ICMP, UDP and TCP SYN probes over IPv4 and IPv6, paris-traceroute flow
  stability, an adaptive maximum TTL, kernel receive timestamps, MPLS label
  stacks (RFC 4950), loop detection, and route changes that ignore silent
  (rate-limited) hops.
- A crash-safe, bounded on-disk spool: records survive restarts and network
  outages and are delivered exactly once after deduplication.
- Outputs: JSONL, HTTP to the server (zstd, bearer or basic auth, mTLS,
  custom headers) and Prometheus metrics with bounded labels.
- SIGHUP reloads the configuration; `--once` traces a few targets and exits.

**Server** (`hopcorder server`)

- Ingest with deduplication on `(agent_id, seq)` into ClickHouse, over its
  HTTP interface; the schema is created and migrated on startup.
- Hops enriched with their origin AS (iptoasn, refreshed daily) and reverse
  DNS, resolved in the background.
- Route changes, outage windows and loops derived per stream; a read API for
  them, the cycles and the topology; retention (90 days by default).
- A `path_probes` view with the prototype's column names, so its analysis
  queries run on hopcorder data.

**Dashboards and UI**

- A Grafana dashboard for the ClickHouse data source: reachability, outages,
  two-target correlation, the last responding hop of failed cycles, real loss
  versus rate limiting, per-hop heatmaps, route changes and loops.
- The server's own UI at `/ui/`: a time scrubber over the reachability
  timeline, the path topology (hops per TTL, coloured by AS, with the selected
  cycle, route changes and loops highlighted) and every cycle hop by hop.

**Known limitations**

- The agent runs on Linux only.
- hopcorder does no authentication: put the server behind a reverse proxy.
- The server runs as a single replica (each stream's history is kept in
  memory).
- The 24-hour soak test of the agent was replaced by an accelerated one.
- On phone screens the UI's hop graph labels crowd (zooming works).
