# Changelog

The section for a version is the text of its GitHub release.

## Unreleased

**Added**

- The server chart can run a single ClickHouse pod next to the server
  (`clickhouse.deploy.enabled`, opt-in): the official image, a persistent
  volume, a generated password kept across upgrades, an optional read-only
  user for Grafana, and a NetworkPolicy. No operator.

**Fixed**

- UI: a click on the timeline while it reloads no longer throws.

**Security: server ingest**

<!-- round three: ingest -->

**Security: read side and Grafana**

- Grafana's read-only ClickHouse user (the compose stack's, and the server
  chart's `clickhouse.deploy.readOnlyUser`) can no longer make ClickHouse
  spill temporary data to disk: one viewer's query with an external sort or
  GROUP BY, or a grace_hash or partial_merge join, filled ClickHouse's volume
  until the server's inserts failed. The spill settings are constant now; such
  a query fails at the memory limit instead. Compose: re-run
  `deploy/compose/clickhouse-init.sql` on an existing volume (it now replaces
  the profile).
- Helm: the server is protected against DNS rebinding. While `kubectl
  port-forward` ran, as the chart's notes suggest, a web page whose name
  resolved to 127.0.0.1 could read the read API and post records (a seq of
  2^64-1 locked its agent out with 409). The chart now sets `allowed_hosts`
  to `localhost`, `127.0.0.1`, `::1`, the Service's names and the ingress
  hosts; other Host names get 421, `/healthz`, `/readyz` and `/metrics`
  still answer any. Upgrading: list the other names clients use (another
  ingress or gateway, a NodePort or LoadBalancer address) in the new
  `allowedHosts` value; the chart refuses NodePort and LoadBalancer Services
  and ingress rules without host until you do (`["*"]` turns the check off).
  A `config.allowed_hosts` of your own replaces the chart's list.
- `allowed_hosts` takes `*.example.net` (one label below `example.net`, as
  an Ingress host of that form), so wildcard ingress hosts work.
- The read API gives up on a client that stops reading its response after
  10 s (each 64 KiB of it must be taken within 10 s), which frees its
  `query.max_concurrent` slot: eight such connections kept the read API and
  the UI at 503 for as long as they stayed open, up to two minutes each.
- The ASN database is bounded by its memory: at most 2 million ranges,
  500,000 distinct ASes and 16 MiB of AS names (the real file has about
  600,000 ranges), stored compactly (31 instead of 73 bytes per range). A
  45 MiB download within the old bounds took the server's heap to 1.35 GB
  and got it OOM-killed in the chart's 1 GiB container on every try; the
  worst file the new bounds allow peaks at about 380 MiB while the old
  database is still in use.

**Security: agent**

<!-- round three: agent -->

**Security: CI, release and deployment**

- Releases carry signed build provenance: every file and both images (by
  digest) are attested by the release job, and the notes list the image
  digests with the `gh attestation verify` commands. The `release`
  environment only gates the repository's own release job; anyone with
  write access can still replace a release's images and files with a
  workflow of their own, and the attestations are how to tell. The
  repository settings that limit it (a tag ruleset for `v*`, immutable
  releases) are listed in `docs/HANDOFF.md`.
- The release job checks, once the release exists, that its tag still
  names the commit it built, and deletes the release again if not.
- systemd: the server can take its socket from systemd
  (`deploy/hopcorder-server.socket` with `listen: systemd`), a unix socket
  that only the reverse proxy's group may open. On `127.0.0.1:8080` every
  local user reached the server around the proxy. Without systemd,
  `listen: unix:<path>` creates a socket with mode 0660. The default stays
  `127.0.0.1:8080`.
- Helm: the agent pods no longer have a liveness probe. It used the metrics
  endpoint, which holds at most 64 connections, so anything that could
  reach the port could have the agent restarted again and again. An opt-in
  NetworkPolicy (`networkPolicy.enabled`, with `networkPolicy.from`) limits
  the metrics port to the peers you list; with `hostNetwork` it does not
  apply, and the chart refuses it.
- Helm: the agent chart also refuses headers such as `Authentication`,
  `Bearer`, `X-Jwt` and `X-AccessToken` in `config` (they would land in a
  ConfigMap). The check goes by name; send credentials only through the
  chart's Secrets.
- The release archives are owned by root (0/0) and reproducible. They
  carried the build user's uid, which GNU tar restores when root unpacks
  them, so that uid owned the unpacked binary.
- Go: install a released version (`go install …@v0.1.1`), not `@latest`:
  the Go module proxy takes any `v*` tag as a version, released or not, and
  keeps it. `go.mod` retracts the withdrawn v0.1.0.

## v0.1.1

The first published release. v0.1.0 was withdrawn: its tag and release are
deleted, and it predated two security reviews whose fixes this release
carries. Everything listed under v0.1.0 below is in it.

**Install**

- Binaries (Linux, amd64 and arm64): the archives below, with `SHA256SUMS`.
  The agent needs `CAP_NET_RAW` (`sudo setcap cap_net_raw+ep hopcorder`, or
  the shipped systemd unit), not root.
- Images: `ghcr.io/saremox/hopcorder-agent:0.1.1` and
  `ghcr.io/saremox/hopcorder-server:0.1.1`.
- Helm: `helm install hopcorder-server oci://ghcr.io/saremox/charts/hopcorder-server --version 0.1.1`
  and `oci://ghcr.io/saremox/charts/hopcorder-agent` (the chart packages are
  also attached below).
- A five-minute try-out with Docker: `docker compose -f deploy/docker-compose.yml up -d --build`
  from a checkout.

**Security: server**

- Records are validated against tight bounds: character sets and lengths for
  `agent_id` and targets, at most 255 hops and 16 MPLS labels per hop,
  consistent loops, hop and loop addresses of the record's address family
  without an IPv6 zone (a zone could carry a megabyte of text), and a `ts`
  within `ingest.max_record_age` (30 days) and at most a day ahead.
  Errors for rejected records are short and never quote the record.
- Ingest is bounded: an NDJSON content type is required (a web page cannot
  post without a CORS preflight), at most 16 agents and 4096 streams per
  request, bodies are read before a request takes one of the
  `ingest.max_concurrent` slots (within 20 s, with a memory cap), zstd
  windows are capped, inserts go in parts of at most 8 MiB, and the defaults
  are 8 MiB per request and 32 MiB decompressed.
- State is bounded: `ingest.max_streams` (20,000) and `ingest.max_agents`
  (10,000), idle ones dropped after an hour; restores of dropped streams
  read a bounded time window, have their own budget, and a stream whose
  restore fails is retried instead of starting over (which left outages open
  for good). The Go memory limit follows the container's.
- A batch far below an agent's stored seqs gets 409 instead of being dropped
  as duplicates.
- The read API runs its ClickHouse queries read-only, so they stop when the
  client goes away, bounds them in time and memory (`query.max_memory_bytes`),
  allows `query.max_concurrent` at once, and streams responses up to
  `query.max_response_bytes` (32 MiB), marking cut ones `"truncated": true`.
  `cycles` takes `limit` up to 1000. Results above 64 KiB no longer come back
  with rows overwritten.
- DNS rebinding: the server answers only the Host names in `allowed_hosts`;
  on a loopback listen address that defaults to the localhost names.
- ClickHouse query parameters are escaped correctly, credentials in
  `clickhouse.url` are refused, `/readyz` no longer returns ClickHouse's error
  text, and the ASN download is size-limited and refuses https-to-http
  redirects.

**Security: dashboards and the compose stack**

- Every dashboard variable is percent-encoded into SQL: Grafana's `sqlstring`
  format left a backslash injection open on ClickHouse.
- Grafana reads ClickHouse with a read-only user whose queries are limited in
  time, memory and concurrency (a viewer could disable the limits before).
- ClickHouse is not published on the host; Grafana and the server answer only
  `localhost` (DNS rebinding), and the Grafana admin password comes from
  `HOPCORDER_GRAFANA_ADMIN_PASSWORD`.

**Security: agent**

- Local processes without privileges can no longer forge hops, loops or path
  ends: probe ids are random, ICMPv4 errors must quote the probe's IP ID,
  echo replies must carry the probe's data (mismatches are counted in
  `hopcorder_probe_mismatched_replies_total`), and the agent holds its ICMP
  echo identifier where ping sockets are allowed.
- The HTTP output never follows redirects (they could resend batches and
  credentials elsewhere); TLS settings with an `http://` URL are refused.
- The metrics endpoint has timeouts and a connection limit; MPLS label
  stacks are capped; IPv4-mapped sources on IPv6 sockets are dropped.
- A corrupt spool file is quarantined and a fresh one started, also for
  damage that crashed the agent before; every new spool starts its seqs at
  the clock, so a wiped spool's records are no longer dropped as duplicates.
- Replies stamped before their probe was sent are ignored.

**Security: build, release and deployment**

- Releases are checked before anything is built (a `v<semver>` tag on a
  commit of `main` whose CI passed, above the latest release, not released
  before) and published only by one job in the `release` environment; until
  then a tag's images exist only as `:sha-<commit>`. Release builds use no
  caches and no write tokens.
- Binaries and images are built with Go 1.27.4 (`toolchain` in `go.mod`;
  before, the binaries used 1.27.0). CI runs govulncheck.
- GitHub Actions are pinned to commits, the Go and BuildKit images by digest,
  and the Dockerfile pulls no frontend image. Dependabot keeps actions,
  modules, npm packages, the Go image and the compose stack's images current.
- The charts refuse credentials that would land in a ConfigMap (inline
  tokens and passwords, credential-like headers in any case, credentials in
  `server.url`, `config.clickhouse.password`) and take them from Secrets;
  neither mounts a service-account token. The server chart uses the server's
  own ingest limits, and its NetworkPolicy requires explicit peers.
- systemd: both units run as a dynamic user, and every secret (token,
  password, client key) goes in as a credential (`LoadCredential=`).
- Docs: the server's `docker run` example and the development ClickHouse
  listen on localhost only, and the development ClickHouse needs a password.

**Upgrading from a v0.1.0 checkout**

- Server behind a proxy that passes the client's Host header on, listening on
  a loopback address: list the public names in `allowed_hosts`.
- Records older than 30 days are refused: raise `ingest.max_record_age` if
  agents spool for longer. `ingest.max_streams` now defaults to 20,000.
- Read API: every response carries `"truncated"`; `cycles` takes `limit` up
  to 1000.
- Agent: a second hopcorder on the same ICMP `flow_id` now fails at startup
  (give it its own `--flow-id`); behind a middlebox that rewrites IPv4 IP
  IDs, hops beyond it go silent (see the mismatched-replies counter).
- Helm: `networkPolicy.enabled=true` needs `networkPolicy.from`; the agent
  chart refuses headers that look like credentials, and the server chart
  `config.clickhouse.password`; the server chart no longer raises the ingest
  limits (set them in `config.ingest` together with
  `resources.limits.memory` if your agents send larger batches).
- Compose: re-run `deploy/compose/clickhouse-init.sql` on an existing volume
  to get the Grafana limits.

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
