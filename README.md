# hopcorder

A flight recorder for network paths. The agent traces the route to a set of
targets every few seconds and records every hop of every cycle, so you can
answer *when* a path broke, *at which hop*, and *whether the route changed*,
after the fact.

It exists because `mtr` cannot answer those questions: it aggregates a run into
one row per hop, and it probes each hop once per second, which trips ICMP rate
limiting on routers and shows 80–97% "loss" on hops that forward traffic fine.
hopcorder sends **one probe per TTL per cycle, all TTLs in parallel**, and
writes one record per cycle with the responding address at every TTL.

Status: milestone 3. The agent, the ingest server (ClickHouse) and the
Grafana dashboards are done; a topology UI is planned. See
[`docs/HANDOFF.md`](docs/HANDOFF.md).

## Try it out

On a Linux host with Docker, from a checkout:

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

This starts ClickHouse, the server, an agent tracing three public resolvers
(ICMP, UDP and TCP; edit [`deploy/compose/agent.yaml`](deploy/compose/agent.yaml)
for your own targets) and Grafana with the dashboards at
<http://localhost:3000>. Data shows up within a minute. Everything listens on
localhost only and nothing has a password: it is a try-out, not a deployment.
`docker compose -f deploy/docker-compose.yml down -v` removes it with its data.

## Install

Requires Linux and Go 1.27+.

```bash
go install github.com/saremox/hopcorder/cmd/hopcorder@latest
```

or from a checkout:

```bash
make build
```

### Privileges: `CAP_NET_RAW`, not root

hopcorder opens raw sockets, which needs `CAP_NET_RAW`. It never needs root.
Grant the capability to the binary once:

```bash
sudo setcap cap_net_raw+ep "$(command -v hopcorder)"
```

or run it under systemd with `AmbientCapabilities=CAP_NET_RAW` (the shipped
unit does). Without the capability the agent exits at startup and says so.

## Quick start

Trace a few targets once and print the records:

```bash
hopcorder agent --once example.net 192.0.2.1
```

```json
{"schema":1,"agent_id":"lab-01","seq":0,"ts":"2026-11-16T22:14:05.123Z","target":"example.net","target_ip":"93.184.215.14","af":4,"protocol":"icmp","flow_id":33434,"reached":true,"path_len":9,"path_hash":"ace3702aca4b1a5a","loops":[],"hops":[{"ttl":1,"status":"ttl-expired","ip":"192.168.187.1","rtt_us":1432},{"ttl":2,"status":"no-reply"},…,{"ttl":9,"status":"reply","ip":"93.184.215.14","rtt_us":26711}]}
```

Run continuously, appending to a file:

```bash
hopcorder agent --spool-dir ./spool --jsonl ./records.jsonl example.net
```

For production use a config file and the systemd unit in
[`deploy/`](deploy/):

```bash
sudo install -m 0755 hopcorder /usr/local/bin/hopcorder
sudo install -D -m 0644 deploy/agent.example.yaml /etc/hopcorder/agent.yaml
sudo install -m 0644 deploy/hopcorder.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hopcorder
```

## How it works

- **One probe per TTL per cycle.** TTLs `1..max_ttl` go out together; each
  router sees one probe per interval, well under typical ICMP rate limits.
- **Paris mode (default).** Every header field that ECMP routers hash on is
  held constant across all TTLs of a cycle and across cycles, so a
  load-balanced path is traced as one consistent path instead of flapping
  between members. ICMP keeps identifier and checksum constant (the payload
  compensates for the sequence number); UDP keeps both ports fixed and makes
  the UDP checksum the probe id; TCP keeps both ports fixed and carries the
  probe id in the SYN's sequence number. `flow_id` (ICMP id / UDP or TCP
  source port) is on every record. `paris: false` gives classic traceroute
  behaviour.
- **TCP SYN probes** reach destinations behind firewalls that drop ICMP and
  UDP. The destination's SYN-ACK or RST counts as `reply` (so does an ICMP
  port unreachable from the destination itself, i.e. a REJECT rule). The
  kernel resets the half-open connection; no connection is ever established.
- **One shared listener per address family**, replies matched to probes by
  parsing the original packet quoted inside ICMP errors. No goroutine per
  probe.
- **Spool first.** Every record is committed to an on-disk spool (bbolt)
  before any output ships it. Outputs keep their own cursors and catch up after
  outages; `kill -9` loses at most the cycle in progress.
- **No enrichment on the agent.** rDNS and ASN lookups are server-side.

### The record

One JSON record per (target, cycle). `seq` is monotonic per agent and survives
restarts; `(agent_id, seq)` is the idempotency key. Per hop, `status` is
`reply` (the destination answered; for UDP, its port-unreachable),
`ttl-expired`, `unreachable` (with the ICMP `code`) or `no-reply`; `ip` and
`rtt_us` are present unless `no-reply`. Routers inside an MPLS tunnel often
report the label stack in their ICMP errors (RFC 4950); hopcorder records it
as `mpls: [{label, tc, s, ttl}, …]` on that hop.

Derived per cycle:

- **Path end**: the lowest TTL answered by the destination (or by an ICMP
  unreachable). If neither happens, `probe_past` silent TTLs are kept beyond
  the last responding hop and the rest dropped. `path_len` is the number of
  hops kept.
- **`path_hash`**: first 16 hex chars of SHA-1 over the hop IPs joined with
  `|`, `*` for silent hops: a fingerprint of the exact snapshot. A hop that
  answers in some cycles and not others alternates between two hashes, so
  don't count route changes by comparing hashes. hopcorder decides route
  changes (for `hopcorder_path_changes_total`) by remembering the last
  address at each TTL and treating silent hops as wildcards: a change is a
  TTL answering from a different address, a reached destination at a
  different distance, or a different target address.
- **`loops`**: addresses that answered *time exceeded* at more than one TTL.

`--once` records carry `seq: 0`; it does not touch the spool, so it can run
next to a running agent.

## Caveats when reading the data

- **Silence is not loss.** A router that does not answer (`no-reply`) may be
  rate-limiting or filtering ICMP while forwarding traffic fine. Real
  forwarding loss shows up at *every hop after it*, including the
  destination. Rate limiting does not propagate.
- **Loops are a floor.** A heavily rate-limited router in a loop answers only
  some cycles, so loop counts undercount.
- **One router, two addresses.** Routers may source ICMP errors from the
  interface the packet arrived on (Linux does for IPv6), so a looping router
  can appear under one address on the first pass and another on the loop
  legs. Loop detection still finds it, because the loop-leg address repeats.
- **ECMP.** In paris mode a target is pinned to one ECMP member; other flows
  (including your real traffic) may take other members. Compare with
  `paris: false` or a second `flow_id` if you suspect a single bad member.
- **Return paths are invisible.** RTTs include the reverse path, which may
  differ from the forward path that traceroute shows.
- **NAT** in front of the agent usually rewrites and restores the probe
  headers transparently; a NAT that does not restore the quoted header breaks
  reply matching (every hop beyond it shows `no-reply`).
- **TCP "reply" may come from a middlebox.** Transparent proxies, SYN proxies
  and some load balancers answer the SYN with the destination's address, so a
  TCP trace can "reach" the target hops early (seen on the first test: port
  443 answered at TTL 2 behind a proxy). Compare with an ICMP or UDP trace to
  the same target when TCP distances look short.
- **Two hopcorder processes on one host** (say `--once` next to the daemon)
  probing the same target with the same `flow_id` can occasionally pick up
  each other's replies. Use a different `flow_id` for side-by-side runs.
- **Beyond the destination**: with `adaptive_max_ttl` (default), once a
  target has been reached, cycles probe only 3 TTLs past its distance. If such
  a cycle neither reaches the destination nor hits an unreachable (the path
  grew, or the destination went silent), the same cycle probes the remaining
  TTLs up to `max_ttl`, so records are identical to full-depth probing.
  Unreached targets are always probed to `max_ttl`. This also keeps the
  destination from being asked for ~20 replies per cycle, which matters for
  UDP, whose port-unreachable replies most hosts rate-limit.

## Configuration

YAML file (`--config`) plus flags; flags override the file's global values,
per-target settings in the file still win. Targets given as arguments replace
the file's targets. The whole config is validated at startup and every problem
is reported at once. See [`deploy/agent.example.yaml`](deploy/agent.example.yaml).

| Key | Default | Flag | Meaning |
| --- | --- | --- | --- |
| `agent_id` | hostname | `--agent-id` | Identifies the agent in records and metrics |
| `interval` | `5s` | `--interval` | Minimum period between cycles of a target (≥ 100ms) |
| `timeout` | `1s` | `--timeout` | Per-probe timeout; must be shorter than the interval |
| `max_ttl` | `30` | `--max-ttl` | Highest TTL probed (1–255) |
| `probe_past` | `2` | `--probe-past` | Silent TTLs kept past the last responder when unreached |
| `protocol` | `icmp` | `--protocol` | Default protocol: `icmp`, `udp` or `tcp` |
| `paris` | `true` | `--paris` | Flow-stable probing |
| `adaptive_max_ttl` | `true` | | Probe only 3 TTLs past the destination's last distance (see below) |
| `dns_ttl` | `5m` | | Re-resolve target names this often |
| `log.level` | `info` | `--log-level` | `debug`, `info`, `warn`, `error` |
| `log.format` | `text` | `--log-format` | `text` or `json` (to stderr) |
| `spool.dir` | — | `--spool-dir` | Spool directory (required unless `--once`) |
| `spool.max_age` | `168h` | | Oldest record kept |
| `spool.max_bytes` | `1GiB` | | Stored (compressed) record bytes kept |

**Targets** (`targets:` list): `host` (name or address, required), and
overrides `protocol`, `port` (destination port: default 33434 for UDP, 443
for TCP), `af` (`4` or
`6`; names with both prefer IPv4 unless set; `--af` applies to targets given as
arguments), `flow_id` (default 33434), `paris`, `adaptive_max_ttl`,
`interval`, `timeout`, `max_ttl`, `probe_past`.

**Outputs** (at least one):

- `jsonl.path` (`--jsonl`): append records to a file, `-` for stdout. Synced to
  disk per batch. After a crash records may repeat (dedup on
  `(agent_id, seq)`); a torn write never runs into the next record. Rotate
  by renaming and sending SIGHUP (see Operations) or with `copytruncate`.
- `http`: POST zstd-compressed NDJSON to `<url>/api/v1/ingest`. Options: `url`,
  `headers`, `bearer_token` or `bearer_token_file`, `basic_auth`
  (`username`, `password` or `password_file`), `tls` (`ca_file`, `cert_file`,
  `key_file`, `server_name`), `timeout` (30s), `batch_max_records` (500),
  `batch_max_bytes` (1MiB), `flush_interval` (10s), `retry_min` (1s),
  `retry_max` (1m). Secret files are re-read per request. Any non-2xx is
  retried with jittered exponential backoff; records stay in the spool until
  accepted. hopcorder does no user authentication itself: secure the server
  behind your reverse proxy (basic auth, OIDC, mTLS).
- `metrics.listen` (`--metrics-listen`), `metrics.path` (`/metrics`):
  Prometheus endpoint.

### Metrics

All carry the constant label `agent_id`; labels are only ever configured
values (`target` = the configured host, `protocol`, `result`, `output`), never
hop addresses.

| Metric | Labels | |
| --- | --- | --- |
| `hopcorder_cycles_total` | target, protocol, result=`reached`\|`unreached` | Completed cycles |
| `hopcorder_e2e_rtt_seconds` | target, protocol | Histogram of destination RTT |
| `hopcorder_path_length` | target, protocol | Hops in the last cycle |
| `hopcorder_path_changes_total` | target, protocol | Route changes (silent hops match anything; see above) |
| `hopcorder_loops_detected_total` | target, protocol | Cycles with at least one loop |
| `hopcorder_cycle_overruns_total` | target, protocol | Cycles longer than the interval |
| `hopcorder_cycle_errors_total` | target, protocol | Cycles that could not run |
| `hopcorder_resolve_errors_total` | target, protocol | Failed DNS lookups |
| `hopcorder_output_errors_total` | output | Failed deliveries |
| `hopcorder_output_pending_records` | output | Spooled, not yet delivered |
| `hopcorder_spool_bytes`, `hopcorder_spool_records` | | Spool contents |
| `hopcorder_spool_records_dropped_total` | | Undelivered records dropped by the bounds |
| `hopcorder_probe_late_replies_total` | | Replies after the timeout |
| `hopcorder_probes_sent_total`, `hopcorder_probe_send_errors_total` | | Probe packets |
| `hopcorder_build_info` | version, goversion | |

## Operations

- **Shutdown** (SIGINT/SIGTERM): probing stops at once (in-flight cycles are
  abandoned), outputs get up to 3 seconds to ship what is spooled, and the rest
  stays in the spool for the next start.
- **Reload** (SIGHUP, `systemctl reload hopcorder`): the config file is read
  again with the same command-line flags applied on top. If it is invalid, the
  error is logged and the running configuration stays. Otherwise the agent
  restarts in-process with it (raw sockets stay open, seq continues); if the
  new configuration cannot start, for example because its metrics address is
  taken, the previous one is started again. Every reload reopens the JSONL
  file, so log rotation can rename it and send SIGHUP instead of using
  `copytruncate`:

  ```
  /var/log/hopcorder/records.jsonl {
      daily
      rotate 14
      compress
      delaycompress
      postrotate
          systemctl reload hopcorder
      endscript
  }
  ```
- **No route to a target** (uplink down) is recorded as a cycle in which
  nothing answered, so outages are visible as data rather than gaps.
- **DNS**: names are re-resolved in the background every `dns_ttl`; if that
  fails the last address keeps being used. Address changes are logged and every
  record carries `target_ip`.
- **Spool disk use** tracks the high-water mark of `max_bytes` plus database
  overhead; the file reuses free space but does not shrink.

## Server

`hopcorder server` receives the agents' batches and stores them in
ClickHouse. It needs no privileges and no ClickHouse client library: it talks
to ClickHouse's HTTP interface, creates the database and applies migrations on
startup, and retries until ClickHouse is reachable (`/readyz` and ingest
answer 503 meanwhile; agents keep spooling).

```bash
hopcorder server --clickhouse-url http://127.0.0.1:8123
```

Point agents at it with `outputs.http.url`. For production use
[`deploy/server.example.yaml`](deploy/server.example.yaml) and
[`deploy/hopcorder-server.service`](deploy/hopcorder-server.service).
hopcorder does **no authentication**: it listens on localhost by default; put a
reverse proxy in front and secure the path prefixes separately.

| Path | For | |
| --- | --- | --- |
| `POST /api/v1/ingest` | agents | zstd/gzip/plain NDJSON of wire records |
| `GET /api/v1/query/targets` | readers | streams seen, with last state and reach ratio |
| `GET /api/v1/query/cycles` | readers | full cycles with enriched hops (`agent`, `target` required) |
| `GET /api/v1/query/path-changes` | readers | route changes with the TTLs and addresses that changed |
| `GET /api/v1/query/loops` | readers | looping addresses per cycle |
| `GET /api/v1/query/outages` | readers | reachability-loss windows (open ones have `end_ts: null`) |
| `GET /healthz`, `/readyz`, `/metrics` | operators | liveness, readiness (schema applied, ClickHouse up), Prometheus |

Query parameters: `agent`, `target`, `protocol`, `from`, `to` (RFC 3339 or Unix
ms; default the last 24h), `limit` (default 1000, max 10000).

**Ingest semantics.** Records are deduplicated on `(agent_id, seq)`: a retried
batch is stored once. Invalid records are rejected for good and reported in the
response (`{"accepted", "duplicates", "rejected", "errors"}`); a storage
failure returns 503 and the agent retries. Agents must never reuse seqs: if you
wipe an agent's spool, also change its `agent_id`, or its new records are
dropped as duplicates (the server logs a warning).

**Enrichment.** Every hop gets its origin AS (`hop_asn`, `hop_as_name`) from
the [iptoasn](https://iptoasn.com) database, downloaded daily and cached on
disk (or a file you maintain), and its reverse DNS name, resolved in the
background so ingest never waits on DNS.

**Tables** (database `hopcorder`): `cycles` (one row per cycle, hops as Nested
columns), `hops` (one row per hop per cycle, the table to query for latency
and loss), `loop_events`, `path_changes` and `outages` (both
`ReplacingMergeTree`: query them with `FINAL`), `agent_seq`. The view
`path_probes` presents `hops` under the prototype's column names, so the
queries in [`docs/prototype/schema.sql`](docs/prototype/schema.sql) run as they
are (database `hopcorder` instead of `netpath`), except Q3, which needs a fix
on current ClickHouse ([`docs/HANDOFF.md`](docs/HANDOFF.md) §10). Data expires after
`clickhouse.retention_days` (default 90). Route changes use
the same rule as the agent's metric: silent hops match anything, so a
rate-limited router flickering does not count.

## Dashboards

[`dashboards/hopcorder.json`](dashboards/hopcorder.json) is a Grafana
dashboard for the [ClickHouse data
source](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/),
built from the prototype's analysis queries. Pick an agent, a target and a
protocol, and a second target to compare with:

- **When does it break?** Unreachable cycles per target, the outage windows,
  and two targets side by side (failing in the same minutes points at a shared
  transit).
- **Where does it break?** For failed cycles, the last hop that still answered
  (with its AS); per-hop no-reply rate and RTT as heatmaps; end-to-end RTT;
  and the *real loss vs rate limiting* table, which flags a hop's loss as real
  only if the end-to-end loss is comparable.
- **Did the route change?** Route changes with the hops that changed and the
  full path before and after (also as annotations on every time series), loops,
  and path length.

To use it with your own Grafana, install the plugin, provision the data
source and dashboard from [`deploy/grafana/provisioning`](deploy/grafana/provisioning)
(it reads `HOPCORDER_CLICKHOUSE_HOST`, `_PORT`, `_DATABASE`, `_USER` and
`_PASSWORD` from Grafana's environment; give it a read-only ClickHouse user),
or import the JSON and pick your ClickHouse data source.

## Development

```bash
go build ./cmd/hopcorder
go test -race ./...
go vet ./...
golangci-lint run
```

Nothing in `go test ./...` needs root, `CAP_NET_RAW` or network access:
everything above `internal/probe` runs against a scripted fake prober, and the
probe engine itself is tested against an in-memory network that answers with
real ICMP bytes. Raw-socket tests live behind the `rawsock` build tag and skip
without privileges:

```bash
go test -c -tags rawsock -o probe.test ./internal/probe
sudo setcap cap_net_raw+ep probe.test
./probe.test -test.v
```

Run as root (or with `CAP_SYS_ADMIN`) and with iproute2 installed, they also
build a four-namespace routed topology to check real TTL expiry and a real
routing loop.

Server tests against a real ClickHouse live behind the `clickhouse` build tag:

```bash
docker run -d --network host -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:25.8
```

```bash
HOPCORDER_CLICKHOUSE_URL=http://127.0.0.1:8123 go test -tags clickhouse ./internal/server/... ./cmd/hopcorder/...
```

They include every dashboard query, expanded the way Grafana and the plugin
would. The `grafana` tag runs them through Grafana itself, against the compose
stack (`make up`, then `make grafana`).

## License

MIT
