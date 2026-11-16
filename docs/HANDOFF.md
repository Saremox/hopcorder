# hopcorder — implementation handoff

A flight recorder for network paths. An agent continuously traces the route to a
set of targets and records every hop of every cycle, so route changes, loops and
outages can be analysed after the fact instead of needing to be caught live.

This document is the source of truth for design decisions. Read it fully before
writing code. Decisions marked **(locked)** were made deliberately; if one looks
wrong, raise it rather than silently changing it.

## 1. Why this exists

The motivating problem: every night around 23:xx, for 30–45 minutes, traffic to
two different datacenters drops, with suspected routing loops inside one transit
provider. A 24h `mtr` run showed the problem but could not
answer *when*, *at which hop*, or *whether the route changed*, because:

- `mtr` aggregates. It collapses a run into one row per hop and stacks multiple
  IPs seen at the same TTL without saying when each was seen.
- `mtr` probes each hop once per second, which hits ICMP rate limiting on
  routers (Linux default `net.ipv4.icmp_ratelimit` is ~1/s, backbone gear is
  stricter). Result: 80–97% "loss" on transit hops that forward traffic fine.
  Real forwarding loss propagates to every downstream hop; rate limiting does
  not.

hopcorder fixes both: it records per-cycle, per-TTL results with the responding
IP, and it probes every TTL once per cycle in parallel, so each router sees one
probe per interval rather than one per second.

A working Python prototype exists in `docs/prototype/pathwatch.py`, with
ClickHouse DDL and analysis queries in `docs/prototype/schema.sql`. Treat it as a
behavioural reference, not code to port line by line. It shells out to
`mtr-packet`; the Go version probes natively.

> **Note (M1):** the prototype files were not present in the repository when M1
> was implemented; M1 was built from this document alone. Add them under
> `docs/prototype/` before starting M2/M3, which depend on `schema.sql`.

## 2. Architecture (locked)

- One Go module, one binary, subcommands: `hopcorder agent`, `hopcorder server`
  (later), `hopcorder version`.
- Agent: native raw-socket probing, local on-disk spool, pluggable outputs
  (JSONL file, HTTP push to server, Prometheus `/metrics` with bounded labels).
- Server (milestone 2): thin HTTP ingest → ClickHouse, with server-side
  enrichment (rDNS, ASN) and derived event tables.
- Dashboard: Grafana dashboards provisioned from the repo first (milestone 3);
  an embedded Vue 3 + TS UI only later, for path-topology views Grafana cannot
  draw.
- Auth is the operator's job, done at the reverse proxy (basic auth / OIDC /
  mTLS). hopcorder itself does not authenticate users. Consequences:
  - Ingest (`/api/v1/ingest`) and read/UI (`/api/v1/query/*`, `/ui/*`) live on
    separate path prefixes so they can be secured differently.
  - The agent must support outbound auth: custom headers, basic auth, bearer
    token, client TLS certificate, custom CA.
  - The server never makes authorization decisions on forwarded headers.
- Wire format: NDJSON, zstd-compressed, with a `schema` version field. No
  protobuf in v1.
- Storage: ClickHouse only. No storage abstraction layer in v1.

## 3. Data model

### 3.1 Wire record — one record per cycle (locked)

The agent emits one record per (target, cycle) with a `hops` array. A cycle is
atomic: either the whole path snapshot is shipped or none of it. The server
flattens to one row per hop at insert time.

```json
{
  "schema": 1,
  "agent_id": "lap-mstrassberger",
  "seq": 184223,
  "ts": "2026-11-16T22:14:05.123Z",
  "target": "dc1.example",
  "target_ip": "198.19.80.226",
  "af": 4,
  "protocol": "icmp",
  "flow_id": 33434,
  "reached": true,
  "path_len": 9,
  "path_hash": "ace3702aca4b1a5a",
  "loops": [{"ip": "198.18.120.1", "ttls": [6, 7]}],
  "hops": [
    {"ttl": 1, "status": "ttl-expired", "ip": "192.168.187.1", "rtt_us": 1432},
    {"ttl": 6, "status": "no-reply"},
    {"ttl": 9, "status": "reply", "ip": "198.19.80.226", "rtt_us": 26711}
  ]
}
```

- `seq`: monotonic per agent, persisted across restarts. The pair
  `(agent_id, seq)` is the idempotency key for retries.
- `status` values: `reply` (destination answered), `ttl-expired`, `unreachable`
  (ICMP dest-unreachable; include `code`), `no-reply` (timeout).
- `rtt_us` and `ip` are omitted on `no-reply`.
- rDNS and ASN are not done by the agent. The server enriches.

### 3.2 Derived values — semantics carried over from the prototype

- **Path end**: the lowest TTL that got `reply` from the destination. If the
  destination never answers, keep `probe_past` (default 2) TTLs beyond the last
  responding hop and drop the rest, so a black-holed target does not produce 30
  `no-reply` hops per cycle.
- **`path_hash`**: first 16 hex chars of SHA-1 over the ordered hop IPs joined
  with `|`, using `*` for non-responding hops. The placeholder is intentional: a
  rate-limited router flickering in and out must not register as a path change.
  A change in `path_hash` == a route change.
- **Loops**: any IP that responded at more than one TTL within one cycle. Note
  the limitation in docs: a heavily rate-limited looping router will be detected
  only in a fraction of cycles, so loop counts are a floor.

## 4. Probing engine — the technical core

Package: `internal/probe`. Interface first, so everything above it is testable
without raw sockets:

```go
type Prober interface {
    // Trace probes TTLs 1..maxTTL in parallel and returns one result per TTL.
    Trace(ctx context.Context, req TraceRequest) ([]HopResult, error)
}
```

Requirements:

1. **Parallel TTLs.** All TTLs for a cycle are sent near-simultaneously. One
   cycle = one probe per TTL. No per-hop repetition within a cycle.
2. **Shared sockets, reply demultiplexing.** One raw ICMP listener per address
   family for the whole process, not per probe or per target. Match replies to
   in-flight probes by parsing the quoted original packet inside ICMP
   Time-Exceeded / Dest-Unreachable (quoted IP header + first 8 bytes of the
   original L4 header) and keying on (dst IP, protocol, id/seq or ports).
   Echo-Reply from the destination is matched on ICMP id/seq directly.
3. **Protocols**: `icmp` (milestone 1), `udp` (milestone 1), `tcp` SYN
   (milestone 1 stretch, else milestone 2).
4. **Paris mode (flow-stable probing).** ECMP hashes on the 5-tuple. Classic
   traceroute varies a hashed field per probe and therefore sees load-balanced
   hops flap between IPs. In paris mode, keep the flow identifier constant
   across all TTLs of a cycle:
   - UDP/TCP: fixed src port, dst port; encode the TTL/probe id somewhere not
     hashed (e.g. IP ID field, or UDP checksum via payload adjustment).
   - ICMP: keep ICMP identifier and checksum constant across probes by adjusting
     the payload to compensate for the changing sequence number.
   - `flow_id` is recorded on every wire record. Make paris mode the default.
5. **IPv4 and IPv6** from the start (`golang.org/x/net/ipv4`, `ipv6`, `icmp`).
6. **Privileges**: require `CAP_NET_RAW`, never root. Fail at startup with a
   clear message telling the user to run `setcap cap_net_raw+ep $(which
   hopcorder)` or set `AmbientCapabilities`.
7. **Timeouts**: per-probe timeout (default 1s); the whole cycle ends at
   `timeout + small grace`. Late replies are dropped and counted in a metric.
8. **No goroutine-per-probe leaks.** Pending-probe table with expiry; verify
   under `-race` and with a long-running soak test.

## 5. Agent runtime

### 5.1 Scheduling

- One scheduler goroutine per target; a slow or black-holed target must not
  delay others.
- Stagger target start offsets evenly across the interval to smooth bursts.
- Interval is a minimum period: if a cycle overruns, start the next immediately
  and count the overrun in a metric. Never queue up cycles.
- Re-resolve target hostnames every `dns_ttl` (default 5m). Record the resolved
  IP on every record; a DNS change is itself useful information.

### 5.2 Spool (locked: this is the most important reliability feature)

The agent measures network outages, so its link to the server will fail during
exactly the windows that matter. Every record goes to the spool before any
output tries to ship it.

- Append-only segment files (zstd NDJSON) or bbolt; pick whichever gives the
  simpler crash-safe design and document why.
- Persist `seq` and per-output delivery cursors.
- Bounded by age and size (defaults: 7 days, 1 GiB); drop oldest on overflow and
  count drops in a metric.
- Push output drains with exponential backoff and jitter; batches by count/size
  (defaults: 500 records or 1 MiB, flush at least every 10s).
- `kill -9` at any point must lose at most the in-progress cycle.

### 5.3 Outputs

- `jsonl`: append wire records to a file or stdout. With this alone the tool is
  useful standalone.
- `http`: POST zstd NDJSON to `<server>/api/v1/ingest`. Configurable headers,
  basic auth, bearer token, client cert/key, CA bundle, timeout.
- `metrics`: Prometheus endpoint (opt-in listen address). Bounded labels only
  (`agent_id`, `target`, `protocol`). Never a hop IP as a label. Suggested:
  - `hopcorder_cycles_total{result="reached|unreached"}`
  - `hopcorder_e2e_rtt_seconds` (histogram, coarse buckets)
  - `hopcorder_path_length`
  - `hopcorder_path_changes_total`
  - `hopcorder_loops_detected_total`
  - `hopcorder_spool_bytes`, `hopcorder_spool_records_dropped_total`
  - `hopcorder_output_errors_total{output}`
  - `hopcorder_probe_late_replies_total`, `hopcorder_cycle_overruns_total`

### 5.4 Configuration

YAML file plus flags for the common cases (flags override file). Example:

```yaml
agent_id: lap-mstrassberger      # default: hostname
interval: 5s
timeout: 1s
max_ttl: 30
probe_past: 2
spool:
  dir: /var/lib/hopcorder
  max_age: 168h
  max_bytes: 1GiB
targets:
  - host: dc1.example
  - host: 198.19.80.226
    protocol: udp
    port: 33434
outputs:
  jsonl:
    path: /var/log/hopcorder.jsonl
  http:
    url: https://hopcorder.example.net
    bearer_token_file: /etc/hopcorder/token
  metrics:
    listen: 127.0.0.1:9469
```

Per-target overrides for `protocol`, `port`, `interval`, `max_ttl`. Validate the
whole config at startup and fail loudly. Reload on SIGHUP (stretch).

### 5.5 Operations

- `log/slog`, JSON or text, level configurable.
- Graceful shutdown on SIGINT/SIGTERM: finish or abandon in-flight cycles, flush
  spool, exit within a few seconds.
- `hopcorder agent --once` runs one cycle per target, prints records, exits.
  Useful for debugging and for integration tests.

## 6. Repo layout (adapt to the existing skeleton)

```
cmd/hopcorder/          main, subcommand wiring
internal/record/        wire types, path_hash, loop detection, path trimming
internal/probe/         Prober interface, raw-socket implementation, fake
internal/agent/         scheduler, target resolution, cycle assembly
internal/spool/         durable queue
internal/output/        jsonl, http, metrics
internal/config/        load, defaults, validation
internal/server/        (milestone 2)
deploy/                 systemd unit, Helm chart, docker-compose (later)
dashboards/             Grafana JSON (milestone 3)
docs/                   this file, prototype/
```

Inspect the existing skeleton (`go.mod`, layout, Makefile, CI) first and fit
into it. If it conflicts with this layout, prefer the skeleton and note it.

> **Note (M1):** the repository was empty apart from `README.md`, `LICENSE`,
> `.gitignore` and `CLAUDE.md`, so the layout above was used as-is.

### Dependencies

Keep them few. Expected: `golang.org/x/net` (icmp/ipv4/ipv6),
`github.com/prometheus/client_golang`, `github.com/klauspost/compress` (zstd), a
YAML library, optionally `go.etcd.io/bbolt` and a CLI library. Standard library
for HTTP, logging, and everything else. Ask before adding anything else.

## 7. Milestones

### M1 — Agent (this handoff)

- [x] `internal/record`: types, `path_hash`, loop detection, trimming;
      table-driven tests
- [x] `internal/probe`: interface + fake; ICMP v4/v6 raw implementation with
      reply demux; UDP; paris mode (TCP SYN deferred to M2; IPv6 verified by
      golden bytes and the in-memory network only, see §10)
- [x] `internal/agent`: per-target scheduler, stagger, DNS refresh, overrun
      handling
- [x] `internal/spool`: durable, bounded, crash-safe; tests incl. simulated
      crash
- [x] `internal/output`: jsonl, http (with auth options + backoff), metrics
- [x] `internal/config`: YAML + flags + validation (SIGHUP reload not done)
- [x] `hopcorder agent` and `hopcorder agent --once`
- [x] README: install, `setcap`, config reference, the rate-limiting/ECMP
      caveats
- [x] systemd unit in `deploy/`

**M1 done when:** against a real target, `hopcorder agent --once` produces
records matching the schema above; a 24h run against a few targets shows no
goroutine or memory growth; killing the network mid-run and restoring it results
in all spooled records being delivered to a mock ingest endpoint with no
duplicates after dedup on `(agent_id, seq)`.

### M2 — Server

HTTP ingest with zstd NDJSON, dedup on `(agent_id, seq)`, flatten to the per-hop
ClickHouse table (see `docs/prototype/schema.sql`, extended with `agent_id`,
`seq`, `af`, `protocol`, `flow_id`, `hop_asn`, `hop_as_name`). Enrichment: rDNS
(cached) and ASN from a local iptoasn / GeoLite2-ASN database refreshed daily.
Materialized views for path-change events, loop events, and reachability-loss
windows. Read API for the future UI. Health and readiness endpoints. Migrations
applied on startup.

### M3 — Grafana

Provisioned dashboards using the ClickHouse datasource: reachability timeline,
per-hop latency/loss heatmap, last-responding-hop during failures, path-change
log, loop log, two-target correlation, real-loss vs rate-limited classifier.
Queries are in `docs/prototype/schema.sql`. Plus `deploy/docker-compose.yml`
(ClickHouse + server + Grafana) for a five-minute try-out.

### M4 — Embedded UI

Vue 3 + TypeScript + Vite, embedded via `embed.FS`, served under `/ui/`. Main
view: path topology over time (hop graph with time scrubber, highlighting path
changes and loops). ECharts or Cytoscape.js.

## 8. Testing

- Everything above `internal/probe` is tested against the fake prober. The fake
  must be able to simulate: rate-limited hops, ECMP alternation, loops, a path
  change at time T, a full black hole, and late replies.
- Raw-socket tests use the build tag `rawsock` and are skipped unless
  `CAP_NET_RAW` is available. They probe `127.0.0.1` / `::1` and, where
  possible, a network namespace with a few routed hops (veth + `ip netns`) to
  test real TTL expiry. Never require root or sudo for `go test ./...`.
- Packet encoding and quoted-packet parsing get golden-byte unit tests,
  including malformed and truncated ICMP payloads.
- CI: `go vet`, `golangci-lint`, `go test -race ./...`.

## 9. Working agreements

- Work milestone by milestone; within M1, land `record` → `probe` (fake first)
  → `agent` + `jsonl` → `spool` → `http` → `metrics`. Each step leaves the
  binary building and tests green.
- Small, reviewable commits with messages that explain why.
- Update this file when a decision changes, in the same commit.
- If something in this document is ambiguous or seems wrong, stop and ask
  instead of guessing.

## 10. Implementation decisions (M1)

Decisions taken while implementing M1 where this document left room. Each was
added in the commit that introduced it.

- **Go version**: `go 1.27.0` in `go.mod` (latest stable at the time; current
  `golang.org/x/net` already requires 1.26).
- **CLI**: standard library `flag` with a hand-rolled subcommand switch; no CLI
  library.
- **Path end on `unreachable`** (extends §3.2): the path also ends at the lowest
  TTL that got ICMP destination-unreachable. That hop is kept, higher TTLs are
  dropped. Without this, a router answering "host unreachable" for every TTL
  beyond it would count as a responding hop at every TTL and defeat the
  `probe_past` trimming. Within one paris cycle every higher-TTL probe crosses
  the same router, so nothing beyond it is observable anyway.
- **`path_len`** is the number of hops in the trimmed `hops` array, which is
  always contiguous from TTL 1. For reached paths that is the destination's
  distance; for unreached ones it includes the `probe_past` silent hops.
- **Loops count only `ttl-expired` answers** (narrows §3.2, which says "any IP
  that responded at more than one TTL"). Found on the first real trace: a
  filtering router that expires TTL k and rejects TTL k+1 with an unreachable
  shows up at two TTLs, and the literal rule logs it as a loop. Only time
  exceeded proves the router forwarded the packet and saw it again. A
  destination that also appears one hop earlier (e.g. a firewall answering
  with the target's address) is likewise excluded.
- **`loops`** and **`hops`** are always present (`[]` when empty), never `null`.
- **`rtt_us`** is at least 1 for responding hops, so `omitempty` only ever drops
  it on `no-reply`. **`code`** is present exactly on `unreachable` hops (as a
  raw ICMPv4 or ICMPv6 code, interpreted per `af`).
- **`ts`** is the cycle start (first probe sent), UTC, truncated to
  milliseconds.
- **Probe identification** (§4.2/§4.4): every probe carries a 16-bit probe id
  from one process-wide counter (1..0xfffe). ICMP puts it in the echo sequence
  number; in paris mode the first payload word is `^seq`, which keeps the ICMP
  checksum constant. UDP makes the probe id the **UDP checksum** (paris
  technique): the first payload word is chosen so the checksum comes out to the
  id. This works identically for IPv4 and IPv6 (IPv6 has no IP ID) and the
  checksum is in the 8 quoted bytes. Demux key: (dst, proto, icmp id, seq) or
  (dst, udp, sport, dport, checksum).
- **`flow_id`** is the ICMP identifier (icmp) or UDP source port (udp),
  default 33434, constant across TTLs *and* cycles so paris keeps a target on
  one ECMP path over time; per-target configurable. Classic mode
  (`paris: false`) varies the ICMP checksum or walks the UDP destination port.
- **Sockets** (Linux only; other OSes get a stub that fails clearly): IPv4
  sends through one `IPPROTO_RAW` (`IP_HDRINCL`) socket, so TTL and IP ID are
  per packet with no `setsockopt` races between targets; one ICMP socket
  receives. IPv6 uses one ICMPv6 socket for echo send + all receive and a
  send-only raw UDP socket (drop-all BPF), with hop limit and source passed
  as ancillary data. Sockets open lazily per family; `Check()` opens IPv4
  eagerly at startup to fail fast without `CAP_NET_RAW`.
- **Timing**: a reply counts only if it arrives within `timeout` of its own
  probe; a cycle ends when every probe is answered or at last-send + `timeout`
  + 20ms grace. Keys of unanswered probes are kept as tombstones for 30s, so
  later replies count as late instead of being mistaken for foreign traffic.
  No goroutine per probe or per trace: one receive loop per socket.
- **No route** (`ErrNoRoute`, e.g. uplink down) is not a skipped cycle: the
  agent records a cycle in which nothing responded, so an outage is visible as
  data rather than a gap indistinguishable from the agent being down.
- **Destination answering for UDP** = ICMP port unreachable from the target
  address. Unreachable from any other address, or with another code, is
  `unreachable`.
- **Probe ids are counted per flow** (dst, protocol, flow id) from a random
  start, so a busy agent never reuses an id within the 30s tombstone window.
  Two hopcorder *processes* on one host (e.g. `--once` next to the daemon)
  both see every ICMP reply; with the same target and `flow_id` a reply can,
  rarely, be attributed to the other process's probe. Give such runs a
  different `flow_id`.
- **Known limitations**: RTTs use user-space receive timestamps (tens of µs of
  scheduling noise; kernel `SO_TIMESTAMPNS` is a possible refinement). MPLS
  label extensions (RFC 4950) are not parsed. All TTLs up to `max_ttl` are
  probed every cycle; TTLs beyond the destination each draw a reply from it.
- **Testing**: the demux engine sits behind a small socket interface and is
  tested in the default run against an in-memory network that answers with
  real ICMP bytes. The `rawsock` tests build a four-namespace routed topology
  (veth + `ip netns`) for real TTL expiry, plus a real two-router loop.
- **Spool = bbolt** (§5.2 left the choice open). Why: appending a record,
  bumping `seq` and enforcing the bounds is one ACID, fsynced transaction, and
  so is advancing a cursor and deleting what every output has delivered. A
  crash leaves the old or the new state; there is no torn-tail detection,
  segment index or cursor file to keep consistent, which segment files would
  need. Layout: `meta` (next seq, byte and record counters), `records` (8-byte
  big-endian seq → 8-byte timestamp + zstd-compressed JSON, compressed per
  record), `cursors` (output name → last delivered seq). Trade-off: a bbolt
  file never shrinks; it reuses freed pages, so disk usage tracks the high-water
  mark of `max_bytes` (which bounds the stored compressed bytes) plus B+tree
  overhead.
- **Spool consumers**: every shipping output (`jsonl`, `http`) is a spool
  consumer with its own cursor and drain loop, so `jsonl` is crash-safe too
  (it may repeat lines after a crash; dedup on `(agent_id, seq)`). Metrics are
  not a consumer; they observe cycles in memory. Records are deleted once every
  consumer has them; with no consumer they are not stored at all. A newly
  configured output starts at the newest record; the cursor of an output
  removed from the config is deleted. Records dropped by the bounds count as
  dropped only if some output had not delivered them.
- **Batching** (§5.2): a partial batch waits until its oldest record is
  `flush_interval` old (or a full batch is pending), then ships everything
  accumulated. Backoff doubles from `retry_min` to `retry_max`, each wait
  jittered to 50–100%.
- **`--once`** does not open the spool (so it can run next to a running agent)
  and prints records with `seq: 0`; the spool assigns seqs from 1.
- **Shutdown**: schedulers stop first (in-flight cycles are abandoned), then
  outputs get up to 3s to drain what is spooled; whatever is left stays in the
  spool for the next start.
- **HTTP output**: any non-2xx is retried (the M2 server must answer 2xx for
  records it already has, since dedup is its job). `bearer_token_file` and
  `basic_auth.password_file` are re-read on every request, so secrets rotate
  without a restart; `tls.ca_file` replaces the system roots; client
  certificates are loaded at startup. A `Host` entry in `headers` sets the
  request's Host.
- **Metrics** beyond §5.3's list: `hopcorder_cycle_errors_total`,
  `hopcorder_resolve_errors_total`, `hopcorder_output_pending_records{output}`,
  `hopcorder_spool_records`, `hopcorder_probes_sent_total`,
  `hopcorder_probe_send_errors_total`, `hopcorder_build_info`, plus the Go and
  process collectors. `agent_id` is a constant label on every hopcorder
  metric; `target` is the configured host. `hopcorder_loops_detected_total`
  counts cycles with at least one loop; `hopcorder_path_changes_total` counts
  cycles whose `path_hash` differs from the previous cycle of that target.
  Overrun and per-target error counters carry `target`/`protocol`.

### Open questions

- **`path_hash` and flickering hops.** §3.2 says the `*` placeholder means "a
  rate-limited router flickering in and out must not register as a path
  change", but with the specified algorithm a hop that answers in cycle N and
  is silent in cycle N+1 *does* change the hash (`…|10.0.0.2|…` vs `…|*|…`).
  The placeholder only keeps positions aligned. M1 implements the algorithm
  exactly as written, and `hopcorder_path_changes_total` counts hash changes,
  so it also counts flicker. A flicker-proof alternative for the metric (and
  for M2's path-change view): call it a change only if some TTL answered in
  both cycles with different addresses, or a reached destination's distance
  changed. Needs a decision before M2 builds its path-change table on it.
