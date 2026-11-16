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
provider (AS1299 / twelve99). A 24h `mtr` run showed the problem but could not
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

> **Note:** the prototype files were not present when M1 and M2 were
> implemented; both were built from this document alone. `schema.sql` was
> added after M2 and the server schema reconciled with it (§10, "Prototype
> reconciliation"). `pathwatch.py` is still not in the repository.

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
  "target": "mumble.saremox.dev",
  "target_ip": "79.99.80.226",
  "af": 4,
  "protocol": "icmp",
  "flow_id": 33434,
  "reached": true,
  "path_len": 9,
  "path_hash": "ace3702aca4b1a5a",
  "loops": [{"ip": "62.115.120.1", "ttls": [6, 7]}],
  "hops": [
    {"ttl": 1, "status": "ttl-expired", "ip": "192.168.187.1", "rtt_us": 1432},
    {"ttl": 6, "status": "no-reply"},
    {"ttl": 9, "status": "reply", "ip": "79.99.80.226", "rtt_us": 26711}
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
  - host: mumble.saremox.dev
  - host: 79.99.80.226
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
deploy/                 systemd units, example configs, docker-compose, Grafana provisioning, Helm charts
dashboards/             Grafana JSON, plus a Go package that reads its queries for tests
ui/                     the web UI's source (Vue 3 + TypeScript + Vite); built into internal/ui/dist
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
      reply demux; UDP; paris mode (TCP SYN added after review, see §10; IPv6 raw sockets run
      in the CI `rawsock` job only, as the development container had no IPv6)
- [x] `internal/agent`: per-target scheduler, stagger, DNS refresh, overrun
      handling
- [x] `internal/spool`: durable, bounded, crash-safe; tests incl. simulated
      crash
- [x] `internal/output`: jsonl, http (with auth options + backoff), metrics
- [x] `internal/config`: YAML + flags + validation; SIGHUP reload (added
      after review, see §10)
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

- [x] HTTP ingest (zstd/gzip/plain NDJSON), dedup on `(agent_id, seq)`
- [x] Per-hop table, extended as listed (plus rDNS name and MPLS)
- [x] Enrichment: cached rDNS; ASN from iptoasn, refreshed daily
- [x] Loop events (materialized view); path changes and reachability-loss
      windows (derived by the server, see §10)
- [x] Read API, `/healthz`, `/readyz`, migrations on startup
- [x] Reconcile the schema with `docs/prototype/schema.sql` (migration 0002, §10)

### M3 — Grafana

Provisioned dashboards using the ClickHouse datasource: reachability timeline,
per-hop latency/loss heatmap, last-responding-hop during failures, path-change
log, loop log, two-target correlation, real-loss vs rate-limited classifier.
Queries are in `docs/prototype/schema.sql`. Plus `deploy/docker-compose.yml`
(ClickHouse + server + Grafana) for a five-minute try-out.

- [x] `dashboards/hopcorder.json`: all seven panels, plus outage windows,
      end-to-end RTT, path length and route-change annotations (§10)
- [x] Data source and dashboard provisioning in `deploy/grafana/provisioning`
- [x] `Dockerfile` and `deploy/docker-compose.yml` (ClickHouse, server, an
      agent, Grafana with the plugin)
- [x] Every dashboard query tested against ClickHouse (`clickhouse` tag) and
      through Grafana and the real plugin (`grafana` tag, CI job `grafana`)

### M4 — Embedded UI

Vue 3 + TypeScript + Vite, embedded via `embed.FS`, served under `/ui/`. Main
view: path topology over time (hop graph with time scrubber, highlighting path
changes and loops). ECharts or Cytoscape.js.

- [x] `/api/v1/query/topology` (nodes, edges, timeline per stream) and
      `order=asc` on `cycles` for stepping
- [x] `ui/`: stream list; stream view with the reachability timeline and
      scrubber, the hop graph (ECharts), the selected cycle, route changes,
      outages and loops
- [x] Embedded from `internal/ui/dist` and served under `/ui/` with a CSP,
      gzip and caching; `ui: false` turns it off
- [x] Unit tests (Go and Vitest), a ClickHouse test of the aggregation, and
      browser tests (Playwright) against a real server in CI (`ui` job)

### Packaging (after M4)

Asked for alongside M4: container images and Helm charts for the agent and
the server.

- [x] `Dockerfile` targets `agent` and `server` (static, on scratch,
      amd64 and arm64), published to `ghcr.io/saremox/hopcorder-{agent,server}`
      by `.github/workflows/images.yml`
- [x] Helm charts `deploy/helm/hopcorder-server` and
      `deploy/helm/hopcorder-agent` (StatefulSet or DaemonSet), pushed to
      `oci://ghcr.io/saremox/charts` on release tags
- [x] CI job `helm`: strict lint, then both charts (the agent in both modes)
      installed into kind with images from the commit, records checked

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
- **TCP SYN probing** (§4.3, done after M1 review): a SYN with an MSS option
  to `port` (default 443), source port `flow_id`. The probe id is the top 16
  bits of the sequence number (ECMP does not hash it; ICMP errors quote it;
  SYN-ACK/RST acknowledge seq+1). Paris keeps both ports fixed; classic mode
  walks the source port, since the destination port is the service being
  reached. The destination's SYN-ACK or RST is `reply`, as is an ICMP port
  unreachable from the destination itself (an iptables REJECT). Replies arrive
  as TCP, so each family gets a raw TCP socket on first use, with a kernel BPF
  filter passing only ACK+SYN or ACK+RST segments; on IPv6 that socket also
  sends the SYNs. The kernel answers the SYN-ACK with an RST, so no
  connection is ever established.
- **Probe ids are counted per flow** (dst, protocol, flow id) from a random
  start, so a busy agent never reuses an id within the 30s tombstone window.
  Two hopcorder *processes* on one host (e.g. `--once` next to the daemon)
  both see every ICMP reply; with the same target and `flow_id` a reply can,
  rarely, be attributed to the other process's probe. Give such runs a
  different `flow_id`.
- **Adaptive max TTL** (`adaptive_max_ttl`, default on; added after M1
  review). Once a target was reached at distance d, cycles probe TTLs
  1..d+3. If that batch neither reaches the destination nor hits an
  unreachable, the same cycle probes d+4..`max_ttl` before the record is
  built, so records never differ from full-depth probing; only the replies
  the destination would send for TTLs past it are saved. Unreached targets
  always get full depth. Implemented with `TraceRequest.MinTTL`.
- **Kernel receive timestamps** (added after review): listeners enable
  `SO_TIMESTAMPNS`, and each reply's receive time is moved back by the lag
  between the kernel's stamp and the user-space read. RTTs stay on the
  monotonic clock; only the difference of two wall-clock readings taken
  microseconds apart is used, and lags outside 0..100ms (a clock step) fall
  back to the user-space time. On loopback this cut the median RTT from
  ~55µs to ~24µs (p90 104 → 35µs), i.e. about 30µs of every RTT was Go
  scheduling delay. Send times are still user-space, taken just before the
  send call.
- **MPLS label stacks** (added after review; **additive change to the locked
  wire record §3.1**, approved with the "agent polish" scope): hops may carry
  `"mpls": [{"label": 24005, "tc": 0, "s": true, "ttl": 1}, …]`, outermost
  label first, parsed from RFC 4884 extensions (RFC 4950 MPLS object) of time
  exceeded and unreachable messages. Absent when the router sent none, so
  existing consumers are unaffected and `schema` stays 1. Both layouts are
  read: the RFC 4884 length field, and the common "length 0, extension after
  a 128-byte original datagram" form, which is accepted only with a valid
  extension checksum. M2 should store the stack per hop (e.g.
  `hop_mpls Array(Tuple(label UInt32, tc UInt8, s UInt8, ttl UInt8))`); it
  shows which hops of a transit backbone sit inside an MPLS tunnel.
- **Known limitations**: all TTLs up to `max_ttl` are probed in cycles of
  unreached targets. Routers that hide MPLS hops (no TTL propagation) stay
  invisible; labels only appear for hops that answer.
- **Testing**: the demux engine sits behind a small socket interface and is
  tested in the default run against an in-memory network that answers with
  real ICMP bytes. The `rawsock` tests build a four-namespace routed topology
  (veth + `ip netns`) for real TTL expiry, plus a real two-router loop. The
  loop is made with host routes (`/32`, `/128`) on r2: an IPv6 `/64` added
  with `ip route replace` gets metric 1024 and loses to the connected `/64`.
  On IPv6, r1 answers the first pass from its agent-facing address and the
  loop legs from its r1–r2 address (Linux sources ICMPv6 errors from the
  inbound interface), so the test accepts either.
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
  accumulated. The wait is capped at `flush_interval` so a wall-clock step
  backwards cannot stall delivery, and at shutdown partial batches ship
  immediately.
- **JSONL torn writes**: a short write to the file is truncated away; if that
  is impossible (stdout), or the file ends mid-line at startup (power loss
  before fsync), the next batch starts on a new line. Readers may therefore
  see repeated records and, after a power loss, at most one unparseable
  fragment line — never a record glued to a fragment. Backoff doubles from `retry_min` to `retry_max`, each wait
  jittered to 50–100%.
- **`--once`** does not open the spool (so it can run next to a running agent)
  and prints records with `seq: 0`; the spool assigns seqs from 1.
- **SIGHUP reload** (§5.4 stretch, done after review): the file is re-read
  and the original command-line flags and positional targets are re-applied.
  Invalid: logged, nothing changes. Valid: the daemon (spool, outputs,
  metrics server, schedulers) shuts down as on SIGTERM and starts again with
  the new config; only the prober and its raw sockets survive. A full
  in-process restart was chosen over diffing configs because it applies every
  setting the same way and has one code path; the cost is a pause of about
  one shutdown (≤3s) and Prometheus counters restarting from zero, which
  `rate()` handles. If the new config fails to start, the previous one is
  started again. Every reload reopens the JSONL file, which makes
  rename-and-HUP log rotation work.
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
  route changes as decided by `record.PathTracker` (see "Decided after
  review" below), not `path_hash` changes.
  Overrun and per-target error counters carry `target`/`protocol`.

### M2 — server

- **Schema without the prototype.** `docs/prototype/schema.sql` was not in the
  repository when M2 was built, so the ClickHouse schema
  (`internal/server/migrations/0001_init.sql`) was designed from this
  document: the per-hop `hops` table carries the fields of §3.1 plus
  `agent_id`, `seq`, `af`, `protocol`, `flow_id`, `hop_asn`, `hop_as_name`,
  `hop_name` (rDNS) and `mpls`. Reconciled with the prototype afterwards, see
  the next section.
- **One insert per batch, flattening in ClickHouse.** The server inserts only
  into `cycles` (one row per cycle, hops and loops as `Nested` columns, plus
  per-hop enrichment). `hops`, `loop_events` and `agent_seq` are materialized
  views of it. So a cycle is stored atomically, and "flatten to one row per hop
  at insert time" happens at insert time inside ClickHouse.
- **No ClickHouse client library.** The server uses ClickHouse's HTTP
  interface through `internal/clickhouse` (standard library, typed
  `{name:Type}` parameters). This avoids a new dependency; the HTTP interface
  covers DDL, `JSONEachRow` inserts and queries.
- **Dedup on `(agent_id, seq)`** with a per-agent high-water mark rather than
  a deduplicating table engine. Agents ship in seq order and advance only on
  success, so "seq ≤ highest stored seq" means "already stored". Each agent's
  batches are processed one at a time under its own lock, and the mark moves
  only after the insert succeeded. A `ReplacingMergeTree` would not do: the
  per-hop view would still see every duplicate insert. The mark is restored
  from `agent_seq` after a restart. Consequence: an agent whose spool was
  wiped (seq restarting at 1) must get a new `agent_id`.
- **Path changes and outages are derived in Go, not by materialized views**
  (deviation from the M2 line above). A path change uses the stateful
  wildcard rule decided after M1 (`record.PathTracker`), and an outage window
  needs to know whether the previous cycle reached the destination; neither
  can be expressed as a per-insert view. The server keeps per-stream state
  (stream = agent, target, protocol, flow), writes `path_changes` and
  `outages` rows, and after a restart replays a stream's last 30 cycles and
  resumes an open outage. Both tables are `ReplacingMergeTree`, so a replay
  writes no duplicates (query with `FINAL`). Events are derived data: if
  storing them fails after the cycles committed, that is logged and counted,
  not retried by the agent. Loop events are a plain view.
- **Outage window** = from the first cycle that did not reach the destination
  to the first that did again (that cycle's time is `end_ts`). A routing loop
  is therefore an outage too. The row is written at open (`end_ts` NULL) and
  at close (higher version). `last_hop_ip`/`last_hop_ttl` are the last
  responding hop of the first failing cycle, which is where it broke.
- **ASN from iptoasn** rather than GeoLite2: free, no licence key, and a TSV
  that the standard library parses (GeoLite2 needs a reader library and a
  MaxMind account). Downloaded daily, cached on disk, used from the cache when
  the download fails; or an operator-managed file.
- **rDNS never blocks ingest**: cache misses return "" and are resolved by a
  worker pool, so the first sightings of a new hop have no name (hops repeat
  every cycle, so this lasts a cycle or two). Names are stored per hop row, as
  they were known at ingest.
- **Invalid records** are rejected for good (reported in the response, not
  retried); a storage failure returns 503 so the agent retries the batch.
  `seq: 0` (from `--once`) is rejected.
- **Startup order**: the HTTP server starts at once and migrations retry every
  2s until ClickHouse answers; until then `/readyz` and ingest return 503 and
  agents keep spooling.
- **Testing**: the default run uses an in-memory store that answers the
  ingester's queries; the `clickhouse` build tag runs migrations, every query,
  a retry and a restart against a real ClickHouse (25.8), plus the full agent
  daemon → server → ClickHouse path. CI runs these with ClickHouse as a
  service container.

### Prototype reconciliation (after M2)

`docs/prototype/schema.sql` arrived after M2 and is kept verbatim. Migration
`0002_prototype_compat.sql` reconciles with it:

- **`path_probes` view** with the prototype's column names over `hops`
  (`src` = `agent_id`, `cycle` = `seq`; hopcorder's extra columns appended),
  so the prototype's seven analysis queries run against hopcorder data with
  only the database name changed. `TestPrototypeQueriesRun` (clickhouse tag)
  runs all seven, read straight from the file.
- **`is_loop` / `loop_ttls` on `hops`**: the prototype keeps loops on the
  per-hop rows; `hops_mv` now fills both from the cycle's loops. Rows stored
  before the migration keep the defaults (`loop_events` has every loop).
- **Retention**: the prototype expires data after 90 days.
  `clickhouse.retention_days` (default 90, 0 = forever) sets a TTL on every
  data table on each start (`agent_seq` never expires; it guards dedup).
- **The prototype's Q3 is broken on current ClickHouse**, even against its
  own `netpath.path_probes`: `min(ts) AS ts` is visible in the same
  subquery's `WHERE ts >= …`, giving `ILLEGAL_AGGREGATION` (with the old and
  the new analyzer, 25.8). Renaming the alias (`min(ts) AS first_ts`, and
  `SELECT first_ts AS ts, …` one level up) fixes it; `neighbor()` also needs
  `allow_deprecated_error_prone_window_functions = 1`. Q3 also detects
  changes by `path_hash`, so it counts rate-limit flicker; the dashboards use
  `path_changes` instead (decided rule, below).
- **Differences that stay**:
  - Status values: hopcorder has `unreachable` (with a code) where the
    prototype had mtr-packet's `no-route` / `network-down`. hopcorder records
    a local no-route as a cycle of `no-reply` hops (§10, M1).
  - Partitioning: monthly instead of the prototype's daily, with
    `ORDER BY (agent_id, target, protocol, ts, ttl)` instead of
    `(target, ttl, ts)`. Neither can be changed without rebuilding the tables,
    and both serve the queries.
  - The prototype's queries filter on `target` only. With several agents or
    protocols per target, add `src` and `protocol` filters.

### Decided after review (2026-11-16)

- **Path change = wildcard comparison** (resolves the contradiction in §3.2:
  the `*` placeholder is meant to keep a flickering rate-limited router from
  counting as a route change, but with the specified algorithm such a hop
  *does* change `path_hash`). `path_hash` stays in the wire format exactly as
  specified. Route changes are decided by `record.PathTracker` instead: it
  remembers the last address seen at each TTL, treats silent hops as
  wildcards, and reports a change when a TTL answers from a different
  address, a reached destination is at a different distance, or the target
  address changed. Gaining or losing reachability alone is not a path change.
  `hopcorder_path_changes_total` uses it; **M2's path-change table must use
  the same algorithm** per `(agent_id, target, protocol, flow_id)` stream
  rather than comparing `path_hash`.
- **The three deviations above are approved**: loops count only `ttl-expired`
  answers; an ICMP unreachable ends the path; `--once` prints `seq: 0` and
  does not open the spool.
- **The 24h run** from the M1 done criteria is skipped for now; the
  3-minute accelerated soak (4,645 cycles, flat goroutines/heap/RSS) stands
  in. Revisit if growth shows up in production.


### M3 — Grafana

- **Plugin**: the official `grafana-clickhouse-datasource`, over ClickHouse's
  HTTP interface (port 8123, like the server; no native port needed). The
  compose stack pins Grafana 12.2.1 and plugin 4.23.5. Queries are raw SQL
  (`editorType: sql`), formats 0 (time series) and 1 (table) as the plugin
  defines them.
- **One dashboard**, `hopcorder-overview`, in three rows following the
  prototype's questions. Variables: `datasource`, `agent`, `target`,
  `protocol`, and `target_b` (the second target to compare, defaulting to
  one other than `target`). Values go into SQL as `${name:sqlstring}`
  (quoted and escaped by Grafana), never inside hand-written quotes.
- **Prototype queries → panels**:
  - Q1 reachability: from `cycles` (one row per cycle, so no `ttl = 1`
    trick), one series per target and protocol. Next to it the server's
    outage windows (`outages FINAL`).
  - Q7 correlation: from `cycles`, `${target}` against `${target_b}`.
  - Q2 last responding hop: from `hops` for the selected stream, as a
    stacked time series and a table, with the hop's AS name; a cycle in
    which nothing answered shows as "no hop answered".
  - Q5 real loss vs rate limiting: from `hops`, the prototype's thresholds
    (own loss < 5 % is clean; real loss if end-to-end loss > half the own
    loss).
  - Q6 per-hop heatmap: two heatmaps (no-reply rate, median RTT) with one
    row per TTL, labelled `TTL nn` rather than by address, because the
    address at a TTL changes with the route. Plus end-to-end RTT (p50, p95).
  - Q3 route changes: `path_changes FINAL` (the wildcard rule, see "Decided
    after review") instead of `path_hash` transitions, with the changed hops
    and the full path before and after, rebuilt from `cycles` by seq. The
    same events are a dashboard annotation.
  - Q4 loops: `hops` with `is_loop`, first TTL only, as in the prototype.
  - Added: path length per stream.
- **The JSON is the source of truth.** It is provisioned read-only
  (`allowUiUpdates: false`); to change it, edit a copy in Grafana, export it
  and replace the file. The data source provisioning reads
  `HOPCORDER_CLICKHOUSE_{HOST,PORT,DATABASE,USER,PASSWORD}` from Grafana's
  environment.
- **Images**: built from the Dockerfile's `agent` and `server` targets
  (originally one image for both commands; split for the published images,
  see "Packaging: images and Helm charts"). In compose the agent runs as
  root with every capability but `NET_RAW` dropped, and with the host's
  network so paths are not traced through Docker's NAT.
- **Compose**: every published port on localhost only; ClickHouse's
  passwordless default user. No `ulimits` for ClickHouse: raising the file
  limit fails on hosts that do not allow it (rootless Docker, this
  development container), and ClickHouse runs without it.
- **Tests**: `go test ./dashboards/` checks the JSON (data sources, grid,
  every variable defined, no hand-quoted variables) and that provisioning
  and compose agree. `TestDashboardQueriesRun` (`clickhouse` tag) runs every
  panel, variable and annotation query with the plugin's macro expansions
  reproduced from its source (`$__timeFilter(c)` becomes
  `c >= toDateTime(from) AND c <= toDateTime(to)`, `$__timeInterval(c)`
  becomes `toStartOfInterval(toDateTime(c), INTERVAL n second)`). The CI job
  `grafana` builds and starts the compose stack, sends records through the
  server's ingest API, and runs every query through Grafana's
  `/api/ds/query` with the real plugin (`grafana` tag).
- **Verified in CI only**: the development container cannot install the
  plugin (grafana.com is unreachable), so Grafana's side is checked by the
  `grafana` job alone. Its first run installed plugin 4.23.5, passed the data
  source health check, and all 17 queries returned rows. The rest of the
  stack also ran in the development container: provisioning loaded, and the
  agent traced and shipped through the server to ClickHouse. How the panels
  look has not been reviewed by a person yet.

### M4 — Embedded UI

- **ECharts, not Cytoscape.js**: one library draws both the hop graph and
  the timeline (Cytoscape.js would need a second one for the timeline).
  Imported per chart type, so only the graph, bar and line charts and their
  components are bundled (about 670 KB, 230 KB gzipped).
- **Dependencies** (the npm side of "ask before adding dependencies"; the
  handoff named Vue 3, TypeScript, Vite and ECharts): runtime `vue` and
  `echarts`; build and test `vite`, `@vitejs/plugin-vue`, `typescript` (5.9:
  `vue-tsc` does not run on the native TypeScript 7 yet), `vue-tsc`,
  `vitest`, `@playwright/test`. Versions are pinned exactly, with the
  lockfile committed. No router or state library: the view state is a few
  fields kept in the URL hash.
- **The build is committed** (`internal/ui/dist`), so `go build` and
  `go install` need no Node.js. CI rebuilds it and fails if it differs from
  the committed one.
- **Hash routing and relative URLs**: assets load relative to `index.html`
  and the API is `../api/v1/query/` from the UI, so the UI works under any
  reverse-proxy prefix and the server needs no SPA fallback. `/` and `/ui`
  redirect to `ui/` with a relative `Location` for the same reason.
- **Serving**: files are read from the embedded build at startup; hashed
  assets are cached for a year, `index.html` is revalidated by ETag; text
  files are gzipped once at startup. Every response carries a CSP that only
  allows the UI's own scripts and API; inline styles are allowed because
  ECharts styles its tooltips inline. Tooltip text is HTML-escaped (hop
  names come from reverse DNS, AS names from a downloaded file).
- **Topology API**: nodes are (TTL, address) pairs that answered, with
  cycles, median RTT, AS, name and whether they were seen in a loop; edges
  join consecutive answering hops of a cycle, so an edge spans the silent
  TTLs between (drawn dashed). Silent hops are not nodes: a rate-limited
  router would otherwise add a `*` node per TTL. The timeline has at most
  400 buckets of round sizes (5 s to 1 day). At most 2000 nodes and 5000
  edges, busiest first, with `truncated: true` beyond.
- **Layout**: one column per TTL; within a column, nodes are ordered by the
  cycle-weighted mean position of their predecessors (barycenter), so routes
  run straight where they can, and by traffic where there is no predecessor.
  Node size and edge width follow the share of cycles; colour is the origin
  AS (the seven busiest ASes get their own colour).
- **Selected moment**: the newest cycle at or before it (`cycles?to=…&
  limit=1`); without one, the UI follows the latest cycle and refreshes
  relative ranges every 30 s.
- **Found by the browser tests**: the timeline query failed for a bucket in
  which no cycle reached the destination (`toUInt32` of a NaN median); the
  medians now default to 0 there, and the ClickHouse test covers it.
- **Not done**: on phone screens the graph's labels crowd (zoom works);
  there is no per-address history view yet.

### Packaging: images and Helm charts

- **Two images from one Dockerfile** (`--target agent`, `--target server`,
  the default), so each has the right user, entrypoint and data directory.
  Static Go binaries on `scratch` with CA certificates only, about 18 MB.
  The build stage runs on the build platform and cross-compiles
  (`GOOS`/`GOARCH` from `TARGETOS`/`TARGETARCH`), so arm64 images need no
  emulation.
- **The agent image runs as root.** Raw sockets need `CAP_NET_RAW`;
  container runtimes give capabilities only to root, and Kubernetes has no
  ambient capabilities, while a file capability on the binary would be
  ignored under `allowPrivilegeEscalation: false`. So the agent runs as uid 0
  with every capability but `NET_RAW` dropped, a read-only root file system
  and no privilege escalation. That needs Pod Security `baseline`; the server
  runs as uid 65532 and fits `restricted`.
- **Publishing** (`images.yml`): pull requests build both images without
  pushing; `main` pushes `:main` and `:sha-<commit>`; `v*` tags push
  `:<version>`, `:<major>.<minor>` and `:latest`, and package the charts with
  that version and appVersion into `oci://ghcr.io/saremox/charts`. Charts
  default to their appVersion as the image tag, so a chart runs the images of
  its own release.
- **Releases**: after the images and charts of a `v*` tag are published, the
  `release` job creates the GitHub release: static Linux binaries (amd64,
  arm64; with the license, README, example configs and systemd units), the
  chart packages and `SHA256SUMS`, with the tag's section of `CHANGELOG.md`
  as the notes (the job fails without one). On pull requests and `main` the
  job builds and packages everything as a dry run. The binaries carry the
  version (`hopcorder version` prints `v<version>`).
- **Server chart**: always one replica with the `Recreate` strategy. The
  server keeps each stream's path tracker and open outage in memory
  (restored from ClickHouse on start), so two servers receiving one agent's
  batches would each derive events from half its cycles, and two ingesting
  at once could race the per-agent dedup. Scaling out would need
  stream-sticky routing first. The chart owns `listen`, the ASN cache path
  (an emptyDir, or a PVC with `persistence.enabled`) and, with
  `clickhouse.passwordSecret`, `password_file`; everything else in
  `values.config` is the server's config file as-is. No ClickHouse is
  bundled.
- **Agent chart**: `mode: statefulset` (default) gives every agent a stable
  name, used as `--agent-id`, and its own spool volume, so sequence numbers
  stay unique and streams stay continuous across restarts and rescheduling.
  A Deployment would get a new name with an empty spool on every
  reschedule. `mode: daemonset` uses the node's name as `--agent-id` and a
  hostPath spool, so an agent keeps its identity and spool on its node.
  `hostNetwork` traces from the node's network namespace. The chart owns
  `spool.dir`, `outputs.http.url` (from `server.url`) and `outputs.metrics`;
  the rest of `values.config` is the agent's config file.
- **Tested in CI** (`helm` job, kind): ClickHouse, the server chart, two
  StatefulSet agents and a hostNetwork DaemonSet agent tracing the server's
  Service over TCP; the job passes when every agent's cycles arrive and a
  TCP stream reached its destination. The development container cannot run
  kind (its cgroup v1 needs `failCgroupV1: false`, and then nested runc may
  not set `oom_score_adj`), so there the rendered manifests were validated
  with kubeconform (strict) and both images ran in Docker with the
  chart-rendered config files and the charts' security settings (read-only
  root file system, every capability dropped but the agent's `NET_RAW`, no
  privilege escalation): the agent's TCP and UDP cycles reached the server.

### Security review (after v0.1.0)

A review of the whole code base (four areas, each finding re-checked against
the code; ClickHouse-dependent ones against ClickHouse 25.8) led to four
fixes. The decisions:

**Server**
- **Records are bounded and narrowed** (`record.Validate`, shared with the
  agent's config check): `agent_id` from `[A-Za-z0-9._:-]` (at most 128
  characters), targets from `[A-Za-z0-9._:%-]` (255), protocol one of
  `icmp|udp|tcp`, at most 255 hops (TTLs and path lengths are UInt8), loops
  consistent with the hops (at most one per hop, increasing TTLs within the
  path), at most 16 MPLS labels per hop, ICMP codes 0-255, RTTs within UInt32,
  ts 1970-2262, path_hash at most 64 printable characters. Before, loops and
  hops were unbounded: one 57 KiB request held gigabytes. The character sets
  also make agent and target names safe to quote anywhere (dashboards, logs).
- **Bounded request processing**: ingest requires an NDJSON content type
  (refusing what a web page can send without a CORS preflight), at most 16
  agents and 4096 streams per request, `ingest.max_concurrent` (4) requests
  at once (others wait 10 s, then 503); zstd frames may not declare a window
  above 32 MiB (the decoder allocates it up front); defaults lowered to
  `max_body_bytes` 8 MiB and `max_decoded_bytes` 32 MiB; the HTTP server has
  read, write and idle timeouts and a 64 KiB header limit.
- **Bounded state**: a stream keeps only its previous cycle's seq, path_hash
  and path_len, not the record; idle streams and agents are dropped after
  `ingest.stream_idle_timeout` (1 h) and the least recently used beyond
  `ingest.max_streams` (100,000). A dropped stream is restored from ClickHouse
  like after a restart, a dropped agent's high-water mark is re-read.
- **No silent loss on stale batches**: a batch whose seqs all lie more than
  100,000 below the agent's stored ones gets 409 instead of 200 "duplicates",
  so the agent keeps it and its errors show the problem (a reset spool, or
  someone else using the agent_id). A jump limit on seqs was not added: it
  would not stop a spoofer who stays under it, and authentication of agents is
  the proxy's job (§2).
- **Inserts are not cancelled with the request**: if the agent disconnects
  after ClickHouse committed, the high-water mark still moves; after a failed
  insert it is re-read from storage before the next batch.
- **Read API**: at most `query.max_concurrent` (8) requests at once; every
  SELECT carries `max_execution_time` (the client timeout minus 5 s) and
  `cancel_http_readonly_queries_on_client_close=1`.
- **ClickHouse client**: parameter values are escaped for ClickHouse's
  escaped format (a trailing backslash used to fail the query, `\t` turned
  into a tab); credentials in `clickhouse.url` are refused (use `user` and
  `password`); `/readyz` no longer returns ClickHouse's error text.
- **ASN database**: at most 64 MiB downloaded and 256 MiB uncompressed (it
  is about 7 and 30 MB), 5 million ranges, AS names cut to 128 bytes without
  control characters; redirects from https to http are refused; a plain-http
  URL is warned about at startup.

