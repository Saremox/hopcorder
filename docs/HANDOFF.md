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
  Since round two of the security review also `toolchain go1.27.4`: the Go
  release that builds the binaries and the images (CI keeps the line and the
  Dockerfile's `golang:<version>` tag equal).
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
- **Probe ids are random** (since round two of the security review; they
  were counted per flow from a random start): drawn per probe, never one
  whose key is in flight, a tombstoned one only when the flow has no other
  free id. Two hopcorder *processes* on one host (e.g. `--once` next to the
  daemon) need different `flow_id`s (`--flow-id`): for ICMP the second fails
  at startup, as the first holds the echo identifier; with UDP or TCP both
  start, and a reply can, rarely, be attributed to the other process's
  probe.
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
  and prints records with `seq: 0`; a new spool starts its seqs at the clock
  (time in microseconds, since round two), an existing one keeps counting.
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
  per-hop rows; `hops_mv` now fills both from the cycle's loops (since
  migration 0003 from each hop's loop TTLs, which the server sends with the
  cycle; security review, round three). Rows stored before migration 0002
  keep the defaults (`loop_events` has every loop).
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
  compose stack pins Grafana 13.3.1 (12.2.1 until a Dependabot update, which
  CI's grafana job checked) and plugin 4.23.5. Queries are raw SQL
  (`editorType: sql`), formats 0 (time series) and 1 (table) as the plugin
  defines them.
- **One dashboard**, `hopcorder-overview`, in three rows following the
  prototype's questions. Variables: `datasource`, `agent`, `target`,
  `protocol`, and `target_b` (the second target to compare, defaulting to
  one other than `target`). Values go into SQL as
  `decodeURLComponent('${name:percentencode}')`: Grafana's `sqlstring`
  format turned out unsafe for ClickHouse (see "Security review").
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
  and no privilege escalation. That needs Pod Security `privileged` (baseline
  does not allow adding `NET_RAW`, nor hostNetwork and hostPath; corrected
  after the security review): a namespace of its own. The server runs as uid
  65532 and fits `restricted`.
- **Publishing** (`images.yml`): pull requests build both images without
  pushing; `main` pushes `:main` and `:sha-<commit>` (built with the GitHub
  Actions layer cache). A `v*` tag that passes `verify-tag` is built without
  caches and pushed as `:sha-<full commit>` only; the `publish` job, in the
  `release` environment, then adds `:<version>`, `:<major>.<minor>` and
  `:latest` (a pre-release only `:<version>`) and pushes the charts with
  that version and appVersion to `oci://ghcr.io/saremox/charts`. Charts
  default to their appVersion as the image tag, so a chart runs the images
  of its own release.
- **Releases**: `release-assets` builds the static Linux binaries (amd64,
  arm64; with the license, README, example configs and systemd units), the
  chart packages and `SHA256SUMS`, with the tag's section of `CHANGELOG.md`
  as the notes (it fails without one), using
  `scripts/release-build.sh` with go.mod's toolchain. On pull requests and
  `main` that is a dry run; for a tag the files go to `publish`, which
  attests them and both image digests and creates the GitHub release from
  them, the notes followed by the digests and how to verify them (round
  three). The binaries and images carry the
  version (`hopcorder version` prints `v<version>`).
- **Server chart**: always one replica with the `Recreate` strategy. The
  server keeps each stream's path tracker and open outage in memory
  (restored from ClickHouse on start), so two servers receiving one agent's
  batches would each derive events from half its cycles, and two ingesting
  at once could race the per-agent dedup. Scaling out would need
  stream-sticky routing first. The chart owns `listen`, the ASN cache path
  (an emptyDir, or a PVC with `persistence.enabled`), `password_file` (with
  `clickhouse.passwordSecret` or `clickhouse.deploy`) and, with
  `clickhouse.deploy`, `clickhouse.url` and `clickhouse.user`, and, unless
  `config.allowed_hosts` is set, `allowed_hosts` (round three); everything
  else in `values.config` is the server's config file as-is.
- **Bundled ClickHouse (opt-in, `clickhouse.deploy.enabled`)**, added after
  v0.1.1 for deployments where hopcorder is a monitoring aid: a one-replica
  StatefulSet of the official `clickhouse/clickhouse-server` image (25.8, the
  LTS line every test uses) with a persistent volume, in the server's chart.
  Not a third-party chart: Bitnami's moved its free images to the unpatched
  `bitnamilegacy` repository in August 2025, Altinity's is built around its
  operator, and the small community charts that render exactly this
  StatefulSet are harder to vouch for than the hundred lines it takes. No
  operator, replication or backups (snapshot the volume; `retention_days`
  bounds the data); for high availability use an operator or a managed
  service and leave it off.
  - The image's entrypoint creates the server's user from
    `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` (all databases, for the
    migrations) and removes the passwordless `default`. The password comes
    from `clickhouse.passwordSecret` or a Secret the chart generates on
    install and reads back (`lookup`) on upgrades; users defined in config
    files follow the Secret on every start, so a reinstall with a new
    password works with the old volume.
  - `config.d/hopcorder.xml`: console logging only (no log files in the
    container), caches sized for about 2 GiB (`mark_cache_size` 256 MiB,
    `uncompressed_cache_size` 128 MiB, `index_mark_cache_size` 64 MiB),
    `max_server_memory_usage_to_ram_ratio` 0.8 of the container's limit, and
    a TTL (`systemLogRetentionDays`, 7) on the system log tables that 25.8
    enables without one (query, part, trace, metric, text and error logs,
    among others), which otherwise grow for good.
  - `readOnlyUser` (for Grafana) is defined in `users.d/readonly.xml`, its
    password read from the environment (`from_env`): `GRANT SELECT` on the
    hopcorder database only and the compose stack's bounded profile
    (`readonly = 2`, time, memory and concurrency limits with `MIN 1`, the
    cancel setting and, since round three, the spill settings const).
    Checked on 25.8: it reads the tables, and writes, `system.users`,
    `url()`, `max_execution_time = 0`, changing the cancel setting and
    spilling to disk are refused.
  - Runs as the image's `clickhouse` user (uid 101, `fsGroup` 101) without
    capabilities or privilege escalation; the root file system stays
    writable because the entrypoint writes its user file into
    `/etc/clickhouse-server/users.d` at start (the chart mounts its two files
    with `subPath`, so the image's own `config.d` stays). A NetworkPolicy (on
    by default) admits only the server's pods and
    `clickhouse.deploy.networkPolicy.from`. User and database names go into
    the XML and a `GRANT`, so the chart accepts only identifiers.
- **Agent chart**: `mode: statefulset` (default) gives every agent a stable
  name, used as `--agent-id`, and its own spool volume, so sequence numbers
  stay unique and streams stay continuous across restarts and rescheduling.
  A Deployment would get a new name with an empty spool on every
  reschedule. `mode: daemonset` uses the node's name as `--agent-id` and a
  hostPath spool, so an agent keeps its identity and spool on its node.
  `hostNetwork` traces from the node's network namespace. The chart owns
  `spool.dir`, `outputs.http.url` (from `server.url`) and `outputs.metrics`;
  the rest of `values.config` is the agent's config file.
- **Tested in CI** (`helm` job, kind): the server chart with its bundled
  ClickHouse (and the read-only user), two StatefulSet agents and a
  hostNetwork DaemonSet agent tracing the server's Service over TCP; the job
  passes when every agent's cycles arrive, a TCP stream reached its
  destination, and ClickHouse's read-only user can read the cycles but not
  write. The development container cannot run
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
  is about 7 and 30 MB), 5 million ranges (2 million, 500,000 ASes and
  16 MiB of names since round three), AS names cut to 128 bytes without
  control characters; redirects from https to http are refused; a plain-http
  URL is warned about at startup.

**Dashboards and the compose stack**
- **Variables are percent-encoded**: every dashboard query takes values as
  `decodeURLComponent('${name:percentencode}')`. Grafana's `sqlstring`
  format only doubles `'`, but ClickHouse also reads `\'` as an escaped
  quote, so a value with a backslash ended the string: an agent_id like
  `!\' OR 1=1 UNION ALL SELECT name FROM system.users --` ran its SQL for
  every viewer (confirmed on ClickHouse 25.8; the `!` sorted it first in the
  agent variable). `percentencode` only emits `[A-Za-z0-9-_.~%]` (checked in
  Grafana's source), and the server's new character sets refuse such values
  anyway. `go test ./dashboards/` refuses any other variable form, and
  `TestDashboardQueriesInjection` (clickhouse tag) runs every query with that
  payload.
- **Grafana reads ClickHouse with a read-only user** in the compose stack
  (`deploy/compose/clickhouse-init.sql`): SELECT on the hopcorder database,
  `readonly = 2` (no writes; the plugin may still set query settings), and no
  privileges for table functions (`url()`, `file()`). Any viewer, anonymous
  ones included, can send Grafana SQL, so that user is the boundary.
- **ClickHouse is no longer published** on the host (`docker compose exec
  clickhouse clickhouse-client` instead), so web pages a user visits cannot
  post SQL to `127.0.0.1:8123`. The server uses its own ClickHouse user; the
  passwords are fixed, as the stack is a try-out.

**Agent**
- **The HTTP output never follows redirects** (`CheckRedirect` returns the
  3xx, which is retried and logged). Go's default would resend the batch, on
  the same host its credentials even after a downgrade to plain http, and
  custom headers to any host.
- **Plain-http ingest URLs**: `tls` settings are refused (they were loaded and
  silently unused); credentials or headers over http are warned about at
  startup but allowed (in-cluster plain http behind a mesh is common).
  `batch_max_bytes` is capped at 16 MiB, below the server's default limit.
- **Metrics endpoint**: read, write and idle timeouts, a 16 KiB header limit
  and at most 64 connections, so idle clients cannot pin goroutines and file
  descriptors the agent needs for probing.
- **MPLS**: only the first label-stack object of an ICMP extension, cut at 16
  labels (one packet carried 334 before, about 200 times a normal record).
- **IPv4-mapped sources on IPv6 sockets are dropped**: some kernels deliver
  them, and the engine picks the parser by the source address, so such a
  packet was parsed as an IPv4 reply and could inject hops into IPv4 probes.
- **Corrupt spool files are quarantined**: bbolt's consistency check runs at
  start, with faults on the memory-mapped file turned into errors. A file
  that is invalid, fails the check or faults is renamed to
  `spool.db.corrupt-<unix time>` and an empty spool starts. Its first seq is
  the current time in microseconds, above any counter the old file can have
  reached, so the server neither drops the new records as duplicates nor
  refuses them as stale. Other open errors (locked, permissions, a full disk)
  leave the file alone.

**CI and deployment**
- **Actions are pinned to commits** (with the version in a comment), the Go
  base image by digest; `.github/dependabot.yml` keeps them, the Go modules
  and the UI's npm packages current (weekly, minor and patch grouped).
- **Write tokens only where they are used**: pull requests build images and
  release assets in jobs without write permissions (`image-check`,
  `release-assets`); `packages: write` is only on the `image` job (main, and
  the `:sha-` push of a tag) and on `publish`, `contents: write` only on
  `publish`, which builds nothing (round two). No checkout keeps its
  credentials (`persist-credentials: false`). Release builds use no caches
  (no Go module or build cache, no Docker layer cache), so nothing another
  run wrote ends up in a release. The registry token is passed in an
  environment variable.
- **Releases**: a `v*` tag is checked by `scripts/release-verify.sh` (see
  round two) before anything is built, and published only by the `publish`
  job in the `release` environment (as far as this workflow goes: round
  three found that any workflow with write tokens can publish). The release
  build is
  `scripts/release-build.sh`, shared with the dry run.
- **When workflows run**: `ci` runs on pull requests, on main and by hand,
  not on every branch push (a pull request's branch ran everything twice,
  once per event) and not on tags (a release tag must be a commit of main
  whose ci run succeeded, which `verify-tag` checks). A new commit on a pull request cancels the older runs of both
  workflows. `images` runs one at a time per ref: two merges in a row would
  otherwise race to push, and whichever finished last would own `:main`. A
  run still waiting is replaced by a newer one, so that commit gets no
  `:sha-` image. Every job has a timeout. `go vet` runs inside golangci-lint
  (with all build tags), not as a step of its own. The images are still built
  by three pull request jobs (the compose stack for Grafana and the kind test
  for amd64, `image-check` for both platforms). They run in parallel, and
  sharing one build would make two of them wait for it.
- **Still to do in the repository settings** (not possible from here):
  enforce the main ruleset (it is disabled), add a tag ruleset limiting who
  may create `v*` tags, and add required reviewers to the `release`
  environment.
- **Exposure in the docs**: the server's `docker run` example and the
  development ClickHouse publish on 127.0.0.1 only (the ClickHouse one had
  host networking with a passwordless default user); since round two the
  development ClickHouse also needs a password).
- **Helm**: the server chart gets an optional NetworkPolicy (off by default;
  enabling it requires `networkPolicy.from`, round two);
  neither chart mounts a service-account token; the agent chart takes basic
  auth and TLS material from Secrets and refuses inline credentials (they
  would land in a ConfigMap, readable with the built-in `view` role); its
  metrics bind address is configurable (with hostNetwork, `0.0.0.0` is every
  node address).
- **systemd**: the units run as a dynamic user, which can only read
  world-readable files in `/etc/hopcorder`; secrets go in as credentials
  (`LoadCredential=`, `/run/credentials/<unit>/...`), as the units and
  example configs now describe.

### Security review, round two

A second review of the whole code base (the same four areas; every finding
re-checked against the code, ClickHouse-dependent ones against ClickHouse
25.8) found that three round-one fixes did not fully hold (query
cancellation, spool quarantine, the record bounds) and that the server chart
had brought back the large ingest limits. Four fixes followed. The
decisions:

**Read side, enrichment and the browser**
- **Read queries run with `readonly = 2`.** Round one's
  `cancel_http_readonly_queries_on_client_close = 1` did nothing: ClickHouse
  applies it only to queries with `readonly > 0`, and a POST does not get
  that by default. So a query whose reader had disconnected kept running
  until `max_execution_time`, while its `query.max_concurrent` slot was
  already free. 30 topology requests, each abandoned after 0.4 s, left 27
  queries using 13.8 GiB running at once with a limit of 8 (ClickHouse 25.8).
  With `readonly = 2` (URL settings still allowed) they stop within a second.
  All of the server's `Query` calls are SELECTs; `Exec` and inserts are
  unchanged.
- **Read-API responses are streamed and bounded**: `query.max_response_bytes`
  (32 MiB) of rows per response. Rows that do not fit are left out, and every
  response now ends with `"truncated": true|false` (topology already had
  it). Before, every row was held and then encoded twice into memory: one
  `cycles?limit=10000` over valid 255-hop records with 16 MPLS labels each
  returned 2.07 GB and took the server from 18 MB to 7.0 GB RSS; with the
  limit (and the cycles cap below), three such requests at once peak at
  33 MB. Row lists are written every 64 KiB. A failure before the first
  write still gets 502; a failure after it aborts the response
  (`http.ErrAbortHandler`) rather than ending the JSON cleanly. Topology is
  kept whole until its three queries are done (at most the budget), so a
  failure in any of them is a clean 502; its timeline comes first, and the
  parts after the one that filled the budget stay empty.
- **ClickHouse-side limits for read queries only** (through a context
  setting, so the ingester's queries are unchanged): `max_memory_usage` =
  `query.max_memory_bytes` (1 GiB), and `max_result_bytes` at twice the
  response budget with `result_overflow_mode = break`. The latter is only a
  backstop: it counts in-memory columns, not JSON (an MPLS label is 7 bytes
  there and 44 in JSON), and stops between blocks, so the Go budget is what
  truncates.
- **`cycles` takes `limit` up to 1000** (other endpoints keep 10000).
  ClickHouse formats such a result as one block: 3000 maximal cycles needed
  1.64 GiB in ClickHouse and failed the 1 GiB limit, 1000 need about
  650 MiB. The UI asks for one cycle at a time.
- **Rows passed to `Query` callbacks are copies**: `bufio.Scanner` reuses
  its buffer, and the read API kept rows across callbacks. A result above
  the scanner's first 64 KiB came back with earlier rows overwritten.
- **Host check against DNS rebinding** (`allowed_hosts`, `--allowed-hosts`).
  A web page whose own name resolves to 127.0.0.1 could read every path and
  post records (same-origin for the browser, so the ingest content-type rule
  does not apply). The server now answers only the listed Host names (any
  port), others get 421 (`hopcorder_server_host_refused_total`, a warning at
  most once a minute). Default without the option: on a loopback listen
  address, `localhost`, `127.0.0.1`, `::1` and the listen host; on any other
  address no check, because the server is reached under service names, pod
  IPs and ingress hosts it cannot know (Helm, containers; the server chart
  sets the list since round three). `"*"` turns it off. `/healthz`, `/readyz` and `/metrics` answer every host (probes and
  scrapers use addresses). A proxy on the same host that passes the client's
  Host on (Caddy, Traefik; nginx sends the upstream's by default) needs its
  public names listed. The compose stack's server listens on 0.0.0.0 in its
  container, so its config lists `localhost` and `127.0.0.1`.
- **Grafana in compose answers only `localhost`**
  (`GF_SERVER_DOMAIN=localhost`, `GF_SERVER_ENFORCE_DOMAIN=true`): other
  Host names get a 301 to localhost, which a rebinding page cannot follow.
  Before, anonymous data-source queries worked under any Host, and so did
  the API with the default `admin:admin`, which could add a data source
  with the stack's fixed ClickHouse credentials (full SQL). The admin
  password is `HOPCORDER_GRAFANA_ADMIN_PASSWORD` (default `admin`), applied
  when Grafana's volume is created. The CI `grafana` job uses
  `http://localhost:3000`.
- **Grafana's ClickHouse user has bounded queries** (settings profile
  `grafana_limits` in `deploy/compose/clickhouse-init.sql`).
  `readonly = 2` lets a query set any setting, so an anonymous viewer could
  run `SELECT sum(number) FROM numbers(1e15) SETTINGS max_execution_time = 0`
  (`numbers()` needs no privilege), and it kept running after the client
  gave up. Now: `max_execution_time` 60 s (at most 300), `max_memory_usage`
  1 GB, `max_memory_usage_for_user` 2 GB, `max_concurrent_queries_for_user`
  16, `cancel_http_readonly_queries_on_client_close = 1` CONST. The
  constraints need `MIN 1`: 0 means unlimited and passes any `MAX`. A query
  may lower these, not raise them, and cannot switch profiles. The
  plugin's own query timeout must stay at or below 300 s. The script runs on
  the first start of an empty volume and can be re-run on an older one
  (every statement is `IF NOT EXISTS`; since round three the profile is
  `OR REPLACE`, so a re-run also updates it).
- **ASN names are copied out of their line** (`strings.Clone`): a name was
  a substring of the line it came from, so a small, compressible download
  with 60 KB lines kept up to the whole 256 MiB it inflated to (measured:
  500 such lines kept 32 MB; with the copy, under 1 MB). Since round three
  the copy goes into the database's one string of names, and
  `TestParseDoesNotPinLines` still holds.
- **Checked and unchanged**: the UI escapes everything it puts into
  tooltips (rendered in Chromium with HTML in AS names and in IPv6 zones,
  `{b}` and `$'` template tokens included; nothing was injected, no CSP
  violations), UI serving (map lookup, no traversal, relative redirects),
  the dashboards' `percentencode`, read-API parameter escaping, and rDNS
  (Go's resolver drops PTR names that are not host names; bounded cache and
  queue).

**Ingest and storage**
- **IPv6 zones are bounded** (`record.Validate`, `record.ValidTarget`).
  netip accepts any text after `%`, so `target_ip`, hop and loop addresses
  could carry a megabyte of anything (quotes, markup, newlines): 30 zstd
  requests totalling 126 KiB made the server hold 953 MiB of stream state,
  and the text was stored and served by the read API. Hop and loop
  addresses may not have a zone at all (routers' addresses never do; the
  agent strips them). `target_ip` keeps a zone of at most 15 characters from
  `[A-Za-z0-9._-]` (an interface name or index), because a link-local target
  such as `fe80::1%eth0` is legitimate; `ValidTarget` applies the same rule
  to address-literal targets, so the agent's config check refuses what the
  server would. Hop and loop addresses must also match the record's `af`
  (IPv4-mapped ones are not IPv6).
- **A record's `ts` must lie within [now − `ingest.max_record_age`, now +
  1 day]** (default 720h; the agent's spool keeps records for 7 days).
  Outside, the record is rejected like any invalid one. Before, any ts from
  1970 to 2262 was accepted, and tables are partitioned by month: 28
  requests (6 s) created 2290 partitions in `cycles`, permanent because the
  TTL's `toDateTime(ts)` wraps past 2106, and a restore of an unrelated
  agent's stream went from 16 ms to 270 ms. An agent offline for longer
  than `max_record_age` loses its older records (raise the option if
  spools are kept longer).
- **Restores read a bounded window.** The sort key is (agent_id, target,
  protocol, ts) and flow_id and seq are not in it, so the replay query read
  the whole history of every flow of the agent, target and protocol (2M rows
  / 247 MiB per restore in the measurement; 1M rows for a single-flow
  stream). It now looks at the day before the new cycle first and, only if
  that holds none of the stream's cycles, at `max_record_age` before it
  (measured: unbounded 199k rows, 30 days 118k, one day 27k for a 60-day
  stream). The outage-cycle count is bounded by the outage's start. The
  open-outage lookup is not bounded by time: an outage can be older than
  any window, and `outages` holds a row per outage, not per cycle. A change
  across a gap longer than `max_record_age` is not reported.
- **Restores have their own budget**: at most 256 per request, within 5 s
  of its first restore, separate from the 30 s insert budget; storing a
  batch's events has its own 10 s. Before, one request with 4096 new
  streams held an ingest slot for 20 s of serial restore queries, and slow
  restores used up the insert budget so that the batch's events were lost.
- **A stream whose restore fails or does not fit the budget stays
  unloaded**: its cycles in that batch derive no events (counted in
  `hopcorder_server_unrestored_cycles_total`, one warning per batch) and it
  is restored with its next batch, from storage, which then holds them.
  Before, it started from scratch: an outage open in storage was never
  closed and a second one opened, permanently. If a restore finds the
  stream down but no open outage stored (its first cycle was in a skipped
  batch, or its event insert failed), it opens one at the first of the
  trailing unreached cycles it replayed. Consequence: if restores fail for
  good (ClickHouse broken for reads only), streams derive no events until
  they succeed; the metric and the warning show it.
- **Request bodies are read before a request takes an ingest slot**, into
  64 KiB chunks: at most 64 requests read at once (others wait 10 s, then
  503), each body within 20 s (`http.ResponseController.SetReadDeadline`;
  agents give a request 30 s; then 408), and all of them together hold at
  most the larger of 32 MiB and twice `max_body_bytes`; a request waits for
  chunks others hold rather than adding memory. Before, the slot was taken
  first, so four connections trickling bodies blocked all ingest for the
  server's 2-minute ReadTimeout. A slow client can still fill the 64
  reading slots, which needs 64 connections renewed every 20 s; a buffering
  reverse proxy (nginx's default) removes that.
- **Cycles are inserted in parts of at most 8 MiB**, each moving the
  high-water mark; if a later part fails, the retry stores only the rest,
  and the events of the stored parts are still derived. Enrichment makes a
  row up to about seven times its record (a 128-byte AS name and a 253-byte
  PTR name on every hop, which an attacker gets by putting one address with
  a PTR it controls at every TTL): one 19 KiB request (31 MiB decoded) held
  507 MiB at insert time, now 65 MiB. An agent's normal batch (1 MiB) is
  still one insert. The zstd decoder (and its window) is closed before the
  records are stored. A request being processed holds up to about three
  times `max_decoded_bytes` (corrected in round three; this said twice).
- **The Go memory limit follows the cgroup's**: at start, unless GOMEMLIMIT
  is set, the server sets it to 90% of the memory limit of its cgroup (v2:
  the lowest `memory.max` from its own cgroup up; v1:
  `memory.limit_in_bytes`); no limit, no change. Without it the heap grows
  to twice what is live before a collection, so 600 MiB of live state is
  OOM-killed in a 1 GiB container.
- **`ingest.max_streams` defaults to 20,000** (was 100,000). A stream costs
  about 1 KiB (15 IPv4 hops) to 9 KiB (255 IPv6 hops, 255-character target),
  measured; 100,000 worst-case streams were 860 MiB, more than the Helm
  chart's 1 GiB limit allows with GOGC's headroom. 20,000 hold at most about
  170 MiB.
- **`ingest.max_agents` (10,000) bounds agent states** like streams: least
  recently used first (with their streams), restored from storage (the
  high-water mark is re-read) when they come back. Before, a request could
  name 16 new agent_ids and each kept its state for an hour: 5000 requests
  left 80,000 states (850 B each) while `max_streams` was 1000. 10,000 is
  far above the agents one server serves (normally fewer agents than
  streams) and costs under 10 MiB. The sweep walks all state at most every
  minute, or every second while over a limit, so its cost is bounded by
  the two limits plus one second's growth.
- **Errors reported for rejected records are at most 200 bytes and never
  quote the record**: validation messages name fields, JSON errors are
  reduced to their offset or field (Go's messages quote the text of bad
  addresses and times). Before, a 233-byte zstd request (a 1 MiB invalid
  `status` of invalid UTF-8, each byte becoming a 3-byte U+FFFD) wrote a
  3 MiB log line, 28 MiB of log per second from one client.
- **Stale batches: the gap that stays.** The 409 needs every seq of the
  batch to lie more than 100,000 below the stored mark. An agent whose mark
  is at most 100,000 (in its first day or so) and whose spool is wiped
  had its new records dropped as duplicates until its seq passed the old
  mark, as long as a new spool started at seq 1. Closed on the agent's side
  in the same round: every new spool now starts at the current time in
  microseconds (see Agent below), which holds while the agent's clock is
  right; the server cannot tell a reset from a retry.
- **Recovering an agent_id suppressed by a huge seq.** One record with
  `seq` 2^64−1 under someone else's agent_id makes every later batch of
  that agent 409 for good (`agent_seq` never expires). Authenticating
  agents stays the proxy's job (§2); to recover, delete the spoofed cycles
  and reset the mark from what is left, then restart the server (it keeps
  the mark in memory):

  ```sql
  ALTER TABLE hopcorder.cycles DELETE WHERE agent_id = 'AGENT' AND seq >= SPOOFED SETTINGS mutations_sync = 2;
  ALTER TABLE hopcorder.agent_seq DELETE WHERE agent_id = 'AGENT' SETTINGS mutations_sync = 2;
  INSERT INTO hopcorder.agent_seq SELECT agent_id, max(seq) FROM hopcorder.cycles
      WHERE agent_id = 'AGENT' GROUP BY agent_id;
  ```

  (checked on 25.8; `hops`, `loop_events`, `path_changes` and `outages`
  rows of the spoofed seqs can be deleted the same way). Deleting only the
  `agent_seq` row would reset the mark to 0, and the agent's retries of
  stored batches would be stored again.

**CI, release and deployment**
- **Releases are published only after the `release` environment.** A
  tag's images used to be pushed as `:<version>`, `:<major>.<minor>` and
  `:latest` by the `image` job, which had no environment. Anyone who could
  push a `v*` tag could therefore publish any commit of main without
  approval, an old one before the fixes included, or replace the images of
  an existing version. Now:
  - `verify-tag` runs `scripts/release-verify.sh --wait-for-ci` before
    anything of the tag is built. It checks that the tag is
    `v<major>.<minor>.<patch>[-<pre-release>]`, points at the commit the run
    builds, and that commit is on main. It checks that the `ci` workflow
    succeeded on that commit, on main or run by hand; it waits up to 25
    minutes for a run still in progress, and a pull request's run does not
    count because it tested a merge commit. No release of the tag may exist
    yet, drafts included. The latest release must be older: its commit an
    ancestor, its version lower.
  - The `image` job then builds the tag without caches and pushes only
    `:sha-<full commit>`. That is the long form, so it never overwrites the
    `:sha-<short>` of main's build of the same commit, which used the
    cache and prints `main`. It hands its digests to `publish` as job
    outputs.
  - `release-assets` (no write token, no caches) builds the binaries, chart
    packages, `SHA256SUMS` and notes and uploads them as an artifact. On
    pull requests and main it is the dry run.
  - `publish` is the only job in the `release` environment and builds
    nothing. It runs the same checks again without waiting, because a
    release may have appeared during the approval. It then tags the image
    digests with `docker buildx imagetools create` (a pre-release gets only
    `:<version>`), pushes the chart packages that are attached to the
    release, and creates the GitHub release (`--prerelease` for
    pre-releases).
  - One approval covers all of it, and `publish` runs one at a time across
    tags, so two approved tags cannot both pass the checks.
  - A version is released once. To fix a release, release the next version
    (or delete the release, and its tag, by hand).
  - (Round three: this holds for `images.yml` only. The environment gates
    that one job, not the packages and releases, which take `GITHUB_TOKEN`
    from any workflow of the repository; anyone with write access can
    still publish with a workflow of their own. See round three.)
- **One Go release for binaries and images.**
  - `go.mod` has `toolchain go1.27.4`. The release binaries and CI used to
    build with Go 1.27.0, because setup-go installs the version from the
    `go` line when there is no toolchain line (`parseGoVersionFile`,
    checked in setup-go v7.0.0). The images built with the Go image's
    1.27.4.
  - The Dockerfile's image is now `golang:1.27.4@sha256:…`, so its tag names
    the release. ci's `test` job checks that the toolchain line, the
    Dockerfile tag and the installed Go agree. A Dependabot update of the
    image therefore needs the toolchain line in the same pull request;
    Dependabot does not update that line.
  - `scripts/release-build.sh` refuses to build with any other Go.
  - The `go` line stays `1.27.0`: it is the minimum for `go install`.
    Locally, `GOTOOLCHAIN=auto` switches to the toolchain line.
- **govulncheck** runs in ci (`vuln` job): pinned v1.9.0 via `go run`, which
  leaves `go.mod` alone, under the toolchain line's Go. It fails on known
  vulnerabilities in the modules or the standard library. Updated by hand.
- **No moving images in the build.**
  - The Dockerfile has no `# syntax=` line. That line made every build pull
    `docker/dockerfile:1` by tag and run it as the frontend; now the
    builder's own frontend is used, and it supports `RUN --mount`.
  - setup-buildx runs BuildKit pinned by digest in `driver-opts`
    (`moby/buildkit:v0.34.2@sha256:…`, until now the moving
    `buildx-stable-1`). The pin is updated by hand: Dependabot does not read
    `driver-opts`.
- **Dependabot** also covers the compose stack (`docker-compose` ecosystem,
  `/deploy`), but not ClickHouse. 25.8 is the LTS line every test uses
  (ci.yml's services, deploy/helm/ci, compose), and its tag follows that
  line's patch releases; moving to another line is a decision for all of
  them at once. The CI service images, tool versions given to actions
  (golangci-lint, helm) and govulncheck are also updated by hand.
- **Server chart: the server's ingest limits.** `values.yaml` had set
  `max_body_bytes` 64 MiB and `max_decoded_bytes` 512 MiB, written before
  the review lowered the defaults. A request holds up to about three times
  its decompressed size while it is parsed (measured: 255 MiB of NDJSON held
  507 MiB of records; up to three times for silent hops, round three). So
  one 119 KiB zstd body that expands to 512 MiB
  went past the pod's 1 GiB limit. The block is gone: the defaults apply,
  with a comment to raise the memory limit together with
  `max_decoded_bytes × max_concurrent`.
- **No credentials in ConfigMaps.**
  - The agent chart's check used to catch only a header named exactly
    `Authorization`. It now compares header names in any case and refuses
    `Authorization`, `Proxy-Authorization`, `Cookie` and any name with a
    `-`/`_`-separated part such as key, api-key, token, secret, password,
    pass, auth, credential(s), session or signature (`X-Api-Key`,
    `X-Auth-Token`, `Private-Token`, …). It also refuses credentials in
    `server.url`; the agent refuses them at startup anyway.
  - There is no Secret-backed way to set headers, because the agent has no
    header-from-file option. Header-based ingress auth therefore needs
    bearer or basic auth or mTLS instead.
  - The server chart refuses `config.clickhouse.password`; use
    `clickhouse.passwordSecret`.
  - ci's helm job checks each refusal.
- **NetworkPolicy without self-assigned labels.** The default `from` let
  in pods labelled `app.kubernetes.io/name: hopcorder-agent` in any
  namespace, a label anyone who may create a pod can set. `from` now
  defaults to `[]`, and the chart fails when the policy is enabled
  without peers. `values.yaml` has an example that selects peers by
  `kubernetes.io/metadata.name`, which Kubernetes sets and namespace users
  cannot change. ci lints the policy with `deploy/helm/ci/server-netpol-values.yaml`.
- **Secrets under systemd.**
  - The example configs used to advise inline secrets "then keep this file
    unreadable to others", and put the mTLS key in
    `/etc/hopcorder/client.key`. The shipped units run as a dynamic user,
    which reads only world-readable files, so following that advice made
    the secret readable by every local user.
  - The examples and units now take every secret, the client key included,
    as a credential (`LoadCredential=`, `/run/credentials/<unit>/<name>`).
    systemd reads credentials when the unit starts, so the unit must be
    restarted after a change.
- **Development ClickHouse with a password.** Binding to 127.0.0.1 does not
  stop a web page in the developer's browser. ClickHouse 25.8 answers a
  plain-text POST from any origin with `Access-Control-Allow-Origin: *`
  (confirmed), so a page could run SQL as the passwordless `default` user
  and read the result.
  - The Makefile and README now start the test ClickHouse with a random
    `CLICKHOUSE_PASSWORD`. `make clickhouse` passes
    `HOPCORDER_CLICKHOUSE_URL`, `_USER` (default `default`) and `_PASSWORD`
    to both packages' tests (the cmd/hopcorder end-to-end test now reads
    them too).
  - CI keeps its passwordless service: runners are single-use and the
    browser tests only load hopcorder's UI.
- **Docs**: install instructions, `docker run` examples and chart versions
  point at 0.1.1. v0.1.0 was withdrawn: its tag and release are deleted, and
  it predated both reviews. The `docker run` examples use the release
  image, not `:main`, which is built with the shared Actions cache. Chart
  version and appVersion in `Chart.yaml` are 0.1.1, so installs from a
  checkout run 0.1.1 images too.
- **Still to do in the repository settings** (superseded by round three's
  list, which makes immutable releases and the tag ruleset required: the
  `release` environment alone gates nothing for a collaborator):
  - Add required reviewers to the `release` environment; until then
    `publish` runs without approval, though after all the checks. (Wrong
    as a gate, round three found: with reviewers too, a collaborator's own
    workflow publishes without `publish`.)
  - Restrict it to `v*` tags (deployment tag policy).
  - Enforce the main ruleset with `ci` as a required check.
  - Add a tag ruleset for `v*`.
  - Optionally turn on immutable releases (required since round three).

**Agent**
- **Corrupt spool files are read in full before bbolt's check.** The
  round-one quarantine turned faults on the mapped file into panics with
  `debug.SetPanicOnFault`, which covers only the calling goroutine, and
  `tx.Check` runs in a goroutine of its own. A leaf element pointing outside
  the mapping (bbolt pages have no checksums) killed the agent with
  "fatal error: fault" on every start; 3 of 400 random 16-byte corruptions
  did. `openDB` now walks every bucket with a cursor in its own goroutine,
  checking key order and reading every value, before `tx.Check`. Such
  damage then faults where it is recovered, and the file is quarantined.
  Values are read too because the check never reads them, and a drain that
  reads one later is not protected.
- **Probe ids are random.** Each probe id is drawn from ChaCha8, seeded from
  `crypto/rand`. An id whose key is in flight is never chosen, and a
  tombstoned one only when the flow has no other free id. This replaces
  "probe ids are counted per flow from a random start" (§10). Why: an
  unprivileged local process can see the ids of the probes that reach the
  destination. A UDP socket with `IP_RECVERR` on the flow's port is handed
  every ICMP error quoting them, and a ping socket on the echo identifier
  gets the echo replies. With consecutive ids, that gave away the ids of
  the silent hops before them. A netns PoC then planted a fake hop and a
  fake loop, or ended the path early, with one datagram per cycle, for UDP
  and for ICMP.
- **ICMPv4 errors must quote the probe's IP ID.** The probe id is already
  the IP ID of every IPv4 probe. Unprivileged sockets choose ports, the UDP
  checksum and the echo sequence, but not the IP ID. A mismatch is dropped
  and counted in `hopcorder_probe_mismatched_replies_total`. Trade-off: a
  middlebox that rewrites IP IDs makes every hop beyond it silent on IPv4.
  The counter shows this. TestInnerPath (real Linux routers in netns)
  passes with the check.
- **Echo replies must echo the probe's data.** Destinations echo the
  request's data, but an echo from someone else carries their own data:
  for example, a ping run with a chosen identifier, which our ping socket
  cannot stop when it is a raw-socket ping binary. Counted as a
  mismatched reply.
- **The agent holds its ICMP echo identifiers**, with one unprivileged
  ping socket per identifier. The socket is bound to the identifier, has
  `SO_REUSEADDR` cleared and drops everything it receives. Ping sockets
  start with `SO_REUSEADDR` set, and the kernel lets two of them share an
  identifier unless one has it cleared. IPv4 and IPv6 ping sockets share
  one table of identifiers (`ping_get_port` compares only the identifier),
  so the one socket holds it for both families: an IPv4 socket, or an IPv6
  one where IPv4 ping sockets cannot be opened. A socket per family, as
  first written, collided with itself on hosts with IPv6 (CI caught it; the
  development container has no IPv6), and the netns test checks that an
  IPv6 ping socket cannot take an identifier held over IPv4.
  - The daemon does this first, at every start and reload, and `--once`
    does too (`probe.FlowHolder`, `Raw.HoldFlows`).
  - An identifier held by another process fails the start with
    `ErrFlowInUse` and a hint to use `--flow-id`. A failed reload reverts
    as before. This replaces the "two hopcorder processes" caveat for
    ICMP.
  - Without ping sockets for the agent's group
    (`net.ipv4.ping_group_range`) nothing is held. This is logged once:
    at info if other groups may open ping sockets, at debug if nobody can
    (the kernel default, "1 0").
- **UDP and TCP source ports are not held** (a deviation from the plan).
  - A bound UDP port swallows the datagrams sent to it, so the host
    stopped answering them with port unreachable. Another hopcorder
    tracing this host with UDP defaults (destination port 33434, the same
    as our default flow_id) would never see it reached.
  - A TCP port cannot be bound without `SO_REUSEADDR` for 60 s after any
    connection on it entered TIME_WAIT. 33434 lies in Linux's ephemeral
    range, so on busy hosts starts would fail at random. With
    `SO_REUSEADDR` the hold can be shared, so it would protect nothing.
  - Both checked on Linux 6.18.
  - The gain would have been small. Forged ICMP errors for UDP and TCP
    probes fail the IP ID check. A SYN-ACK or RST cannot be provoked
    without choosing the SYN's sequence number. With random ids, the ids
    an unprivileged process can still observe say nothing about the
    others.
  - So two processes on the same UDP or TCP flow still start, and the
    caveat stays for them.
- **`flow_id` has a global default and a `--flow-id` flag**, so a `--once`
  run next to the daemon can take another flow without a config file.
- **Every new spool starts its seqs at the clock** (time in microseconds),
  not only a quarantined one. The server drops seqs at or below an
  agent's stored high-water mark as duplicates, and it answers 409 only when
  a whole batch lies more than 100,000 below it. So a deleted spool that
  restarted at 1 under the same `agent_id` lost its records silently until
  its seq passed the old mark. A spool counts up by one per record, far
  slower than the clock, so a new one starts above anything an earlier
  one reached, as long as the clock is right. An existing spool keeps its
  counter. This replaces "the spool assigns seqs from 1" (§10).
  - On a host without an RTC whose clock is still at 1970 when a new
    spool is created, the first seq is tiny: the old behaviour, no worse.
  - Seqs near 1.8·10^15 stay below 2^53, so JavaScript numbers hold them
    exactly until the year 2255.
- **No reply before the send.** A reply whose receive time (the kernel's
  stamp, up to 100 ms before the read) lies before its probe's send time is
  ignored, and the probe stays pending. It used to be recorded with a
  negative RTT, clamped to 1 µs.
- **Accepted, not changed:**
  - Neither the spool directory nor the JSONL file is opened with
    `O_NOFOLLOW` or an ownership check. That only matters if an operator
    puts them in a directory that other users can write, and
    `O_NOFOLLOW` would break deliberate symlinks.
  - 64 idle keep-alive connections can block scrapes of the metrics
    endpoint. That is inherent to a connection cap. Bind the endpoint to
    localhost or restrict it with a NetworkPolicy. (Round three: the agent
    chart's liveness probe on `/metrics` turned this into restarts of the
    agent. The probe is gone, and the chart has an opt-in NetworkPolicy.)
- **Tests:**
  - `TestDamagedLeafIsQuarantined` opens damaged spools in a child
    process, so a regression fails the test instead of killing the binary.
  - `TestNewSpoolStartsAtTheClock` covers four cases: a new spool, a
    reopened spool that keeps counting, a recreated spool that starts
    above the old one, and a spool created when seqs started at 1, which
    keeps its counter. Spool, drain and daemon tests now count seqs from
    each spool's first seq.
  - Engine tests cover the IP ID check, echo data, random ids, id
    allocation around in-flight and tombstoned keys, and early replies.
  - The rawsock subtest `TestRawNetns/hold-and-forge` checks that a child
    running as uid 65534 cannot bind a held identifier, and that one it
    holds first fails the agent's start.
  - The same subtest checks that ICMP probes on a held identifier still
    get their replies.
  - It also runs the review's PoC, given the exact id of the silent TTL 2:
    the PoC can no longer plant a hop there or end the path there. It
    could with the check disabled.

### Security review, round three

A third review of the whole code base (the same four areas, every finding
reproduced, ClickHouse-dependent ones against ClickHouse 25.8) found no
high-severity issue. Four fixes followed. The decisions:

**Ingest and storage**

- **Stream state is bounded however busy an agent is.** A sweep cannot
  touch an agent that a request holds locked, and it skipped those; nothing
  else bounded the streams. One agent_id with two requests always in flight
  (4096 new streams each) was never swept, so its state grew without bound
  (256k streams and 503 MiB after 41 s in the review; 17,800 streams in
  0.13 s against `max_streams` 2000 in the test), and since the count stayed
  over the limit, every sweep (every second) dropped every other agent's
  streams: an agent with 300 streams lost the events of 44 cycles (beyond
  the restore budget) in every batch. Now:
  - A stream gets state only while there is room: fewer than `max_streams`
    in all (checked and counted in one compare-and-swap) and fewer than the
    new `ingest.max_streams_per_agent` of its agent, by default 4096 (what
    one request may carry) or half of `max_streams` if less, so that no one
    agent can fill the state (at most `max_streams`). Cycles of a stream
    without room are stored but derive no events, and are counted
    (`hopcorder_server_unrestored_cycles_total`) and warned about once per
    batch like unrestored ones; the stream is restored once there is room.
  - At `max_streams` the sweep frees 10% from the agents with the most
    streams: it finds the level that cutting every agent above it to it
    frees enough, counting all agents, and cuts those, least recently used
    streams first. A locked agent above the level is given it and cuts
    itself under its own lock when its request ends. So the agent with more
    than its share pays, and an agent below the level loses nothing.
    Before, the excess was taken from every agent that could be swept, by
    age.
  - Accepted: many agent_ids are still many agents. A flood of new ones,
    each with fewer streams than a real agent, still makes the real agents
    the largest and cuts them (the agent LRU and authentication at the
    proxy, §2, bound that). When the other agents' streams plus one agent's
    share exceed `max_streams`, that agent can keep the state full, so new
    streams of others get none until a sweep cuts it back (existing ones
    stay): keep `max_streams` above the real streams plus one share. A real
    agent with more streams than its share derives no events for the rest
    (the warning names the option).
  - Tests: `TestLockedAgentStreamsBounded`; `TestLockedAgentDoesNotEvictOthers`
    (the other agent's cycles without events per batch during the attack:
    `[44 0 0 0 0 0]`, as without one, was `[44 44 44 44 44 44]`);
    `TestHeavyAgentPaysAtMaxStreams` (five agents of 240 streams keep them
    all while a locked agent fills the rest; also fails if the level leaves
    locked agents out); `TestStreamsPerAgentBound`, `TestWaterLevel`,
    `TestServerStreamsPerAgent`.
- **`hops_mv` no longer copies a cycle's loops onto every hop row**
  (migration 0003). It looked up each hop's loop TTLs with
  `indexOf(loops.ip, hops.ip)` after `ARRAY JOIN hops`, and ARRAY JOIN
  copies every column still needed onto each row: the cycle's whole
  `loops.ip` and `loops.ttls`, per hop, hops x loops per cycle (and loops
  need not match the hops). Peak memory of one 8 MiB cycles insert (25.8):
  255 hops without loops 101 MiB; one loop of 255 TTLs (the review's
  record) 388 MiB; IPv6 hops each seen twice, 127 loops of two TTLs (what
  `DetectLoops` makes of such a path) 617 MiB; 255 loops of 255 TTLs over
  1.6 GiB, failing with code 241. Now the server sends each hop's loop TTLs
  with the cycle (`hops.loop_ttls Array(Array(UInt8))` in `cycles`: those of
  the first loop with the hop's address, as the view chose them, else
  empty), and the view only formats them, row by row (`is_loop` is
  `notEmpty`). `hops` and `path_probes` get the same values as before
  (prototype compatibility; `TestPrototypeQueriesRun` unchanged).
- **The size of an insert counts what the view writes.** Parts were sized
  by the cycles JSON alone, while the view writes the loop's TTLs as text on
  every hop row (911 bytes on each of 255 hops, 232 KB a record, none of it
  in the cycles row). A row now counts its JSON (which carries the per-hop
  TTLs), and for each hop row 64 bytes (the cycle's columns repeated on it
  and the hop's own fixed-size ones; ClickHouse writes about 80) plus its
  `loop_ttls` text. The largest 8 MiB part of each shape above now peaks at
  20 MiB without loops (also for 255 silent hops), 30 MiB for the IPv6
  pairs, 45 MiB for 255 x 255 and 68 MiB for one 255-TTL loop.
  - Accepted: loop-heavy batches go in many small parts. The review's
    23 KB zstd request (31 MiB decoded) is 60 inserts of about 100 ms
    instead of 3, and still writes 478 MiB into `hops` (2.4 MiB on disk):
    that is the prototype's per-hop `loop_ttls`. Batches of long paths take
    about twice as many parts as before; an agent's normal batch is still
    one insert.
  - Upgrade: cycles stored before the migration read as empty lists (their
    hop rows came from the old view). A server of the previous version
    inserting during a rolling upgrade sends no `hops.loop_ttls`; the
    column's default (an empty list per hop) keeps its inserts working, and
    their hop rows get no loop marks (`loop_events` has the loops).
  - Tests: `TestInsertPartsCountHopRows` (each insert's JSON plus the hop
    rows, estimated from the rows sent, within the budget; before, one
    insert of 40 such records came to 10.5 MB), `TestCycleRowHopLoopTTLs`,
    `TestLoopRecordsInsertClickHouse` (clickhouse tag: 20 records of 255
    loops of 255 TTLs took 821 MiB in one insert, now 27 MiB in two; their
    hop rows carry the TTLs).
- **A body reserves its read-buffer chunks at once.** Readers took 64 KiB
  chunks one at a time as data came, plus a one-byte one to see the end of
  the body, and kept them until the handler returned. So readers could hold
  every chunk between them, each waiting for one more: seven max-size
  bodies at full speed (129 chunks each, 512 in the pool) mostly did, and
  from `max_body_bytes` 16 MiB two could. All of them then stalled until
  the 20 s read timeout (408 or 503), and every other request waited
  behind them. Now a body reserves all it may need before it reads: by its
  Content-Length, or `max_body_bytes` without one (what it did not fill
  goes back once read). Reservations are granted in arrival order (a large
  one is not passed over) by a small weighted semaphore of the standard
  library (`chunkPool`), so every holder has all it needs and finishes or
  times out: no set of readers can deadlock. The end of a body is checked
  with a one-byte read into the stack. Chunks go back as soon as the body
  is decoded rather than when the handler returns (they were held through
  the inserts, the body long garbage). The bounds are unchanged: 64
  readers, 20 s each, a pool of the larger of 32 MiB and twice
  `max_body_bytes`, 503 when no chunks free up in time, 408 for a body not
  received in time.
  - Accepted: a client that announces a max-size body and sends nothing
    now holds its whole reservation (128 chunks by default) until its read
    timeout, so four such connections hold the default pool for 20 s;
    before, four that sent 8 MiB each and stalled did the same. A buffering
    reverse proxy removes both. A large reservation waiting first holds
    back smaller ones behind it.
  - Tests: `TestReadBufferNoDeadlock` (two bodies of four chunks sent in
    halves, six chunks in the pool: both 503 at the 3 s timeout before, both
    200 at once now), `TestReadBufferManyMaxBodies` (ten 8 MiB bodies and an
    agent's batch: `[503 200 503 408 200 200 408 200 503 408]` after 4 s
    before, all 200 in 0.5 s now), `TestChunkPoolOrder`;
    `TestReadBufferBounded` and `TestSlowBodiesHoldNoIngestSlot` adapted.
- **A request holds up to about three times `max_decoded_bytes`, not
  twice** (corrected here, in round two's text, `server.example.yaml` and
  the chart's `values.yaml`). A hop is 88 bytes in memory for as little as
  31 of JSON (a silent one), so decoded records take up to 3.0 times their
  NDJSON (255 silent hops a record; 1.6 times for answering hops, whose
  addresses lengthen the JSON). The decoder also left slices with the spare
  capacity of growing them by doubling: records of 9 silent hops took 3.6
  times. A record now keeps only what it holds (`compact`), 2.3 times for
  those. While a request is stored, one insert's rows and request body come
  on top, up to about 24 MiB: a whole request of 31 MiB of 255 silent hops
  peaked at 114 MiB (3.6 times). So ingest needs about `max_concurrent x
  (3 x max_decoded_bytes + 24 MiB)`, plus the stream state and the read
  buffer: with the defaults 480 + 170 + 32 MiB, within the chart's 1 GiB.
  Test: `TestDecodedRecordsMemory` (at most 3.2 times for records of 9 and
  of 255 silent hops; 3.6 before).

**Read side, enrichment and the browser**
- **Grafana's ClickHouse user cannot spill to disk** (`grafana_limits` in
  `deploy/compose/clickhouse-init.sql`, `hopcorder_readonly` in the chart's
  `readonly.xml`). `readonly = 2` let a query set the spill settings, and
  ClickHouse wrote the temporary data to its data volume: one
  `ORDER BY rand()` over `numbers(1e10)` with
  `max_bytes_before_external_sort` wrote about 100 MB/s, filled a 2 GB
  volume in 20 s, and the server's inserts failed with NOT_ENOUGH_SPACE
  (25.8). External GROUP BY and the grace_hash, partial_merge, auto and
  prefer_partial_merge joins spill the same way.
  `max_temporary_data_on_disk_size_for_user`/`_for_query` did not hold
  (temporary data grew past 1.5 GB against a 500 MB cap). Now
  `max_bytes_before_external_{sort,group_by}` and
  `max_bytes_ratio_before_external_{sort,group_by}` are `0 CONST`, and
  `join_algorithm` is `'direct,parallel_hash,hash' CONST` (25.8's default:
  in-memory joins only). A query that does not fit `max_memory_usage` fails
  instead, also without settings (the ratios default to 0.5, spilling at
  half the memory limit). Setting a constant to its own value still works.
  - Checked on 25.8, for the compose user and for the chart's rendered
    `readonly.xml`: every variant (sort and GROUP BY by bytes and by ratio,
    each spilling join algorithm, in the query and as URL parameters) fails
    with SETTING_CONSTRAINT_VIOLATION and writes no temporary data; switching
    to the `default` profile does not lift the constraints.
  - The plugin sends none of these settings: 4.23.5 sends the data source's
    custom settings (ours has none) and `limit` with a row limit, and
    clickhouse-go 2.50.0 adds `max_execution_time` from the query's deadline
    (read in their source). The CI `grafana` job runs every dashboard query
    through the plugin.
  - The server's own read queries use the server's user, which has no such
    profile: unchanged.
  - The compose profile is created with `OR REPLACE`, so re-running the
    script on an existing volume applies it (`IF NOT EXISTS` kept the old
    profile).
  - Accepted: `system.query_log` keeps each query's text (at most what the
    client sent; in the chart for `systemLogRetentionDays`), so a viewer can
    still write to disk at the rate they upload SQL, without amplification.
  - Tests: `TestGrafanaUserClickHouse` (`clickhouse` tag) creates the
    compose user from the script, runs every dashboard query with it and
    every spill variant, which it refused only with the fix;
    `deploy/helm/ci/smoke.sh` runs spill settings as the chart's user in
    kind, and the CI `helm` job checks that the rendered `readonly.xml`
    makes them constant.
- **The server chart sets `allowed_hosts`.** The chart listens on 0.0.0.0,
  where the server's default is no Host check, and its NOTES send the
  operator to `kubectl port-forward` and `http://127.0.0.1:8080/ui/`; port
  forwarding passes the Host header through, so a page whose name was
  rebound to 127.0.0.1 read the read API and posted records (reproduced:
  `{"accepted":1}` for seq 2^64-1 under a real agent_id, which 409s that
  agent for good). Round two's fix did not cover the documented Helm path.
  The chart now renders `allowed_hosts`: `localhost`, `127.0.0.1`, `::1`
  (the port-forward), `<fullname>`, `<fullname>.<ns>`, `<fullname>.<ns>.svc`
  and `<fullname>.<ns>.svc.<clusterDomain>` (the agents; `clusterDomain`,
  default `cluster.local`, is a new value), the ingress hosts, and the new
  `allowedHosts` value for other names; `"*"` in it renders `["*"]`, no
  check. A `config.allowed_hosts` of the operator's own is used as it is.
  The list matches names without port, so every Service port works.
  - Nothing that worked stops: kubelet probes (Host = pod IP) and Prometheus
    (pod IP) only use `/healthz`, `/readyz` and `/metrics`, which round two
    already exempted; agents use the Service's names; the smoke test uses
    the port-forward. In kind the smoke test checks that a foreign Host gets
    421 while `/readyz` and `/metrics` answer under a pod-IP Host, and that
    exactly that one request was refused
    (`hopcorder_server_host_refused_total`).
  - Names the chart cannot know: a NodePort or LoadBalancer Service (node
    and load-balancer addresses, external DNS names) and an ingress rule
    without host (any name). For those the chart fails at template time
    until `allowedHosts` lists something: an upgrade stops with the
    instructions instead of agents getting 421 at run time. Other ways in
    that the chart does not render (a second Ingress, a Gateway, a proxy
    that passes the client's Host on, pod IPs) are refused with 421 and
    logged with the Host, once a minute; they go into `allowedHosts`.
  - Wildcard ingress hosts (`*.example.net`) would have made the server
    refuse its config, so `allowed_hosts` now accepts `*.domain`: names one
    label below the domain, as Kubernetes routes them (not `example.net`
    itself, not `a.b.example.net`). Safe for the same reason as a listed
    name: a rebinding page sends its own name, which an attacker cannot put
    under the operator's domain.
  - NOTES print the list (or warn when it is `*`). Entries are checked at
    template time (host name, `*.domain` or address, no port), so a bad one
    fails the install instead of crash-looping the server.
  - Tests: `TestHostCheck` and `TestHostAllowlist` cover `*.domain`; the CI
    `helm` job renders the list (service names, ingress host, extra names,
    `*`, `config.allowed_hosts`) and the refusals; smoke.sh as above.
- **A read-API client that stops reading loses its slot after 10 s.** A
  request held its `query.max_concurrent` slot for the whole response, and
  the only bound on writing it was the server's 2-minute `WriteTimeout`: 8
  connections that sent `cycles?limit=1000` and never read kept every other
  read request and the UI at 503 (the review: 503 at 2, 52 and 102 s, 200
  at 137 s), and could start over. Now every write of a read-API response,
  in parts of at most 64 KiB (topology writes its response at once), and
  every flush get a deadline 10 s from then, never past the request's
  `WriteTimeout` (`server.WriteTimeout`, which the http.Server uses; a
  write deadline replaces the server's). 64 KiB in 10 s is 6.4 KB/s, far
  below any real link; 10 s is also how long a request waits for a slot.
  - The slot is still held while the response is sent rather than given
    back once ClickHouse has finished (`wait_end_of_query`): ClickHouse
    keeps a finished result in a temporary file, and the query in
    `system.processes`, until the server has read it (measured on 25.8: a
    2M-row result read at 20 KB/s sat in a 261 MB file in
    `/var/lib/clickhouse/tmp`). Without the slot, unread results would pile
    up there without bound.
  - Accepted: a client that keeps reading at least 64 KiB every 10 s holds
    its slot until its response is done or `WriteTimeout`, as a slow
    download does; and clients can still take every slot by opening new
    connections every 10 s, as they can with ordinary requests. Both are
    for a rate limit at the proxy (a buffering proxy, nginx's default,
    absorbs slow readers entirely).
  - Tests: `TestNonReadingClientLosesQuerySlot` (one slot, small socket
    buffers, a client that never reads): another reader gets 200 within a
    second; without the deadlines it got 503 after the 10 s queue wait.
    `TestSlowReaderGetsWholeResponse`: a client reading 1.6 MB/s gets a
    2.4 MB topology response whole with a 0.5 s deadline per part; with one
    deadline for the whole write it was cut off after 0.6 s.
- **The ASN database's bounds bound its memory.** Round one's limits (64
  MiB download, 256 MiB uncompressed, 5 million ranges) let a 45 MiB gzip
  of 5M ranges with unique AS names through; parsing it peaked at 1.35 GB
  of live heap (a 56-byte range, a 40-byte `ASNInfo` with two strings and
  an index-map entry per line), and the server in the chart's 1 GiB
  container was OOM-killed. The old database stays loaded during a parse
  and the cache is written only after a successful one, so it crash-looped
  for as long as the file was served. Now:
  - New bounds: 2 million ranges, 500,000 distinct (AS, country, name) and
    16 MiB of countries and names together. The real file has about
    600,000 ranges, 7 MB gzipped and 30 MB uncompressed (not re-measured:
    iptoasn.com is not reachable from the development container); the
    figure of about 80,000 ASes is roughly the number of ASes in BGP.
  - Compact storage: IPv4 ranges as two `uint32` and an index (12 bytes),
    IPv6 ranges as two pairs of `uint64` (40 bytes), each AS as 12 bytes
    pointing into one string that holds every country and name. The index
    used while parsing maps a seeded `maphash` of AS, country and name to
    the AS (a collision moves on to the next hash value), so it holds no
    copy of the names. `ASNInfo` strings returned by `Lookup` share that
    string, so an old database's text (at most 16 MiB) lives as long as a
    caller holds one of its names (rows being inserted).
  - Measured (`TestASNWorstCaseMemory`, opt-in with
    `HOPCORDER_ASN_MEMTEST=1`): the worst file the bounds allow (2M IPv6
    ranges, 500,000 ASes, 16 MiB of names; 14 MiB gzipped) is about 105 MB
    in memory; parsing it while another is in use, with a 64 MiB download
    held, peaks at 383 MiB of live heap (GOGC 5; 534 MiB at GOGC 100) and
    keeps 273 MiB. With the old storage the same test peaked at 563 MiB
    (788 MiB), and the new range and AS bounds alone, without the one on
    the names' total, allowed 604 MiB (824 MiB) even without a download
    held: they were not enough by themselves. The server in a 1 GiB
    container (Go memory limit 966 MB from the cgroup) downloaded it three
    times at a 1-minute refresh, each parse next to the previous database,
    without exceeding 600 MiB. The review's 45 MiB file now fails after
    0.4 s (more than 500,000 ASes) at a peak of 135 MiB.
  - Lookups are unchanged (`TestParseAndLookup`; IPv4 and IPv6 at the top
    of the address space and every generated range in the new tests).
  - Tests: `TestParseMemoryBounds` (each bound, lowered) and
    `TestASNDatabaseIsCompact` (200,000 ranges keep 31 bytes each; with the
    old storage 73, over the test's limit of about 54) both failed before.
- **The topology edges zip the hop arrays instead of capturing them.** The
  edges query paired consecutive answering hops with
  `arrayMap(k -> (rt[k], ri[k], rt[k + 1], ri[k + 1]), …)`. ClickHouse
  copies the arrays a lambda captures for every element it runs on, so a
  cycle cost the square of its hops: about 160 KB for 255 answering hops.
  20,000 such cycles took 3.1 GiB and 29 s (ClickHouse 25.8), past
  `query.max_memory_bytes`, so an agent's long paths broke its topology
  view, and every such request held that memory while it lasted. Now
  `arrayZip(arrayPopBack(rt), arrayPopBack(ri), arrayPopFront(rt),
  arrayPopFront(ri))`: the same edges, 49 MiB and 0.4 s. Found by the
  ingest fix's review of `hops_mv`, whose view had the same pattern (fixed
  there). No other query captures arrays in a lambda: the remaining ones
  (topology's timeline, `arrayFilter` on the hops, the dashboards'
  `arrayMap`s) take their arrays as arguments.
  - Test: `TestTopologyLongPathsClickHouse` (2,000 cycles of 255 answering
    hops with 128 MiB per query; before: 502). `TestTopologyClickHouse`
    still checks the edges themselves.

<!-- round three: agent -->

**CI, release and deployment**

- **The `release` environment gates the `publish` job, not publishing.**
  `publish` has no credential of its own: it uses `GITHUB_TOKEN`, and a
  workflow file pushed to any branch (or in a same-repository pull request)
  can ask for `packages: write` and `contents: write` and leave out
  `environment:`. ghcr grants writes per repository, not per workflow or
  environment, and release assets take any token with `contents: write`.
  So a collaborator, or a stolen token of theirs with the `workflow` scope,
  can overwrite `:<version>`, `:<major>.<minor>`, `:latest`, the charts and
  the release files (`gh release upload --clobber`), with all of round
  two's settings applied. Round two's "until then publish runs without
  approval" read as if reviewers closed this; they do not. What the code
  does now:
  - **Build provenance attestations** (`actions/attest` v4.2.2, pinned by
    commit; the action that `attest-build-provenance` v4 wraps, which its
    README recommends for new workflows). `id-token: write` and
    `attestations: write` are on `publish` only. It attests every file in
    `dist/` (archives, chart packages, `SHA256SUMS`) and both image digests
    (the multi-arch indexes, by `subject-digest`) before it publishes
    anything. The attestations live in GitHub's attestation store; they are
    not pushed to ghcr, since `gh attestation verify oci://…` fetches them
    from GitHub by digest, and so no `artifact-metadata` permission or
    storage records are needed.
  - The Sigstore certificate names the workflow file, the ref
    (`refs/tags/vX.Y.Z`) and the commit. A collaborator's own workflow can
    attest too, but its certificate names its own file or a branch, and
    with the tag ruleset (below) a run for a `v*` tag means a maintainer
    pushed the tag. So `gh attestation verify … --signer-workflow
    Saremox/hopcorder/.github/workflows/images.yml --source-ref
    refs/tags/vX.Y.Z` tells a replaced file or image from a released one.
    It detects replacement, it does not prevent it, and helps only users
    who verify.
  - **Digests in the release notes**: `scripts/release-notes.sh` appends
    the commit, both image digests and the verify commands to the
    CHANGELOG section. Every dry run runs it with made-up digests. `publish`
    also checks that each tag it sets names the attested digest
    (`imagetools create` with one source copies the index as it is). The
    notes are a convenience; the attestations are the proof.
  - Charts: `helm pull`, `gh attestation verify` on the `.tgz` (an OCI
    chart's content layer is that file), then install from the file.
    `--set image.tag=X.Y.Z@sha256:…` pins the image in both charts without
    a template change.
  - **No environment-only credential.** Considered: an environment secret
    (a bot's classic PAT, as ghcr takes nothing finer) with a fallback to
    `GITHUB_TOKEN`. It only helps once no other workflow can write the
    packages, that is with this repository's Actions access on them set to
    Read. But the `image` job pushes `:main` and `:sha-…` to the same
    packages with `GITHUB_TOKEN`, so that would break main's images, and
    keeping them would take a separate package for main. Release files are
    protected by immutable releases, not by a credential. Left to the
    owner; not implemented.
  - Accepted: whoever can push a workflow file can publish something. The
    defence is detection plus the repository settings.
- **The tag is checked again once the release exists.** A release belongs
  to a tag name, and GitHub ignores `--target` (`target_commitish`) for a
  tag that exists, so `--target` would not pin the commit. checkout v7.0.1
  fails if the tag moved before the job checked out, but `publish` creates
  the release about a minute later. Now it resolves the tag through the API
  after creating the release, peeling an annotated tag. Unless the tag
  names `$GITHUB_SHA`, it deletes the release (the tag stays) and fails; an
  API error counts as a mismatch. With immutable releases the tag cannot
  move once the release exists, so the check is final. Without them it can
  move later anyway, which only the tag ruleset prevents. The images,
  charts and files stay: they were built and attested from this commit.
- **verify-tag does not see drafts.** GitHub lists draft releases only to
  tokens with push access, and verify-tag's has `contents: read`. `publish`
  (`contents: write`) runs the same check and sees them, so "no release of
  the tag, drafts included" still holds before anything is published.
  Noted in `release-verify.sh`.
- **The `image` job has `packages: write`.** README said a tag's images
  were built "without write tokens"; corrected. The job pushes
  `:sha-<full commit>` for a tag and `:main`/`:sha-<short>` for main, so it
  keeps the permission. The Dockerfile's `RUN` steps cannot see the token,
  only the job's actions can.
- **The server can listen on a unix socket, its own or systemd's.** On a
  systemd host the docs put the server on 127.0.0.1 behind the reverse
  proxy, but every local user reached that port without the proxy's
  authentication: they could read everything and inject records, among
  them round two's `seq` 2^64−1, which leaves an agent refused with 409
  until the manual recovery.
  - `listen: systemd` takes the socket systemd passes (socket activation:
    `LISTEN_FDS`/`LISTEN_PID`, fd 3, read with `os.NewFile` and
    `net.FileListener`, no dependency). `deploy/hopcorder-server.socket`
    creates `/run/hopcorder-server.sock`, `root:<SocketGroup>`, mode 0660,
    so only root and the proxy's group connect. Exactly one socket; the
    variables are unset once used, and ignored when `LISTEN_PID` is another
    process (inherited).
  - `listen: systemd` without a passed socket fails at start, and so does a
    passed socket with any other `listen`: the server would listen
    elsewhere, and the socket's connections would wait forever.
  - The unit ships `SocketGroup=hopcorder-proxy`, a group that does not
    exist, so the socket fails to start until the admin names the proxy's
    group (or creates that one). Every process with the group reaches the
    server unauthenticated, and on Debian nginx's `www-data` is often
    shared with PHP-FPM and the like; hence the advice of a group of its own.
  - `listen: unix:<absolute path>` creates the socket itself (outside
    systemd, or with a sidecar proxy sharing a volume), mode 0660 after the
    bind. Between bind and chmod it has the mode the umask leaves
    (accepted; the docs say to use a directory only the server and proxy
    can enter). A stale socket (connection refused) is replaced, a live one
    or any other file is not; a clean shutdown removes it. Abstract sockets
    (`unix:@name`) are refused: they have no permissions. Paths are at most
    107 bytes.
  - The Host allowlist: on a unix socket there is no check unless
    `allowed_hosts` lists names. DNS rebinding needs a TCP listener the
    browser can reach, which is now the proxy's, and the proxy
    authenticates and picks the Host header it sends (nginx's `unix:`
    upstream sends `localhost`, Caddy the client's). Accepted: a proxy that
    admits clients by address rather than by credentials would forward a
    rebinding page's request; the docs say to list its names then. A TCP
    socket from systemd is judged by its address, like a configured
    `listen` (127.0.0.1 gets the localhost names).
  - `/healthz`, `/readyz` and `/metrics` are served on the socket as well;
    Prometheus scrapes through the proxy. The default stays
    `127.0.0.1:8080` (containers, the charts, quick installs; the server
    chart owns `listen`).
  - Tests: a socket in a temporary directory (mode, stale, live and
    regular files, removal), `serve` on a socket (health, metrics, the Host
    check with and without `allowed_hosts`), socket activation in a child
    test binary that gets a listening socket as fd 3, and the refusals.
    Checked by hand with `systemd-socket-activate` (unix and TCP) and
    `systemd-analyze verify`.
- **The agent chart has no liveness probe.** It probed `/metrics` (1 s
  timeout, three failures), whose listener takes 64 connections and keeps
  one that sends nothing for 10 s. So any workload that could reach the pod
  (or, with hostNetwork, the node's port) could hold 64 idle connections
  and have the agent restarted with growing backoff, leaving gaps in the
  recording; the review reproduced 11 of 11 failed probes with 120 held
  connections. A live `/metrics` says nothing about the probing loop
  anyway, and the agent serves no traffic, so there is no readiness probe
  either (one on `/metrics` would only take it out of the Service). A hung
  probing loop was never caught by the probe, since the metrics listener
  runs on its own; alert on the rate of `hopcorder_cycles_total` instead.
  ci's helm job fails if the chart renders any probe, in either mode.
- **Opt-in NetworkPolicy for the agent** (`networkPolicy.enabled`, off by
  default): inbound connections only to the metrics port and only from
  `networkPolicy.from`, required when the metrics are exposed, as in the
  server chart. With hostNetwork the chart refuses it: NetworkPolicy does
  not apply to host-network pods, and a policy that does nothing would look
  like protection. Replies to the agent's probes are not new connections;
  routers' ICMP errors count as related traffic in conntrack-based
  implementations, but this was not tested in a cluster (kind's CI run does
  not enable it), so `values.yaml` says to check that hops still answer.
  ci lints it and checks both refusals.
- **The agent chart's header check is wider, and called a heuristic.** It
  accepted `Authentication`, `X-Jwt`, `Bearer`, `X-Access-Tokens` and
  `X-AccessToken`. Names containing token, secret, passw, credential,
  cookie, signature, session, jwt, bearer, api-key, authoriz/authoris or
  authent anywhere are refused now, and so are names with a part key(s),
  auth, pass or sig. It still goes by name only, so `values.yaml` and the
  README say to pass credentials only through the Secrets. ci checks the
  new names, and that `X-Tenant`, `X-Scope-OrgID` and `X-Request-Source`
  still pass.
- **Release archives are owned by root, and reproducible.** `tar -czf`
  stored the runner's uid (1001), and GNU tar run as root restores stored
  owners, so README's `curl … | tar -xz` as root left uid 1001 owning the
  directory and binary, free to replace it before `install` or `setcap`.
  `scripts/release-build.sh` now writes `--owner=0 --group=0
  --numeric-owner --sort=name --mtime=@<commit time>` (`SOURCE_DATE_EPOCH`
  if set) with modes normalised to 0644/0755, through `gzip -n`, and fails
  unless every entry lists 0/0 and the binary is 0755; it runs on every pull
  request as the dry run. Two builds of the same commit give the same
  archives (checked). The chart packages are not reproducible: `helm
  package` stamps the current time.
- **A tag is a Go release, whatever the workflow says.** proxy.golang.org
  serves any `vX.Y.Z` tag as a version once someone asks for it, and
  `@latest` resolves to the highest: off main, without CI, even when
  verify-tag fails. It caches it for good; deleting the tag does not help,
  and undoing it takes a higher version whose `go.mod` retracts it, which
  pushes the version numbers up for good. README recommended
  `go install …@latest`.
  - README now recommends the release archives, or
    `go install …@vX.Y.Z` of a version with a GitHub release, and says why.
  - `go.mod` retracts v0.1.0 (withdrawn, its tag deleted). Whether the
    proxy cached it was not checked, to keep the module path off an outside
    service. The retraction takes effect with the next release, whose
    `go.mod` carries it; `go mod tidy` keeps it, and the toolchain checks
    (ci, `release-build.sh`) read only the `toolchain` line.
  - Only a tag ruleset (a repository setting) keeps stray `v*` tags out;
    nothing in the repository's files can.
- **Still to do in the repository settings** (the owner; replaces round
  two's list):
  - A tag ruleset for `refs/tags/v*` restricting creation, update and
    deletion, with only the maintainers (the admin role) as bypass, not
    GitHub Actions or an app. It keeps stray tags out of the Go module proxy
    and the release workflow, and makes `--source-ref refs/tags/v…` mean a
    maintainer's tag.
  - Immutable releases: a published release's files and tag can no longer
    change (and `gh release verify` / `gh release verify-asset` check
    GitHub's attestation of the release).
  - Required reviewers on the `release` environment, and its deployment
    tag policy limited to `v*`. That gates `images.yml`'s publishing
    against mistakes and a stray tag run, not against collaborators.
  - The ghcr packages (`hopcorder-agent`, `hopcorder-server`,
    `charts/hopcorder-server`, `charts/hopcorder-agent`): under Manage
    Actions access, only this repository, with no other repository granted
    write. Read for this repository needs main's images moved first (see
    the first bullet).
  - Enforce the main ruleset with `ci` as a required check (from round
    two).
  - Give write access only to people you would let release.
  - Artifact attestations need a public repository (or GitHub Enterprise
    Cloud): if the repository is private, `publish` fails at the first
    attest step, before it publishes anything.
