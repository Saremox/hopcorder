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
  (dst, udp, sport, dport, checksum). Since round three of the security
  review the ids are random (round two), and the 16 bits of the key are not
  all a reply must match: ICMP and UDP payloads are `comp word | 4-byte
  random nonce | "hopcorder\0"` (the paris word now compensates for seq and
  the nonce), and the IPv4 IP ID is random on its own; see round three.
- **`flow_id`** is the ICMP identifier (icmp) or UDP source port (udp),
  default 33434, constant across TTLs *and* cycles so paris keeps a target on
  one ECMP path over time; per-target configurable. Classic mode
  (`paris: false`) varies the ICMP checksum or walks the UDP destination port.
  (Since round five of the review, two targets of one host and protocol
  need different ones: the server tells streams apart by it.)
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
  SYN-ACK/RST acknowledge seq+1); since round three the low 16 bits are
  random, and a reply must acknowledge exactly seq+1 (an ICMP error quote
  all 32 bits). Paris keeps both ports fixed; classic mode
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
  daemon) need different `flow_id`s (`--flow-id`): for ICMP the second does
  not probe that flow's targets (since round three; it failed at startup
  before), as the first holds the echo identifier; with UDP or TCP both
  probe, and a reply can, very rarely, be attributed to the other process's
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
  metrics, schedulers) shuts down as on SIGTERM and starts again with
  the new config; only the prober and its raw sockets survive (and, since
  round three of the security review, the metrics listener while its
  address is unchanged). A full
  in-process restart was chosen over diffing configs because it applies every
  setting the same way and has one code path; the cost is a pause of about
  one shutdown (≤3s) and Prometheus counters restarting from zero, which
  `rate()` handles. If the new config fails to start, the previous one is
  started again (and retried until it starts, since round three). Every
  reload reopens the JSONL file, which makes rename-and-HUP log rotation
  work.
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
  at insert time" happens at insert time inside ClickHouse. (Not so, round
  four found: the views run after the `cycles` part is committed, and one
  that fails leaves the cycle without its rows. The server now repairs that
  on the retry, and writes `agent_seq` itself; see round four.)
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
  wiped (seq restarting at 1) must get a new `agent_id`. (Round four: "the
  insert succeeded" is not enough, since an insert that fails or times out
  may have stored cycles all the same; the mark now moves only past cycles
  stored with their hop and loop rows, and a retry looks up what is stored.)
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
  of its own release (since round four by digest: `image.digest`, written
  into the packages).
- **Releases**: `release-assets` builds the static Linux binaries (amd64,
  arm64; with the license, README, example configs and systemd units), the
  chart packages and `SHA256SUMS`, with the tag's section of `CHANGELOG.md`
  as the notes (it fails without one), using
  `scripts/release-build.sh` with go.mod's toolchain. On pull requests and
  `main` that is a dry run; for a tag the files go to `publish`, which
  attests them and both image digests and creates the GitHub release from
  them, the notes followed by the digests and how to verify them (round
  three; since round four the `attest` job attests, and `publish` creates
  the release as a draft and checks it before publishing). The binaries and
  images carry the
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
    (`readonly = 1` since round four, with only `max_execution_time`
    changeable; time, memory and concurrency limits with `MIN 1`, the
    memory limits from `readOnlyUser.maxMemoryUsage` and
    `maxMemoryUsageForUser`; the cancel setting and, since round three, the
    spill settings const). Checked on 25.8: it reads the tables, and
    writes, `system.users`, `url()`, `max_execution_time = 0`, changing the
    cancel setting and spilling to disk are refused.
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
  insert it is re-read from storage before the next batch. (Round four: the
  re-read mark came from a view and could claim cycles whose hop rows were
  missing, or miss cycles stored after all; the retry now stops the insert
  and looks up which of its cycles are stored.)
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
  `readonly = 2` (no writes; the plugin may still set query settings; since
  round four `readonly = 1`, see there), and no
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
- **Repository settings** (not possible from here): enforce the main
  ruleset, add a tag ruleset limiting who may create `v*` tags, and add
  required reviewers to the `release` environment. All done; see round
  three's list.
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
  650 MiB (round four: most of it for formatting the block's text in
  parallel, now off; 52 MiB). The UI asks for one cycle at a time.
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
  plugin's own query timeout must stay at or below 300 s (295 s: the plugin
  adds 5). The script runs on
  the first start of an empty volume and can be re-run on an older one
  (every statement is `IF NOT EXISTS`; since round three the profile is
  `OR REPLACE`, so a re-run also updates it). Round four: the list of
  constrained settings was not a boundary; `readonly = 1`, 400 MB and
  600 MB, and the user is `OR REPLACE` too.
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
  any window, and `outages` holds a row per outage, not per cycle. (Round
  four: a stream that flaps every cycle has a row per two cycles, so the
  lookup is now bounded by the last replayed cycle that reached the
  destination, when there is one.) A change across a gap longer than
  `max_record_age` is not reported.
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
  (Round four: the parts bounded the cycles only; a batch's events went in
  one insert per table, and are now stored part by part too.)
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
- **Repository settings** (superseded by round three's list, which makes
  immutable releases and the tag ruleset required: the `release`
  environment alone gates nothing for a collaborator; all done since):
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
  reads one later is not protected. (Round three found this incomplete:
  `tx.Check` also reads branch keys, which a cursor never touches, and
  trusts overflow counts and child pointers; the page structure is now
  checked first, see there.)
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
  the IP ID of every IPv4 probe (corrected in round three: that added no
  bits against someone guessing ids, so the IP ID is now drawn on its own).
  Unprivileged sockets choose ports, the UDP
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
    does too (`probe.FlowHolder`, `Raw.HoldFlows`). (Round four: during a
    reload the supervisor holds both configurations' flows, and releases
    the old ones once the new configuration runs.)
  - An identifier held by another process fails the start with
    `ErrFlowInUse` and a hint to use `--flow-id`. A failed reload reverts
    as before. This replaces the "two hopcorder processes" caveat for
    ICMP. (Changed in round three: any local user could stop the whole
    agent that way; now only that flow's ICMP targets wait.)
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
    probes fail the IP ID check. With random ids, the ids an unprivileged
    process can still observe say nothing about the others.
  - Corrected in round three: "a SYN-ACK or RST cannot be provoked without
    choosing the SYN's sequence number" was wrong. A process that binds
    the flow's TCP port and loops `connect()` to the target provokes them,
    and the kernel's ISN is a free 16-bit guess at the probe id, since only
    the top 16 bits were checked; the review got 2 of 38 cycles recorded as
    `reached` that way. Replies must now acknowledge the exact 32-bit
    sequence number; see round three.
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
  (Round four: not for a request of events, all held and inserted at once,
  213 MiB beyond the body. Events now go part by part, and the figure is
  about `max_concurrent x (3 x max_decoded_bytes + 32 MiB)`, 512 + 170 +
  32 MiB with the defaults.)
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
    Wrong, as round four found: the profiler and logging settings, still
    changeable, made ClickHouse write about 1.5 MB/s of its own logs, and
    even without them every query writes a few hundred bytes of logs,
    however short its SQL (see round four).
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
    under the operator's domain. (Round four: that holds only for a domain
    whose names the operator alone can create. `*.com` allowed
    `attacker.com`, and on a shared domain such as `github.io` anyone gets
    names; a single label after `*.` is refused now, and the docs warn about
    shared domains.)
  - NOTES print the list (or warn when it is `*`). Entries are checked at
    template time (host name, `*.domain` or address, no port), so a bad one
    fails the install instead of crash-looping the server. (Round four: the
    check was looser than the server's, so `10.0.0.5:8080`, `cafe:443`,
    `::::` and a 300-character name still crash-looped it; fixed there.)
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
    download does (round four: until then, the ClickHouse client's 30 s
    timeout, which also covered reading the result, cut a streamed response
    30 s after its query started, however fast its client read); and
    clients can still take every slot by opening new connections every
    10 s, as they can with ordinary requests. Both are
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

- **A reply must match at least 32 random bits, not 16.** Round two's checks
  rested on the 16-bit probe id: the IPv4 IP ID was the probe id itself,
  the echo data was `^seq` and the magic, TCP compared the top 16 bits of
  the ack, and IPv6 had no second check. An off-path sender guessing ids
  hit a probe in flight with probability (probes in flight)/65534 per
  packet. In the review's netns PoC, forged time exceeded at 20k pps put
  4–12 fake hops (and a fake loop) into 11 of 13 UDP cycles, and forged echo
  replies at 5k pps recorded a black-holed target as `reached` in 11 of 12
  ICMP cycles: the outages hopcorder exists to record. Now:
  - The IPv4 IP ID is drawn on its own (1..0xffff; for 0 Linux would pick
    one), and ICMPv4 errors must still quote it.
  - ICMP and UDP probes carry a random 4-byte nonce after the compensation
    word (16 payload bytes, were 12). Echo replies must echo it. An ICMP
    error must quote unchanged whatever it quotes of the payload, cut at
    the length the quoted IP header states, so RFC 4884 padding is not
    compared. On IPv4 a quote of 8 bytes (RFC 792's minimum), or one padded
    with zeros right after them, still counts: the IP ID covers it.
  - On IPv6, ICMP errors for ICMP and UDP probes must quote the nonce.
    Checked only "where quoted", a spoofer would quote 8 bytes and face 16
    bits again (IPv6 has no IP ID), and RFC 4443 requires as much of the
    packet as fits in 1280 bytes. Trade-off: an IPv6 router that quotes
    less stays silent, counted as mismatched, as an IP ID rewriting
    middlebox does on IPv4.
  - TCP SYNs carry 16 random bits in the low half of the sequence number;
    a SYN-ACK or RST must acknowledge exactly seq+1, an ICMP error must
    quote all 32 bits (`pendingProbe.seq`).
  - Paris: the compensation word cancels seq and the nonce, so the ICMP
    checksum stays constant per flow; ports, identifier and lengths do not
    change (TestParisKeepsFlowConstant, TestInnerPath).
  - Random bits a forgery must guess: echo reply 48 (id and nonce);
    ICMPv4 error 32 (id and IP ID), 64 if it quotes the payload, 48 for
    TCP; ICMPv6 error 48, 32 for TCP; TCP reply 32.
  - Measured with the review's spoofer, rerun as TestRawNetns/spoof (every
    hop and the target silent, 3 cycles of 30 TTLs per protocol, two
    runs): before, at least 6 fake hops in every UDP cycle at 20k pps and
    4–10 fake `reply` hops in every ICMP cycle at 10k pps; after, none,
    while 28 and 14 forgeries hit a probe in flight and were counted as
    mismatched.
- **Local processes can no longer forge TCP replies** (corrects
  round two, which said they cannot be provoked without choosing the
  SYN's sequence number). An unprivileged process that binds the unheld
  TCP flow port and loops `connect()` to the target gets SYN-ACKs or RSTs
  for the kernel's ISN, and only its top 16 bits were compared: the review
  got 2 of 38 cycles recorded as `reached` at about 270 connects/s as uid
  65534. With the exact ack an attempt matches with probability N/2^32 for
  N probes in flight; at that rate, with 30 probes in flight a fifth of the
  time (1 s timeout, 5 s interval), about one forged reply per month of
  continuous attack, at a random TTL of one cycle. The TCP source port is
  still not held: round two's reasons stand (TIME_WAIT makes the bind fail
  at random on busy hosts, and with `SO_REUSEADDR` the hold can be shared),
  and against a 1-in-2^32 guess a hold would add little.
- **Accepted (forged replies):** forgeries still count in
  `hopcorder_probe_mismatched_replies_total` (it measures attempts), and a
  forgery for a timed-out probe's key counts as a late reply without the
  further checks; neither reaches a record.
- **Tests (forged replies):** TestEngineForgedRepliesRefused (15
  forgeries, one per check and family; each forged reply is refused and
  counted, and the probe stays pending for the genuine reply, which
  arrives 20 ms later). Undoing any one fix in a scratch copy (IP ID =
  probe id; no quote check; top 16 bits of the ack; the old echo data)
  fails its cases. Also TestEngineIPIDIndependentOfProbeID,
  TestEngineShortQuotes, TestQuoteMatches, and golden bytes regenerated by
  an independent Python script. TestRawNetns/spoof fails on the pre-fix
  code with fake hops in every cycle. TestParisKeepsFlowConstant now
  checks ids for uniqueness per key: classic mode varies a port, and the
  check failed about 1 run in 200.
- **A reload never stops the agent, and never frees the metrics port.**
  `runDaemon` closed the metrics listener on SIGHUP and bound it again only
  after the outputs and `spool.Open` (whose full read of the spool widens
  the window). A local user polling `bind()` took the port: the new
  configuration failed, the revert failed the same way, `superviseDaemon`
  returned, and the agent exited; with `Restart=always` every restart
  failed while the port was held, and scrapes got the squatter's numbers
  (the review won on the first SIGHUP as uid 65534; README tells operators
  to reload daily from logrotate). Now:
  - The supervisor owns the listener (`output.MetricsServer`), like the
    prober. A configuration with the same `metrics.listen` gets the same
    listener and only swaps what it serves (`Serve(path, metrics)`); while
    no configuration runs it answers 503, so Prometheus sees `up == 0`.
  - A new address is bound before the configuration starts, and the old
    listener is closed only once the new configuration runs; if it fails,
    the revert finds the old listener still bound. Cost: moving to an
    address that overlaps the old one on the same port (`127.0.0.1:9469` to
    `:9469`) fails the bind and is reverted; that needs a restart (README).
  - Only the first start can end the supervisor. When the revert fails too
    (anything else that can fail at start: a JSONL directory gone, the
    spool locked, a certificate unreadable), the previous configuration is
    retried with a wait doubling from `restartBackoffMin` (1 s) to
    `restartBackoffMax` (1 min), logging an error each time, until it
    starts or a reload brings one that does. Nothing is probed meanwhile;
    retrying rather than probing without outputs, because every record
    must go through the spool. `runDaemon` returning on its own (it does
    not today) restarts the configuration the same way.
  - (Round four found two ways a reload still stopped the agent: a
    `metrics.path` that panicked `http.ServeMux`, and a JSONL write that
    blocked for good, which the stopping configuration waited for. Both
    are fixed; see there.)
  - Accepted: a local user who binds the port before the agent's first
    start still fails that start (exit 1, systemd retries), as for any
    service on an unprivileged port; the first start failing loudly is
    §5.4's rule ("validate the whole config at startup and fail loudly").
  - Tests (fake prober): TestReloadKeepsTheMetricsPort (a goroutine polls
    `net.Listen` on the port during the reload; it never binds, the new
    configuration runs and the port serves the agent's metrics),
    TestReloadRetriesWhenTheRevertFails (the reload fails, the previous
    configuration's JSONL directory is gone: the supervisor keeps running,
    holds the port and answers 503, and resumes once the directory is
    back), TestReloadMovesTheMetricsAddress, TestMetricsServer. The first
    two fail against the previous supervisor ("address already in use",
    and the supervisor exits).
- **An echo identifier in use stops only its ICMP targets.** Where
  `ping_group_range` admits everyone (systemd's default), a local user who
  bound a ping socket to identifier 33434 before the agent started made
  every start fail with `ErrFlowInUse`, UDP and TCP targets included (the
  review: two starts as uid 65534, both exit 1). Now `HoldFlows` holds what
  it can and returns the rest; `Raw.Trace` refuses an ICMP flow whose
  identifier it was asked for but could not hold, after trying to hold it
  again, so the targets come back by themselves once it is free (each
  cycle retries: a socket and a bind). The targets' cycles fail: the
  runner logs an error when it starts and info when it ends (debug in
  between), `hopcorder_cycle_errors_total` counts them, and the gauge
  `hopcorder_flow_in_use{target,protocol}` is 1 meanwhile (the
  `agent.Observer`'s `CycleFailed` now gets the error). `runDaemon` and
  `--once` log the hold failure and go on. `--once` prints the other
  targets' records and exits 1, as for any target it cannot probe.
  - Refusing rather than probing unprotected: with another process on the
    identifier, its ping socket sees our echo replies and errors (with
    their nonces) and can send echoes with our identifier, and a second
    hopcorder on the flow is a misconfiguration to show, not to hide.
  - (Round four: a reload that failed and was reverted had freed the
    running configuration's identifier while the new one started, so a
    local process could take it then too; both configurations' flows are
    now held until the new one runs.)
  - Tests: TestDaemonFlowInUseStopsOnlyItsTargets (fake prober,
    `Fake.SetHoldErr` per flow: the TCP and UDP targets are recorded, the
    ICMP one not, the gauge is 1, and after the identifier frees the ICMP
    target is recorded without a reload; before, `runDaemon` failed at
    start). TestInnerHoldFlows (rawsock) checks the second prober's UDP
    flow traces while its ICMP flow is refused, and that an identifier a
    uid 65534 process held and let go is taken back by the next trace.
- **The spool's page structure is checked before bbolt reads it, and
  damage found while running is quarantined at once.** Round two's walk
  did not cover everything bbolt trusts. In the review's 3000 random
  corruptions, opened in child processes: one faulted inside `tx.Check`
  (it reads branch keys, which a cursor never touches) on every start; 54
  hung or grew to 1.5 GB in 16 s, all from a damaged page header
  `overflow` field (`verifyPageReachable` loops up to 2^32 times); one
  passed the check and panicked on the first Ack ("page already freed": an
  overflow span covering a free page, which `tx.Check` compares with the
  freelist only at its first page). A leaf element flagged as a nested
  bucket rooted at its own page made `walkBucket` recurse until the stack
  overflowed, on every start. Now:
  - `checkPages` reads the file (`ReadAt`, not the mapping) under a shared
    lock (a read-only bbolt open, which reads only the meta pages and still
    gives `ErrLocked` while an agent has the file) and checks: the meta page
    bbolt will use (the valid one with the higher txid, as bbolt picks it;
    corrected in round four: "valid" meant more here than to bbolt, which
    checks only magic, version and checksum, so a crafted meta took bbolt
    to a tree nothing had checked)
    with the high water mark within the file; every page reachable from the
    root and the freelist page for id, type, overflow within the high water
    mark, and no page reached twice (so no cycles) or reachable and free;
    every element within its page's span, no empty branch, depth at most
    64; buckets only in the root and none in them, inline ones included;
    freelist ids within the file and unique, read in 64 KiB chunks; every
    page reachable or free. bbolt's on-disk format has not changed since
    boltdb (version 2), and the check reads it in the host's byte order,
    as bbolt does. Then bbolt opens the file read-write, the cursor walk
    (which now also refuses nested buckets) and `tx.Check` run as before,
    on a structure in which they cannot fault, loop or recurse.
  - Chosen over running the check in a child process with a timeout and
    memory cap: a re-exec would depend on `/proc/self/exe` and on the
    systemd unit's and the container's sandboxing allowing it, a child's
    memory counts against the same cgroup limit (the chart's 256 MiB), and
    it would still leave damage that passes the check to crash the agent
    later. The check is cheap: opening a 48 MB spool (12,000 records, page
    cache warm) took 16 ms instead of 12.
  - Every transaction runs under `guard`: `debug.SetPanicOnFault` for its
    goroutine (drains and schedulers read and write in goroutines of their
    own) and a recover. A panic or fault there fails that operation with an
    error wrapping `errCorrupt`, and `Spool.replace` closes the database
    (giving up after 5 s, in case bbolt's locks stayed held), moves the
    file aside as `spool.db.corrupt-<time>` and opens an empty spool in its
    place at once, seqs from the clock. Operations hold a read lock on the
    database (`dbMu`), the replacement the write lock. If the replacement
    fails (say the rename does), every operation tries again. Accepted: a
    panic from a bug of our own inside a transaction is taken for damage
    too; the file is kept for inspection.
  - Not covered: a file cut below two pages still fails `bolt.Open` with a
    plain error and is not quarantined, as before. (A freelist count that
    claims more ids than its pages hold is refused before the writable
    open, which would otherwise allocate them.)
  - Tests, each opening the file in a child process (`TestDamagedChild`,
    which bounds its run to 20 s and its heap to 512 MiB, uses both
    consumers like the drains and appends like the schedulers):
    TestDirectedDamageIsQuarantined (a nested bucket rooted at its own
    page, a leaf's and the freelist's overflow count, a branch key outside
    the mapping, a branch element pointing at its own page, an overflow
    over a free page), TestDamageInUseIsQuarantined (a key moved outside
    the mapping under the open database, and a panicking write
    transaction), TestRandomDamageCorpus (cases 0..63 of the review's
    generator and six cases that grew without bound before). Against the
    previous spool every directed and in-use case fails (stack overflow,
    the heap bound, "fatal error: fault", a kept file) and the corpus
    sample with six failures; with the fix the full 3000-case campaign
    (`HOPCORDER_SPOOL_CORPUS=3000`) keeps 2273 files and quarantines 727,
    none failing, and the review's three kept files are quarantined at
    open.

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
    `attestations: write` are on `publish` only (round four: on the
    `attest` job only, which runs nothing else). It attests every file in
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
    (Round four: not with these flags. gh matches `--signer-workflow` as a
    regex anchored at the start only, so `images.yml.evil.yml` passed, and
    `--source-ref` ignoring case, so a tag `VX.Y.Z` passed; and without
    `--source-digest` any commit did. README and the notes now use
    `--cert-identity …/images.yml@refs/tags/vX.Y.Z`, compared exactly,
    and the tag's commit; see round four.)
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
    a template change. (Round four: a verified chart still pulled the
    mutable tag unless one did; a release's charts now carry their images'
    digests, and README installs the release's `.tgz`.)
  - **No environment-only credential.** Considered: an environment secret
    (a bot's classic PAT, as ghcr takes nothing finer) with a fallback to
    `GITHUB_TOKEN`. It only helps once no other workflow can write the
    packages, that is with this repository's Actions access on them set to
    Read. But the `image` job pushes `:main` and `:sha-…` to the same
    packages with `GITHUB_TOKEN`, so that would break main's images, and
    keeping them would take a separate package for main. Release files are
    protected by immutable releases, not by a credential. Left to the
    owner; not implemented. (Round four: once published. Until then anyone
    with write access can create a release of the pushed tag, or add files
    to the draft that `gh release create` uploads into, and the release
    attestation then vouches for those too; `publish` now checks a draft
    and fails loudly, see round four.)
  - Accepted: whoever can push a workflow file can publish something. The
    defence is detection plus the repository settings.
- **The tag is checked again once the release exists.** A release belongs
  to a tag name, and GitHub ignores `--target` (`target_commitish`) for a
  tag that exists, so `--target` would not pin the commit. checkout v7.0.1
  fails if the tag moved before the job checked out, but `publish` creates
  the release about a minute later. Now it resolves the tag through the API
  after creating the release, peeling an annotated tag. Unless the tag
  names `$GITHUB_SHA`, it deletes the release (the tag stays) and fails; an
  API error counts as a mismatch. (Round four: it peeled one level, so a
  genuine release of a tag of a tag was deleted, and so was one after any
  API error. It now peels every level, retries, and deletes only on a
  confirmed mismatch.) With immutable releases the tag cannot
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
- **Repository settings** (set by the owner on 2026-11-17; replace round
  two's list):
  - A tag ruleset (`VersionTags`) for `refs/tags/v*` restricting creation,
    update and deletion, bypassed only by the Repository admin role, not by
    GitHub Actions or an app. It keeps stray tags out of the Go module
    proxy and the release workflow, and makes `--source-ref refs/tags/v…`
    mean a maintainer's tag. (Round four: gh compares `--source-ref`
    ignoring case, and whether `v*` also matches `V…` was not checked, so
    README now names the tag in `--cert-identity`, compared exactly; a
    ruleset on all tags is recommended.)
  - Immutable releases: a published release's files and tag can no longer
    change (and `gh release verify` / `gh release verify-asset` check
    GitHub's attestation of the release). (Round four: that attestation
    shows a file is part of the release, not who built it; and nothing
    protects a release before it is published.)
  - Required reviewers on the `release` environment, whose deployment
    branches and tags are limited to `v*` tags (no branches). That gates
    `images.yml`'s publishing against mistakes and a stray tag run, not
    against collaborators. A release's `publish` job waits for approval
    under "Review deployments" in its run; with a single maintainer,
    "Prevent self-review" must stay off.
  - The ghcr packages (`hopcorder-agent`, `hopcorder-server`,
    `charts/hopcorder-server`, `charts/hopcorder-agent`): Actions access
    for this repository only, with the Write role, not Admin (the
    workflows push, but never delete versions or change a package's
    settings). Read for this repository needs main's images moved first
    (see the first bullet).
  - The main ruleset is enforced: force pushes are blocked, and these
    checks are required: `test`, `lint`, `vuln`, `rawsock`, `clickhouse`,
    `grafana`, `ui` and `helm` (ci.yml), `image-check (agent)`,
    `image-check (server)` and `release-assets` (images.yml). Both
    workflows run on every pull request without path filters, so each
    required check reports on each one; the tag-only jobs (`verify-tag`,
    `image`, `publish`) are skipped on pull requests and not required.
    Rename a required job and the ruleset must follow, or every pull
    request waits for a check that never comes. With `vuln` required, a
    newly published advisory blocks merges until the dependency is
    updated.
  - Give write access only to people you would let release.
  - Artifact attestations need a public repository (or GitHub Enterprise
    Cloud): if the repository were private, `publish` would fail at the
    first attest step, before it publishes anything.

### Security review, round four

A fourth review of the whole code base (the same four areas, with the
code added in round three first; every finding reproduced or, where only
GitHub could confirm it, marked as reasoned) found that Grafana's
read-only ClickHouse user was not a boundary, and that a batch's events
and its retries escaped the ingest bounds. Five fixes followed. The
decisions:

**Grafana's ClickHouse user**

- **Grafana's ClickHouse user has `readonly = 1`, an allowlist**
  (`grafana_limits` in `deploy/compose/clickhouse-init.sql`,
  `hopcorder_readonly` in the chart's `readonly.xml`). With `readonly = 2`
  and a list of constrained settings, every setting off the list was the
  viewer's to change, and some of them were not harmless (ClickHouse 25.8,
  2 GiB containers, the review's measurements):
  - Memory tracking off: `SELECT length(groupArray(number)) FROM
    numbers(300000000) SETTINGS max_untracked_memory = 100000000000,
    memory_profiler_step = 0` OOM-killed ClickHouse (without the settings
    it fails at the 1 GB limit), and so did 14 queries with
    `max_threads = 1000, use_concurrency_control = 0` (1,882 threads, each
    with up to 4 MiB untracked).
  - `use_query_cache = 1, query_cache_ttl = 100000000`: 1,100 queries
    pinned 1.1 GiB for good (the cache counted 4.3 MB of it).
  - The profilers (`memory_profiler_sample_probability = 1` with
    `max_untracked_memory` and `memory_profiler_step` at 1,
    `trace_profile_events`, `query_profiler_*`), `log_processors_profiles`,
    `opentelemetry_*`, `log_query_threads` and `log_query_views`: about
    1.5 MB/s of `trace_log` from 8 queries, the chart's 20 GiB volume in
    about four hours. `log_queries = 0` or `log_queries_min_type` kept a
    query out of `query_log`.

  Now a query may change only settings marked `CHANGEABLE_IN_READONLY`:
  `max_execution_time` (60 s, `MIN 1 MAX 300`), which the plugin sends.
  Every other one is refused with READONLY (164), in the query, as a
  request parameter and through a profile switch; "setting" one to the
  value it already has passes (ClickHouse skips unchanged values).
  `readonly` itself is CONST. `cancel_http_readonly_queries_on_client_close`
  still works, as it needs `readonly > 0`. The round-three spill CONSTs and
  the `MIN 1` limits stay, harmless, for the profile with `readonly = 2`.
  - What the plugin sends, read in the source and seen in ClickHouse's
    request log: 4.23.5 (`pkg/plugin/driver.go`) passes the data source's
    `customSettings` and, with `enableRowLimit`, `limit` as connection
    settings; clickhouse-go 2.50.0's HTTP transport adds `database`,
    `default_format=Native`, `client_protocol_version=54460` and, from the
    query's deadline, `max_execution_time` (the data source's
    `queryTimeout`, 60 s, plus 5), and `query_id` or `quota_key` only when
    set, which the plugin does not do. ClickHouse takes all of these but
    `max_execution_time` and `limit` as request parameters, not settings.
    Our data source has no custom settings and no row limit; either needs
    `CHANGEABLE_IN_READONLY`, and `queryTimeout` must stay at 295 s or
    below.
  - Checked with the real Grafana (13.3.1, the compose stack with the
    signed 4.23.5 plugin from Grafana's artifact bucket, as grafana.com is
    unreachable from the development container): its requests were
    exactly `client_protocol_version=54460&database=hopcorder&default_format=Native&max_execution_time=64`
    (14 for the health check), the health check passed and all 17
    dashboard queries returned rows (`go test -tags grafana ./dashboards/`).
    The review's attack queries, run against the chart's rendered
    `readonly.xml`, were refused before they ran.
  - Upgrade: the compose user is `OR REPLACE` now. A user's own settings
    win over its profile's, so the old user's `readonly = 2` would have
    survived a re-run of the script (checked: the old script, then the new
    one, gives `readonly = 1` and keeps the grant).
- **Grafana's memory limits fit the bundled ClickHouse**: 400 MB per query
  and 600 MB for all of the user's queries, were 1 GB and 2 GB (the chart's
  `readOnlyUser.maxMemoryUsage` and `maxMemoryUsageForUser`, the compose
  script the same). 2 GB was more than the chart's ClickHouse may use at
  all (0.8 × 2Gi, 1.6 GiB); 600 MB is 35% of it, and the rest is for the
  server's inserts and reads, ClickHouse's caches (at most 448 MiB) and
  ClickHouse itself (200–400 MiB measured). The dashboards need far less:
  on a week of eight streams (161,280 cycles, 1.9 million hop rows) their
  largest query took 17 MiB and all 17 together 41 MiB, over 24 hours or
  seven days. In a 2 GiB container with the chart's configuration, viewer
  queries holding what the old limits allowed (three of 255 MiB) got the
  server's 600 MB query killed in 3 of 3 runs (ClickHouse's overcommit
  tracker picks the query using the most); with the new limits (four of
  128 MiB, near the 600 MB cap) it succeeded 3 of 3 times. The values are
  sized for the default `resources.limits.memory` and must be scaled with
  it; the chart refuses a `maxMemoryUsage` below 1 (or not a number) or
  above `maxMemoryUsageForUser`.
  - Not taken: `memory_overcommit_ratio_denominator = 1` for the user, so
    that ClickHouse cancels Grafana's queries first when it runs out of
    memory as a whole. It did pick them, but cancelling them did not free
    the memory in time: the server's query still failed in 3 of 6 runs
    (4 of 6 without).
- **Accepted, and left to the owner: every query writes ClickHouse's own
  logs.** Round three's "without amplification" was wrong (corrected
  there): even with no setting changeable, each query, however short and
  whether it runs or is refused, adds rows to `query_log`,
  `processors_profile_log` and `trace_log`, 361 B per query after merging
  with the chart's configuration (plus about 640 B of `text_log` in
  compose, whose ClickHouse logs at trace level). One anonymous viewer got
  1,238 queries/s to ClickHouse through the compose stack's Grafana (50
  `SELECT 1` per `/api/ds/query`, 8 connections; a third refused as too
  many at once, and logged as well): about 0.45 MB/s with the chart's
  logging, which fills its 20 GiB volume in about 13 hours, well inside
  the 7 days ClickHouse keeps the logs. The profile cannot bound that
  without keeping queries out of `query_log`: refused queries are logged
  too, so a ClickHouse quota would not, and logging less per query (no
  processor profiles, settings or profile events) only took it to 103 B.
  The bound is the request rate at Grafana: keep anonymous and untrusted
  users away from it (the compose stack is a try-out on localhost), limit
  `/api/ds/query` at a proxy, watch the ClickHouse volume, or lower
  `systemLogRetentionDays`.
- Tests:
  - `TestGrafanaUserClickHouse` (`clickhouse` tag, CI job `clickhouse`)
    sends queries as the plugin does (its request parameters; not the
    server's client, which adds `readonly = 2` and other settings) and runs
    every dashboard query, checks `readonly = 1`, the cancel setting and the
    memory limits against the chart's budget, that `max_execution_time`
    can be set from 1 to 300 s, that a query stops when its client goes
    away, and that 41 settings (each variant above, the limits, `readonly`
    and the spill settings) are refused in the query and 6 as request
    parameters. With the old profile, 23 of the settings and 5 of the
    parameters were accepted.
  - `deploy/helm/ci/smoke.sh` (CI job `helm`, kind) checks that the chart's
    user has `readonly = 1` and the memory limits of
    `deploy/helm/ci/server-values.yaml`, and runs 24 of the settings as that
    user; the `helm` job's lint step checks the rendered `readonly.xml`
    (`readonly` 1 and CONST, `max_execution_time` the only
    `changeable_in_readonly`, the CONSTs, the limits from the values, the
    defaults under half of what a 2Gi ClickHouse may use) and refuses bad
    memory values. Both fail on the old chart. kind cannot run in the
    development container, so the smoke checks ran there against a
    ClickHouse container with the chart-rendered configuration.
  - The CI `grafana` job runs every dashboard query through the real
    plugin, unchanged.

**Read API**

- **Read views of planted data stay within a read budget that fits
  ClickHouse.** Data posted through ingest made the common read views cost
  ClickHouse about 1 GiB each (25.8, configured like the chart's, 2 GiB):
  20,000 cycles of 255 new addresses (26 s to post) took topology's nodes
  793 MiB and its edges 681 MiB (the number of groups, not round three's
  lambda), their 10,000 newest path changes (the UI's limit) 537 MiB for a
  response cut at 32 MiB (a 144 MB result block, which `max_result_bytes`
  with `break` cannot cut), 1000 of 10,000 maximal cycles 765 MiB, and the
  stream list over 660,000 planted streams 494 MiB. `query.max_memory_bytes`
  (1 GiB) times `max_concurrent` (8) was far more than the chart's
  ClickHouse may use (1.6 GiB): two simulated UI viewers of such a stream got
  502 for 9 of 12 topology and path-change requests, four for 15 of 16
  (ClickHouse's overcommit killed readers; ingest stayed at 200). Now, on the
  same data, no read query of the stream view or the stream list takes more
  than 117 MiB, and both runs got 200 throughout (ingest 80 of 80):
  - Topology's nodes and edges and the stream list have a group limit
    (`max_rows_to_group_by`, `group_by_overflow_mode = 'any'`: ClickHouse
    keeps aggregating the groups it has and skips new ones): 20,000
    distinct nodes and edges, 10 and 4 times the caps, so that a response
    it cuts has more rows than the cap and says `truncated`; and
    `ingest.max_streams` streams (or `limit`, if more), about 800 bytes each
    per thread. Nodes now 64 MiB, edges 117 MiB, targets 74 MiB.
  - `path-changes` runs in two parts: the newest rows' times and a lower
    bound of their JSON (computed before the sort:
    `query_plan_execute_functions_after_sorting = 0`; otherwise ClickHouse
    carries the arrays through it), then whole rows only from the time of
    the first row past the response budget, which cuts the response there at
    the latest. 54 and 96 MiB, the same response.
  - `cycles` with a protocol filters the stream by equalities, so ClickHouse
    reads in the sort key's order and stops after `limit` (the `'' OR` form
    of the stream filter defeated that; the primary key was used either
    way): 52 MiB. Without a protocol (streams merged, so sorted) a first
    query finds the times of the cycles to return, and the sort gets only
    those: 82 MiB.
  - Every read query: `max_threads = 4` (memory for reading and aggregating
    grows with the threads, which default to the node's cores; reading 255
    new addresses per cycle costs about 40 MiB a thread),
    `max_block_size = 1024` (the result backstop stops only between blocks,
    and a group limit is exceeded by up to a block of new keys per thread),
    `output_format_parallel_formatting = 0` (it keeps the text of whole
    blocks: 1000 maximal cycles were one 200 MB block, 522 MiB for the query,
    106 MiB without) and `max_bytes_before_remerge_sort` a quarter of the
    budget (a sort with LIMIT keeps only its top rows once it holds that
    much; ClickHouse's default waits for 1 GB).
  - **Budget**: with `clickhouse.deploy` the chart sets
    `query.max_concurrent` to 2 and `query.max_memory_bytes` to 80% of the
    memory limit / 3 / `max_concurrent` (273 MiB for the default 2Gi; it
    fails on a limit it cannot read), unless `config.query` sets them:
    reads get a third of what ClickHouse may use, the rest is for its
    caches (up to 448 MiB), inserts, merges and the read-only user. Two at
    a time with more each rather than four with less: a query over its
    budget fails, a request beyond the slots only waits (up to 10 s), and
    the stream view's five requests take milliseconds on real data. The
    rule for an external ClickHouse, in the docs: their product at most
    about a third of its `max_server_memory_usage`. The server's defaults
    stay (8 x 1 GiB, for a large ClickHouse).
  - Accepted: planted data still fills the views of its own streams, now
    cut by the caps and the budget (the cost of ingest without
    authentication, §2). A node's median keeps up to 8192 samples, so a
    stream with some 20,000 distinct nodes of thousands of cycles each
    (several hundred thousand planted cycles) can still exceed the budget;
    that view then fails (502) at the budget, not at ClickHouse's limit.
    The test's layout of parts made the edges 190 MiB; with a smaller
    ClickHouse than 2Gi (the CI's 1Gi: 136 MiB a query) that view of
    planted addresses fails the same way.
  - Tests: `TestReadViewsOfPlantedDataClickHouse` (`clickhouse` tag) plants
    the review's data by SQL (20,000 cycles of new addresses with their
    path changes, 1500 maximal cycles, 660,000 streams) and runs every view
    with 273 MiB a query: before, each got 502; now each answers within it,
    truncated where it should be and newest first. `TestReadQuerySettings`,
    `TestReadGroupLimits`, `TestPathChangesReadOnlyWhatFits` and
    `TestCyclesOrder` check the settings, the limits and the two-part
    queries; the CI `helm` job checks the chart's budget.
- **A streamed response lasts as long as its client reads.** The ClickHouse
  client's `http.Client.Timeout` (`clickhouse.timeout`, 30 s) also covered
  reading the result, so a streamed read-API response was cut off 30 s after
  its query started, however fast its client read: `cycles?limit=1000` of
  MPLS-heavy cycles (33 MB) read at 500 KB/s ended after 17.9 MB, aborted,
  with an ERROR logged and a query error counted, and no client below about
  1.1 MB/s could fetch a full-size response. `bufio.Scanner` also passed the
  line the timeout cut on as a last row (the review's "unexpected end of
  JSON input"; unreshaped, the partial row went into the response). Now:
  - No `http.Client.Timeout`. Statements, inserts and pings keep the
    timeout for the whole exchange (a context deadline; unchanged). A query
    has it for sending and for ClickHouse's answer, which comes once the
    query has finished (`wait_end_of_query`; ClickHouse stops it at the
    timeout less 5 s), and then for each wait for more of the result,
    timed only while a read waits for ClickHouse, not while the caller
    handles rows.
  - A result that ends inside a row is an error, and the row is not passed
    on: a split function refuses a last line without its newline
    (JSONEachRow ends every row with one); the scanner reports the read
    error, if there was one.
  - Accepted: a caller can take as long as it likes over a result; the read
    API bounds its clients (each part within `writeProgress`, all of it
    within `WriteTimeout`), and ClickHouse keeps the finished result in a
    temporary file meanwhile (round three).
  - Tests: `TestQueryResultOutlastsTimeout` (300 ms timeout; a fake
    ClickHouse sending two rows every 100 ms for 1.5 s, and 8 MB at once
    to a caller taking 1.6 s: every row; before, 6 of 30 and 705 of 8000,
    the last of them cut), `TestQueryCutRowIsAnError` (a broken connection,
    a stall past the timeout and a clean end, each inside the second row:
    the first row and an error; before, the cut row was passed on),
    `TestTimeouts` (queries, inserts and statements without an answer in
    time still fail).
- **The chart's Host check matches the server's.** Its regular expression
  took `10.0.0.5:8080`, `cafe:443`, `::::` and 300-character names, which the
  server refuses at start: the pod crash-looped instead of the install
  failing. Both now take host names by label rules (labels of 1 to 63
  letters, digits, `-` and `_`, not starting or ending with `-`; at most 253
  characters and a trailing dot; the last label not all digits, which
  would be a malformed IPv4 address), IPv4 as a dotted quad (0-255, no
  leading zeros), IPv6 in RFC 3986's forms, bare or in brackets, and
  `*.domain` of two labels at least; no ports. The chart may refuse what the
  server takes (an IPv6 zone, an IPv4 address in brackets), never the
  reverse, and it checks `config.allowed_hosts` the same way now.
  - Checked: 600,000 random host-like strings through the chart's
    expressions (rebuilt in Go): 35,877 accepted, every one by the server
    too; 51 chosen entries through `helm template`, the same.
  - Tests: `TestHostAllowlist`; the CI `helm` job's refusal checks (both
    values, and that IPv6, wildcards and a 253-character name pass).
- **Wildcards on a top-level domain are refused.** `*.com` allowed
  `attacker.com`: round three's argument holds only for a domain whose names
  the operator alone can create. A single label after `*.` is refused, in
  the config and the chart. Accepted: shared domains (`*.github.io`, hosting
  or dynamic-DNS domains, `*.co.uk`) cannot be told apart without a public
  suffix list; the docs warn about them. Tests: `TestHostAllowlist`, the
  `helm` job.
- **Out-of-range times are the client's error.** `from`/`to` outside
  1970-2262 (a record's ts range), e.g. `to=9223372036854775807` or negative
  milliseconds, gave 502, an ERROR log line and a query error per request
  from any client; now 400, and so does a span of more than 400 days (the
  timeline's largest buckets are a day, at most 400 of them; longer ranges
  take several requests). Test: `TestReadTimeRange` (no query runs).
- **Read-API responses carry `Cache-Control: no-store` and
  `X-Content-Type-Options: nosniff`**, errors included (the UI's had
  nosniff already). Test: `TestReadAPIHeaders`.

**Ingest and storage**

- **A batch's events are stored part by part.** `derive` collected the
  events of a whole batch and `insertEvents` sent them in one insert per
  table. Records without hops whose `reached` and `target_ip` flip every
  cycle make a route change (reason target) and an outage row each: one
  32 MiB request held 213 MiB beyond its body (inserts of 48 and 43 MiB),
  and four clients OOM-killed the server under the chart's 1 GiB in 3 of 3
  runs (GOMEMLIMIT is a soft limit). Now the events of each cycles part
  are derived once the part is stored, inserted in inserts of at most
  `maxInsertBytes` (8 MiB) of JSON, encoded one insert at a time, and
  dropped before the next part. Measured with `measureStore` (live heap
  after a collection at each insert, beyond the request body): the review's
  request 213 MiB before, 66 MiB now past validation (which refuses it,
  next item); its cheapest valid form, a reply hop every other record,
  146 → 61 MiB; two IPv6 paths of 255 hops in turn (a route change at
  every TTL) 114 → 52 MiB. Sizing: about `max_concurrent x (3 x
  max_decoded_bytes + 32 MiB)` plus the stream state and the read buffer,
  512 + 170 + 32 MiB with the defaults (corrected in round three's text,
  `server.example.yaml` and the chart's `values.yaml`).
  - Test: `TestRequestMemoryEventFlood` (both shapes at 32 MiB: every
    insert within 8 MiB, events in several, peak within 3 x 32 + 32 MiB;
    before, one 36 MiB `path_changes` insert and 146 MiB).
- **Records must be what `Finalize` makes of their hops** (`record.Validate`):
  the path ends at its first `reply` or `unreachable`, `reached` says
  whether a reply came, and `path_hash` and `loops` are those of the hops.
  `reached: true` without a reply hop was accepted, the flood's cheapest
  form. Every agent sends records as `Finalize` made them (its rules have
  not changed since before the spool existed), so only records no agent
  produces are refused; the checks are one SHA-1 and one loop scan per
  record. Tests: `TestValidateRejects` (eight new cases, all accepted
  before), `TestFlipRecordsValid`.
- **A retried batch is stored once, with its hop and loop rows, and derives
  its events once** (this replaces the M2 and round one reasoning). An
  insert into `cycles` is not all-or-nothing: ClickHouse 25.8 commits the
  `cycles` part, then runs its views one after another, in an order that
  differs between databases; an exception in one leaves the part and the
  views before it written, skips the rest, and fails the insert. An insert
  can also outlive the client's timeout and commit later. The mark came
  from `agent_seq_mv`, one of those views, and was re-read after a failure,
  so the review saw: the retry answered "duplicates" while 51,000 hop rows
  and the batch's outage were gone; or every cycle stored twice (2.1% of an
  agent's cycles, 150 s against a ClickHouse short of memory); or a late
  commit's events never derived. Now:
  - A part lies within one month (a partition of `cycles` and its views),
    so ClickHouse stores it as one part of `cycles` (checked: 21,000 rows,
    8 MiB, one part): whole or not at all. Parts are inserted in seq order
    and a batch stops at the first that fails.
  - The server writes the mark into `agent_seq` (migration 0004 drops
    `agent_seq_mv`) once a part is stored with its hop and loop rows (the
    insert succeeded, so every view ran: the ingester's writes set
    `materialized_views_ignore_errors = 0` and `async_insert = 0`, whatever
    the user's profile says) and its events were derived. The
    mark in memory moves with it; a failed mark insert is logged and only
    makes the stored mark lag.
  - After any failed write, and when an agent's state is loaded (a restart,
    or the state was dropped), the agent is unsure. Its next batch first
    stops the agent's writes that still run: every write of its cycles has
    the query_id `hopcorder/<agent_id>/<random>`, and `KILL QUERY WHERE
    startsWith(query_id, …) SYNC` ends them, a previous process's too, so
    what is stored no longer changes. Then, per part, it looks up which of
    the part's seqs `cycles` holds (by agent, the part's seq range and its
    ts range: ts is in the sort key, seq is not), writes their missing hop
    and loop rows from `cycles` with the views' own queries, only where
    missing (`INSERT … SELECT … WHERE (seq, ttl) NOT IN (…)`, so it can run
    any number of times), inserts only the other records, derives the
    events of all of them in seq order and moves the mark. A part whose last
    record is not stored has nothing of the agent stored above it (parts
    are whole and in order), and the agent is sure again. Without failures
    this costs one KILL and one lookup for each agent whose state is
    loaded: after a restart, and for an agent_id new to the server or
    dropped by it.
  - The guarantee: while one server at a time writes an agent's cycles,
    after any mix of view failures, client timeouts with late commits,
    ClickHouse or server restarts and agent retries, with any batch
    boundaries, each cycle the agent sends is stored once in `cycles` and
    its hop and loop rows once in `hops` and `loop_events`, the mark (stored
    and in memory) never covers a cycle whose rows are missing, and each
    stored cycle's events are derived once, in seq order, by the batch that
    stores or finds it (none if its stream has no state then: restore
    budget or no room, as in rounds two and three). Only a crash between
    storing a part's events and its mark (milliseconds) derives them a
    second time, from the stream as a restore leaves it; `path_changes` and
    `outages` are `ReplacingMergeTree`s read with `FINAL`, keyed by stream
    and seq, so the second rows replace the first (a restored tracker can
    know fewer of the earlier addresses, so a route change may list fewer
    TTLs).
  - Why not ClickHouse's insert deduplication (`insert_deduplication_token`
    with `non_replicated_deduplication_window` and
    `deduplicate_blocks_in_dependent_materialized_views`): a token
    deduplicates only a retry of the same block, and after a restart the
    server no longer has the failed part, while the agent's retry may be
    shorter or longer (its spool grows; enrichment changes row sizes and so
    the parts). The window would also have to outlast every retry's backoff
    under all agents' inserts. Looking at what is stored works across both
    and needs no table settings. Making `cycles`, `hops` and `loop_events`
    `ReplacingMergeTree`s would not do either: the dashboards, the read API
    and the prototype's queries read them without `FINAL`.
  - The server's ClickHouse user must see and stop its own queries:
    `GRANT SELECT(query_id, user, query) ON system.processes` (the default
    user and the images' `CLICKHOUSE_USER` have it; without it KILL QUERY
    fails with ACCESS_DENIED, checked on 25.8). Migrate checks it with a
    KILL that matches nothing and keeps the server unready otherwise.
  - `hopcorder_server_records_total{result="recovered"}` counts cycles
    found stored; each such batch logs one line.
  - Accepted: a write ClickHouse has received but not started when the
    KILL runs (it would have to hold a received request for longer than
    the 30 s client timeout) can still commit after the lookup, and its
    cycles are then stored twice. Two servers writing the same agent's
    cycles at once (the mark is kept in memory, as before) can store a
    batch twice; the chart runs one replica with `Recreate`. Cycles a
    failed insert stored that the agent never sends again (its spool
    dropped them past `max_age`) keep their missing rows and derive no
    events. Events whose own insert fails (three attempts within the
    batch's 10 s) are still lost, logged and counted; the mark moves on.
    Damage done before the upgrade (marks past missing rows, duplicates)
    is not repaired. A lookup reads the agent's cycles within the part's
    ts range once per unsure part: a part spanning the 30 days of
    `max_record_age` reads that much.
  - Rolling back below migration 0004: recreate `agent_seq_mv` first (the
    statement is in the migration), or that version, which does not write
    the mark, stores retried batches twice after its next restart. Rolling
    forward again: drop it (the server refuses to start while it exists).
  - Tests (`clickhouse` tag unless noted): `TestViewFailureRetriedExactlyOnce`
    (a constraint on `hops`, or on `loop_events`, fails `hops_mv` or
    `loop_events_mv` for the part of seqs 41–60; retried as 41–50, 41–90
    and 91–120, in the same process and after a restart; every table
    compared with an agent that ingested the same records cleanly, rows
    counted with their multiplicity, events as `FINAL` reads them. Before:
    20 cycles stored twice, or 570 of 909 hop rows, 4 of 24 loop rows, 6 of
    29 route changes and 2 of 13 outages missing),
    `TestLateCommitRetriedExactlyOnce` (`hops_mv` slowed by a constraint
    that sleeps, a client timeout of 700 ms; retried while the insert runs
    and after it committed, with and without a restart. Before: 23 of 29
    route changes and 10 of 13 outages never derived; without the KILL,
    810 hop rows twice),
    `TestPartFailureMidBatchClickHouse` (the third of several parts fails;
    a restarted server's retries in other boundaries), `TestFillMatchesViews`,
    `TestRetryAfterCommittedFailure` (default run: an insert that stores
    and fails; before, the batch's route change and outage were lost),
    `TestWriteSettings`.
- **Views change with `ALTER TABLE … MODIFY QUERY`, never DROP and CREATE.**
  Migrations 0002 and 0003 drop and recreate `hops_mv`, assuming no insert
  can come between: another server can (a rolling upgrade), and stores
  cycles without hop rows; a migration that dies between the two
  statements, followed by a rollback to a version that has it recorded as
  applied, leaves no view, and every cycle from then on has no hop rows,
  silently. Their comments are corrected; the rule is in `Migrate`'s
  comment, and `fillViews`' queries change with the views. Migrate also
  checks at every start that `hops_mv` and `loop_events_mv` exist and
  `agent_seq_mv` does not, and keeps the server unready otherwise, naming
  the migration to recreate a view from. Test: `TestMigrateChecksViews`.
- **The restore's open-outage lookup is bounded by the last replayed cycle
  that reached the destination** (the outage started after it), when there
  is one. Round two left it unbounded because "outages holds a row per
  outage, not per cycle"; a stream that flaps every cycle, an attacker's
  own for instance, has one per two cycles (review: 1M rows read, 45 to
  78 ms per restore with 2M cycles). A stream down for the whole replay is
  still looked up without a bound. Test: `TestRestoreOutageLookupBounded`
  (default run).

**Agent**

- **The metrics path is compared, not parsed.** `outputs.metrics.path` was
  only checked for its leading `/` and then registered with
  `http.ServeMux`, which panics on a pattern it cannot parse (`/metrics x`
  reads as a method, `/{x`, `/a{b}`, `/ `, `/x/{a}/{a}`, `/{x...}/y`). The
  panic hit `runDaemon`'s goroutine, so a reload to such a path ended the
  agent with status 2, and systemd's restarts failed the same way until the
  file was fixed. Now `Finalize` accepts only `/` and then unreserved URL
  characters and slashes (`[A-Za-z0-9/._~-]`, 256 characters in all), so
  such a path is an ordinary configuration error (a reload keeps the
  running configuration, a start fails with the message), and
  `MetricsServer` compares the request's path with it instead of using a
  ServeMux, so no value can panic it whatever reaches `Serve`.
  - Cost: a path ending in `/` serves only itself, not the subtree ServeMux
    gave it; `//metrics` and other unclean forms get 404, not a redirect.
  - Tests: TestMetricsPath (the review's six paths and others refused),
    TestMetricsServer (a path ServeMux panicked on is served exactly),
    TestReloadRefusesBadMetricsPath (six reloads to the review's paths keep
    the agent running; before, the first panicked the test binary).
- **A reload holds both configurations' flows until the new one runs.**
  `runDaemon` called `HoldFlows` with the new configuration's flows first,
  which released the running one's echo identifiers before anything else
  of the new configuration had started. When the start then failed (the
  JSONL file, TLS or secret files, the spool lock with its 1 s timeout,
  `agent.New`), the supervisor reverted, but the old identifier had been
  free meanwhile: the review's three uid 65534 squatters won it at the
  first try, and the reverted configuration's ICMP targets were refused
  (`hopcorder_flow_in_use` 1) until they let go. That is the denial round
  three accepted only before the first start. Now the supervisor owns the
  holds, as it owns the metrics listener: `supervisor.start` holds the
  flows of the configuration that started last together with the new
  one's, and releases the old ones (`HoldFlows` with the new flows alone)
  only once the new configuration runs. A failed start leaves both held;
  the revert's start holds the old flows alone again, which releases the
  failed configuration's. `runDaemon` holds flows itself only without a
  supervisor (`daemonEnv.flowsHeld`), and `--once` as before.
  - Tests: TestFailedReloadKeepsTheFlowsHeld (fake prober recording every
    `HoldFlows`: a reload to another ICMP `flow_id` with a missing JSONL
    directory never leaves the running flow out, the revert ends with it
    alone, and a reload that starts holds both until it runs, then the new
    one alone; before, the failing start held only its own flow).
    TestInnerHoldFlows (rawsock) checks what this rests on: a uid 65534
    process can bind neither identifier while both are held, and gets the
    old one once only the new one is.
- **A configuration that stops waits a bounded time for its outputs.**
  After `a.Run` returned, `runDaemon` gave the outputs up to
  `shutdownGrace` to catch up, cancelled the drains, and then waited for
  them without a bound, while `JSONL.Deliver` ignored its context. A write
  that blocks (a FIFO for a log shipper that is not reading, stdout under a
  blocking log driver, a hung NFS mount) held up the reload or the
  shutdown for as long as it blocked: in the review, after SIGHUP with the
  JSONL output on a FIFO nobody read, nothing was probed again, scrapes got
  200 with frozen numbers, and SIGTERM was ignored (systemd kills after
  `TimeoutStopSec=15s`). Now:
  - `output.StartDrains` and `Drains.Stop` replace the WaitGroup. Stop
    cancels the drains and waits up to `shutdownGrace` again; a drain still
    delivering then is left behind, with an error in the log and
    `hopcorder_output_abandoned_total{output}` (kept for the process: the
    stuck delivery outlives its configuration). Its batch is not
    acknowledged, so the next configuration delivers it again. A stop takes
    at most twice `shutdownGrace` (6 s), plus closing the spool.
  - JSONL writes take deadlines where the file does: a FIFO or pipe opened
    by path (Go makes it non-blocking and polls it, on Linux) or a
    non-blocking stdout. When the drain's context ends, `context.AfterFunc`
    sets a deadline in the past, so such a write returns at once and
    nothing is left behind. Regular files and a blocking stdout take no
    deadlines; the bound is for them.
  - The metrics endpoint answers 503 as soon as `a.Run` returns, not only
    once `runDaemon` does.
  - Found on the way: JSONL to a FIFO or pipe never worked. `fsync` fails
    with EINVAL there, so every delivery failed after its write and was
    written again on every retry. Only regular files are synced now.
  - Accepted: a delivery left behind keeps its goroutine and its batch (up
    to 1 MiB) until its write returns, one per stop that meets it. On a
    blocking stdout the next configuration's JSONL writes queue behind it
    (Go serializes writes to a file), so the next stop leaves that drain
    behind too. A spool on a hung file system still hangs the agent, whose
    appends block as well.
  - Tests, without root (a FIFO in a temporary directory, and a full
    blocking pipe as `os.Stdout`): TestReloadWithJSONLOnAFullFIFO and
    TestReloadWithJSONLOnABlockedStdout (the reload completes, metrics
    answer 503 in between; the FIFO's write stops at its deadline, the
    stdout drain is left behind and counted once),
    TestShutdownWithJSONLOnAFullFIFO and
    TestShutdownWithJSONLOnABlockedStdout (cancelled as SIGTERM does, the
    supervisor returns within twice the grace plus 2 s),
    TestJSONLWriteHeedsItsContext, TestJSONLToAFIFO and
    TestDrainsStopLeavesStuckDeliveryBehind. Against the previous code the
    four daemon tests hang (the reload never completes, the supervisor
    does not stop), and the two JSONL tests fail (the write ignores its
    context; "sync: invalid argument").
- **`checkPages` picks the meta page exactly as bbolt does.** It refused a
  meta page whose page size differed from the file's and fell back to the
  other one, while bbolt (`DB.meta`, v1.5.0) validates only magic, version
  and checksum and uses the one with the higher txid. A crafted file (the
  older meta rewritten with txid + 1, twice the page size and a valid
  checksum, rooted at a free page) passed: `checkPages` checked the honest
  tree, bbolt used the crafted one, and `tx.Check` ran on it unguarded in
  its own goroutine. The review's three files faulted ("fatal error:
  fault") or descended for ever (the heap past 512 MiB, the real binary at
  1.66 GB in 6 s) on every start, and were never quarantined. Random damage
  cannot do this (the checksum covers the page size); it takes write access
  to the spool file (the agent's user, root, a pod sharing the hostPath).
  Now `meta` orders the pages by txid and validates them as bbolt does,
  then requires the page size of the one it chose to be the file's: a file
  whose meta bbolt would use fails the check, and is quarantined, instead
  of being checked by the other meta. The page size the check is given is
  bbolt's (`Info().PageSize` of the read-only open, which takes it from
  page 0 when that meta is valid), so the two now read the same pages.
  - Tests: TestCraftedMetaIsQuarantined (the review's three files, built
    from `buildSpool` with meta page 0 the newer: `checkPages` refuses
    each, and two starts in the child process quarantine it; before,
    `checkPages` accepted all three, and two of them crashed the child, by
    a fault and by the heap bound).
- **A stored record decodes to at most 8 MiB.** The spool's zstd decoder
  had the library's default bound, 64 GiB, and `Read` decoded every record
  whole (`DecodeAll`). A 48 KiB stored value (RLE blocks, no content size)
  decodes to 1.5 GiB: every start went past the child's 512 MiB heap bound,
  and the real binary was OOM-killed in a 256 MiB cgroup (the chart's
  limit) on 3 of 3 starts. Neither bbolt nor `checkPages` can see this, so
  the file was never quarantined, and the records after it never shipped.
  Now `WithDecoderMaxMemory(maxRecordSize)`, 8 MiB: the largest record the
  agent can write is about 220 KB of JSON (255 hops, each with an IPv6
  address with zone and 16 MPLS labels, plus loops; measured), so the
  format has room to grow more than 35 times over, and 8 MiB is also the
  largest window any zstd encoder level declares (the spool writes
  SpeedFastest, at most 4 MiB), which the decoder caps at the same bound,
  so no record a spool wrote is refused. A bomb costs at most 8 MiB per
  read of it. `ErrDecoderSizeExceeded` takes the path of every
  undecodable record: `Read` skips it, logs its seq at error level, and
  moves the cursor past it (it is not counted in a metric, as before).
  - Accepted: the zstd library prints one line to stderr ("runDecoder:
    maxDecodedSize exceeded") for each such record it refuses.
  - Tests: TestDecompressionBombIsSkipped (the review's bomb, opened in
    the child process twice: skipped, the other records delivered; before,
    the heap bound on both starts) and TestRecordSizeIsBounded (a value
    just over the bound is skipped, a 2 MiB record still decodes; before,
    the value was delivered).
- **The spoofing floor is stated, and alerting on it recommended.** Round
  three's 32 random bits hold (the review re-ran TestRawNetns/spoof: no
  fake hop), but at line rate they are a rate, not a wall. An ICMPv4 error
  for an IPv4 probe that quotes 8 bytes needs the probe id and the IP ID,
  a TCP SYN-ACK or RST the 32-bit sequence number. Measured in the review,
  3.4·10^-4 of forgeries hit a key in flight (2.7·10^-4 for TCP), so about
  5·10^-9 per packet get through. Against a target whose probes stay in
  flight (one that does not answer), a spoofer at 1 GbE line rate (about
  1.4 Mpps) plants a fake hop or a fake TCP `reached` about every 2 to 3
  minutes with probes always in flight, about every 12 minutes at the
  default 1 s timeout and 5 s interval, and about every 1 to 2 minutes at
  10 GbE (the review's estimates). Echo replies need 48 bits, ICMPv4
  errors that quote the payload 64.
  - Not changed: the TCP timestamp option would give SYN-ACKs 32 more bits,
    but only for TCP, and only where the destination and every middlebox
    keep the option.
  - Every accepted forgery is preceded by about 65,536 near misses, which
    `hopcorder_probe_mismatched_replies_total` counts. README gives a rule
    (`increase(...[10m]) > 600`): a slow spoofer has about a 1% chance of
    one forged hop when it fires, one at line rate is caught within a
    scrape or two, about when its first hop lands.
- **Targets with the same host and protocol share their series, and the
  flow-in-use gauge counts them.** Configs may list one host twice with
  another `port` or `af` (dual-stack, or two TCP ports), and the dedup key
  allows it, but per-target metrics are labelled with the configured host
  and protocol only. So both targets write the same series: counters add
  up, the RTT histogram mixes them, `hopcorder_path_length` is the last
  cycle's, and `hopcorder_flow_in_use`, set to 0 or 1 by each cycle,
  flapped when only one of them was refused (with two `flow_id`s), so an
  alert on it with a `for:` never fired. Not refused in the config:
  dual-stack and several ports to one name are what operators want to
  record, and giving them apart labels (port, af) would change every
  per-target series for everyone. Instead the gauge is the number of the
  series' targets refused (1 for one target, as before), the agent warns
  at start about targets that share series, and README says so; the labels
  stay as bounded as they were.
  - Tests: TestFlowInUseCountsTheSeriesTargets (two targets on one series:
    one refused, the other's cycle leaves the gauge at 1; before, 0) and
    TestWarnSharedSeries.
  - Corrected in round five: with one `flow_id`, which the config allowed
    and is the default, such targets were not only one series but one
    stream on the server, whose route changes and outages were wrong
    (every cycle a `target` change). They are still what operators want to
    record, but each now needs its own `flow_id`, and the agent refuses
    them otherwise; see round five. Their metrics still share series.

**CI, release and deployment**

- **Verifying names the workflow file, the tag and the commit exactly.**
  README's command (`--signer-workflow …/images.yml --source-ref
  refs/tags/vX.Y.Z`, no `--source-digest`) let a collaborator's build pass,
  as gh 2.89 reads the flags (its source, with sigstore-go 1.1.4):
  `--signer-workflow` becomes the regex `^https://github.com/<value>`,
  anchored at the start only and with unescaped dots, so a workflow
  `images.yml.evil.yml` (or `imagesXyml.yml`) of any tagged commit passed;
  `--source-ref` and `--source-digest` are compared ignoring case, so a tag
  `V0.1.2` at their own commit, whose `images.yml` triggers on `V*`, passed
  for `v0.1.2` (if the `v*` ruleset does not cover `V*`, which the review
  could not check); and an empty `--source-digest` checks nothing. Then
  ghcr Write let them repoint `:0.1.2`, `:latest` and the chart. Now:
  - `--cert-identity
    https://github.com/Saremox/hopcorder/.github/workflows/images.yml@refs/tags/vX.Y.Z`
    (sigstore-go compares the certificate's SAN with `!=`: exact, case
    included), `--source-ref refs/tags/vX.Y.Z`, `--source-digest <commit>`
    and `--deny-self-hosted-runners`, for files and `oci://` images, in
    README (in full, as a shell function `verify`) and in the notes
    (`scripts/release-notes.sh`).
  - README takes the commit from the tag, not from the notes, which belong
    to the release (anyone with write access can create a release of a
    pushed tag before `publish` does):
    `git ls-remote … 'refs/tags/vX.Y.Z^{}'`, keeping exactly that ref, since
    ls-remote matches patterns against the end of ref names and also lists
    a tag `a/refs/tags/vX.Y.Z^{}`; `${commit:?}` refuses to verify without
    one.
  - The SAN's spelling (`Saremox/hopcorder`, as `github.repository` has it)
    and the flags were checked against gh's source, not against a real
    attestation: v0.1.1 has none. The first release after this is the test.
  - `scripts/verify_test.go` runs the notes' commands and README's blocks
    with a fake `gh` and `git` and checks every `gh attestation verify`
    against a model of gh 2.89's checks: the release's certificate passes;
    a `V` tag, `images.yml.evil.yml`, `imagesXyml.yml`, a branch, another
    commit and another repository fail; no verify runs without the commit;
    round three's flags accept the `V` certificate, as the review found.
- **The release is published only once it is exactly this run's.**
  Immutable releases protect a release once it is published, not before.
  After the tag is pushed, `publish` waits for verify-tag (ci, up to 25
  minutes), the builds and the approval; meanwhile anyone with write access
  could create and publish a release of the tag (the tag ruleset is not
  involved). And `gh release create` with files creates a draft, uploads,
  then publishes, and a draft can be changed: an added file stays for good.
  GitHub's attestation of the release then vouches for their files:
  `gh release verify-asset` checks a SAN of `*.releases.github.com` only,
  so it proves a file is part of the release, not who built it. Now
  `scripts/release-publish.sh` (the last step of `publish`):
  - creates the release with `--draft` and finds it again by its id (`gh
    release view/edit <tag>` ask for the published release and a draft of
    the tag at once and take whichever answers first; GitHub allows
    several drafts of a tag, and a draft next to a published release). It
    must be the only release of the tag, drafts included.
  - compares its files with `dist/` by name and GitHub's SHA-256 digest of
    each asset (uploaded ones; a digest not computed yet is asked for
    again): none missing, none different, none added; and its tag, title,
    notes and pre-release flag.
  - checks the tag (below), publishes by id (`PATCH draft=false`, which is
    what gh does after uploading; if the answer is lost, the release says
    whether it was published), then checks the files and the tag again.
    A warning if GitHub does not report the release as immutable.
  - Before publishing, any difference deletes this run's draft and fails,
    saying that someone else created or changed the release, with each
    asset's uploader; someone else's release stays, to be looked at. After
    publishing, a difference GitHub confirms deletes the release (the tag
    stays), as a moved tag did.
  - `release-verify.sh` says what "a release of vX.Y.Z exists already"
    means: no run of this workflow published it, so someone else did; look
    at it, point no one at it, release the next version. A draft (which
    only `publish` sees) is a cancelled run's or someone else's: look at
    it, delete it, run `publish` again.
  - Accepted: someone else's release of a pushed tag, published first, stays
    and is immutable; `publish` only refuses to add its own and says so.
    The build provenance tells theirs apart (`verify` fails for files
    `images.yml` did not build for the tag's commit). Fewer people with
    write access is the only prevention.
  - `scripts/publish_test.go` runs the script against a fake GitHub
    (releases, refs and annotated tags kept between calls; failing calls;
    someone else acting between the script's calls): a release of the tag
    published or drafted by someone else, a file added to, replaced in or
    removed from the draft, its notes changed, a file added and someone
    else's release published right before publishing, digests computed
    late, a missing tag. The step it replaces passes none of the
    someone-else cases.
- **The tag check peels every annotated tag and deletes only on a confirmed
  mismatch.** It peeled one level, so a tag of a tag (`git tag -a v0.2.0
  v0.2.0-rc.1`, about which git only prints a hint) passed every earlier
  check (`^{commit}` peels fully) and then had its genuine release deleted,
  after the images, charts and attestations were out; and any API error
  counted as a mismatch. Now it follows annotated tags (up to 10 levels),
  tries each API call up to 5 times, 10 s apart, and deletes the release
  only when GitHub says the tag names something else; when GitHub cannot
  tell, it fails without deleting and says what to check by hand. It runs
  before publishing (a moved tag deletes the draft; nothing is published)
  and after. Tested: lightweight, annotated and nested tags kept; a tag
  moved before and while publishing; API errors that pass; errors that do
  not, before (draft deleted) and after publishing (release kept).
- **Install: download, check, unpack without owners.** README piped
  v0.1.1's archive into `tar -xz`. Its entries belong to 1001/1001 (round
  three's root-owned archives start with the next release), so unpacked as
  root uid 1001 owned the binary, and nothing was saved to check. Now
  `curl -fLO`, `sha256sum -c SHA256SUMS` (v0.1.1 has no attestations; README
  says this checks the download, not who built it) or `verify` for later
  releases, then `tar --no-same-owner -xzf`; `TestReadmeInstall` fails if a
  README block pipes into tar, unpacks without `--no-same-owner` or before
  a check. Move the examples to v0.1.2 once it is out.
- **A release's charts run its images by digest.** A verified chart still
  pulled `:X.Y.Z` (`IfNotPresent`), which any workflow with `packages:
  write` can repoint, so new nodes would run a replaced root agent.
  - `release-assets` needs the `image` job and, for a tag, its success; on
    pull requests (`image` skipped) and main (its result ignored) it runs
    as before, as the dry run, with made-up digests. A tag's binaries and
    charts are now built after its images, not alongside.
  - `release-build.sh` takes both digests, writes them into the packaged
    charts' `image.digest` and checks that each package renders
    `ghcr.io/saremox/<chart>:<version>@<digest>`.
  - Both charts render `repository:appVersion@digest` while `image.tag` is
    empty, and the tag alone without a digest (a checkout, as before). A
    tag of your own replaces both: otherwise `--set image.tag=main` with a
    released chart would run the release by digest while claiming `main`
    (a reference with both resolves by digest); `tag: X.Y.Z@sha256:…` still
    pins. A digest that is not `sha256:<64 hex digits>` is refused. ci's
    helm job checks all four renderings for both charts (the agent in both
    modes) and the refusal; the dry run renders the packages.
  - README installs the chart `.tgz` from the release (immutable once
    published) rather than from ghcr, and checks it with `verify`.
- **Signing has a job of its own.** `id-token: write` and `attestations:
  write` were on `publish`, so every action there (checkout,
  download-artifact, setup-helm, login-action) could have signed provenance
  as `images.yml` for the tag. The `attest` job has them (and `contents:
  read`), runs only download-artifact and attest (and a digest check in
  bash), and `publish` lost them. It runs after `release-assets` and before
  `publish`, so still before anything is published, but now before the
  approval rather than after it. Provenance says how the files and images
  were built; the release says they were released. A rejected run's builds
  verify only for that tag's commit (`--source-digest`), and a version
  without a release is not offered to anyone. The alternative, `attest`
  in the `release` environment too, would ask for a second approval: each
  job in an environment waits for its own.
- setup-helm v5.1.0 has `checksum-policy`; images.yml sets `required`, so a
  helm without its `.sha256sum` fails rather than installs with a warning
  (the checksum comes from the same host, get.helm.sh: this catches a
  broken download, not that host). ci.yml's helm job keeps `auto`: round
  four changes ci.yml only by its assertions.
- Dependabot waits 7 days after a version is published before proposing it
  (`cooldown: {default-days: 7}`, all five ecosystems); security updates do
  not wait.
- **Concurrency groups** are the repository's, shared by every workflow:
  `images-publish` could be joined by any workflow with
  `cancel-in-progress` to cancel a publish half-way. They are now named
  after the file (`github.workflow_ref` for runs, `.github/workflows/
  images.yml publish` for publish) rather than the workflow's name, which
  another workflow may share. Accepted: any workflow can still join a group
  on purpose (a pull request's run uses the pull request's workflow files,
  from a fork too), a denial of service only. A cancelled `publish` can be
  run again: tagging the images and pushing the charts repeat harmlessly,
  and a draft it left makes `release-verify.sh` stop and say to delete it.
  Cancelled after publishing the release, check that release by hand
  (files against `SHA256SUMS`, the tag).
- **Only on a real tag push**: the attestations' SAN with `--cert-identity`;
  GitHub's asset `digest` and `immutable` fields and the release's notes as
  sent (the fake has them as gh's source reads them); `PATCH draft=false`
  on a draft of an existing tag; the `attest` job's permissions (artifact
  download and attest without `actions: read`); matrix outputs of `image`
  reaching `release-assets`; `checksum-policy: required` against
  get.helm.sh.
- **Repository settings** still recommended (round three's list is set):
  - The tag ruleset on all tags (`~ALL`), or at least `V*` too: whether
    `v*` matches case-insensitively was not checked. README's flags no
    longer accept a `V` tag, but the ruleset is what makes a tag's run a
    maintainer's.
  - Main: require a pull request with one approval and code-owner review
    for `.github/**`, `scripts/**`, `Dockerfile`, `deploy/**`,
    `go.mod`/`go.sum` and `ui/package*.json`; dismiss stale approvals and
    require approval of the latest push (a single maintainer: an admin
    bypass for pull requests only). Required checks alone constrain no one
    with write access, since a pull request can redefine `test` and the
    rest.
  - Actions: require actions pinned to full-length commit SHAs, an
    allow-list of action owners, and approval for every fork pull request
    run of an outside contributor (also against joining a concurrency
    group).
  - ghcr: `:main` in packages of its own, so the release packages can be
    Read for Actions and pushed only with an environment credential: the
    only fix for collaborators overwriting images and charts.
  - Fewer people with write access: any of them can create a release of a
    pushed tag before `publish` does.
  - Still to be checked with GitHub's documentation (blocked here):
    whether a collaborator can edit an immutable release's notes, and
    whether an immutable release can be deleted and its tag name reused
    (`publish` deletes a release on a confirmed difference; if the name
    cannot be reused, that version is skipped, and the next one released).


### Security and correctness review, round five

A fifth review (the same four areas, for correctness as well as security,
with the code added in round four first) found three high findings, all on
the read side: Grafana's read-only ClickHouse user could still exhaust
ClickHouse's memory through the hyperscan cache, every time window was
shifted with a ClickHouse whose time zone is not UTC, and two targets of
one host (dual-stack, or two ports) made one stream. Medium and lower
findings are listed at the end, for the owner to decide on. The decisions:

**Time zones**

- **Every time parameter names UTC.** The columns are `DateTime64(3, 'UTC')`
  and `clickhouse.Time` sends UTC wall-clock text, but the queries typed
  their parameters `{from:DateTime64(3)}`, which ClickHouse parses in its
  own server time zone. The official image runs in UTC, so compose, the
  chart's bundled ClickHouse and CI never showed it; a ClickHouse installed
  from packages takes the host's zone. 17 parameters were affected: the
  read API's `from`/`to`/`last`/`oldest` (cycles, loops, path changes,
  outages, streams, topology) and the restore's `since` and `start_ts` (the
  round-four lookup of stored seqs already named UTC). In Europe/Berlin a
  stream with 720 cycles in the last hour listed 1 and its 719 route
  changes of the day 0 (review, round five); in America/New_York a
  restored outage counted 4 of its 10 cycles. All now read
  `DateTime64(3, 'UTC')`. Functions on the columns (`toStartOfInterval(ts,
  …)`) use the column's zone and were right.
  - The rule: a date or time parameter always names its zone.
    `TestTimeParametersNameTheirZone` checks the server's query text for a
    parameter without one (it found 17 before the fix).
  - CI's `clickhouse` job runs its ClickHouse in America/New_York (`TZ`), so
    the integration tests see a zone other than UTC: before the fix,
    `TestIngestEndToEndClickHouse`, `TestTopologyClickHouse`,
    `TestTopologyLongPathsClickHouse`, `TestReadViewsOfPlantedDataClickHouse`
    and `TestReadBudgetClickHouse` failed there; all pass now. The `ui` and
    `grafana` jobs keep UTC, as deployed by compose and the chart.

**Grafana's ClickHouse user**

- **No hyperscan for Grafana's user** (compose profile `grafana_limits` and
  the chart's `hopcorder_readonly`: `allow_hyperscan = 0`, constant for the
  readonly = 2 case). `readonly = 1` bounds the settings, not the
  functions: `multiFuzzyMatchAny(s, 2, [1,500 patterns of 100 characters])`
  took 380 MiB and 47 s inside the 400 MB query limit, and left its compiled
  database in ClickHouse's global hyperscan cache, outside every query and
  user limit and without expiry. Each distinct pattern set added 95 to 140
  MB: on the chart's configuration ClickHouse grew from 212 MB to 1.36 GB
  over nine queries and the tenth OOM-killed it (review, round five), with
  the server's reads at 502 and ingest at 503 meanwhile. With the setting
  the functions fail with FUNCTION_NOT_ALLOWED; no dashboard uses them.
  - Tests: `TestGrafanaUserClickHouse` runs `multiFuzzyMatchAny` and
    `multiMatchAny` as the compose user (FUNCTION_NOT_ALLOWED; before: both
    ran) and refuses `allow_hyperscan = 1`; `smoke.sh` does the same as the
    chart's user, and the helm job checks the rendered `readonly.xml`.
  - Accepted: other global caches a viewer's queries fill are capped by
    ClickHouse (compiled expressions 128 MiB, the query condition cache 100
    MiB) and not counted in the chart's cache figures; a viewer can still
    keep ClickHouse's CPUs busy within the limits (16 concurrent queries of
    up to 300 s slowed topology from 60 ms to 1.2 s; ingest was not slowed).
    Keep untrusted users away from Grafana (round four).

**One stream per target**

- **The agent refuses targets the server would record as one stream.** A
  target with the same host, protocol and `flow_id` as an earlier one is a
  configuration error: `targets[1] (dual.example): same host, protocol and
  flow_id (33434) as targets[0], so the server would record both as one
  stream; give each its own flow_id`. A start fails (status 2, before
  anything runs), a reload keeps the running configuration, and `--once`
  refuses them too. Round four's duplicate check (host, protocol, port,
  af) stays; an exact duplicate is reported as that, once.
  - Why: a record names its target by the configured host, protocol and
    `flow_id`; the port and the af asked for are not on it (§3.1, locked),
    so the server's stream (agent, target, protocol, flow_id; M2) is all
    it can tell apart. Round four allowed one host twice with another
    `af` or `port` under one `flow_id` (the default) and only warned that
    their metrics share series. On the server their cycles alternated in
    one stream. The review fed 40 alternating IPv4/IPv6 cycles of
    `dual.example` with IPv6 down through ingest: 39 `target` route
    changes and 20 one-cycle outages instead of one ongoing IPv6 outage,
    and the UI said "Route changes 39, Outages 20, Reached 50%".
  - Not changed instead: the server's stream key. The port would have to
    be added to the locked wire record, and an `af` in the key would split
    a target whose name moves between families (af 0 prefers IPv4 and
    falls back to IPv6), which is one target with a `target` route change
    today.
  - Not chosen: deriving a `flow_id` for the second of two such targets
    (the review's alternative). The `flow_id` is the stream's name on the
    server and the ICMP identifier or source port on the host, which each
    hopcorder process needs its own of. A derived one depends on the order
    of the list (reordering it swaps two streams' histories), can collide
    silently with another process's `--flow-id` or another configured
    flow, and names a stream by a value nobody wrote. Refusing changes
    nothing for a config that loaded without round four's warning: same
    `flow_id`s, same records.
  - Accepted: configs that relied on that warning no longer load until
    each such target has its own `flow_id` (upgrade note in CHANGELOG);
    the targets' earlier cycles stay in one stream under the old
    `flow_id`. Agents of earlier versions still send them so; the server
    cannot take them apart. Their metrics still share series, and the
    agent still warns about that.
  - Tests: TestTargetsNeedTheirOwnStream (dual-stack, two TCP ports, an
    own `flow_id` equal to the inherited one, each also with `--once`;
    with their own `flow_id`s they load and keep them),
    TestBuildConfigRefusesTargetsSharingAStream (`--flow-id` makes them
    collide) and TestReloadRefusesTargetsSharingAStream (the reload is
    refused and the running targets keep probing; with their own
    `flow_id`s both flows run). All three failed before: the configs
    loaded, and the reload ran them.
- **The read API reads one flow.** Every endpoint takes `flow_id` (0 to
  65535, else 400 before any query; without it, any flow, as before, so
  older links and clients keep working). `streamFilter` has it, `cycles`
  adds it to its window as an equality, and `targets` returns `last_af`
  (the last cycle's, like `last_target_ip`).
  - Why: with their own `flow_id`s the events were already right (the
    stream key has the flow), but nothing could read one flow: the
    review's two rows in the stream list opened one view, which merged
    IPv4 and IPv6 nodes and showed 50% reached.
  - `flow_id` is not in the `cycles` sort key (agent_id, target, protocol,
    ts), so it only filters the rows read in that order; the round-four
    read-in-order path is unchanged.
  - Tests: TestReadFilterByFlow (each endpoint, with and without
    `protocol`, passes the flow and selects by it; malformed and
    out-of-range values are 400 without a query; before, ignored),
    TestDualStackStreamsClickHouse (the review's 40 alternating cycles on
    two flows, stored in ClickHouse: `targets` gives two rows with their
    af and reach, and `cycles`, `outages` and `topology` of one flow give
    that flow only, without `flow_id` both; before, every read gave both
    flows and no af) and TestDualStackStreams (ingest: on two flows no
    route change and one open IPv6 outage; on one flow, what the agent
    no longer sends, the review's 39 `target` changes and 20 outages).
- **The UI's stream is (agent, target, protocol, flow).** `StreamKey` has
  `flow`, the URL `flow_id`, the stream list's links carry it, and a
  view's every request sends it, so the two rows of a dual-stack name open
  two views. The list shows each stream's af (`last_af`) and flow, the
  view's header its flow and the shown cycle's af.
  - Links without `flow_id` or `protocol` (every link before) name no
    single stream. They show the stream list narrowed to the streams they
    match (`targets` with their agent, target and protocol) with a note,
    and if that is one stream, it opens, the URL replaced by its full link
    (range and moment kept). So the view that merged a target's protocols
    when a link had none is gone: a view is one stream.
  - Tests: Vitest (the URL round-trips `flow_id`, the two rows' links
    differ and parse back to their flows, bad `flow_id`s are dropped;
    every request of a view sends the flow; a link resolves to its only
    stream, else none) and browser tests (two flows of one name are two
    rows, IPv6 with 0% reached, no route change and one outage, IPv4 with
    100%, none and none; a link without `flow_id` opens the only stream,
    or lists both). The three new browser tests failed against a server
    built from the commit before these fixes, run locally.
  - Not changed: the Grafana dashboard's variables are agent, target and
    protocol, so its per-target panels (reachability, path length, the
    hop panels) merge a target's flows. Its route-change and outage
    panels read the per-stream tables and are right. A `flow_id`
    variable is left for the owner; another round-five fix changes
    Grafana's side.

### Security and correctness review, round six

A sixth review (the same four areas; round five's fixes re-checked, every
one held) found two more ways for a Grafana viewer to take ClickHouse down
through the read-only user, and medium and lower findings elsewhere. The
decisions:

**Grafana's ClickHouse user**

- **user_files is an empty directory ClickHouse cannot write**
  (`<user_files_path>/srv/</user_files_path>`: compose's
  `compose/clickhouse-config.xml`, the chart's `hopcorder.xml`). ClickHouse
  25.8's `sqlite()` opens, and so creates, its database file under
  user_files before it checks the caller's grant: the read-only user got
  ACCESS_DENIED and still left a 0-byte file, any name it chose, never
  removed. 1,513 a second from 16 connections; on a data directory of
  40,000 inodes, 42,000 queries in 28.6 s made inserts fail (no space to
  create the part directory) and reads 502 (review, round six). The
  chart's 20 GiB ext4 volume has about 1.3 million inodes: 15 to 18
  minutes. `/srv` is root-owned and empty in the image; the chart runs
  ClickHouse as uid 101, and compose sets `CLICKHOUSE_DO_NOT_CHOWN` so the
  root entrypoint does not hand it to the clickhouse user. `sqlite()` now
  fails with PATH_ACCESS_DENIED and creates nothing; no hopcorder query
  uses user_files. About 30 other table functions were checked: all refuse
  before any side effect.
  - Checks: CI's `grafana` job runs `sqlite()` as the compose stack's
    Grafana user and requires `/srv` empty and no file in the old
    user_files; `smoke.sh` does the same for the chart's user in kind; the
    helm job checks the rendered `hopcorder.xml`. Verified before the fix:
    a refused call left `user_files/probe.db`.
  - Compose: an existing ClickHouse container takes the new config when it
    is recreated (`docker compose up -d`).

**Route changes**

<!-- round six: distance -->

**TCP flows**

<!-- round six: tcp flows -->

**Spool and its volume**

<!-- round six: spool volume -->

