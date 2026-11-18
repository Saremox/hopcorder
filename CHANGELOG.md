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

**Fixed (review round five)**

- With a ClickHouse whose time zone is not UTC (a host installed from
  packages takes the host's zone), every read-API and UI time window was
  shifted by the zone's offset: the server sends times as UTC, but
  ClickHouse read the queries' time parameters in its own zone. In
  Europe/Berlin a stream with 720 cycles in the last hour listed 1, and the
  day's 719 route changes showed as 0; west of UTC, restored outages
  counted too few cycles. Every time parameter now names UTC. The official
  ClickHouse image (compose, the chart's bundled ClickHouse) runs in UTC
  and was not affected.
- Grafana's read-only ClickHouse user may no longer use the hyperscan
  functions (`multiMatchAny`, `multiFuzzyMatchAny` and the like). ClickHouse
  keeps each compiled pattern set in a global cache, outside every query and
  user memory limit, for good: queries that each stayed under the user's
  400 MB grew ClickHouse by 95 to 140 MB apiece, and the tenth OOM-killed
  the chart's 2 GiB ClickHouse. No dashboard uses them.
- Agent: two targets of one host with the same protocol and `flow_id`
  (IPv4 and IPv6 of one name, or two TCP ports, under the default
  `flow_id`) are refused at load. The server tells streams apart by host,
  protocol and `flow_id`, so it recorded both as one: their cycles
  alternated, and 40 of them with IPv6 down gave 39 route changes and 20
  one-cycle outages instead of one outage. **Upgrading:** a config that
  relied on round four's warning about such targets no longer loads until
  each has its own `flow_id` (`targets[1] (…): same host, protocol and
  flow_id (33434) as targets[0] …; give each its own flow_id`): the agent
  does not start, and a reload keeps the running configuration. Their
  earlier cycles stay in one stream under the old `flow_id`. Every other
  config keeps its `flow_id`s and records.
- Read API: every endpoint takes `flow_id`, so two flows of one target
  (IPv4 and IPv6, or two ports) can be read apart; without it, any flow,
  as before. `targets` also returns each stream's `last_af`.
- UI: the two flows of one target are two streams. The list shows each
  with its address family and `flow_id`, and each row opens its own view;
  both opened one view of both flows (50% reached for a dual-stack name
  with IPv6 down). A link without `flow_id` opens its stream if it
  matches only one, else lists those it matches.

**Fixed (review round six)**

- ClickHouse's `user_files_path` (compose and the chart's bundled
  ClickHouse) is now an empty directory ClickHouse cannot write. Its
  `sqlite()` table function created a file there before it checked the
  caller's grant, so Grafana's read-only user, refused every time, still
  left an empty file per query, never removed: about 1,500 a second, until
  the data volume ran out of inodes and ingest failed. Compose users: the
  setting takes effect when the ClickHouse container is recreated
  (`docker compose up -d`).
- Docs: Grafana's read-only ClickHouse user protects the data, not
  ClickHouse's availability. Every time zone a query names makes ClickHouse
  keep a table of about 5 MiB for good, outside every memory limit, and no
  setting or grant prevents it: a viewer's queries OOM-killed a ClickHouse
  of 2 GiB within seconds. Give only trusted users access to Grafana's
  ClickHouse data source; the compose try-out's anonymous viewers are for
  localhost only.
- Route changes: one lost or late reply from the destination, with the
  next TTL's reply in, counted as two route changes on an unchanged path
  (to one hop further, and back), in `hopcorder_path_changes_total`, the
  server's path changes and the UI and Grafana: about 400 a day per target
  at 1% loss. A destination further away now counts only if a TTL from its
  old distance on answered as a router; behind silent TTLs it keeps its
  distance.
- Agent: a local user could make a TCP target look down. A connection from
  the TCP flow's source port to the target's port (`nc -p 33434 target
  443`, or any local connection the kernel happened to give that port:
  33434 lies in Linux's ephemeral range) made the target answer every
  probe for that connection, so every cycle was recorded as not reaching
  the target, with no error and no metric. The agent now holds its TCP
  source ports (in classic mode every port it walks), as it holds ICMP echo
  identifiers: nothing else on the host can use them while it runs. A port
  that is already in use stops that flow's TCP targets, visibly (an error
  in the log, `hopcorder_flow_in_use` 1, cycle errors), and they resume by
  themselves once it is free. **Upgrading:** a TCP `flow_id` (default
  29434, see below) must be a port nothing else on the agent's host uses. A server
  listening on it, or a second hopcorder on the same TCP flow (`--once`
  next to the daemon), now stops those TCP targets: give them another
  `flow_id`. A connection from the port that closed less than a minute
  before the agent starts (TIME_WAIT) delays them by up to that minute.

- The agent's spool no longer loses its newest records when its disk
  fills. `spool.max_bytes` counted only the stored records, and the spool
  file is about 1.5 times that, so the agent chart's volume (1Gi, with
  `max_bytes` 1GiB) filled first during a long outage: from then on every
  record was lost, the oldest kept, and nothing was counted as dropped.
  `max_bytes` now bounds the spool file itself, records and database
  together. At the bound, and on a disk that fills before it (shared with
  other files, or `max_bytes` larger than the disk), the oldest records are
  dropped and counted in `hopcorder_spool_records_dropped_total`; the spool
  keeps a little space allocated ahead of its pages for that, and grows
  again once there is room. Acknowledgements work on a full disk too.
  **Upgrading:** the same `max_bytes` now keeps half to two thirds as many
  records (`hopcorder_spool_bytes` stays the records' bytes). A spool file
  larger than `max_bytes` keeps its size, with at most `max_bytes` of it in
  use. The agent chart sets `config.spool.max_bytes`, unless given, to 75%
  of `persistence.size` (at most 1GiB): 768MiB for the default 1Gi volume.
  It refuses a `max_bytes` above 90% of the volume, so a release that set
  `max_bytes: 1GiB` with the 1Gi volume fails to upgrade until it lowers
  or removes it (`persistence.size` applies only to new volumes). That
  includes `helm upgrade --reuse-values` and values files copied from an
  earlier chart, which carry its default `max_bytes: 1GiB`: add `--set
  config.spool.max_bytes=null` (or use `--reset-then-reuse-values`).

**Fixed (review round seven)**

- ClickHouse's `format_schema_path` (compose and the chart's bundled
  ClickHouse) is now the same empty directory ClickHouse cannot write as
  `user_files_path`, and Grafana's read-only user may no longer have
  CapnProto or Protobuf schemas generated. A CapnProto or Protobuf output
  without a schema (`SELECT formatRow('CapnProto', 1)`, `FORMAT
  ProtobufList`) writes a generated schema to a temporary file there, and
  one that fails to generate was never removed: about 1,000 empty files a
  second from one viewer, kept across restarts, until the data volume ran
  out of inodes and ingest failed.
- ClickHouse (compose and the chart's bundled ClickHouse) may no longer
  connect out. `hasColumnInTable('host:port', ...)`, which no grant
  covers, let Grafana's read-only user make ClickHouse connect to any host
  and port and resolve any name. The chart's NetworkPolicy now allows
  ClickHouse no egress at all, DNS included, compose keeps ClickHouse on an
  internal network, and `remote()` no longer looks up the host it is then
  refused for.
- ClickHouse's own logs (compose and the chart's bundled ClickHouse) can no
  longer fill its volume. One failing query of Grafana's read-only user
  wrote its whole exception message, up to the user's memory limit, into
  `query_log` and `text_log`: about 100 MB a query, so a few hundred
  stopped ingest. The logs now live on a disk of their own that leaves a
  quarter of the volume to the data (log writes stop and are dropped
  below that), and compose keeps them for 7 days like the chart. That
  covers every log ClickHouse keeps: a refused `BACKUP` still wrote its
  whole destination into `backup_log`, and a traced query its exception
  into `opentelemetry_span_log`, which is now switched off.
- ClickHouse's own log files and console log no longer repeat a failing
  query's exception message: one query of Grafana's read-only user wrote
  it about five times, 300 MB, into compose's log files on the host's
  disk. The loggers that did are set to `fatal` (`query_log` keeps the
  exception), and compose caps its files at 10M × 2.
- Route changes: an outage that the router before the destination answered
  with an ICMP unreachable (an ARP failure on its LAN, a REJECT rule)
  counted as two route changes on an unchanged route, at its start and its
  end. Unreachable hops are no longer compared with the addresses seen at
  their TTL.
- Agent: the default `flow_id` is now 29434 (was 33434). It is the TCP
  source port the agent holds, and 33434 lies in Linux's ephemeral port
  range: on a host-network agent, any unrelated connection the kernel
  happened to give that port stopped the agent's TCP targets until it
  ended. 29434 is below the ephemeral and NodePort ranges. Every target
  without its own `flow_id` is a new stream (records carry the new
  `flow_id`); the UDP destination port's default stays 33434.
- Compose: Grafana's read-only ClickHouse user may no longer take away
  grants. `GRANT CURRENT GRANTS ON *.* TO <user> WITH REPLACE OPTION` ran
  as that user (a ClickHouse 25.8 bug) and removed every privilege of the
  grantee, Grafana's or the server's, for good. The user is created with
  `GRANTEES NONE`.
- Agent: a reload (SIGHUP) or a restart during an outage that also takes
  DNS down no longer leaves targets given by name without records. Each
  configuration and each start resolved them anew, and with DNS down every
  cycle failed: no record for the rest of the outage, while targets given
  as addresses recorded it. A reload now keeps each name's address and
  refreshes it in the background, and the spool keeps the last address
  across restarts: if a name cannot be resolved at start, its target is
  probed at the address it had, with a warning. In the chart a config
  change restarts the pods, so this covers it too.
- The agent's spool no longer loses every new record on a file system
  without `fallocate` (NFS before 4.2, many FUSE file systems, some CIFS
  and ZFS versions). With `max_bytes` larger than the free space and an
  output down, its file grew until the volume was full, and from then on
  every record was lost, after a restart too, with nothing counted as
  dropped. There the spool now allocates its little space ahead by writing
  zeros, and checks the free space before its file grows, so a full disk
  drops the oldest records, counted, as with `fallocate`.
- The agent's spool no longer rewrites the list of its free pages on every
  write. Once a backlog had filled the spool file and been delivered, every
  record appended and every delivery acknowledged wrote and synced that
  list, 1.4 MiB each with the chart's 768MiB `max_bytes`, for as long as the
  file existed: about 5 MiB/s of writes on a 10-target agent, wearing flash
  and slowing probing. bbolt now keeps the list in memory and finds the
  free pages again when the spool opens (which takes a little longer with a
  large backlog), so a write costs about 17 KiB. Spool files written before
  are read as they are.
- ClickHouse (compose and the chart's bundled ClickHouse) no longer writes
  a host name a query passes to `hasColumnInTable()` to its log: up to
  1 MB, about 15 MB of the chart's console log per query of Grafana's
  read-only user, from the `DNSResolver` and `Cluster` loggers, which are
  now set to `fatal`. Its unused MySQL, PostgreSQL and interserver
  listeners are removed.
- ClickHouse (compose and the chart's bundled ClickHouse) caps its
  temporary data at 2 GB for all queries together
  (`clickhouse.deploy.maxTemporaryDataOnDisk` in the chart). With
  `wait_end_of_query=1` in the URL, which no setting refuses, an HTTP client
  with the password of Grafana's read-only user made ClickHouse buffer a
  result on the data volume until it was full, and the server's inserts
  failed meanwhile.
- Agent chart: `agentIdPrefix` goes before every agent ID, the pod's or
  the node's name. Agent IDs must be unique per server, but with the chart
  in several clusters shipping to one server, every cluster had an agent
  `hopcorder-agent-0`, and node names often repeat too: of two agents with
  one ID, one was refused for good (or its records taken for the other's
  duplicates) and lost its records, and their streams were merged. In
  daemonset mode there was no way to tell them apart. Give each cluster
  its own prefix; the chart refuses one that cannot make a valid agent ID.
- Server chart: the bundled ClickHouse's generated passwords (without
  `clickhouse.passwordSecret`, and the read-only user's) are now in
  immutable Secrets. Argo CD, Kustomize's `helmCharts` and `helm template`
  cannot read them back and rendered new passwords every time, which the
  next sync applied; ClickHouse and the server read them only at start, so
  the next restart of either (a config change restarts the server) broke
  authentication until the other restarted too, and Grafana's data source
  with it. Now applying such a render fails and the passwords stay. With
  those tools, set `clickhouse.passwordSecret` and
  `clickhouse.deploy.readOnlyUser.passwordSecret`; `helm install` and
  `helm upgrade` work as before.
- Server: a failed write to ClickHouse no longer looks successful when the
  server's ClickHouse user has `send_progress_in_http_headers = 1` in its
  profile. ClickHouse then answers 200 once a request has run for 100 ms
  and puts a later exception into the body, so an insert of about 1 MB over
  a network link, whose hop rows a view failed to write, was taken as
  stored: the agent's retry was answered "duplicates", and those hop and
  loop rows were lost for good, with no error. Every request now turns
  progress headers off, and any answer with an exception code is an error.
  A profile that makes the setting constant at 1 makes every request fail
  instead, visibly: the server stays unready and says why.
- The agent's spool no longer stops an output for good after damage to
  its file (bit rot) that raised the output's delivery cursor, or the
  newest record's key, above the last seq. The output delivered nothing
  appended after that, and records it had not delivered could be deleted.
  At start the spool now puts such a cursor back before the oldest record
  it holds, so the output delivers those records, some again (the server
  ignores duplicates; a jsonl file repeats them), and gives such a record
  the next seq. Both are logged as errors; no record is lost.
- An ongoing outage showed 1 cycle in the read API, the UI's event list and
  Grafana's Outages panel until it ended: its row was written once, when it
  began. The server now writes it again after each batch in which the
  outage went on, with its cycles so far; the row written when it ends
  still replaces every earlier one.
- UI: when loading a stream failed (a busy server's "too many queries", a
  network error), the stream view went on showing the previous stream's or
  range's figures, graph and cycle under the new stream's header. It now
  clears them as soon as another stream or range is chosen, and a failed
  load, a failed refresh included, shows the error and "not loaded" in
  their place.
- Read API: a list cut at its `limit` now says `truncated: true`, as one cut
  at the response's byte budget did; it said `false`. The UI asks for
  10,000 events of each kind and showed what it got as the range's whole
  count ("Loops 10000" for 24,000 loops). A cut list's count now reads
  "10000+", and the stream view and the stream list say that only the
  newest (or, for streams, the first) are listed.
- UI: ICMPv6 destination unreachable codes were named as ICMPv4's: an
  IPv6 address unreachable (the destination host down) read "port
  unreachable", and administratively prohibited read "host unreachable".
  Codes are now named by the cycle's address family.

**Fixed (review round twelve)**

- Server: an outage that ended in a batch whose events could not be stored
  is written again with the stream's next batch, instead of the stream
  being rebuilt from storage. The rebuild recorded the stream's next
  outage with a later start, or not at all, and lost the next batch's
  route changes when it could not run; outages lost whole with the failed
  batch are now recorded as well.

**Fixed (review round eleven)**

- Server: an outage whose open row could not be stored is recorded with its
  real start again. Since #85 its stream was rebuilt from storage, which
  never had the row: a catch-up batch recorded the outage with a later
  start, and a skipped restore not at all.
- Server chart: the bundled ClickHouse's startup and liveness probes only
  connect to its native port; readiness still queries. A client holding
  ClickHouse's connections could make the querying probes fail, and after
  any restart during the attack ClickHouse never became ready again.
- Server chart: the ClickHouse probe takes the password from its
  environment instead of its command line, where any process on the node
  could read it; a password beginning with `-` no longer makes every probe
  fail.

**Fixed (review round ten)**

- ClickHouse (compose and the chart's bundled ClickHouse) no longer logs a
  stack trace for an unauthenticated request whose body it cannot read
  after refusing it (a bad chunk header suffices): about 8 MB/s of the
  chart's console log.
- Server chart: the bundled ClickHouse's probes query over its native port
  on loopback instead of HTTP `/ping`. A client that held ClickHouse's HTTP
  connections open would have made the probes fail and Kubernetes restart
  ClickHouse over and over. The chart now says plainly that it needs a CNI
  that enforces NetworkPolicy.

- Server: an outage whose close could not be stored (ClickHouse went away
  right after the cycles that ended it were stored) no longer stays
  ongoing until the server restarts, or for good. The stream is restored
  at its next cycle, which closes the outage in storage.

**Fixed (review round nine)**

- ClickHouse (compose and the chart's bundled ClickHouse) no longer logs a
  stack trace for each answer a client closed early on: unauthenticated
  requests that did so wrote megabytes a second to the chart's console.
- ClickHouse (compose and the chart's bundled ClickHouse) no longer logs
  the user name of a login a query makes it attempt to itself
  (`hasColumnInTable('0.0.0.0:9000', ...)`): up to 1 MB per query of
  Grafana's read-only user, about 100 MB/s of the chart's console log.
- ClickHouse (compose and the chart's bundled ClickHouse) bounds what a
  client may send before it logs in: at most 256 connections, 32 HTTP
  header fields of at most 8 KiB, and 64 KiB of URI. With ClickHouse's
  defaults a few unauthenticated requests with large headers, or a few
  thousand idle connections, OOM-killed it (where a pod could reach its
  HTTP port, that is without a CNI that enforces the chart's
  NetworkPolicy).
- Server: an outage that ended while the server could not restore its
  stream's state no longer stays ongoing for good. After a restart, or
  once a stream had been idle, the cycles of a stream whose restore
  failed, ran out of time or did not fit the 256 restores one request does
  are stored but derive no events; when the destination answered again in
  them, the outage's end was never recorded, and the read API, the UI and
  Grafana showed it ongoing since its start. With no fault at all, that
  happened at a restart to an agent with more than 256 targets, for an
  outage ending in its first batch after the restart. If the stream later
  went down again while its restores failed, the new outage was merged
  into the old one, reached cycles included. The next restore that works
  now closes such an outage at the first cycle that reached the
  destination.

**Fixed (review round eight)**

- Server: an outage that had ended is no longer taken up again by a stream
  whose state the server restores. The lookup for the stream's open outage
  returned its last outage whether it had ended or not, so a stream back
  after more than 30 days without cycles (an agent off for a month, a
  target re-added), or one whose restores had failed for a while after a
  restart, continued that old outage: the read API, the UI and Grafana
  showed it ongoing since its old start or ending at the new cycle, with a
  wrong cycle count, and the new outage was not recorded.
- ClickHouse (compose and the chart's bundled ClickHouse) takes native
  protocol connections from its own container only. Nothing of hopcorder
  uses that protocol, but it listened on every address, and ClickHouse
  reserves memory for the user name a login declares before it checks the
  password: two logins of about 1 KB each, with no credentials, from any
  pod (without a CNI that enforces the chart's NetworkPolicy) OOM-killed
  it. It also let Grafana's read-only user make ClickHouse log in to
  itself and write about 1 MB of console log per query.
- Server chart: the bundled ClickHouse's temporary-data cap is now a tenth
  of `clickhouse.deploy.persistence.size` unless
  `clickhouse.deploy.maxTemporaryDataOnDisk` sets it (at most a fifth of
  the volume). The fixed 2 GB filled volumes under about 8 GiB, such as
  the chart's own 1Gi test volume.
- Docs: Grafana's read-only ClickHouse user belongs on a ClickHouse
  without other data. On ClickHouse 25.8 no grant covers `joinGet()` on a
  Join table or `IN` with a Set table, so that user reads such tables of
  any database; hopcorder's own stacks have none.
- Agent: a spool back at an earlier state (a VM snapshot reverted, a
  volume restored from a backup, a disk that lost writes it had confirmed)
  no longer reuses seqs the server has. It went on from the seq it held,
  and the server dropped the cycles recorded after it as duplicates,
  silently, until their seqs passed the ones it had stored (in a
  reproduction, 12 of 36 cycles). Each time the agent opens its spool (a
  start or a reload) the seqs now go on from the clock, as a new spool's
  start there, whenever that is ahead: above every seq the agent sent
  before, as long as the clock is right (a clock behind is logged). So the
  seqs skip ahead at every start and reload; the pending count (the
  `hopcorder_output_pending_records` metric, batching, the wait at
  shutdown) counts records, not seqs. The server answers a batch far below
  an agent's seqs as duplicates, not 409, when it holds every one of its
  cycles as they are: a restored spool's backlog lies that far below, and
  its delivery would have stalled until the age bound dropped it.

**Security: server ingest**

- One agent can no longer crowd the others out of the stream state. An agent
  kept busy by back-to-back requests was never swept, so it escaped
  `ingest.max_streams` without bound and made every sweep drop every other
  agent's streams, whose cycles then derived no path changes or outages. The
  limit now always holds, the new `ingest.max_streams_per_agent` (4096, or
  half of `max_streams` if less) bounds one agent, and at the limit the
  agents with the most streams give theirs up first. Cycles of streams
  beyond the limits are stored, and counted in
  `hopcorder_server_unrestored_cycles_total`.
- Loops can no longer blow up inserts in ClickHouse. The per-hop view copied
  a cycle's loops onto each of its hop rows, so one 8 MiB insert of valid
  records could need 1.6 GiB and fail. The server now sends each hop's loop
  TTLs with the cycle (migration 0003, applied on start; `hops` and
  `path_probes` keep their columns and values), and the size of an insert
  counts the hop rows the view writes. During a rolling upgrade, cycles that
  a server of the previous version stores get no loop marks on their hop
  rows (`loop_events` has the loops).
- Concurrent large uploads can no longer stall ingest. A body took its part
  of the read buffer chunk by chunk, so a few bodies of `max_body_bytes`
  (seven with the defaults, two from 16 MiB) could hold all of it between
  them, each waiting for more, until their read timeout. A body now reserves
  what it needs at once, in turn, and gives it back once it is decoded.
- Memory sizing, corrected: a request being processed holds up to about
  three times `max_decoded_bytes` (not twice), plus about 32 MiB while it is
  stored (one part of it and its events at a time); plan for
  `max_concurrent` of them. Decoded records no longer keep spare capacity,
  which made it up to 3.6 times.
- A batch's path changes and outages no longer go to ClickHouse in one
  unbounded insert: records whose target address and reached flag flipped
  every cycle made an event each, one 32 MiB request held 213 MiB on top of
  its body, and four such requests OOM-killed the server under the chart's
  1 GiB. Events are now derived and stored with each part of the batch, in
  inserts of at most 8 MiB, and such a request holds about 60 MiB.
- Records whose `reached`, `path_hash` or `loops` contradict their hops, or
  that go on past the destination's reply, are rejected; agents never send
  such records (`reached: true` without a reply was the flood's cheapest
  form).
- A failed or interrupted insert no longer loses hop rows, loop rows or
  events, or stores cycles twice. ClickHouse stores the cycles before it
  runs the views that write their hop and loop rows, so a view failing (out
  of memory, a full disk, a restart) left cycles without those rows, and
  depending on the order the views ran in, the agent's retry was answered
  "duplicates" and the batch's outages and route changes were never
  recorded, or every cycle was stored twice; an insert that outlived its
  timeout and committed later lost its events the same way. Now the
  server writes the deduplication mark itself once a part is complete
  (migration 0004 drops `agent_seq_mv`, applied on start), and after any
  failed write, or a restart, it stops the agent's writes still running,
  looks up what they stored, fills in the missing rows and derives the
  events: each cycle is stored once with its rows, whatever the retry's
  batch boundaries. `hopcorder_server_records_total{result="recovered"}`
  counts such cycles. Upgrading: the server's ClickHouse user must be able
  to see and stop its own queries (`GRANT SELECT(query_id, user, query) ON
  system.processes`; the default user and the images' `CLICKHOUSE_USER`
  can), or the server stays unready and says so. Rolling back below this
  release: recreate `agent_seq_mv` first (the statement is in migration
  0004).
- The server refuses to become ready while `hops_mv` or `loop_events_mv` is
  missing, naming the migration to recreate it from: a migration that
  stopped between dropping and recreating a view, followed by a rollback,
  stored every cycle without hop rows from then on. Later view changes use
  `ALTER TABLE ... MODIFY QUERY`.
- Restoring a stream's state after a restart reads its outages only after
  the last cycle that reached the destination, not all of them (a flapping
  stream has one per two cycles).

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
- The topology view's edges cost ClickHouse memory in proportion to a
  path's hops, not to their square: 20,000 cycles of 255 answering hops
  took 3.1 GiB and 29 s, more than a read query may use (1 GiB), so long
  paths, real or sent to that end, broke the view; now 49 MiB and 0.4 s.
- Grafana's read-only ClickHouse user (the compose stack's, and the server
  chart's `clickhouse.deploy.readOnlyUser`) has `readonly = 1`: its queries
  may change `max_execution_time` (up to 300 s; the Grafana plugin sends it)
  and no other setting. With `readonly = 2`, the settings left off its list
  let any viewer switch off ClickHouse's memory tracking and OOM-kill it
  with one query, pin about 1 GiB of memory in the query cache for good,
  make ClickHouse write about 1.5 MB/s of its own logs, or keep queries out
  of `query_log`. A data source with custom settings or a row limit, or a
  query timeout above 295 s, now gets its queries refused: allow such a
  setting with `CHANGEABLE_IN_READONLY` in the profile.
- The same user's memory is bounded to 400 MB per query and 600 MB for all
  of its queries (were 1 GB and 2 GB, more than the chart's 2Gi ClickHouse
  may use: two viewer queries got a query of the server killed). Helm: the
  new `readOnlyUser.maxMemoryUsage` and `maxMemoryUsageForUser`; scale them
  with `clickhouse.deploy.resources.limits.memory`. Compose: re-run
  `deploy/compose/clickhouse-init.sql` on an existing volume (it now
  replaces the user too).
- Known: every query still makes ClickHouse write a few hundred bytes of its
  own logs. Through the compose stack's Grafana one anonymous viewer sent
  1,200 queries/s, about 0.45 MB/s with the chart's logging: keep untrusted
  users away from Grafana, or rate-limit it.
- The read API's views no longer cost ClickHouse up to about 1 GiB a query
  on data posted to that end: topology, path changes, cycles and the
  stream list of planted data took 494 to 793 MiB each, and two UI viewers
  of such a stream got 502 from the chart's ClickHouse; now at most about
  120 MiB, cut to the views' caps. Topology aggregates at most 20,000
  distinct nodes and edges and the stream list at most `ingest.max_streams`
  streams (beyond, the busiest of those ClickHouse came across first), and
  read queries use at most 4 threads.
- Helm: with `clickhouse.deploy`, the chart sets `query.max_concurrent` to 2
  and `query.max_memory_bytes` so that reads get a third of what the bundled
  ClickHouse may use (273 MiB a query for its 2Gi; they were 8 x 1 GiB),
  unless `config.query` sets them. With your own ClickHouse, keep their
  product to about a third of its memory: the defaults are for a large one.
- Streamed read-API responses are no longer cut off 30 s after their query
  started (the ClickHouse timeout also covered reading the result): slow
  clients get them whole. A result cut off inside a row is an error, not a
  row.
- Helm: `allowedHosts` and `config.allowed_hosts` are checked as the server
  checks them, so `10.0.0.5:8080`, `cafe:443` or a 300-character name fail
  the install instead of crash-looping the server.
- `allowed_hosts` refuses a wildcard on a top-level domain (`*.com` allowed
  every `.com` name, an attacker's too). Use `*.domain` only for a domain
  whose names only you can create, not a shared one such as `*.github.io`.
- The read API answers 400 to `from`/`to` outside 1970 to 2262 or more than
  400 days apart (they gave 502 and an error in the log), and its responses
  carry `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`.

**Security: agent**

- Spoofed packets from anywhere can no longer forge hops, loops or a
  reached destination by guessing the 16-bit probe id: a reply must also
  match the probe's random IPv4 IP ID, a random nonce in its payload (which
  destinations echo and ICMP errors quote) or the SYN's exact, random
  sequence number. The review's spoofer, at 5,000–20,000 packets per
  second, put fake hops into nearly every cycle; now none. This also stops local
  processes from forging TCP replies with `connect()`. IPv6 routers must
  quote the whole probe, as RFC 4443 requires; one that quotes only 8 bytes
  now shows `no-reply`.
- A reload (SIGHUP) keeps the metrics listener when its address is
  unchanged, and binds a new address before releasing the old one, so a
  local user can no longer take the port during a reload, make the agent
  exit and serve their own metrics. A reload never stops the agent: if the
  previous configuration cannot start again either, it is retried until it
  does, while the endpoint answers 503.
- Spool files with damage that still crashed or hung the agent (a fault
  inside bbolt's check, an overflow count that made it loop for minutes and
  take gigabytes, a self-referencing bucket, a page freed twice) are now
  quarantined at start: the page structure is checked before bbolt reads
  it. Damage bbolt only finds while running fails that operation, and the
  file is moved aside and an empty spool started at once, instead of the
  agent crashing.
- A local user holding the agent's ICMP echo identifier (one ping socket)
  no longer stops the agent: only the ICMP targets of that `flow_id` wait,
  with an error in the log and `hopcorder_flow_in_use` at 1, and they
  resume by themselves once the identifier is free. Every other target is
  probed. `--once` prints the other targets' records and exits 1.
- A metrics `path` the endpoint could not serve (`/metrics x`, `/{x`,
  `/a{b}` and the like) passed validation and then panicked the agent at
  start, or on the reload that brought it: exit 2, and again on every
  restart until the file was fixed. `metrics.path` now takes `/` and then
  only `[A-Za-z0-9/._~-]` (at most 256 characters), so a reload to another
  one keeps the running configuration, and the endpoint serves exactly that
  path (a path ending in `/` no longer serves the paths below it).
- A reload that fails and is reverted no longer frees the ICMP echo
  identifier to a local user. The new configuration released the running
  one's identifiers before it had started, so when it then failed (a JSONL
  directory missing, a TLS file unreadable, the spool locked), a process
  polling `bind()` took the old identifier, and the reverted
  configuration's ICMP targets were not probed until it let go. Both
  configurations' identifiers are now held while the new one starts, and
  the old ones released once it runs.
- A JSONL write that blocks (a FIFO for a log shipper that is not reading,
  stdout under a blocking log driver, a hung NFS mount) no longer hangs a
  reload or the shutdown. The agent waited for it for good: after SIGHUP
  nothing was probed, the metrics endpoint answered 200 with frozen
  numbers, and SIGTERM was ignored until systemd killed the agent. Writes
  to FIFOs and pipes now give up when the outputs stop; a delivery still
  blocked 3 s later is left behind, logged and counted in the new
  `hopcorder_output_abandoned_total{output}` (its records stay spooled).
  The metrics endpoint answers 503 from the moment probing stops. JSONL to
  a FIFO or pipe also works now: every batch failed at its fsync after it
  was written, and was written again on every retry.
- A spool file crafted to get past the page check (a meta page with a
  higher txid and another page size, which the check passed over for the
  other one, while bbolt used it) crashed or hung the agent on every start.
  The check now picks the meta page as bbolt does and quarantines such a
  file. It takes write access to the spool file; random damage cannot do
  this.
- A spooled record decodes to at most 8 MiB (the largest the agent writes
  is about 220 KB). A 48 KiB stored value that decoded to 1.5 GiB got the
  agent OOM-killed on every start, and was never quarantined; such a record
  is now skipped like any undecodable one.
- `hopcorder_flow_in_use` counts the targets of its series that are not
  probed. Two targets with the same host and protocol (another `port` or
  `af`) share every per-target series, and the gauge flapped between 0 and
  1 when only one of them was refused; the agent now also warns at start
  about targets that share series.
- Documented: the 32 random bits a forged reply must guess are a rate at
  line rate (a fake hop about every 2 to 3 minutes at 1 GbE while probes
  are in flight, about every 12 minutes with the defaults). README has an
  alert rule on `hopcorder_probe_mismatched_replies_total`, which counts
  the many near misses that come before.

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
- Verify a release with `--cert-identity
  https://github.com/Saremox/hopcorder/.github/workflows/images.yml@refs/tags/vX.Y.Z`,
  `--source-ref refs/tags/vX.Y.Z` and `--source-digest` with the tag's
  commit (README has the full commands, the notes the same with the commit
  filled in). README's previous command (`--signer-workflow`, no
  `--source-digest`) also accepted builds of a workflow file whose name
  only starts with `images.yml`, and of a tag `VX.Y.Z` at another commit:
  gh matches that flag as a prefix, and refs and commits ignoring case.
- Install: README now downloads the archive, checks it, and unpacks it with
  `tar --no-same-owner`. Piped into `tar -xz` as root, v0.1.1's archive
  left uid 1001 owning the binary, and nothing was kept to check. v0.1.1
  has no attestations: its `SHA256SUMS` shows the download is intact, not
  who built it.
- The release job creates the release as a draft and publishes it only if
  it is the only release of the tag and has exactly the files the job built
  (by SHA-256, none missing or added), then checks again. Until a release
  is published, anyone with write access could create one for the pushed
  tag or add files to its draft, and GitHub's release attestation (`gh
  release verify-asset`) vouches for whatever a release holds: it shows a
  file is part of the release, not who built it. If the release job fails
  with "a release of vX.Y.Z exists already", someone else released that
  tag.
- The release job no longer deletes a genuine release when its tag is a tag
  of a tag (`git tag -a v1.0.0 v1.0.0-rc.1`) or when GitHub's API fails: it
  peels every annotated tag, retries, and deletes only on a confirmed
  mismatch.
- Helm: a release's charts run its images by digest (`image.digest`), so
  repointing `:X.Y.Z` in ghcr no longer changes what a verified chart runs.
  Install the chart `.tgz` from the GitHub release. Charts from a checkout,
  and `image.tag` set by you, work as before.
- The release's signing runs in a job of its own that does nothing else, and
  Dependabot proposes a new version only once it is 7 days old.

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
