# hopcorder

A flight recorder for network paths: an agent continuously traces routes to a set
of targets and records every hop of every cycle for later analysis; an optional
server ingests into ClickHouse for dashboards.

**Read `docs/HANDOFF.md` before starting any work.** It holds the design
decisions, data model, and milestone plan. Decisions marked (locked) are not to
be changed without asking.

## Commands

```bash
go build ./cmd/hopcorder
go test -race ./...
go vet ./...
golangci-lint run
# raw-socket tests (need CAP_NET_RAW on the test binary):
go test -tags rawsock ./internal/probe/...
```

## Conventions

- Go, latest stable. Standard library first; ask before adding dependencies.
- Logging via `log/slog`. No `fmt.Println` outside `cmd/`.
- Errors wrapped with context (`fmt.Errorf("resolve %s: %w", host, err)`).
- `context.Context` as the first argument for anything that does I/O or blocks.
- Everything above `internal/probe` is tested against the fake prober; no test
  in the default `go test ./...` run may need root, `CAP_NET_RAW`, or network.
- Prometheus labels must be bounded. Never a hop IP, hostname of a hop, or any
  per-probe value as a label.
- The agent never does rDNS or ASN lookups; that is server-side enrichment.
- Keep `docs/HANDOFF.md` current: if a decision changes, update it in the same
  commit.

## Reference material

- `docs/prototype/pathwatch.py` — working Python prototype (behavioural reference)
- `docs/prototype/schema.sql` — ClickHouse DDL and analysis queries for M2/M3
