// Package output ships spooled records: to a JSONL file or stdout, to the
// hopcorder server over HTTP, and as Prometheus metrics. See
// docs/HANDOFF.md §5.3.
package output

import "context"

// Output receives batches of records, each one JSON document without a
// trailing newline, in seq order. Deliver either accepts the whole batch or
// returns an error, in which case the same batch (possibly with more records
// appended) is offered again later, so delivery is at-least-once.
type Output interface {
	// Name identifies the output's delivery cursor in the spool.
	Name() string
	Deliver(ctx context.Context, lines [][]byte) error
	Close() error
}
