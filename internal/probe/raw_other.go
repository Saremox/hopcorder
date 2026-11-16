//go:build !linux

package probe

import (
	"context"
	"errors"
	"log/slog"
)

var errUnsupportedOS = errors.New("probe: raw-socket probing is only implemented on Linux")

// Raw is the raw-socket Prober. On this operating system it is a stub.
type Raw struct{}

// NewRaw returns a stub prober whose methods fail.
func NewRaw(*slog.Logger) *Raw { return &Raw{} }

// Check always fails on this operating system.
func (*Raw) Check() error { return errUnsupportedOS }

// Trace always fails on this operating system.
func (*Raw) Trace(context.Context, TraceRequest) ([]HopResult, error) {
	return nil, errUnsupportedOS
}

// Stats returns zero counters.
func (*Raw) Stats() Stats { return Stats{} }

// Close does nothing.
func (*Raw) Close() error { return nil }
