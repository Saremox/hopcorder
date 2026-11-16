package output

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/saremox/hopcorder/internal/spool"
)

// Source is the spool as seen by a drainer.
type Source interface {
	Read(consumer string, maxRecords, maxBytes int) ([]spool.Entry, error)
	Ack(consumer string, seq uint64) error
	Notify(consumer string) <-chan struct{}
	Pending(consumer string) uint64
}

// DrainOptions controls batching and retries.
type DrainOptions struct {
	// MaxRecords and MaxBytes bound one batch.
	MaxRecords int
	MaxBytes   int
	// FlushInterval is how long a partial batch may wait for more records,
	// measured from its oldest record. Zero delivers as soon as records
	// arrive.
	FlushInterval time.Duration
	// RetryMin and RetryMax bound the exponential backoff after a failed
	// delivery. Each wait is jittered to between half and all of the
	// current backoff, so agents recovering from the same outage do not
	// retry in lockstep.
	RetryMin, RetryMax time.Duration
	// OnError is called for every failed delivery (for metrics).
	OnError func(error)
	// FlushNow, once closed, makes partial batches go out immediately
	// instead of waiting for FlushInterval. Close it at shutdown so the
	// last records get a chance to ship.
	FlushNow <-chan struct{}
	Logger   *slog.Logger
}

func (o *DrainOptions) defaults() {
	if o.MaxRecords <= 0 {
		o.MaxRecords = 500
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = 1 << 20
	}
	if o.RetryMin <= 0 {
		o.RetryMin = time.Second
	}
	if o.RetryMax < o.RetryMin {
		o.RetryMax = max(time.Minute, o.RetryMin)
	}
	if o.OnError == nil {
		o.OnError = func(error) {}
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Drain delivers records from src to out until ctx is cancelled. A batch is
// acknowledged in the spool only after out accepted it, so records survive
// any outage of out (up to the spool's bounds) and are delivered at least
// once.
func Drain(ctx context.Context, src Source, out Output, o DrainOptions) {
	o.defaults()
	name := out.Name()
	log := o.Logger.With("output", name)
	notify := src.Notify(name)
	backoff := o.RetryMin
	failing := false

	for ctx.Err() == nil {
		batch, err := src.Read(name, o.MaxRecords, o.MaxBytes)
		if err != nil {
			log.Error("cannot read spool", "err", err)
			sleep(ctx, o.RetryMax)
			continue
		}
		if len(batch) == 0 {
			select {
			case <-ctx.Done():
			case <-notify:
			}
			continue
		}
		if o.FlushInterval > 0 && !full(batch, o) && !closed(o.FlushNow) {
			// A partial batch waits until its oldest record is FlushInterval
			// old or a full batch is pending, then goes out with whatever
			// has accumulated by then. The wait is capped at FlushInterval
			// so a wall-clock step backwards (records stamped "in the
			// future") cannot stall delivery.
			wait := min(time.Until(batch[0].TS.Add(o.FlushInterval)), o.FlushInterval)
			if wait > 0 {
				waitForBatch(ctx, src, name, notify, o.FlushNow, wait, o.MaxRecords)
				if ctx.Err() != nil {
					return
				}
				if batch, err = src.Read(name, o.MaxRecords, o.MaxBytes); err != nil || len(batch) == 0 {
					continue
				}
			}
		}

		lines := make([][]byte, len(batch))
		for i, e := range batch {
			lines[i] = e.Data
		}
		if err := out.Deliver(ctx, lines); err != nil {
			if ctx.Err() != nil {
				return
			}
			o.OnError(err)
			wait := backoff/2 + rand.N(backoff/2+1) //nolint:gosec // jitter
			if !failing {
				log.Warn("delivery failed; records stay spooled and will be retried", "err", err, "retry_in", wait)
			} else {
				log.Debug("delivery failed again", "err", err, "retry_in", wait)
			}
			failing = true
			sleep(ctx, wait)
			backoff = min(backoff*2, o.RetryMax)
			continue
		}
		if failing {
			log.Info("delivery recovered")
		}
		failing, backoff = false, o.RetryMin
		if err := src.Ack(name, batch[len(batch)-1].Seq); err != nil {
			log.Error("cannot advance spool cursor; batch may be delivered again", "err", err)
			sleep(ctx, o.RetryMin)
		}
	}
}

func full(batch []spool.Entry, o DrainOptions) bool {
	if len(batch) >= o.MaxRecords {
		return true
	}
	n := 0
	for _, e := range batch {
		n += len(e.Data)
	}
	return n >= o.MaxBytes
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false // also for a nil channel
	}
}

// waitForBatch waits until the flush deadline passes, enough records are
// pending for a full batch, flushNow is closed, or ctx ends.
func waitForBatch(ctx context.Context, src Source, name string, notify, flushNow <-chan struct{}, wait time.Duration, maxRecords int) {
	t := time.NewTimer(wait)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-flushNow:
			return
		case <-t.C:
			return
		case <-notify:
			if src.Pending(name) >= uint64(maxRecords) { //nolint:gosec // positive
				return
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
