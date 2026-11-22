package snapshot

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

const (
	defaultRetryInitial = 100 * time.Millisecond
	defaultRetryMax     = 30 * time.Second
)

// deliverer sends batches to the sink with the database's retry,
// on_permanent_error and sink.timeout settings, the way the engine does. It
// has no LSN to confirm: a snapshot that fails part-way is simply run again.
type deliverer struct {
	name        string
	sink        event.Sink
	deadLetter  event.DeadLetter // nil: a skipped batch is only logged and counted
	retry       config.RetryConfig
	onPermanent string
	timeout     time.Duration
	metrics     event.Metrics
	log         *slog.Logger
	sleep       func(context.Context, time.Duration) error // test seam
}

// send returns nil once the batch is durable at the sink or was skipped under
// on_permanent_error: skip, after the dead letter, if any, stored it. A failed
// dead-letter write is retried like a sink call and then fails the snapshot.
func (d *deliverer) send(ctx context.Context, b event.Batch) error {
	err := d.retrying(ctx, b, d.attempt)
	if err == nil || d.onPermanent != "skip" || !event.IsPermanent(err) {
		return err
	}
	if d.deadLetter != nil {
		cause := err
		if err := d.retrying(ctx, b, func(ctx context.Context, b event.Batch) error {
			return d.withTimeout(ctx, func(ctx context.Context) error { return d.deadLetter.SendFailed(ctx, b, cause) })
		}); err != nil {
			return fmt.Errorf("dead-letter write failed (batch rejected by the sink: %v): %w", cause, err)
		}
	}
	d.log.Error("skipping batch after permanent sink error", "events", len(b.Events), "err", err)
	d.metrics.Count("events_skipped_total", float64(len(b.Events)), "database", d.name)
	return nil
}

func (d *deliverer) retrying(ctx context.Context, b event.Batch, call func(context.Context, event.Batch) error) error {
	initial, maxBackoff := d.retry.Initial, d.retry.Max
	if initial <= 0 {
		initial = defaultRetryInitial
	}
	if maxBackoff <= 0 {
		maxBackoff = defaultRetryMax
	}
	backoff := initial
	for attempt := 1; ; attempt++ {
		err := call(ctx, b)
		if err == nil || event.IsPermanent(err) {
			return err
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%w: last error: %w", ctx.Err(), err)
		}
		switch d.retry.Policy {
		case "forever":
		case "max_attempts":
			if d.retry.MaxAttempts > 0 && attempt >= d.retry.MaxAttempts {
				return fmt.Errorf("giving up after %d attempts: %w", attempt, err)
			}
		default:
			return err
		}
		wait := backoff/2 + rand.N(backoff-backoff/2+1)
		d.log.Warn("sink send failed, retrying", "attempt", attempt, "backoff", wait.String(), "err", err)
		if err := d.sleep(ctx, wait); err != nil {
			return err
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (d *deliverer) attempt(ctx context.Context, b event.Batch) error {
	return d.withTimeout(ctx, func(ctx context.Context) error {
		start := time.Now()
		err := d.sink.Send(ctx, b)
		d.metrics.Observe("sink_latency_seconds", time.Since(start).Seconds(), "database", d.name)
		if err == nil {
			d.metrics.Count("events_total", float64(len(b.Events)), "database", d.name)
		}
		return err
	})
}

// withTimeout runs f with the sink timeout, when set, as its deadline.
func (d *deliverer) withTimeout(ctx context.Context, f func(context.Context) error) error {
	if d.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}
	return f(ctx)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
