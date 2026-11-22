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
	retry       config.RetryConfig
	onPermanent string
	timeout     time.Duration
	metrics     event.Metrics
	log         *slog.Logger
	sleep       func(context.Context, time.Duration) error // test seam
}

// send returns nil once the batch is durable at the sink or was skipped under
// on_permanent_error: skip.
func (d *deliverer) send(ctx context.Context, b event.Batch) error {
	err := d.retrying(ctx, b)
	if err == nil || d.onPermanent != "skip" || !event.IsPermanent(err) {
		return err
	}
	d.log.Error("skipping batch after permanent sink error", "events", len(b.Events), "err", err)
	d.metrics.Count("events_skipped_total", float64(len(b.Events)), "database", d.name)
	return nil
}

func (d *deliverer) retrying(ctx context.Context, b event.Batch) error {
	initial, maxBackoff := d.retry.Initial, d.retry.Max
	if initial <= 0 {
		initial = defaultRetryInitial
	}
	if maxBackoff <= 0 {
		maxBackoff = defaultRetryMax
	}
	backoff := initial
	for attempt := 1; ; attempt++ {
		err := d.attempt(ctx, b)
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
	if d.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}
	start := time.Now()
	err := d.sink.Send(ctx, b)
	d.metrics.Observe("sink_latency_seconds", time.Since(start).Seconds(), "database", d.name)
	if err == nil {
		d.metrics.Count("events_total", float64(len(b.Events)), "database", d.name)
	}
	return err
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
