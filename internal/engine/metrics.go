package engine

import (
	"context"
	"time"

	"github.com/benipranata/walx/internal/event"
)

// withMetrics times every attempt of send (sink_latency_seconds) and counts
// the batch's events in events_total once an attempt succeeds. It sits
// innermost, so retries are timed one by one and a skipped batch is not
// counted as delivered.
func (e *Engine) withMetrics(send func(context.Context, event.Batch) error) func(context.Context, event.Batch) error {
	return func(ctx context.Context, b event.Batch) error {
		start := time.Now()
		err := send(ctx, b)
		e.cfg.Metrics.Observe("sink_latency_seconds", time.Since(start).Seconds(), "database", e.cfg.Name)
		if err == nil {
			e.cfg.Metrics.Count("events_total", float64(len(b.Events)), "database", e.cfg.Name)
		}
		return err
	}
}
