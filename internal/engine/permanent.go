package engine

import (
	"context"

	"github.com/benipranata/walx/internal/event"
)

// withPermanent wraps send with the on_permanent_error policy. Only errors
// marked event.Permanent can be skipped; any other error, such as
// *postgres.TxTooLargeError, passes through. A skipped batch returns nil, so
// the caller confirms its EndLSN.
func (e *Engine) withPermanent(send func(context.Context, event.Batch) error) func(context.Context, event.Batch) error {
	return func(ctx context.Context, b event.Batch) error {
		err := send(ctx, b)
		if err == nil || e.cfg.OnPermanentError != "skip" || !event.IsPermanent(err) {
			return err
		}
		events := make([]event.Event, len(b.Events))
		for i, ev := range b.Events {
			events[i] = *ev
		}
		e.log.Error("skipping batch after permanent sink error", "commit_lsn", b.CommitLSN.String(),
			"end_lsn", b.EndLSN.String(), "events", len(b.Events), "err", err, "batch_events", events)
		e.cfg.Metrics.Count("events_skipped_total", float64(len(b.Events)), "database", e.cfg.Name)
		return nil
	}
}
