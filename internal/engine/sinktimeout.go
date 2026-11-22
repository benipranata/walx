package engine

import (
	"context"

	"github.com/benipranata/walx/internal/event"
)

// withSinkTimeout gives every attempt of send its own deadline when
// SinkTimeout is set. It sits inside withRetry, so an expired attempt is an
// ordinary failure that is retried and never confirmed. During shutdown the
// deadline still applies on top of ShutdownTimeout.
func (e *Engine) withSinkTimeout(send func(context.Context, event.Batch) error) func(context.Context, event.Batch) error {
	d := e.cfg.SinkTimeout
	if d <= 0 {
		return send
	}
	return func(ctx context.Context, b event.Batch) error {
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return send(ctx, b)
	}
}
