package event

import "context"

// Sink delivers batches to a destination. Send returns nil only when the
// batch is durable there; any other error means it is not.
type Sink interface {
	Send(ctx context.Context, b Batch) error
}

// SinkFunc adapts a function to a Sink.
type SinkFunc func(ctx context.Context, b Batch) error

// Send calls f.
func (f SinkFunc) Send(ctx context.Context, b Batch) error { return f(ctx, b) }
