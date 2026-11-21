package event

import (
	"context"
	"errors"
)

// Sink delivers batches to a destination. Send returns nil only when the
// batch is durable there; any other error means it is not.
type Sink interface {
	Send(ctx context.Context, b Batch) error
}

// SinkFunc adapts a function to a Sink.
type SinkFunc func(ctx context.Context, b Batch) error

// Send calls f.
func (f SinkFunc) Send(ctx context.Context, b Batch) error { return f(ctx, b) }

// Metrics records counters and gauges. Labels are key/value pairs.
type Metrics interface {
	Count(name string, delta float64, labels ...string)
	Gauge(name string, value float64, labels ...string)
}

// NopMetrics is a Metrics that discards everything.
type NopMetrics struct{}

// Count does nothing.
func (NopMetrics) Count(string, float64, ...string) {}

// Gauge does nothing.
func (NopMetrics) Gauge(string, float64, ...string) {}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as not worth retrying. It returns nil for a nil err.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

// IsPermanent reports whether err, or anything it wraps, was marked Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
