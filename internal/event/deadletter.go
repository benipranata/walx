package event

import "context"

// DeadLetter stores batches the sink rejected with a Permanent error, so the
// engine can skip them without losing them. SendFailed returns nil only when
// the batch is durable in the dead-letter store; cause is the sink error.
// Close releases the store's resources.
type DeadLetter interface {
	SendFailed(ctx context.Context, b Batch, cause error) error
	Close() error
}
