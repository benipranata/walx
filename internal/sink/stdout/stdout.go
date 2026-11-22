// Package stdout is the sink that writes one JSON line per event to a writer
// (os.Stdout in production).
package stdout

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/jsonl"
)

// Sink writes events as JSON lines to a writer it does not own.
type Sink struct {
	mu     sync.Mutex
	w      io.Writer
	closed bool
}

// New returns a Sink writing to w. Close never closes w.
func New(w io.Writer) *Sink { return &Sink{w: w} }

// Factory builds the stdout sink of a database; it writes to os.Stdout.
func Factory(database string, cfg config.SinkConfig) (event.Sink, error) {
	return New(os.Stdout), nil
}

// Send writes one JSON line per event, in order, in a single Write. An event
// that cannot be encoded fails the batch with a Permanent error before
// anything is written; a write error is returned as is, so the batch is
// retried (a partial write then repeats lines, which consumers dedupe by
// Event.ID).
func (s *Sink) Send(ctx context.Context, b event.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	buf, err := jsonl.Encode(b.Events)
	if err != nil {
		return event.Permanent(fmt.Errorf("stdout sink: %w", err))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("stdout sink closed")
	}
	n, err := s.w.Write(buf)
	if err == nil && n < len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("stdout sink: write: %w", err)
	}
	return nil
}

// Close marks the sink closed; the writer stays open. Calling it again is a
// no-op.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
