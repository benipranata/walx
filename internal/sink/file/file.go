// Package file is the sink that appends one JSON line per event to a file.
package file

import (
	"context"
	"errors"
	"fmt"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/appendlog"
	"github.com/benipranata/walx/internal/sink/jsonl"
)

// File is the destination of a Sink; *os.File implements it.
type File = appendlog.File

// Sink appends events as JSON lines to a File it owns and closes.
type Sink struct {
	log *appendlog.Log
}

// New returns a Sink writing to f. Close closes f.
func New(f File) *Sink { return &Sink{log: appendlog.New(f)} }

// Factory builds the file sink of a database. It opens (and creates if
// missing) cfg.Path in append mode, so an unusable path is a startup error
// naming the path.
func Factory(database string, cfg config.SinkConfig) (event.Sink, error) {
	if cfg.Path == "" {
		return nil, errors.New("file sink needs sink.path")
	}
	l, err := appendlog.Open(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("file sink %w", err)
	}
	return &Sink{log: l}, nil
}

// Send appends one JSON line per event, in order, in a single Write, then
// fsyncs; it returns nil only after the fsync. An event that cannot be
// encoded fails the batch with a Permanent error before anything is written.
// A write or fsync error is returned as is, so the batch is retried; the
// failed attempt may have left lines (or a torn last line) in the file, so a
// retry can repeat lines, which consumers dedupe by Event.ID.
func (s *Sink) Send(ctx context.Context, b event.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	buf, err := jsonl.Encode(b.Events)
	if err != nil {
		return event.Permanent(fmt.Errorf("file sink: %w", err))
	}
	if err := s.log.Append(buf); err != nil {
		if errors.Is(err, appendlog.ErrClosed) {
			return errors.New("file sink closed")
		}
		return fmt.Errorf("file sink: %w", err)
	}
	return nil
}

// Close closes the file. Calling it again is a no-op.
func (s *Sink) Close() error { return s.log.Close() }
