// Package file is the sink that appends one JSON line per event to a file.
package file

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

// fileMode is the mode of a created file: owner read/write, others read, so
// log shippers running as another user can tail it (the umask still applies).
const fileMode = 0o644

// File is the destination of a Sink; *os.File implements it.
type File interface {
	io.WriteCloser
	Sync() error
}

// Sink appends events as JSON lines to a File it owns and closes.
type Sink struct {
	mu     sync.Mutex
	f      File
	closed bool
	torn   bool
}

// New returns a Sink writing to f. Close closes f.
func New(f File) *Sink { return &Sink{f: f} }

// Factory builds the file sink of a database. It opens (and creates if
// missing) cfg.Path in append mode, so an unusable path is a startup error
// naming the path.
func Factory(database string, cfg config.SinkConfig) (event.Sink, error) {
	if cfg.Path == "" {
		return nil, errors.New("file sink needs sink.path")
	}
	f, err := os.OpenFile(cfg.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileMode)
	if err != nil {
		return nil, fmt.Errorf("file sink path %q is not writable: %w", cfg.Path, err)
	}
	return New(f), nil
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("file sink closed")
	}
	if s.torn {
		// A failed write may have left a partial line; end it so the retry
		// starts on a fresh line instead of corrupting its first event.
		buf = append([]byte{'\n'}, buf...)
	}
	n, err := s.f.Write(buf)
	if err == nil && n < len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.torn = n > 0
		return fmt.Errorf("file sink: write: %w", err)
	}
	s.torn = false
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("file sink: fsync: %w", err)
	}
	return nil
}

// Close closes the file. Calling it again is a no-op.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.f.Close()
}
