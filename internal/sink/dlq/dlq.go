// Package dlq is the dead-letter sink: it appends the events of batches the
// main sink rejected with a Permanent error to a file, one envelope line per
// event.
package dlq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/appendlog"
	"github.com/benipranata/walx/internal/sink/jsonl"
)

// envelope is one dead-letter line. Event holds the event JSON exactly as the
// main sink would have received it.
type envelope struct {
	FailedAt time.Time       `json:"failed_at"`
	Database string          `json:"database"`
	Sink     string          `json:"sink"`
	Error    string          `json:"error"`
	Event    json.RawMessage `json:"event"`
}

// Sink appends envelopes to a file it owns and closes.
type Sink struct {
	log      *appendlog.Log
	database string
	sinkType string
	now      func() time.Time
}

// Option customises a Sink.
type Option func(*Sink)

// WithClock sets the clock that stamps failed_at.
func WithClock(now func() time.Time) Option { return func(s *Sink) { s.now = now } }

// New returns a Sink writing to l for the entry database whose main sink has
// type sinkType. Close closes l.
func New(l *appendlog.Log, database, sinkType string, opts ...Option) *Sink {
	s := &Sink{log: l, database: database, sinkType: sinkType, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Open opens (and creates if missing) path in append mode, so an unusable
// path is a startup error naming the path. A last line torn by a crash is
// ended first, so the redelivered batch does not lose its first line to it.
func Open(path, database, sinkType string, opts ...Option) (*Sink, error) {
	if path == "" {
		return nil, errors.New("dead-letter file needs dlq.path")
	}
	l, err := appendlog.OpenEndingLine(path)
	if err != nil {
		return nil, fmt.Errorf("dead-letter file %w", err)
	}
	return New(l, database, sinkType, opts...), nil
}

// SendFailed appends one envelope line per event of b, in order, in a single
// write, then fsyncs; it returns nil only after the fsync. An event that
// cannot be encoded fails the batch with a Permanent error before anything is
// written (the main sink could not encode it either). A write or fsync error
// is returned as is, so the caller retries; a retry can repeat lines, which
// replay dedupes by event id.
func (s *Sink) SendFailed(ctx context.Context, b event.Batch, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	failedAt := s.now().UTC()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, ev := range b.Events {
		line, err := jsonl.Encode([]*event.Event{ev})
		if err != nil {
			return event.Permanent(fmt.Errorf("dead-letter file: %w", err))
		}
		env := envelope{FailedAt: failedAt, Database: s.database, Sink: s.sinkType, Error: msg, Event: bytes.TrimSuffix(line, []byte("\n"))}
		if err := enc.Encode(env); err != nil {
			return event.Permanent(fmt.Errorf("dead-letter file: encode envelope of %s: %w", ev.ID, err))
		}
	}
	if err := s.log.Append(buf.Bytes()); err != nil {
		return fmt.Errorf("dead-letter file: %w", err)
	}
	return nil
}

// Close closes the file. Calling it again is a no-op.
func (s *Sink) Close() error { return s.log.Close() }
