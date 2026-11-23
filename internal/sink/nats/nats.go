// Package nats is the sink that publishes one event per JetStream message.
package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/subject"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// DefaultAckWait bounds the wait for a batch's acks when the Send context
	// has no deadline (sink.timeout, default 60s, sets one).
	DefaultAckWait = 30 * time.Second

	// chunkSize is how many messages are in flight before their acks are
	// awaited, kept below the client's pending limit.
	chunkSize = 1000
)

// Options configure a Sink.
type Options struct {
	Database string            // the {database} placeholder
	Template *subject.Template // subject template; nil means subject.DefaultTemplate
	// FSMColumns maps "schema.table" to its fsm_column; those tables use the
	// new state as the {token} instead of the op.
	FSMColumns map[string]string
	AckWait    time.Duration // 0 means DefaultAckWait
	// OnClose runs once from Close, to release the connection; may be nil.
	OnClose func() error
}

// Sink publishes events to JetStream subjects and returns only after the
// stream acknowledged every message of the batch.
//
// Messages are published asynchronously in order, then their acks are
// awaited. Nats-Msg-Id is Event.ID, so a retried batch is deduplicated by the
// stream within its duplicate window (Duplicates, 2 minutes by default); a
// longer outage can duplicate events, which consumers dedupe by Event.ID. If
// one message fails while later ones were accepted, the retry republishes the
// whole batch and those later events appear twice, out of order, outside the
// dedup window.
type Sink struct {
	mu       sync.Mutex
	pub      Publisher
	database string
	tmpl     *subject.Template
	fsm      map[string]string
	ackWait  time.Duration
	onClose  func() error
	closed   bool
}

// New returns a Sink publishing through pub.
func New(pub Publisher, o Options) *Sink {
	s := &Sink{pub: pub, database: o.Database, tmpl: o.Template, fsm: o.FSMColumns, ackWait: o.AckWait, onClose: o.OnClose}
	if s.tmpl == nil {
		s.tmpl, _ = subject.Parse(subject.DefaultTemplate)
	}
	if s.ackWait <= 0 {
		s.ackWait = DefaultAckWait
	}
	return s
}

// Send publishes the batch in order and returns nil after every ack. An event
// whose subject cannot be built (an empty token) or whose body cannot be
// encoded fails the batch with a Permanent error before anything is
// published. One exceeding the server's maximum payload is also Permanent but
// is found while publishing, so earlier messages of the batch may already be
// in the stream. Any other failure, a publish error, a missing ack, a stream
// rejecting the message or the ack wait expiring, is retryable.
func (s *Sink) Send(ctx context.Context, b event.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	msgs := make([]*nats.Msg, len(b.Events))
	for i, e := range b.Events {
		m, err := s.message(e, b.CommitLSN)
		if err != nil {
			return event.Permanent(fmt.Errorf("nats sink: %w", err))
		}
		msgs[i] = m
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("nats sink closed")
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.ackWait)
		defer cancel()
	}
	for start := 0; start < len(msgs); start += chunkSize {
		if err := s.publish(ctx, msgs[start:min(start+chunkSize, len(msgs))]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) publish(ctx context.Context, msgs []*nats.Msg) error {
	futs := make([]jetstream.PubAckFuture, 0, len(msgs))
	for _, m := range msgs {
		f, err := s.pub.PublishMsgAsync(m)
		if errors.Is(err, nats.ErrMaxPayload) {
			return event.Permanent(fmt.Errorf("nats sink: event %s: %w", m.Header.Get(nats.MsgIdHdr), err))
		}
		if err != nil {
			return fmt.Errorf("nats sink: publish %s: %w", m.Header.Get(nats.MsgIdHdr), err)
		}
		futs = append(futs, f)
	}
	for _, f := range futs {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return fmt.Errorf("nats sink: publish %s: %w", f.Msg().Header.Get(nats.MsgIdHdr), err)
		case <-ctx.Done():
			return fmt.Errorf("nats sink: waiting for ack of %s: %w", f.Msg().Header.Get(nats.MsgIdHdr), ctx.Err())
		}
	}
	return nil
}

// Close releases the connection. Calling it again is a no-op.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.onClose != nil {
		return s.onClose()
	}
	return nil
}
