// Package engine moves decoded transactions from a replication stream to a
// sink and confirms them once they are durable.
package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/postgres"
)

const defaultQueueSize = 16

// Filter turns a batch into the batch to deliver. It must not mutate its
// input. A nil Filter delivers every batch unchanged.
type Filter func(event.Batch) event.Batch

// Config wires one database pipeline.
type Config struct {
	Name      string             // the databases entry name, logged as "db"
	Source    Source             // a started stream
	Buffer    *postgres.TxBuffer // decodes pgoutput messages into batches
	Filter    Filter
	Sink      event.Sink
	Retry     config.RetryConfig // zero value (empty policy) stops on the first sink error
	QueueSize int                // decoded batches held between reader and sink, 16 when <= 0
	Logger    *slog.Logger       // discards when nil
	Metrics   event.Metrics      // discards when nil

	// NewSource starts a fresh stream after the current Source failed; nil
	// disables reconnecting, so a Source error ends Run. resume is the highest
	// LSN the engine delivered and confirmed, or 0 before the first confirm.
	// The factory reconnects (postgres.Conn.Reconnect, which also resets the
	// relation cache), checks the slot, starts streaming at resume and returns
	// a Standby seeded with max(slot confirmed_flush_lsn, resume). It checks the
	// slot without auto-create, so a slot dropped meanwhile ends Run with
	// ErrSlotMissing instead of being recreated past the lost WAL. It closes
	// the failed connection. Any error it returns is retried with backoff
	// unless postgres.IsFatal or permanent, so it must not retry fatal errors
	// itself; postgres.ErrSlotActive is retried here but is fatal at first
	// start, which the caller does before New.
	NewSource func(ctx context.Context, resume pglogrepl.LSN) (Source, error)

	// OnPermanentError is "stop" (default) or "skip": skip logs, counts and
	// confirms a batch the sink rejected with event.Permanent.
	OnPermanentError string

	// DeadLetter receives the batches skipped under on_permanent_error: skip
	// and is retried like the sink. A skip is confirmed only after it returns
	// nil. Nil keeps the batch only in the error log. Run closes it after the
	// sink.
	DeadLetter event.DeadLetter

	// ShutdownTimeout bounds how long a cancelled Run waits for the in-flight
	// sink call; 30s when <= 0.
	ShutdownTimeout time.Duration

	// SinkTimeout bounds each Send attempt; 0 = no timeout (config defaults
	// to 60s). An attempt that hits it fails with context.DeadlineExceeded,
	// an ordinary retryable error.
	SinkTimeout time.Duration
}

// Engine runs one database pipeline: a reader goroutine decodes the stream
// into a bounded queue, and the sink goroutine delivers the queue serially.
type Engine struct {
	cfg Config
	log *slog.Logger

	// Seams for the retry, permanent-error, idle-advance and shutdown tasks.
	decode  func(pglogrepl.XLogData) (*event.Batch, error)
	send    func(ctx context.Context, b event.Batch) error
	confirm func(lsn event.LSN)
	empty   func(b event.Batch)

	pending atomic.Int64  // batches queued or being delivered
	idle    chan struct{} // signalled when pending drops to 0

	srcRuns   sync.WaitGroup         // source Run goroutines
	cur       atomic.Pointer[Source] // the source confirms go to
	confirmed atomic.Uint64          // highest LSN confirmed, the resume point after a reconnect

	timedOut atomic.Bool // the shutdown timeout fired on an in-flight sink call

	stop   context.Context                                  // the Run context; ends retry waits, nil outside Run
	sleep  func(ctx context.Context, d time.Duration) error // retry wait, a fake clock in tests
	timer  func(d time.Duration) (<-chan time.Time, func()) // shutdown deadline, a fake in tests
	jitter func(d time.Duration) time.Duration
}

// New returns an Engine for cfg.
func New(cfg Config) *Engine {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	e := &Engine{cfg: cfg, log: log.With("db", cfg.Name), idle: make(chan struct{}, 1)}
	e.setSource(cfg.Source)
	e.decode = e.decodeMessage
	e.sleep = sleepCtx
	e.timer = realTimer
	e.jitter = jitterBackoff
	if cfg.Metrics == nil {
		e.cfg.Metrics = event.NopMetrics{}
	}
	e.send = e.withPermanent(e.withRetry(e.withSinkTimeout(e.withMetrics(cfg.Sink.Send))))
	e.confirm = e.confirmToSource
	e.empty = e.confirmEmpty
	return e
}

// Run streams until ctx is done or a stage fails, and returns the first
// error: ctx.Err() on cancellation, the stream or decode error, or the sink
// error. A sink error that arrives after a stop (the call fails, or retry
// stops after a failed attempt) is returned instead of context.Canceled; a
// stop during retry backoff returns context.Canceled. It
// returns only after every goroutine has stopped and closes the sink exactly
// once, except after ErrShutdownTimeout: then an abandoned sink goroutine and
// an unawaited Close may outlive Run. See shutdown.go.
func (e *Engine) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	e.stop = ctx
	// The source outlives ctx until the deliverer is done, so its final
	// status update carries the last confirmed LSN.
	srcCtx, cancelSrc := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSrc()

	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
	)
	fail := func(err error) {
		once.Do(func() { first = err })
		cancel()
	}
	queue := make(chan event.Batch, e.cfg.QueueSize)

	e.log.Info("engine started", "queue_size", e.cfg.QueueSize)
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer close(queue)
		if err := e.stream(ctx, srcCtx, queue); err != nil {
			fail(err)
		}
	}()
	go func() {
		defer wg.Done()
		defer cancelSrc()
		if err := e.deliver(ctx, queue); err != nil {
			fail(err)
		}
	}()
	wg.Wait()
	e.srcRuns.Wait()
	e.closeSink()

	if first == nil {
		first = ctx.Err()
	}
	if first != nil && !errors.Is(first, context.Canceled) {
		e.log.Error("engine stopped", "err", first)
	} else {
		e.log.Info("engine stopped")
	}
	return first
}

// read decodes stream messages into queue until ctx ends, a decode error
// occurs or src's Run returns (runErr, reported as *sourceError). A full queue blocks it, which stops
// it taking from the Source; Standby keeps answering the server. progressed
// reports whether any message arrived.
func (e *Engine) read(ctx context.Context, src Source, queue chan<- event.Batch, runErr <-chan error) (progressed bool, err error) {
	msgs := src.Messages()
	kas := src.Keepalives()
	for {
		select {
		case <-ctx.Done():
			return progressed, nil
		case err := <-runErr:
			return progressed, &sourceError{err}
		case lsn, ok := <-kas:
			if !ok {
				kas = nil
				continue
			}
			e.keepalive(event.LSN(lsn))
		case xld, ok := <-msgs:
			if !ok {
				msgs = nil
				continue
			}
			progressed = true
			b, err := e.decode(xld)
			if err != nil {
				return progressed, err
			}
			if b == nil {
				continue
			}
			e.pending.Add(1)
			select {
			case queue <- *b:
			case <-ctx.Done():
				return progressed, nil
			}
		}
	}
}

func (e *Engine) decodeMessage(xld pglogrepl.XLogData) (*event.Batch, error) {
	if len(xld.WALData) == 0 {
		return nil, fmt.Errorf("parse pgoutput message at %s: empty payload", event.LSN(xld.WALStart))
	}
	msg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return nil, fmt.Errorf("parse pgoutput message at %s: %w", event.LSN(xld.WALStart), err)
	}
	return e.cfg.Buffer.Push(msg, event.LSN(xld.WALStart))
}

// deliver sends batches one at a time, in queue order. The batch's EndLSN is
// confirmed only after send returns nil.
func (e *Engine) deliver(ctx context.Context, queue <-chan event.Batch) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case b, ok := <-queue:
			if !ok || ctx.Err() != nil {
				return nil
			}
			if e.cfg.Filter != nil {
				b = e.cfg.Filter(b)
			}
			if len(b.Events) == 0 {
				e.empty(b)
				e.release()
				continue
			}
			if err := e.sendInFlight(ctx, b); err != nil {
				if errors.Is(err, ErrShutdownTimeout) {
					e.log.Error("shutdown timed out", "commit_lsn", b.CommitLSN.String(), "end_lsn", b.EndLSN.String(), "err", err)
					return err
				}
				if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
					return nil
				}
				e.log.Error("sink send failed", "commit_lsn", b.CommitLSN.String(),
					"end_lsn", b.EndLSN.String(), "events", len(b.Events), "err", err)
				return fmt.Errorf("sink send: %w", err)
			}
			e.confirm(b.EndLSN)
			e.release()
			e.log.Debug("batch delivered", "commit_lsn", b.CommitLSN.String(),
				"end_lsn", b.EndLSN.String(), "events", len(b.Events))
		}
	}
}
