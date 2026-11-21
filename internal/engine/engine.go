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

	// OnPermanentError is "stop" (default) or "skip": skip logs, counts and
	// confirms a batch the sink rejected with event.Permanent.
	OnPermanentError string
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

	sleep  func(ctx context.Context, d time.Duration) error // retry wait, a fake clock in tests
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
	e := &Engine{cfg: cfg, log: log.With("db", cfg.Name)}
	e.decode = e.decodeMessage
	e.sleep = sleepCtx
	e.jitter = jitterBackoff
	if cfg.Metrics == nil {
		e.cfg.Metrics = event.NopMetrics{}
	}
	e.send = e.withPermanent(e.withRetry(cfg.Sink.Send))
	e.confirm = func(lsn event.LSN) { cfg.Source.SetConfirmed(pglogrepl.LSN(lsn)) }
	e.empty = func(b event.Batch) {
		// Confirming an empty batch is the idle-advance task; until then it
		// is neither sent nor confirmed.
		e.log.Debug("empty batch not sent", "commit_lsn", b.CommitLSN.String(), "end_lsn", b.EndLSN.String())
	}
	return e
}

// Run streams until ctx is done or a stage fails, and returns the first
// error: ctx.Err() on cancellation, the stream or decode error, or the sink
// error. It returns only after every goroutine has stopped.
func (e *Engine) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
	wg.Add(3)
	go func() {
		defer wg.Done()
		if err := e.cfg.Source.Run(ctx); err != nil {
			fail(err)
		}
	}()
	go func() {
		defer wg.Done()
		defer close(queue)
		if err := e.read(ctx, queue); err != nil {
			fail(err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := e.deliver(ctx, queue); err != nil {
			fail(err)
		}
	}()
	wg.Wait()

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

// read decodes stream messages into queue. A full queue blocks it, which
// stops it taking from the Source; Standby keeps answering the server.
func (e *Engine) read(ctx context.Context, queue chan<- event.Batch) error {
	msgs := e.cfg.Source.Messages()
	for {
		select {
		case <-ctx.Done():
			return nil
		case xld, ok := <-msgs:
			if !ok {
				return nil
			}
			b, err := e.decode(xld)
			if err != nil {
				return err
			}
			if b == nil {
				continue
			}
			select {
			case queue <- *b:
			case <-ctx.Done():
				return nil
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
		select {
		case <-ctx.Done():
			return nil
		case b, ok := <-queue:
			if !ok {
				return nil
			}
			if e.cfg.Filter != nil {
				b = e.cfg.Filter(b)
			}
			if len(b.Events) == 0 {
				e.empty(b)
				continue
			}
			if err := e.send(ctx, b); err != nil {
				if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
					return nil
				}
				e.log.Error("sink send failed", "commit_lsn", b.CommitLSN.String(),
					"end_lsn", b.EndLSN.String(), "events", len(b.Events), "err", err)
				return fmt.Errorf("sink send: %w", err)
			}
			e.confirm(b.EndLSN)
			e.log.Debug("batch delivered", "commit_lsn", b.CommitLSN.String(),
				"end_lsn", b.EndLSN.String(), "events", len(b.Events))
		}
	}
}
