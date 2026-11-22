package engine

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/postgres"
)

// sourceError carries the error a Source's Run returned, to tell it from a
// decode error.
type sourceError struct{ err error }

func (e *sourceError) Error() string { return "source: " + e.err.Error() }

func (e *sourceError) Unwrap() error { return e.err }

// stream feeds queue from the current Source and, when NewSource is set,
// replaces the Source after it fails with a retryable error.
//
// Before the replacement starts, the batches already queued or in flight are
// delivered and confirmed (waitDrained). The old source is dead, so their
// confirms go to the new one, and the resume LSN handed to NewSource then
// covers them: the stream restarts exactly after the last delivered batch.
// Dropping the queue instead would make the sink send them twice, and an
// in-flight call cannot be taken back anyway. A transaction still being
// buffered is discarded, and the server resends it whole (same IDs).
func (e *Engine) stream(ctx, srcCtx context.Context, queue chan<- event.Batch) error {
	rc := e.retryBounds()
	backoff := rc.Initial
	src := *e.cur.Load()
	for {
		progressed, err := e.session(srcCtx, ctx, src, queue)
		var se *sourceError
		if !errors.As(err, &se) {
			return err
		}
		cause := se.err
		if cause == nil || (srcCtx.Err() != nil && errors.Is(cause, srcCtx.Err())) {
			return nil
		}
		if e.cfg.NewSource == nil || ctx.Err() != nil || postgres.IsFatal(cause) || event.IsPermanent(cause) {
			return cause
		}
		if progressed {
			backoff = rc.Initial
		}
		if e.cfg.Buffer != nil {
			e.cfg.Buffer.Reset()
		}
		if err := e.waitDrained(ctx); err != nil {
			return nil
		}
		next, nextBackoff, err := e.reconnect(ctx, cause, backoff, rc.Max)
		if err != nil || next == nil {
			return err
		}
		backoff = nextBackoff
		src = next
		e.setSource(src)
		e.log.Info("reconnected", "resume_lsn", pglogrepl.LSN(e.confirmed.Load()).String())
	}
}

// session runs src and reads it until it fails, a decode error occurs or ctx
// ends. A source error is returned as *sourceError. After ctx ends it waits
// for src.Run, which stops once the deliverer cancels srcCtx. After a decode
// error it cannot wait: the run context is cancelled only after stream
// returns, so Run (see srcRuns) is awaited there instead.
func (e *Engine) session(srcCtx, ctx context.Context, src Source, queue chan<- event.Batch) (bool, error) {
	runErr := make(chan error, 1)
	e.srcRuns.Add(1)
	go func() {
		defer e.srcRuns.Done()
		runErr <- src.Run(srcCtx)
	}()
	progressed, err := e.read(ctx, src, queue, runErr)
	if err == nil {
		if rerr := <-runErr; rerr != nil {
			err = &sourceError{rerr}
		}
	}
	return progressed, err
}

// waitDrained blocks until no batch is queued or in flight, or ctx ends.
func (e *Engine) waitDrained(ctx context.Context) error {
	for e.pending.Load() != 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.idle:
		}
	}
	return nil
}

// reconnect calls NewSource until it succeeds, waiting a growing backoff
// before every attempt. It returns a nil source and a nil error when ctx ends,
// and the error of a fatal failure. The returned backoff is the one for the
// next reconnect should the new stream make no progress.
func (e *Engine) reconnect(ctx context.Context, cause error, backoff, maxBackoff time.Duration) (Source, time.Duration, error) {
	for attempt := 1; ; attempt++ {
		resume := pglogrepl.LSN(e.confirmed.Load())
		wait := e.jitter(backoff)
		e.log.Warn("reconnecting", "attempt", attempt, "backoff", wait.String(),
			"resume_lsn", resume.String(), "err", cause)
		if err := e.sleep(ctx, wait); err != nil {
			return nil, backoff, nil
		}
		backoff = min(backoff*backoffFactor, maxBackoff)
		src, err := e.cfg.NewSource(ctx, resume)
		if err == nil {
			return src, backoff, nil
		}
		if ctx.Err() != nil {
			return nil, backoff, nil
		}
		if postgres.IsFatal(err) || event.IsPermanent(err) {
			return nil, backoff, err
		}
		cause = err
	}
}

func (e *Engine) setSource(src Source) { e.cur.Store(&src) }

// confirmToSource records lsn as the resume point and confirms it on the
// current source. Standby ignores an LSN below its own, so a late confirm
// never moves the slot back.
func (e *Engine) confirmToSource(lsn event.LSN) {
	for {
		cur := e.confirmed.Load()
		if uint64(lsn) <= cur || e.confirmed.CompareAndSwap(cur, uint64(lsn)) {
			break
		}
	}
	(*e.cur.Load()).SetConfirmed(pglogrepl.LSN(lsn))
}

// release marks one queued batch as done and wakes waitDrained at zero.
func (e *Engine) release() {
	if e.pending.Add(-1) == 0 {
		select {
		case e.idle <- struct{}{}:
		default:
		}
	}
}
