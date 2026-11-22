package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/benipranata/walx/internal/event"
)

const defaultShutdownTimeout = 30 * time.Second

// ErrShutdownTimeout is returned by Run when the in-flight sink call did not
// finish within the shutdown timeout. Its batch was not confirmed, so it is
// redelivered on the next start.
var ErrShutdownTimeout = errors.New("shutdown timeout: in-flight sink call unfinished, exiting without confirming")

// sendInFlight sends b on a context that the run context does not cancel, so
// a stop lets the call finish. Once the run context is done it waits for the
// call at most ShutdownTimeout, then cancels it and gives up without
// confirming. The call runs on its own goroutine because a sink may ignore
// its context; such a call is abandoned and its result dropped.
func (e *Engine) sendInFlight(ctx context.Context, b event.Batch) error {
	sendCtx, cancelSend := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSend()
	res := make(chan error, 1)
	go func() { res <- e.send(sendCtx, b) }()

	select {
	case err := <-res:
		return err
	case <-ctx.Done():
	}
	e.log.Info("shutting down, waiting for in-flight sink call", "end_lsn", b.EndLSN.String())
	expired, stopTimer := e.timer(e.shutdownTimeout())
	defer stopTimer()
	select {
	case err := <-res:
		return err
	case <-expired:
		e.timedOut.Store(true)
		return fmt.Errorf("%w (after %s)", ErrShutdownTimeout, e.shutdownTimeout())
	}
}

func (e *Engine) shutdownTimeout() time.Duration {
	if e.cfg.ShutdownTimeout <= 0 {
		return defaultShutdownTimeout
	}
	return e.cfg.ShutdownTimeout
}

// stopping reports whether the run context is done.
func (e *Engine) stopping() bool { return e.stop != nil && e.stop.Err() != nil }

// waitCtx returns ctx that also ends when the run context does, for retry
// waits, plus a release func.
func (e *Engine) waitCtx(ctx context.Context) (context.Context, func()) {
	if e.stop == nil {
		return ctx, func() {}
	}
	wctx, cancel := context.WithCancel(ctx)
	unhook := context.AfterFunc(e.stop, cancel)
	return wctx, func() { unhook(); cancel() }
}

// closeSink closes the sink after the in-flight call is done. After a
// shutdown timeout the call may still be stuck and Close may block too, so it
// runs unawaited then. The flag is separate from the returned error because
// Run keeps only the first error, which may not be the timeout.
func (e *Engine) closeSink() {
	closeIt := func() {
		if err := e.cfg.Sink.Close(); err != nil {
			e.log.Error("sink close failed", "err", err)
		}
	}
	if e.timedOut.Load() {
		go closeIt()
		return
	}
	closeIt()
}

func realTimer(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTimer(d)
	return t.C, func() { t.Stop() }
}
