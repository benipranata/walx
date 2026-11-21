package engine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/benipranata/walx/internal/event"
)

const (
	defaultRetryInitial = 100 * time.Millisecond
	defaultRetryMax     = 30 * time.Second
	backoffFactor       = 2
)

// withRetry wraps send with the configured retry policy. An empty policy
// behaves like "stop", so an unconfigured engine fails on the first error.
// Permanent errors are returned unchanged for the permanent-error handling.
func (e *Engine) withRetry(send func(context.Context, event.Batch) error) func(context.Context, event.Batch) error {
	rc := e.cfg.Retry
	if rc.Initial <= 0 {
		rc.Initial = defaultRetryInitial
	}
	if rc.Max <= 0 {
		rc.Max = defaultRetryMax
	}
	return func(ctx context.Context, b event.Batch) error {
		backoff := rc.Initial
		for attempt := 1; ; attempt++ {
			err := send(ctx, b)
			if err == nil || event.IsPermanent(err) || ctx.Err() != nil {
				return err
			}
			switch rc.Policy {
			case "forever":
			case "max_attempts":
				if rc.MaxAttempts > 0 && attempt >= rc.MaxAttempts {
					return fmt.Errorf("giving up after %d attempts: %w", attempt, err)
				}
			default:
				return err
			}
			wait := e.jitter(backoff)
			e.log.Warn("sink send failed, retrying", "attempt", attempt, "backoff", wait.String(),
				"commit_lsn", b.CommitLSN.String(), "end_lsn", b.EndLSN.String(), "err", err)
			if err := e.sleep(ctx, wait); err != nil {
				return err
			}
			backoff = min(backoff*backoffFactor, rc.Max)
		}
	}
}

// sleepCtx waits d or until ctx is done, returning ctx.Err() in that case.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// jitterBackoff returns a random duration in [d/2, d], so the cap holds.
func jitterBackoff(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(d-half+1)
}
