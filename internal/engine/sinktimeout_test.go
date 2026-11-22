package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

func TestSinkTimeout(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "a call that outlives the timeout is retried and confirmed after success", run: func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			var calls atomic.Int32
			var deadlines atomic.Int32
			sink := &closeSink{send: func(c context.Context, b event.Batch) error {
				if _, ok := c.Deadline(); ok {
					deadlines.Add(1)
				}
				if calls.Add(1) < 3 {
					<-c.Done()
					return c.Err()
				}
				require.Empty(t, src.Confirmed(), "nothing is confirmed before the call succeeds")
				return nil
			}}
			e := New(Config{Name: "a", Source: src, Sink: sink, SinkTimeout: 10 * time.Millisecond,
				Retry: config.RetryConfig{Policy: "forever"}})
			stubDecode(e)
			clock := &fakeClock{}
			e.sleep = clock.sleep
			done := runEngine(ctx, e)
			feed(ctx, src, 1)
			require.Eventually(t, func() bool { return len(src.Confirmed()) == 1 }, 5*time.Second, time.Millisecond)
			require.Equal(t, []pglogrepl.LSN{10}, src.Confirmed())
			require.EqualValues(t, 3, calls.Load())
			require.EqualValues(t, 3, deadlines.Load())
			require.Len(t, clock.waits, 2, "each timeout went through the retry backoff")
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
		{name: "a timeout is not permanent, so skip mode does not drop the batch", run: func(t *testing.T) {
			src := newFakeSource()
			sink := &closeSink{send: func(c context.Context, b event.Batch) error {
				<-c.Done()
				return c.Err()
			}}
			e := New(Config{Name: "a", Source: src, Sink: sink, SinkTimeout: 5 * time.Millisecond,
				OnPermanentError: "skip", Retry: config.RetryConfig{Policy: "stop"}})
			stubDecode(e)
			done := runEngine(context.Background(), e)
			feed(context.Background(), src, 1)
			err := wait(t, done)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.False(t, event.IsPermanent(err))
			require.Empty(t, src.Confirmed())
		}},
		{name: "max_attempts gives up after repeated timeouts", run: func(t *testing.T) {
			src := newFakeSource()
			var calls atomic.Int32
			sink := &closeSink{send: func(c context.Context, b event.Batch) error {
				calls.Add(1)
				<-c.Done()
				return c.Err()
			}}
			e := New(Config{Name: "a", Source: src, Sink: sink, SinkTimeout: 5 * time.Millisecond,
				Retry: retryCfg("max_attempts", 2)})
			stubDecode(e)
			e.sleep = (&fakeClock{}).sleep
			done := runEngine(context.Background(), e)
			feed(context.Background(), src, 1)
			require.ErrorIs(t, wait(t, done), context.DeadlineExceeded)
			require.EqualValues(t, 2, calls.Load())
			require.Empty(t, src.Confirmed())
		}},
		{name: "no timeout configured leaves the call context without a deadline", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			var hasDeadline atomic.Bool
			sink := &closeSink{send: func(c context.Context, b event.Batch) error {
				_, ok := c.Deadline()
				hasDeadline.Store(ok)
				return nil
			}}
			e := New(Config{Name: "a", Source: src, Sink: sink})
			stubDecode(e)
			done := runEngine(ctx, e)
			feed(ctx, src, 1)
			require.Eventually(t, func() bool { return len(src.Confirmed()) == 1 }, 5*time.Second, time.Millisecond)
			require.False(t, hasDeadline.Load())
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
		{name: "the timeout still applies during shutdown and is not a shutdown timeout", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			entered := make(chan struct{})
			sink := &closeSink{send: func(c context.Context, b event.Batch) error {
				close(entered)
				<-c.Done()
				return c.Err()
			}}
			e := New(Config{Name: "a", Source: src, Sink: sink, SinkTimeout: 50 * time.Millisecond, ShutdownTimeout: time.Hour})
			stubDecode(e)
			done := runEngine(ctx, e)
			feed(ctx, src, 1)
			<-entered
			cancel()
			err := wait(t, done)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NotErrorIs(t, err, ErrShutdownTimeout)
			require.Empty(t, src.Confirmed())
		}},
		{name: "the timeout context is cancelled after a successful call", run: func(t *testing.T) {
			var got context.Context
			send := (&Engine{cfg: Config{SinkTimeout: time.Hour}}).withSinkTimeout(func(c context.Context, _ event.Batch) error {
				got = c
				return nil
			})
			require.NoError(t, send(context.Background(), event.Batch{}))
			require.ErrorIs(t, got.Err(), context.Canceled)
		}},
		{name: "a sink error other than the deadline passes through", run: func(t *testing.T) {
			boom := errors.New("boom")
			send := (&Engine{cfg: Config{SinkTimeout: time.Hour}}).withSinkTimeout(func(context.Context, event.Batch) error { return boom })
			require.ErrorIs(t, send(context.Background(), event.Batch{}), boom)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
