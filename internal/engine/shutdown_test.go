package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

// closeSink counts Close calls and delegates Send.
type closeSink struct {
	send   func(ctx context.Context, b event.Batch) error
	closed atomic.Int32
}

func (s *closeSink) Send(ctx context.Context, b event.Batch) error { return s.send(ctx, b) }

func (s *closeSink) Close() error { s.closed.Add(1); return nil }

// finalSource records how many LSNs were confirmed when its context ended,
// which is when Standby sends its final status update.
type finalSource struct {
	*fakeSource
	atFinal chan int
}

func (f *finalSource) Run(ctx context.Context) error {
	err := f.fakeSource.Run(ctx)
	f.atFinal <- len(f.Confirmed())
	return err
}

func TestShutdown(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "cancel mid-call lets the call finish, confirms, then closes once", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := &finalSource{fakeSource: newFakeSource(), atFinal: make(chan int, 1)}
			entered := make(chan struct{})
			release := make(chan struct{})
			var calls atomic.Int32
			var sendCtxErr atomic.Value
			sink := &closeSink{send: func(c context.Context, b event.Batch) error {
				calls.Add(1)
				close(entered)
				<-release
				sendCtxErr.Store(c.Err() == nil)
				return nil
			}}
			e := New(Config{Name: "a", Source: src, Sink: sink})
			stubDecode(e)
			done := runEngine(ctx, e)
			feed(ctx, src.fakeSource, 3)
			<-entered
			cancel()
			select {
			case <-done:
				t.Fatal("engine stopped before the in-flight call finished")
			case <-time.After(100 * time.Millisecond):
			}
			require.Empty(t, src.Confirmed())
			require.Zero(t, sink.closed.Load())
			close(release)
			require.ErrorIs(t, wait(t, done), context.Canceled)
			require.Equal(t, []pglogrepl.LSN{10}, src.Confirmed())
			require.Equal(t, true, sendCtxErr.Load(), "sink context must stay live")
			require.Equal(t, 1, <-src.atFinal, "source stops after the confirm")
			require.EqualValues(t, 1, calls.Load(), "queued batches are not sent")
			require.EqualValues(t, 1, sink.closed.Load())
		}},
		{name: "cancel during backoff stops without confirming", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			waiting := make(chan struct{})
			sink := &closeSink{send: func(context.Context, event.Batch) error { return errors.New("down") }}
			e := New(Config{Name: "a", Source: src, Sink: sink, Retry: config.RetryConfig{Policy: "forever"}})
			stubDecode(e)
			e.sleep = func(c context.Context, _ time.Duration) error {
				close(waiting)
				<-c.Done()
				return c.Err()
			}
			done := runEngine(ctx, e)
			feed(ctx, src, 1)
			<-waiting
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
			require.Empty(t, src.Confirmed())
			require.EqualValues(t, 1, sink.closed.Load())
		}},
		{name: "a stuck sink hits the shutdown timeout without confirming", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			entered := make(chan struct{})
			release := make(chan struct{})
			sink := &closeSink{send: func(context.Context, event.Batch) error {
				close(entered)
				<-release
				return nil
			}}
			e := New(Config{Name: "a", Source: src, Sink: sink, ShutdownTimeout: 7 * time.Second})
			stubDecode(e)
			fire := make(chan time.Time)
			var got atomic.Int64
			e.timer = func(d time.Duration) (<-chan time.Time, func()) {
				got.Store(int64(d))
				return fire, func() {}
			}
			done := runEngine(ctx, e)
			feed(ctx, src, 1)
			<-entered
			cancel()
			require.Eventually(t, func() bool { return got.Load() != 0 }, 5*time.Second, time.Millisecond)
			fire <- time.Time{}
			err := wait(t, done)
			require.ErrorIs(t, err, ErrShutdownTimeout)
			require.Contains(t, err.Error(), "7s")
			require.EqualValues(t, 7*time.Second, got.Load())
			require.Empty(t, src.Confirmed())
			close(release)
			require.Eventually(t, func() bool { return sink.closed.Load() == 1 }, 5*time.Second, time.Millisecond)
		}},
		{name: "source error mid-call still lets the call finish and confirm", run: func(t *testing.T) {
			src := newFakeSource()
			entered := make(chan struct{})
			release := make(chan struct{})
			sink := &closeSink{send: func(context.Context, event.Batch) error {
				close(entered)
				<-release
				return nil
			}}
			e := New(Config{Name: "a", Source: src, Sink: sink})
			stubDecode(e)
			done := runEngine(context.Background(), e)
			feed(context.Background(), src, 1)
			<-entered
			boom := errors.New("stream broke")
			src.runErr <- boom
			close(release)
			require.ErrorIs(t, wait(t, done), boom)
			require.Equal(t, []pglogrepl.LSN{10}, src.Confirmed())
			require.EqualValues(t, 1, sink.closed.Load())
		}},
		{name: "source error first, then a stuck sink and blocking Close, still returns", run: func(t *testing.T) {
			src := newFakeSource()
			entered := make(chan struct{})
			release := make(chan struct{})
			closeEntered := make(chan struct{})
			sink := &blockCloseSink{
				closeSink:    closeSink{send: func(context.Context, event.Batch) error { close(entered); <-release; return nil }},
				closeEntered: closeEntered,
				closeRelease: release,
			}
			e := New(Config{Name: "a", Source: src, Sink: sink, ShutdownTimeout: time.Second})
			stubDecode(e)
			fire := make(chan time.Time)
			var armed atomic.Bool
			e.timer = func(time.Duration) (<-chan time.Time, func()) {
				armed.Store(true)
				return fire, func() {}
			}
			done := runEngine(context.Background(), e)
			feed(context.Background(), src, 1)
			<-entered
			boom := errors.New("stream broke")
			src.runErr <- boom
			require.Eventually(t, armed.Load, 5*time.Second, time.Millisecond)
			fire <- time.Time{}
			require.ErrorIs(t, wait(t, done), boom)
			close(release)
		}},
		{name: "default shutdown timeout", run: func(t *testing.T) {
			require.Equal(t, 30*time.Second, New(Config{Source: newFakeSource(), Sink: event.SinkFunc(nil)}).shutdownTimeout())
		}},
		{name: "close error is logged and does not fail the run", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			src := newFakeSource()
			e := New(Config{Name: "a", Source: src, Sink: errCloser{}})
			done := runEngine(ctx, e)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
		{name: "real timer fires and stops", run: func(t *testing.T) {
			ch, stop := realTimer(time.Millisecond)
			defer stop()
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatal("timer did not fire")
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

type errCloser struct{}

func (errCloser) Send(context.Context, event.Batch) error { return nil }

func (errCloser) Close() error { return errors.New("close boom") }

// blockCloseSink blocks Close until closeRelease is closed.
type blockCloseSink struct {
	closeSink
	closeEntered chan struct{}
	closeRelease chan struct{}
}

func (s *blockCloseSink) Close() error {
	close(s.closeEntered)
	<-s.closeRelease
	return nil
}
