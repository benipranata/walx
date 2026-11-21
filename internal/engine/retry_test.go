package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

// fakeClock records requested waits and returns at once.
type fakeClock struct{ waits []time.Duration }

func (c *fakeClock) sleep(_ context.Context, d time.Duration) error {
	c.waits = append(c.waits, d)
	return nil
}

func retryCfg(policy string, attempts int) config.RetryConfig {
	return config.RetryConfig{Policy: policy, Initial: 100 * time.Millisecond, Max: 30 * time.Second, MaxAttempts: attempts}
}

func TestRetry(t *testing.T) {
	boom := errors.New("boom")
	perm := event.Permanent(boom)
	secs := time.Second
	tests := []struct {
		name      string
		cfg       config.RetryConfig
		failures  int // sink fails this many times, then succeeds
		err       error
		wantCalls int
		wantWaits []time.Duration
		wantErr   error
		wantMsg   string
	}{
		{name: "forever succeeds after N failures", cfg: retryCfg("forever", 0), failures: 3, err: boom,
			wantCalls: 4, wantWaits: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}},
		{name: "forever caps backoff at max", cfg: retryCfg("forever", 0), failures: 11, err: boom,
			wantCalls: 12, wantWaits: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
				800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond, 6400 * time.Millisecond,
				12800 * time.Millisecond, 25600 * time.Millisecond, 30 * secs, 30 * secs}},
		{name: "max_attempts succeeds within budget", cfg: retryCfg("max_attempts", 3), failures: 2, err: boom,
			wantCalls: 3, wantWaits: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}},
		{name: "max_attempts gives up", cfg: retryCfg("max_attempts", 3), failures: 10, err: boom,
			wantCalls: 3, wantWaits: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}, wantErr: boom,
			wantMsg: "giving up after 3 attempts"},
		{name: "max_attempts zero is unlimited", cfg: retryCfg("max_attempts", 0), failures: 5, err: boom,
			wantCalls: 6, wantWaits: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
				800 * time.Millisecond, 1600 * time.Millisecond}},
		{name: "stop returns the first error", cfg: retryCfg("stop", 0), failures: 1, err: boom, wantCalls: 1, wantErr: boom},
		{name: "empty policy stops", cfg: config.RetryConfig{}, failures: 1, err: boom, wantCalls: 1, wantErr: boom},
		{name: "permanent is returned unchanged", cfg: retryCfg("forever", 0), failures: 1, err: perm, wantCalls: 1, wantErr: perm},
		{name: "zero durations use defaults", cfg: config.RetryConfig{Policy: "forever"}, failures: 1, err: boom,
			wantCalls: 2, wantWaits: []time.Duration{100 * time.Millisecond}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls int
			sink := event.SinkFunc(func(context.Context, event.Batch) error {
				calls++
				if calls <= tt.failures {
					return tt.err
				}
				return nil
			})
			clock := &fakeClock{}
			e := New(Config{Name: "a", Source: newFakeSource(), Sink: sink, Retry: tt.cfg})
			e.sleep = clock.sleep
			e.jitter = func(d time.Duration) time.Duration { return d }
			err := e.send(context.Background(), event.Batch{})
			require.Equal(t, tt.wantCalls, calls)
			require.Equal(t, tt.wantWaits, clock.waits)
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.Equal(t, tt.wantErr == perm, err == perm, "permanent error must be unchanged")
			require.ErrorIs(t, err, tt.wantErr)
			if tt.wantMsg != "" {
				require.ErrorContains(t, err, tt.wantMsg)
			}
		})
	}
}

func TestRetryCancel(t *testing.T) {
	tests := []struct {
		name   string
		inWait bool
	}{
		{name: "cancel during backoff", inWait: true},
		{name: "cancel during the call"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls int
			sink := event.SinkFunc(func(ctx context.Context, _ event.Batch) error {
				calls++
				if !tt.inWait {
					cancel()
					return ctx.Err()
				}
				return errors.New("boom")
			})
			e := New(Config{Name: "a", Source: newFakeSource(), Sink: sink, Retry: retryCfg("forever", 0)})
			if tt.inWait {
				e.sleep = func(ctx context.Context, _ time.Duration) error {
					cancel()
					return sleepCtx(ctx, time.Hour)
				}
			}
			err := e.send(ctx, event.Batch{})
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, calls)
		})
	}
}

func TestSleepCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, sleepCtx(context.Background(), time.Millisecond))
	cancel()
	require.ErrorIs(t, sleepCtx(ctx, time.Hour), context.Canceled)
}

func TestBackoffJitter(t *testing.T) {
	for range 200 {
		d := jitterBackoff(time.Second)
		require.GreaterOrEqual(t, d, time.Second/2)
		require.LessOrEqual(t, d, time.Second)
	}
	require.Equal(t, time.Duration(0), jitterBackoff(0))
}

func TestRetryLog(t *testing.T) {
	var out syncBuffer
	log := slog.New(slog.NewJSONHandler(&out, nil))
	var calls int
	sink := event.SinkFunc(func(context.Context, event.Batch) error {
		calls++
		if calls == 1 {
			return errors.New("boom")
		}
		return nil
	})
	e := New(Config{Name: "orders", Source: newFakeSource(), Sink: sink, Retry: retryCfg("forever", 0), Logger: log})
	e.sleep = (&fakeClock{}).sleep
	e.jitter = func(d time.Duration) time.Duration { return d }
	require.NoError(t, e.send(context.Background(), event.Batch{}))
	for _, want := range []string{`"level":"WARN"`, `"msg":"sink send failed, retrying"`, `"db":"orders"`,
		`"attempt":1`, `"backoff":"100ms"`, `"err":"boom"`} {
		require.Contains(t, out.String(), want)
	}
}

// TestRetryHoldsLSN checks that the LSN is not confirmed while the sink fails.
func TestRetryHoldsLSN(t *testing.T) {
	const failures = 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := newFakeSource()
	var calls atomic.Int32
	var confirmedDuring atomic.Int32
	sink := event.SinkFunc(func(context.Context, event.Batch) error {
		if int(calls.Add(1)) <= failures {
			confirmedDuring.Add(int32(len(src.Confirmed())))
			return errors.New("boom")
		}
		return nil
	})
	e := New(Config{Name: "a", Source: src, Sink: sink, Retry: retryCfg("forever", 0)})
	stubDecode(e)
	e.sleep = (&fakeClock{}).sleep
	done := runEngine(ctx, e)
	feed(ctx, src, 1)
	require.Eventually(t, func() bool { return len(src.Confirmed()) == 1 }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, int32(failures+1), calls.Load())
	require.Zero(t, confirmedDuring.Load())
	require.Equal(t, []pglogrepl.LSN{10}, src.Confirmed())
	cancel()
	require.ErrorIs(t, wait(t, done), context.Canceled)
}
