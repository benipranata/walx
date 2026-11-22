package snapshot

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	evmock "github.com/benipranata/walx/internal/event/mock"
)

// newCountMetrics returns a MockMetrics that sums every Count by name.
func newCountMetrics(t *testing.T) (*evmock.MockMetrics, map[string]float64) {
	m := evmock.NewMockMetrics(t)
	counts := map[string]float64{}
	add := func(name string, d float64, _ ...string) { counts[name] += d }
	m.EXPECT().Count(mock.Anything, mock.Anything).Run(add).Maybe()
	m.EXPECT().Count(mock.Anything, mock.Anything, mock.Anything).Run(add).Maybe()
	m.EXPECT().Observe(mock.Anything, mock.Anything, mock.Anything).Maybe()
	m.EXPECT().Gauge(mock.Anything, mock.Anything, mock.Anything).Maybe()
	return m, counts
}

func TestDeliver(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name      string
		retry     config.RetryConfig
		onPerm    string
		results   []error
		wantCalls int
		wantErr   string
		wantTotal float64
		wantSkip  float64
	}{
		{"ok", config.RetryConfig{Policy: "stop"}, "stop", []error{nil}, 1, "", 2, 0},
		{"forever retries", config.RetryConfig{Policy: "forever"}, "stop", []error{boom, boom, nil}, 3, "", 2, 0},
		{"max attempts gives up", config.RetryConfig{Policy: "max_attempts", MaxAttempts: 2}, "stop", []error{boom, boom, nil}, 2, "giving up after 2 attempts", 0, 0},
		{"stop policy fails at once", config.RetryConfig{Policy: "stop"}, "stop", []error{boom, nil}, 1, "boom", 0, 0},
		{"empty policy behaves as stop", config.RetryConfig{}, "stop", []error{boom, nil}, 1, "boom", 0, 0},
		{"permanent stops", config.RetryConfig{Policy: "forever"}, "stop", []error{event.Permanent(boom)}, 1, "boom", 0, 0},
		{"permanent skipped", config.RetryConfig{Policy: "forever"}, "skip", []error{event.Permanent(boom)}, 1, "", 0, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			s := event.SinkFunc(func(context.Context, event.Batch) error {
				calls++
				return tt.results[calls-1]
			})
			m, counts := newCountMetrics(t)
			d := &deliverer{name: "db", log: slog.New(slog.DiscardHandler), sink: s, retry: tt.retry, onPermanent: tt.onPerm, metrics: m,
				sleep: func(context.Context, time.Duration) error { return nil }}
			err := d.send(context.Background(), event.Batch{Events: []*event.Event{{}, {}}})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantCalls, calls)
			require.Equal(t, tt.wantTotal, counts["events_total"])
			require.Equal(t, tt.wantSkip, counts["events_skipped_total"])
		})
	}
}

func TestDeliverTimeoutAndCancel(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		cancel  bool
		wantErr error
	}{
		{"sink timeout is retryable then stops", 10 * time.Millisecond, false, context.DeadlineExceeded},
		{"cancel stops the retry", 0, true, context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := event.SinkFunc(func(ctx context.Context, _ event.Batch) error {
				if tt.cancel {
					cancel()
					return errors.New("transient")
				}
				<-ctx.Done()
				return ctx.Err()
			})
			d := &deliverer{name: "db", log: slog.New(slog.DiscardHandler), sink: s, retry: config.RetryConfig{Policy: "max_attempts", MaxAttempts: 2},
				timeout: tt.timeout, metrics: event.NopMetrics{}, sleep: sleepCtx}
			err := d.send(ctx, event.Batch{})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// deadLetterRecord holds what a MockDeadLetter saw.
type deadLetterRecord struct {
	calls   int
	batches []event.Batch
	causes  []error
}

// newDeadLetter returns a MockDeadLetter that fails with errs in order, then stores batches.
func newDeadLetter(t *testing.T, errs []error) (*evmock.MockDeadLetter, *deadLetterRecord) {
	dl := evmock.NewMockDeadLetter(t)
	rec := &deadLetterRecord{}
	dl.EXPECT().SendFailed(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, b event.Batch, cause error) error {
			rec.calls++
			if len(errs) > 0 {
				err := errs[0]
				errs = errs[1:]
				if err != nil {
					return err
				}
			}
			rec.batches = append(rec.batches, b)
			rec.causes = append(rec.causes, cause)
			return nil
		}).Maybe()
	return dl, rec
}

func TestDeliverDeadLetter(t *testing.T) {
	boom := errors.New("boom")
	down := errors.New("disk full")
	tests := []struct {
		name       string
		retry      config.RetryConfig
		onPerm     string
		sinkErr    error
		dlqErrs    []error
		wantErr    string
		wantCalls  int
		wantStored int
		wantSkip   float64
	}{
		{"skip stores the batch", config.RetryConfig{Policy: "stop"}, "skip", event.Permanent(boom), nil, "", 1, 1, 2},
		{"dead-letter write is retried", config.RetryConfig{Policy: "forever"}, "skip", event.Permanent(boom), []error{down, down}, "", 3, 1, 2},
		{"failed dead-letter write fails the snapshot", config.RetryConfig{Policy: "stop"}, "skip", event.Permanent(boom), []error{down}, "dead-letter write failed", 1, 0, 0},
		{"stop mode leaves the dead letter alone", config.RetryConfig{Policy: "stop"}, "stop", event.Permanent(boom), nil, "boom", 0, 0, 0},
		{"plain error leaves the dead letter alone", config.RetryConfig{Policy: "stop"}, "skip", boom, nil, "boom", 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := event.SinkFunc(func(context.Context, event.Batch) error { return tt.sinkErr })
			dl, rec := newDeadLetter(t, tt.dlqErrs)
			m, counts := newCountMetrics(t)
			d := &deliverer{name: "db", log: slog.New(slog.DiscardHandler), sink: s, deadLetter: dl, retry: tt.retry, onPermanent: tt.onPerm,
				metrics: m, timeout: time.Minute, sleep: func(context.Context, time.Duration) error { return nil }}
			b := event.Batch{Events: []*event.Event{{ID: "a"}, {ID: "b"}}}
			err := d.send(context.Background(), b)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantCalls, rec.calls)
			require.Len(t, rec.batches, tt.wantStored)
			if tt.wantStored > 0 {
				require.Equal(t, b, rec.batches[0])
				require.ErrorIs(t, rec.causes[0], boom)
			}
			require.Equal(t, tt.wantSkip, counts["events_skipped_total"])
		})
	}
}
