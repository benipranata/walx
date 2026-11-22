package engine

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/dlq"
)

// fakeDeadLetter records SendFailed calls; errs are returned in order, then nil.
type fakeDeadLetter struct {
	mu      sync.Mutex
	errs    []error
	gate    chan struct{} // when set, SendFailed waits for it to close
	batches []event.Batch
	causes  []error
	calls   int
	closed  int
}

func (f *fakeDeadLetter) SendFailed(ctx context.Context, b event.Batch, cause error) error {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	var err error
	if len(f.errs) > 0 {
		err, f.errs = f.errs[0], f.errs[1:]
	}
	if err == nil {
		f.batches = append(f.batches, b)
		f.causes = append(f.causes, cause)
	}
	return err
}

func (f *fakeDeadLetter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func (f *fakeDeadLetter) snapshot() (calls, closed int, batches []event.Batch, causes []error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.closed, append([]event.Batch(nil), f.batches...), append([]error(nil), f.causes...)
}

func TestDeadLetterSkip(t *testing.T) {
	boom := errors.New("boom")
	dlqDown := errors.New("disk full")
	tests := []struct {
		name        string
		mode        string
		sinkErr     error
		dlqErrs     []error
		policy      string
		wantErr     string
		wantPerm    bool
		wantDLQCall int
		wantStored  int
		wantSkipped float64
	}{
		{name: "permanent error is dead-lettered and skipped", mode: "skip", sinkErr: event.Permanent(boom), policy: "stop", wantDLQCall: 1, wantStored: 1, wantSkipped: 2},
		{name: "dead-letter write is retried per policy", mode: "skip", sinkErr: event.Permanent(boom), dlqErrs: []error{dlqDown, dlqDown}, policy: "forever", wantDLQCall: 3, wantStored: 1, wantSkipped: 2},
		{name: "failed dead-letter write is never skipped", mode: "skip", sinkErr: event.Permanent(boom), dlqErrs: []error{dlqDown}, policy: "stop", wantErr: "dead-letter write failed", wantDLQCall: 1},
		{name: "dead-letter write gives up after max attempts", mode: "skip", sinkErr: event.Permanent(boom), dlqErrs: []error{dlqDown, dlqDown, dlqDown}, policy: "max_attempts", wantErr: "giving up after 2 attempts", wantDLQCall: 2},
		{name: "permanent dead-letter error stops without retry", mode: "skip", sinkErr: event.Permanent(boom), dlqErrs: []error{event.Permanent(dlqDown)}, policy: "forever", wantErr: "disk full", wantPerm: true, wantDLQCall: 1},
		{name: "stop mode does not touch the dead letter", mode: "stop", sinkErr: event.Permanent(boom), policy: "stop", wantErr: "boom", wantPerm: true},
		{name: "plain sink error is not dead-lettered", mode: "skip", sinkErr: boom, policy: "stop", wantErr: "boom"},
		{name: "delivered batch is not dead-lettered", mode: "skip", policy: "stop"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dl := &fakeDeadLetter{errs: tt.dlqErrs}
			sink := event.SinkFunc(func(context.Context, event.Batch) error { return tt.sinkErr })
			var out strings.Builder
			m := &countMetrics{}
			e := New(Config{Name: "orders", Source: newFakeSource(), Sink: sink, DeadLetter: dl, Metrics: m,
				Logger: slog.New(slog.NewJSONHandler(&out, nil)), OnPermanentError: tt.mode, Retry: retryCfg(tt.policy, 2)})
			e.sleep = func(context.Context, time.Duration) error { return nil }
			b := event.Batch{CommitLSN: 5, EndLSN: 6, Events: []*event.Event{{ID: "orders-0/5-1", After: map[string]any{"secret": "s3cr3t"}}, {ID: "orders-0/5-2"}}}

			err := e.send(context.Background(), b)

			calls, _, batches, causes := dl.snapshot()
			require.Equal(t, tt.wantDLQCall, calls)
			require.Len(t, batches, tt.wantStored)
			require.InDelta(t, tt.wantSkipped, m.get("events_skipped_total"), 0)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, tt.wantPerm, event.IsPermanent(err))
				return
			}
			require.NoError(t, err)
			if tt.wantStored == 0 {
				return
			}
			require.Equal(t, b, batches[0])
			require.ErrorIs(t, causes[0], boom)
			require.True(t, event.IsPermanent(causes[0]))
			require.NotContains(t, out.String(), "s3cr3t")
			require.NotContains(t, out.String(), "batch_events")
			var rec map[string]any
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &rec))
			require.Equal(t, "ERROR", rec["level"])
			require.EqualValues(t, 2, rec["events"])
			require.Equal(t, "0/6", rec["end_lsn"])
			require.Equal(t, "boom", rec["err"])
		})
	}
}

func TestDeadLetterConfirmOrder(t *testing.T) {
	tests := []struct {
		name    string
		dlqErrs []error
		wantErr string
	}{
		{name: "confirm waits for the dead-letter write"},
		{name: "failed dead-letter write never confirms", dlqErrs: []error{errors.New("disk full")}, wantErr: "dead-letter write failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			dl := &fakeDeadLetter{errs: tt.dlqErrs, gate: make(chan struct{})}
			sink := event.SinkFunc(func(context.Context, event.Batch) error { return event.Permanent(errors.New("bad")) })
			e := New(Config{Name: "a", Source: src, Sink: sink, DeadLetter: dl, OnPermanentError: "skip"})
			stubDecode(e)
			done := runEngine(ctx, e)
			feed(ctx, src, 1)

			require.Never(t, func() bool { return len(src.Confirmed()) > 0 }, 100*time.Millisecond, 10*time.Millisecond,
				"confirmed before the dead-letter write returned")
			close(dl.gate)
			if tt.wantErr != "" {
				require.ErrorContains(t, wait(t, done), tt.wantErr)
				require.Empty(t, src.Confirmed())
			} else {
				require.Eventually(t, func() bool { return len(src.Confirmed()) == 1 }, 5*time.Second, 5*time.Millisecond)
				cancel()
				require.ErrorIs(t, wait(t, done), context.Canceled)
			}
			_, closed, _, _ := dl.snapshot()
			require.Equal(t, 1, closed, "Run closes the dead letter exactly once")
		})
	}
}

func TestDeadLetterFile(t *testing.T) {
	tests := []struct{ name string }{{"skipped batches land in the file before they are confirmed"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			path := filepath.Join(t.TempDir(), "d.jsonl")
			dl, err := dlq.Open(path, "a", "nats")
			require.NoError(t, err)
			src := newFakeSource()
			sink := event.SinkFunc(func(context.Context, event.Batch) error { return event.Permanent(errors.New("bad")) })
			e := New(Config{Name: "a", Source: src, Sink: sink, DeadLetter: dl, OnPermanentError: "skip"})
			stubDecode(e)
			confirm := e.confirm
			var atConfirm int
			e.confirm = func(lsn event.LSN) {
				if data, err := os.ReadFile(path); err == nil && lsn == 30 {
					atConfirm = strings.Count(string(data), "\n")
				}
				confirm(lsn)
			}
			done := runEngine(ctx, e)
			feed(ctx, src, 3)
			require.Eventually(t, func() bool { return len(src.Confirmed()) == 3 }, 5*time.Second, 5*time.Millisecond)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)

			require.Equal(t, 3, atConfirm, "all three batches were durable when the last was confirmed")
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, 3, strings.Count(string(data), `"error":"bad"`))
			require.NoError(t, dl.Close(), "Run already closed it; closing again is a no-op")
		})
	}
}
