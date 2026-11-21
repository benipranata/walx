package engine

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/postgres"
)

type countMetrics struct {
	mu     sync.Mutex
	counts map[string]float64
	labels map[string][]string
}

func (m *countMetrics) Count(name string, delta float64, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts, m.labels = map[string]float64{}, map[string][]string{}
	}
	m.counts[name] += delta
	m.labels[name] = labels
}

func (m *countMetrics) Gauge(string, float64, ...string) {}

func (m *countMetrics) get(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[name]
}

func TestPermanentErrors(t *testing.T) {
	boom := errors.New("boom")
	tooLarge := &postgres.TxTooLargeError{}
	tests := []struct {
		name      string
		mode      string
		sinkErr   error
		wantErr   error
		wantSkip  bool
		wantCalls int
	}{
		{name: "stop returns the error", mode: "stop", sinkErr: event.Permanent(boom), wantErr: boom, wantCalls: 1},
		{name: "empty mode stops", sinkErr: event.Permanent(boom), wantErr: boom, wantCalls: 1},
		{name: "skip drops the batch without retry", mode: "skip", sinkErr: event.Permanent(boom), wantSkip: true, wantCalls: 1},
		{name: "skip leaves a plain error alone", mode: "skip", sinkErr: boom, wantErr: boom, wantCalls: 1},
		{name: "skip never skips a too-large transaction", mode: "skip", sinkErr: tooLarge, wantErr: tooLarge, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls int
			sink := event.SinkFunc(func(context.Context, event.Batch) error { calls++; return tt.sinkErr })
			var out strings.Builder
			log := slog.New(slog.NewJSONHandler(&out, nil))
			m := &countMetrics{}
			e := New(Config{Name: "orders", Source: newFakeSource(), Sink: sink, Logger: log, Metrics: m,
				OnPermanentError: tt.mode, Retry: retryCfg("stop", 0)})
			e.sleep = func(context.Context, time.Duration) error { return nil }
			b := event.Batch{CommitLSN: 5, EndLSN: 6, Events: []*event.Event{{ID: "orders-0/5-1"}, {ID: "orders-0/5-2"}}}
			err := e.send(context.Background(), b)
			require.Equal(t, tt.wantCalls, calls)
			if !tt.wantSkip {
				require.ErrorIs(t, err, tt.wantErr)
				require.Zero(t, m.get("events_skipped_total"))
				require.Empty(t, out.String())
				return
			}
			require.NoError(t, err)
			require.Equal(t, float64(2), m.get("events_skipped_total"))
			require.Equal(t, []string{"database", "orders"}, m.labels["events_skipped_total"])
			var rec map[string]any
			require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(out.String())), &rec))
			require.Equal(t, "ERROR", rec["level"])
			require.Equal(t, "skipping batch after permanent sink error", rec["msg"])
			require.Equal(t, "0/6", rec["end_lsn"])
			require.Equal(t, "boom", rec["err"])
			evs, ok := rec["batch_events"].([]any)
			require.True(t, ok)
			require.Len(t, evs, 2)
			require.Equal(t, "orders-0/5-2", evs[1].(map[string]any)["id"])
		})
	}
}

func TestPermanentSkipConfirms(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := newFakeSource()
	sink := event.SinkFunc(func(context.Context, event.Batch) error { return event.Permanent(errors.New("bad")) })
	m := &countMetrics{}
	e := New(Config{Name: "a", Source: src, Sink: sink, Metrics: m, OnPermanentError: "skip"})
	stubDecode(e)
	done := runEngine(ctx, e)
	feed(ctx, src, 3)
	require.Eventually(t, func() bool { return len(src.Confirmed()) == 3 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	require.ErrorIs(t, wait(t, done), context.Canceled)
	require.Equal(t, float64(3), m.get("events_skipped_total"))
}
