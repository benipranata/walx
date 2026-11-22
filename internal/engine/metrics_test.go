package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

func TestSinkMetrics(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name        string
		errs        []error
		wantEvents  float64
		wantLatency int
	}{
		{name: "success counts events and observes latency", errs: []error{nil}, wantEvents: 1, wantLatency: 1},
		{name: "failed attempts are timed but not counted", errs: []error{boom, boom, nil}, wantEvents: 1, wantLatency: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			calls := 0
			sink := &closeSink{send: func(context.Context, event.Batch) error {
				calls++
				return tt.errs[calls-1]
			}}
			m := &countMetrics{}
			e := New(Config{Name: "a", Source: src, Sink: sink, Metrics: m, Retry: config.RetryConfig{Policy: "forever"}})
			stubDecode(e)
			e.sleep = (&fakeClock{}).sleep
			done := runEngine(ctx, e)
			feed(ctx, src, 1)
			require.Eventually(t, func() bool { return len(src.Confirmed()) == 1 }, 5*time.Second, time.Millisecond)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
			require.Equal(t, tt.wantEvents, m.get("events_total"))
			require.Equal(t, []string{"database", "a"}, m.labels["events_total"])
			require.Len(t, m.observed("sink_latency_seconds"), tt.wantLatency)
		})
	}
}

func TestSinkMetricsSkippedBatchNotCounted(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{name: "skipped batch is timed, not counted", mode: "skip"},
		{name: "stopped batch is timed, not counted", mode: "stop"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := event.SinkFunc(func(context.Context, event.Batch) error { return event.Permanent(errors.New("bad")) })
			m := &countMetrics{}
			e := New(Config{Name: "a", Source: newFakeSource(), Sink: sink, Metrics: m, OnPermanentError: tt.mode})
			_ = e.send(context.Background(), event.Batch{Events: []*event.Event{{ID: "a-0/1-1"}}})
			require.Zero(t, m.get("events_total"))
			require.Len(t, m.observed("sink_latency_seconds"), 1)
		})
	}
}
