package engine

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/postgres"
)

func TestEmptyBatchConfirms(t *testing.T) {
	tests := []struct{ name string }{{name: "the default hook confirms EndLSN"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := newFakeSource()
			e := New(Config{Name: "a", Source: src, Sink: event.SinkFunc(nil)})
			e.empty(event.Batch{CommitLSN: 1, EndLSN: 2})
			require.Equal(t, []pglogrepl.LSN{2}, src.Confirmed())
		})
	}
}

func TestEmptyBatchWaitsForEarlierBatches(t *testing.T) {
	tests := []struct{ name string }{{name: "an empty batch is confirmed only after the batch before it"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			release := make(chan struct{})
			entered := make(chan struct{}, 1)
			sink := event.SinkFunc(func(ctx context.Context, b event.Batch) error {
				entered <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			drop := func(b event.Batch) event.Batch {
				if b.EndLSN == 20 {
					b.Events = nil
				}
				return b
			}
			e := New(Config{Name: "a", Source: src, Sink: sink, Filter: drop})
			stubDecode(e)
			done := runEngine(ctx, e)
			feed(ctx, src, 2)
			<-entered
			time.Sleep(50 * time.Millisecond)
			require.Empty(t, src.Confirmed(), "the empty batch must wait for the batch in flight")
			close(release)
			require.Eventually(t, func() bool { return len(src.Confirmed()) == 2 }, 5*time.Second, 5*time.Millisecond)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
			require.Equal(t, []pglogrepl.LSN{10, 20}, src.Confirmed())
		})
	}
}

func idleBegin(buf *postgres.TxBuffer) {
	_, _ = buf.Push(&pglogrepl.BeginMessage{FinalLSN: 5}, 1)
}

func TestKeepalive(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(e *Engine)
		want    []pglogrepl.LSN
	}{
		{name: "idle pipeline confirms the WAL end", want: []pglogrepl.LSN{100}},
		{name: "a buffered transaction blocks it", prepare: func(e *Engine) { idleBegin(e.cfg.Buffer) }},
		{name: "a queued or in-flight batch blocks it", prepare: func(e *Engine) { e.pending.Add(1) }},
		{name: "no buffer means no open transaction", prepare: func(e *Engine) { e.cfg.Buffer = nil }, want: []pglogrepl.LSN{100}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := newFakeSource()
			e := New(Config{Name: "a", Source: src, Sink: event.SinkFunc(nil),
				Buffer: postgres.NewTxBuffer("a", postgres.NewRelations(), nil)})
			if tt.prepare != nil {
				tt.prepare(e)
			}
			e.keepalive(100)
			require.Equal(t, tt.want, src.Confirmed())
		})
	}
}

func TestKeepaliveRun(t *testing.T) {
	tests := []struct{ name string }{{name: "a keepalive read by the reader confirms once idle"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			e := New(Config{Name: "a", Source: src, Sink: event.SinkFunc(func(context.Context, event.Batch) error { return nil })})
			done := runEngine(ctx, e)
			src.ka <- 77
			require.Eventually(t, func() bool { return len(src.Confirmed()) == 1 }, 5*time.Second, 5*time.Millisecond)
			close(src.ka) // a closed channel must not spin the reader
			time.Sleep(20 * time.Millisecond)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
			require.Equal(t, []pglogrepl.LSN{77}, src.Confirmed())
		})
	}
}
