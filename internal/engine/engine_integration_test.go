package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/postgres"
)

// memSink is an in-memory sink that records batches and can block.
type memSink struct {
	mu      sync.Mutex
	batches []event.Batch
	gate    chan struct{} // when non-nil, Send waits for it to close
}

func (s *memSink) Send(ctx context.Context, b event.Batch) error {
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, b)
	return nil
}

func (s *memSink) Close() error { return nil }

func (s *memSink) Batches() []event.Batch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]event.Batch(nil), s.batches...)
}

type pipeline struct {
	engine  *Engine
	standby *postgres.Standby
	done    chan error
	cancel  context.CancelFunc
	decoded func() int
}

func startPipeline(t *testing.T, ctx context.Context, url, slot string, sink event.Sink, queue int) *pipeline {
	t.Helper()
	conn := postgres.New(config.PostgresConfig{URL: url}, config.RetryConfig{}, nil)
	t.Cleanup(func() { _ = conn.Close(ctx) })
	_, err := conn.Connect(ctx)
	require.NoError(t, err)
	info, err := postgres.EnsureSlot(ctx, conn.PgConn(), slot, true)
	require.NoError(t, err)
	require.NoError(t, postgres.StartStream(ctx, conn.PgConn(), slot, "p", 0))
	sb, err := postgres.NewStandby(conn.PgConn(), info.ConfirmedFlushLSN, time.Second)
	require.NoError(t, err)

	buf := postgres.NewTxBuffer("itest", postgres.NewRelations(), nil)
	e := New(Config{Name: "itest", Source: sb, Buffer: buf, Sink: sink, QueueSize: queue})
	var mu sync.Mutex
	var n int
	inner := e.decode
	e.decode = func(x pglogrepl.XLogData) (*event.Batch, error) {
		b, err := inner(x)
		if b != nil {
			mu.Lock()
			n++
			mu.Unlock()
		}
		return b, err
	}
	rctx, cancel := context.WithCancel(ctx)
	p := &pipeline{engine: e, standby: sb, cancel: cancel, done: make(chan error, 1), decoded: func() int {
		mu.Lock()
		defer mu.Unlock()
		return n
	}}
	go func() { p.done <- e.Run(rctx) }()
	return p
}

func (p *pipeline) stop(t *testing.T) {
	t.Helper()
	p.cancel()
	select {
	case err := <-p.done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("engine did not stop")
	}
}

func slotFlush(t *testing.T, ctx context.Context, admin *pgx.Conn, slot string) pglogrepl.LSN {
	t.Helper()
	var s string
	require.NoError(t, admin.QueryRow(ctx,
		`SELECT coalesce(confirmed_flush_lsn::text, '0/0') FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&s))
	lsn, err := pglogrepl.ParseLSN(s)
	require.NoError(t, err)
	return lsn
}

func TestEngineIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `CREATE TABLE t (id int PRIMARY KEY); CREATE PUBLICATION p FOR TABLE t`)
	require.NoError(t, err)
	setServerSetting(t, url, "wal_sender_timeout", "3s")

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "ordered batches and an advancing confirmed_flush_lsn", run: func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			sink := &memSink{}
			p := startPipeline(t, ctx, url, "s_order", sink, 4)
			start := slotFlush(t, ctx, admin, "s_order")
			for i := 1; i <= 10; i++ {
				_, err := admin.Exec(ctx, `INSERT INTO t VALUES ($1)`, i+1000)
				require.NoError(t, err)
			}
			require.Eventually(t, func() bool { return len(sink.Batches()) == 10 }, 10*time.Second, 20*time.Millisecond)
			bs := sink.Batches()
			for i, b := range bs {
				require.Len(t, b.Events, 1)
				require.EqualValues(t, i+1001, b.Events[0].After["id"])
				require.Equal(t, "itest", b.Events[0].Source)
				if i > 0 {
					require.Greater(t, b.EndLSN, bs[i-1].EndLSN)
				}
			}
			last := bs[len(bs)-1].EndLSN
			require.Eventually(t, func() bool {
				return slotFlush(t, ctx, admin, "s_order") >= pglogrepl.LSN(last)
			}, 10*time.Second, 100*time.Millisecond)
			require.Greater(t, slotFlush(t, ctx, admin, "s_order"), start)
			p.stop(t)
		}},
		{name: "a sink blocked past wal_sender_timeout keeps the connection and a bounded queue", run: func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			const queue, rows = 2, 40
			sink := &memSink{gate: make(chan struct{})}
			p := startPipeline(t, ctx, url, "s_block", sink, queue)
			before := slotFlush(t, ctx, admin, "s_block")
			for i := 1; i <= rows; i++ {
				_, err := admin.Exec(ctx, `INSERT INTO t VALUES ($1)`, i+2000)
				require.NoError(t, err)
			}
			time.Sleep(7 * time.Second) // more than two wal_sender_timeout periods
			var n int
			require.NoError(t, admin.QueryRow(ctx,
				`SELECT count(*) FROM pg_stat_replication r JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = 's_block'`).Scan(&n))
			require.Equal(t, 1, n, "walsender dropped the blocked consumer")
			require.NoError(t, admin.QueryRow(ctx,
				`SELECT count(*) FROM pg_replication_slots WHERE slot_name = 's_block' AND active`).Scan(&n))
			require.Equal(t, 1, n)
			// one batch in the sink, the queue, and one held by the blocked reader
			require.LessOrEqual(t, p.decoded(), queue+2)
			require.Empty(t, sink.Batches())
			require.Equal(t, before, slotFlush(t, ctx, admin, "s_block"), "nothing may be confirmed while the sink is blocked")

			close(sink.gate)
			require.Eventually(t, func() bool { return len(sink.Batches()) == rows }, 15*time.Second, 50*time.Millisecond)
			for i, b := range sink.Batches() {
				require.EqualValues(t, i+2001, b.Events[0].After["id"])
			}
			p.stop(t)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
