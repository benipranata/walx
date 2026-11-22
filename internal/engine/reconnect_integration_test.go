package engine

import (
	"context"
	"sync"
	"sync/atomic"
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

func TestReconnectIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `CREATE TABLE rc (id int PRIMARY KEY); CREATE PUBLICATION p FOR TABLE rc`)
	require.NoError(t, err)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "killing the walsender resumes the stream without loss and with stable IDs", run: func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			const slot = "s_reconnect"
			rels := postgres.NewRelations()
			buf := postgres.NewTxBuffer("itest", rels, nil)
			conn := postgres.New(config.PostgresConfig{URL: url}, config.RetryConfig{Initial: 20 * time.Millisecond, Max: 200 * time.Millisecond},
				func() { rels.Reset(); buf.Reset() })
			t.Cleanup(func() { _ = conn.Close(ctx) })

			start := func(ctx context.Context, resume pglogrepl.LSN, create bool) (Source, error) {
				info, err := postgres.EnsureSlot(ctx, conn.PgConn(), slot, create)
				if err != nil {
					return nil, err
				}
				if err := postgres.StartStream(ctx, conn.PgConn(), slot, "p", resume); err != nil {
					return nil, err
				}
				return postgres.NewStandby(conn.PgConn(), max(info.ConfirmedFlushLSN, resume), time.Second)
			}
			_, err := conn.Connect(ctx)
			require.NoError(t, err)
			first, err := start(ctx, 0, true)
			require.NoError(t, err)
			var reconnects atomic.Int32
			newSource := func(ctx context.Context, resume pglogrepl.LSN) (Source, error) {
				if _, err := conn.Reconnect(ctx); err != nil {
					return nil, err
				}
				reconnects.Add(1)
				// A dropped slot must not be recreated: its history is gone.
				return start(ctx, resume, false)
			}

			var mu sync.Mutex
			var got []event.Batch
			sink := event.SinkFunc(func(_ context.Context, b event.Batch) error {
				time.Sleep(10 * time.Millisecond)
				mu.Lock()
				defer mu.Unlock()
				got = append(got, b)
				return nil
			})
			e := New(Config{Name: "itest", Source: first, NewSource: newSource, Buffer: buf, Sink: sink,
				Retry: config.RetryConfig{Initial: 20 * time.Millisecond, Max: 200 * time.Millisecond}})
			rctx, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- e.Run(rctx) }()

			const total = 40
			insertErr := make(chan error, 1)
			go func() {
				for i := 1; i <= total; i++ {
					if _, err := admin.Exec(ctx, `INSERT INTO rc VALUES ($1)`, i); err != nil {
						insertErr <- err
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
				insertErr <- nil
			}()
			require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) >= 10 }, 20*time.Second, 10*time.Millisecond)

			killer, err := pgx.Connect(ctx, url)
			require.NoError(t, err)
			defer killer.Close(ctx)
			var killed bool
			require.NoError(t, killer.QueryRow(ctx,
				`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL`, slot).Scan(&killed))
			require.True(t, killed)
			require.NoError(t, <-insertErr)

			seen := func() map[int64]bool {
				mu.Lock()
				defer mu.Unlock()
				m := map[int64]bool{}
				for _, b := range got {
					for _, ev := range b.Events {
						m[ev.After["id"].(int64)] = true
					}
				}
				return m
			}
			require.Eventually(t, func() bool { return len(seen()) == total }, 30*time.Second, 20*time.Millisecond)
			require.GreaterOrEqual(t, reconnects.Load(), int32(1))

			stop()
			require.ErrorIs(t, <-done, context.Canceled)

			mu.Lock()
			defer mu.Unlock()
			idOf := map[int64]string{}
			var order []int64
			var lastEnd event.LSN
			for _, b := range got {
				require.Len(t, b.Events, 1)
				id := b.Events[0].After["id"].(int64)
				if prev, dup := idOf[id]; dup {
					require.Equal(t, prev, b.Events[0].ID, "a redelivered row keeps its event ID")
					continue
				}
				idOf[id] = b.Events[0].ID
				order = append(order, id)
				require.Greater(t, b.EndLSN, lastEnd, "first deliveries follow WAL order")
				lastEnd = b.EndLSN
			}
			require.Len(t, order, total)
			for i, id := range order {
				require.Equal(t, int64(i+1), id, "no gap and no reordering")
			}
			require.Eventually(t, func() bool {
				return slotFlush(t, ctx, admin, slot) >= pglogrepl.LSN(lastEnd)
			}, 10*time.Second, 50*time.Millisecond)
		}},
		{name: "a slot dropped during the outage stops the run instead of being recreated", run: func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			const slot = "s_reconnect_dropped"
			conn := postgres.New(config.PostgresConfig{URL: url}, config.RetryConfig{Initial: 20 * time.Millisecond, Max: 200 * time.Millisecond}, nil)
			t.Cleanup(func() { _ = conn.Close(ctx) })
			_, err := conn.Connect(ctx)
			require.NoError(t, err)
			info, err := postgres.EnsureSlot(ctx, conn.PgConn(), slot, true)
			require.NoError(t, err)
			require.NoError(t, postgres.StartStream(ctx, conn.PgConn(), slot, "p", 0))
			first, err := postgres.NewStandby(conn.PgConn(), info.ConfirmedFlushLSN, time.Second)
			require.NoError(t, err)
			newSource := func(ctx context.Context, resume pglogrepl.LSN) (Source, error) {
				if _, err := conn.Reconnect(ctx); err != nil {
					return nil, err
				}
				_, err := postgres.EnsureSlot(ctx, conn.PgConn(), slot, false)
				return nil, err
			}
			e := New(Config{Name: "itest", Source: first, NewSource: newSource, Buffer: postgres.NewTxBuffer("itest", postgres.NewRelations(), nil),
				Sink: event.SinkFunc(nil), Retry: config.RetryConfig{Initial: 20 * time.Millisecond, Max: 200 * time.Millisecond}})
			done := make(chan error, 1)
			go func() { done <- e.Run(ctx) }()
			_, err = admin.Exec(ctx, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1`, slot)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				_, err := admin.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, slot)
				return err == nil
			}, 10*time.Second, 20*time.Millisecond)
			require.ErrorIs(t, <-done, postgres.ErrSlotMissing)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
