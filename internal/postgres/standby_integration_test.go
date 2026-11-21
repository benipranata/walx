package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestStandbyIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)

	_, err = admin.Exec(ctx, `CREATE TABLE t (id int primary key)`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `CREATE PUBLICATION p FOR TABLE t`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `ALTER SYSTEM SET wal_sender_timeout = '3s'`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `SELECT pg_reload_conf()`)
	require.NoError(t, err)
	time.Sleep(time.Second)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "idle connection survives wal_sender_timeout", run: func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			c := connect(t, ctx, url)
			_, err := pglogrepl.CreateReplicationSlot(ctx, c.PgConn(), "s", "pgoutput",
				pglogrepl.CreateReplicationSlotOptions{})
			require.NoError(t, err)
			require.NoError(t, StartStream(ctx, c.PgConn(), "s", "p", 0))

			s := NewStandby(c.PgConn(), 0, time.Second)
			rctx, rcancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- s.Run(rctx) }()
			got := make(chan pglogrepl.XLogData, 16)
			go func() {
				for x := range s.Messages() {
					got <- x
				}
			}()

			time.Sleep(8 * time.Second)
			var n int
			require.NoError(t, admin.QueryRow(ctx,
				`SELECT count(*) FROM pg_stat_replication`).Scan(&n))
			require.Equal(t, 1, n)

			_, err = admin.Exec(ctx, `INSERT INTO t VALUES (1)`)
			require.NoError(t, err)
			select {
			case x := <-got:
				require.NotZero(t, x.WALStart)
			case <-time.After(10 * time.Second):
				t.Fatal("no XLogData after idle period")
			}

			rcancel()
			require.ErrorIs(t, <-done, context.Canceled)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
