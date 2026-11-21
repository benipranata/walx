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
	setServerSetting(t, url, "wal_sender_timeout", "3s")

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "idle connection survives wal_sender_timeout", run: func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			c := connect(t, ctx, url)
			info, err := EnsureSlot(ctx, c.PgConn(), "s", true)
			require.NoError(t, err)
			require.NotZero(t, info.ConfirmedFlushLSN)
			require.NoError(t, StartStream(ctx, c.PgConn(), "s", "p", 0))

			s, err := NewStandby(c.PgConn(), info.ConfirmedFlushLSN, time.Second)
			require.NoError(t, err)
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
		{name: "missing publication ends the stream with an error", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			info, err := EnsureSlot(ctx, c.PgConn(), "s_nopub", true)
			require.NoError(t, err)
			require.NoError(t, StartStream(ctx, c.PgConn(), "s_nopub", "nopub", 0))

			s, err := NewStandby(c.PgConn(), info.ConfirmedFlushLSN, time.Second)
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- s.Run(ctx) }()
			go func() {
				for range s.Messages() {
				}
			}()

			_, err = admin.Exec(ctx, `INSERT INTO t VALUES (2)`)
			require.NoError(t, err)
			select {
			case err := <-done:
				require.ErrorIs(t, err, ErrPublicationMissing)
			case <-time.After(10 * time.Second):
				t.Fatal("stream kept running with a missing publication")
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestStandbyTimerIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `CREATE TABLE t (id serial PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `CREATE PUBLICATION p FOR TABLE t`)
	require.NoError(t, err)
	setServerSetting(t, url, "wal_sender_timeout", "3s")

	// A consumer that stops reading also stops answering the server's
	// keepalives, so only the status timer can keep the walsender alive.
	tests := []struct {
		name      string
		slot      string
		interval  time.Duration
		wantAlive bool
	}{
		{name: "status timer keeps a stalled consumer connected", slot: "s_alive", interval: time.Second, wantAlive: true},
		{name: "control: without status updates the server drops the consumer", slot: "s_dead", interval: time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := connect(t, ctx, url)
			info, err := EnsureSlot(ctx, c.PgConn(), tt.slot, true)
			require.NoError(t, err)
			require.NoError(t, StartStream(ctx, c.PgConn(), tt.slot, "p", 0))
			s, err := NewStandby(c.PgConn(), info.ConfirmedFlushLSN, tt.interval)
			require.NoError(t, err)
			rctx, rcancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- s.Run(rctx) }()

			_, err = admin.Exec(ctx, `INSERT INTO t DEFAULT VALUES`)
			require.NoError(t, err)
			time.Sleep(6 * time.Second)

			var pid *int
			require.NoError(t, admin.QueryRow(ctx,
				`SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1`, tt.slot).Scan(&pid))
			require.Equal(t, tt.wantAlive, pid != nil)

			rcancel()
			go func() {
				for range s.Messages() {
				}
			}()
			<-done
		})
	}
}
