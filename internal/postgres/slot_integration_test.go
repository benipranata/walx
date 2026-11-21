package postgres

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func connect(t *testing.T, ctx context.Context, url string) *Conn {
	t.Helper()
	c := New(config.PostgresConfig{URL: url}, config.RetryConfig{}, nil)
	t.Cleanup(func() { _ = c.Close(ctx) })
	_, err := c.Connect(ctx)
	require.NoError(t, err)
	return c
}

func TestSlotIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `CREATE TABLE t (id int primary key); CREATE PUBLICATION p FOR TABLE t`)
	require.NoError(t, err)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "create then idempotent re-run", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			first, err := EnsureSlot(ctx, c.PgConn(), "s_create", true)
			require.NoError(t, err)
			require.True(t, first.Created)
			require.NotZero(t, first.ConfirmedFlushLSN)
			second, err := EnsureSlot(ctx, c.PgConn(), "s_create", true)
			require.NoError(t, err)
			require.False(t, second.Created)
			require.Equal(t, first.ConfirmedFlushLSN, second.ConfirmedFlushLSN)
		}},
		{name: "validate-only fails when missing", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := EnsureSlot(ctx, c.PgConn(), "s_missing", false)
			require.ErrorIs(t, err, ErrSlotMissing)
			var n int
			require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots WHERE slot_name = 's_missing'`).Scan(&n))
			require.Zero(t, n)
		}},
		{name: "validate-only accepts existing", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := EnsureSlot(ctx, c.PgConn(), "s_existing", true)
			require.NoError(t, err)
			info, err := EnsureSlot(ctx, c.PgConn(), "s_existing", false)
			require.NoError(t, err)
			require.False(t, info.Created)
		}},
		{name: "foreign slot kinds rejected", run: func(t *testing.T) {
			_, err := admin.Exec(ctx, `SELECT pg_create_physical_replication_slot('s_phys')`)
			require.NoError(t, err)
			_, err = admin.Exec(ctx, `SELECT pg_create_logical_replication_slot('s_test_decoding', 'test_decoding')`)
			if err == nil {
				c := connect(t, ctx, url)
				_, err = EnsureSlot(ctx, c.PgConn(), "s_test_decoding", false)
				require.ErrorContains(t, err, "pgoutput")
			}
			c := connect(t, ctx, url)
			_, err = EnsureSlot(ctx, c.PgConn(), "s_phys", false)
			require.ErrorContains(t, err, "not a logical")
		}},
		{name: "second consumer gets the active-slot error", run: func(t *testing.T) {
			first := connect(t, ctx, url)
			_, err := EnsureSlot(ctx, first.PgConn(), "s_active", true)
			require.NoError(t, err)
			require.NoError(t, StartStream(ctx, first.PgConn(), "s_active", "p", 0))

			second := connect(t, ctx, url)
			_, err = EnsureSlot(ctx, second.PgConn(), "s_active", true)
			require.ErrorIs(t, err, ErrSlotActive)

			third := connect(t, ctx, url)
			err = StartStream(ctx, third.PgConn(), "s_active", "p", 0)
			require.ErrorIs(t, err, ErrSlotActive)
		}},
		{name: "start stream on missing slot", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			err := StartStream(ctx, c.PgConn(), "s_nope", "p", 0)
			require.ErrorIs(t, err, ErrSlotMissing)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestLostSlotIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)

	c := connect(t, ctx, url)
	_, err = EnsureSlot(ctx, c.PgConn(), "s_lost", true)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `CREATE TABLE t (id int primary key); CREATE PUBLICATION p FOR TABLE t`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `ALTER SYSTEM SET max_slot_wal_keep_size = '32MB'`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `SELECT pg_reload_conf()`)
	require.NoError(t, err)

	status := ""
	for i := 0; i < 30 && status != "lost"; i++ {
		for _, stmt := range []string{`INSERT INTO t VALUES (` + strconv.Itoa(i) + `)`, `SELECT pg_switch_wal()`, `CHECKPOINT`} {
			_, err = admin.Exec(ctx, stmt)
			require.NoError(t, err)
		}
		require.NoError(t, admin.QueryRow(ctx,
			`SELECT coalesce(wal_status, '') FROM pg_replication_slots WHERE slot_name = 's_lost'`).Scan(&status))
	}
	require.Equal(t, "lost", status)

	_, err = EnsureSlot(ctx, c.PgConn(), "s_lost", true)
	require.ErrorIs(t, err, ErrSlotLost)

	fresh := connect(t, ctx, url)
	err = StartStream(ctx, fresh.PgConn(), "s_lost", "p", 0)
	require.ErrorIs(t, err, ErrSlotLost)
}
