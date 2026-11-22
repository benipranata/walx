package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestSessionSettingsIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)

	db := pgx.Identifier{admin.Config().Database}.Sanitize()
	_, err = admin.Exec(ctx, `ALTER DATABASE `+db+` SET bytea_output = 'escape'`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `ALTER DATABASE `+db+` SET datestyle = 'SQL, DMY'`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `ALTER DATABASE `+db+` SET intervalstyle = 'postgres_verbose'`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `ALTER DATABASE `+db+` SET extra_float_digits = -3`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `CREATE TABLE t (id int primary key, b bytea, d date, ts timestamp, tz timestamptz, f float8, i interval);
		CREATE PUBLICATION p FOR TABLE t`)
	require.NoError(t, err)

	tests := []struct {
		name      string
		reconnect bool
	}{
		{name: "connect"},
		{name: "reconnect", reconnect: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := connect(t, ctx, url)
			if tt.reconnect {
				_, err := c.Reconnect(ctx)
				require.NoError(t, err)
			}
			slot := "s_" + tt.name
			info, err := EnsureSlot(ctx, c.PgConn(), slot, true)
			require.NoError(t, err)
			require.NoError(t, StartStream(ctx, c.PgConn(), slot, "p", 0))
			s, err := NewStandby(c.PgConn(), info.ConfirmedFlushLSN, time.Second)
			require.NoError(t, err)
			rctx, rcancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- s.Run(rctx) }()
			defer func() {
				rcancel()
				<-done
			}()

			_, err = admin.Exec(ctx, `INSERT INTO t VALUES (`+fmt.Sprint(len(tt.name))+`, '\xdeadbeef', '2024-03-05',
				'2024-03-05 10:11:12.5', '2024-03-05 10:11:12+00', 0.30000000000000004, '1 day 2 hours')`)
			require.NoError(t, err)

			rels := NewRelations()
			timeout := time.After(10 * time.Second)
			for {
				select {
				case x := <-s.Messages():
					msg, err := pglogrepl.Parse(x.WALData)
					require.NoError(t, err)
					if r, ok := msg.(*pglogrepl.RelationMessage); ok {
						rels.Set(r)
						continue
					}
					evs, err := DecodeDML(rels, nil, msg)
					require.NoError(t, err)
					if len(evs) == 0 {
						continue
					}
					after := evs[0].After
					require.Equal(t, []byte{0xde, 0xad, 0xbe, 0xef}, after["b"])
					require.Equal(t, time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC), after["d"])
					require.Equal(t, time.Date(2024, 3, 5, 10, 11, 12, 500_000_000, time.UTC), after["ts"])
					require.Equal(t, time.Date(2024, 3, 5, 10, 11, 12, 0, time.UTC), after["tz"])
					require.Equal(t, 0.30000000000000004, after["f"])
					require.Equal(t, "P1DT2H", after["i"])
					return
				case <-timeout:
					t.Fatal("no insert received")
				}
			}
		})
	}
}
