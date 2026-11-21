package postgres

import (
	"context"
	"fmt"
	neturl "net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

// startPostgres skips unless WALX_TEST_POSTGRES_URL points at a server with
// wal_level=logical, and returns the URL of a fresh throwaway database on it.
func startPostgres(t *testing.T) string {
	t.Helper()
	base := os.Getenv("WALX_TEST_POSTGRES_URL")
	if base == "" {
		t.Skip("WALX_TEST_POSTGRES_URL not set")
	}
	return isolatedDatabase(t, base)
}

func TestConnIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "identify system", run: func(t *testing.T) {
			c := New(config.PostgresConfig{URL: url}, config.RetryConfig{}, nil)
			defer c.Close(ctx)
			sys, err := c.Connect(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, sys.SystemID)
			require.NotZero(t, sys.XLogPos)
			cfg, err := pgx.ParseConfig(url)
			require.NoError(t, err)
			require.Equal(t, cfg.Database, sys.DBName)
		}},
		{name: "terminate backend resumes without loss", run: func(t *testing.T) {
			_, err := admin.Exec(ctx, `CREATE TABLE t (id int primary key)`)
			require.NoError(t, err)
			_, err = admin.Exec(ctx, `CREATE PUBLICATION p FOR TABLE t`)
			require.NoError(t, err)

			resets := 0
			c := New(config.PostgresConfig{URL: url},
				config.RetryConfig{Initial: 10 * time.Millisecond, Max: 100 * time.Millisecond},
				func() { resets++ })
			defer c.Close(ctx)
			_, err = c.Connect(ctx)
			require.NoError(t, err)
			_, err = pglogrepl.CreateReplicationSlot(ctx, c.PgConn(), "s", "pgoutput",
				pglogrepl.CreateReplicationSlotOptions{})
			require.NoError(t, err)

			insert := func(from, to int) {
				_, err := admin.Exec(ctx, `INSERT INTO t SELECT generate_series($1::int, $2::int)`, from, to)
				require.NoError(t, err)
			}
			opts := pglogrepl.StartReplicationOptions{
				PluginArgs: []string{"proto_version '1'", "publication_names 'p'"},
			}
			seen := map[int]bool{}
			collect := func(want int) {
				for len(seen) < want {
					rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
					msg, err := c.PgConn().ReceiveMessage(rctx)
					rcancel()
					require.NoError(t, err)
					id, ok := insertedID(t, msg)
					if ok {
						seen[id] = true
					}
				}
			}

			insert(1, 3)
			require.NoError(t, pglogrepl.StartReplication(ctx, c.PgConn(), "s", 0, opts))
			collect(3)

			var pid int
			require.NoError(t, admin.QueryRow(ctx,
				`SELECT pid FROM pg_stat_replication LIMIT 1`).Scan(&pid))
			_, err = admin.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid)
			require.NoError(t, err)
			insert(4, 6)

			_, err = pglogrepl.IdentifySystem(ctx, c.PgConn())
			require.Error(t, err)

			sys, err := c.Reconnect(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, sys.SystemID)
			require.Equal(t, 1, resets)

			require.NoError(t, pglogrepl.StartReplication(ctx, c.PgConn(), "s", 0, opts))
			collect(6)
			for i := 1; i <= 6; i++ {
				require.True(t, seen[i], "missing id %d", i)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// insertedID extracts the id from a pgoutput Insert of a one-int-column table.
func insertedID(t *testing.T, msg pgproto3.BackendMessage) (int, bool) {
	t.Helper()
	cd, ok := msg.(*pgproto3.CopyData)
	if !ok || len(cd.Data) == 0 || cd.Data[0] != pglogrepl.XLogDataByteID {
		return 0, false
	}
	xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
	require.NoError(t, err)
	if len(xld.WALData) == 0 || xld.WALData[0] != 'I' {
		return 0, false
	}
	id, err := strconv.Atoi(string(xld.WALData[13:]))
	require.NoError(t, err)
	return id, true
}

// isolatedDatabase creates a throwaway database on the server at base, so
// tests sharing one server never see each other's slots or tables.
func isolatedDatabase(t *testing.T, base string) string {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	require.NoError(t, err)
	name := fmt.Sprintf("walx_test_%d", time.Now().UnixNano())
	_, err = admin.Exec(ctx, "CREATE DATABASE "+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		defer admin.Close(ctx)
		_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, name)
		_, _ = admin.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = $1`, name)
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := neturl.Parse(base)
	require.NoError(t, err)
	u.Path = "/" + name
	return u.String()
}

func withUser(t *testing.T, base, user, password string) string {
	t.Helper()
	u, err := neturl.Parse(base)
	require.NoError(t, err)
	u.User = neturl.UserPassword(user, password)
	return u.String()
}
