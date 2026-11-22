package engine

import (
	"context"
	"fmt"
	neturl "net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// startPostgres skips unless WALX_TEST_POSTGRES_URL is set, and returns the
// URL of a fresh throwaway database. It mirrors the helper in internal/postgres.
func startPostgres(t *testing.T) string {
	t.Helper()
	base := os.Getenv("WALX_TEST_POSTGRES_URL")
	if base == "" {
		t.Skip("WALX_TEST_POSTGRES_URL not set")
	}
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

// setDatabaseSetting sets a per-database default for the test database, which
// applies to new connections, walsenders included. It dies with the database,
// so parallel packages never fight over a server-wide setting.
func setDatabaseSetting(t *testing.T, url, name, value string) {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer c.Close(ctx)
	_, err = c.Exec(ctx, `ALTER DATABASE `+pgx.Identifier{c.Config().Database}.Sanitize()+` SET `+name+` = '`+value+`'`)
	require.NoError(t, err)
}
