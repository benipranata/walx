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

// setServerSetting changes a server-wide setting for one test: it refuses to
// overwrite a customised value and resets it afterwards.
func setServerSetting(t *testing.T, url, name, value string) {
	t.Helper()
	ctx := context.Background()
	alter := func(sql string) {
		c, err := pgx.Connect(ctx, url)
		require.NoError(t, err)
		defer c.Close(ctx)
		_, err = c.Exec(ctx, sql)
		require.NoError(t, err)
		_, err = c.Exec(ctx, `SELECT pg_reload_conf()`)
		require.NoError(t, err)
	}
	show := func() (value, source string) {
		c, err := pgx.Connect(ctx, url)
		require.NoError(t, err)
		defer c.Close(ctx)
		require.NoError(t, c.QueryRow(ctx,
			`SELECT current_setting(name), source FROM pg_settings WHERE name = $1`, name).Scan(&value, &source))
		return value, source
	}
	if _, source := show(); source != "default" {
		t.Fatalf("%s is already customised (source %q); reset it with ALTER SYSTEM RESET", name, source)
	}
	t.Cleanup(func() { alter(`ALTER SYSTEM RESET ` + name) })
	alter(`ALTER SYSTEM SET ` + name + ` = '` + value + `'`)
	require.Eventually(t, func() bool {
		v, _ := show()
		return v == value
	}, 10*time.Second, 50*time.Millisecond, "%s never became %s", name, value)
}
