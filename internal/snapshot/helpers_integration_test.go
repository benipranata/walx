package snapshot

import (
	"context"
	"fmt"
	neturl "net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	evmock "github.com/benipranata/walx/internal/event/mock"
	"github.com/benipranata/walx/internal/postgres"
)

// startPostgres skips unless WALX_TEST_POSTGRES_URL is set, and returns the
// URL of a fresh throwaway database. It mirrors the helper in internal/engine.
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

func connect(t *testing.T, url string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func mustExec(t *testing.T, c *pgx.Conn, sql string, args ...any) {
	t.Helper()
	_, err := c.Exec(context.Background(), sql, args...)
	require.NoError(t, err, sql)
}

// collector is a MockSink wired to record batches. onSend, when set, runs
// before the batch n (1-based) is recorded and may fail it.
type collector struct {
	*evmock.MockSink
	mu      sync.Mutex
	batches []event.Batch
	calls   int
	onSend  func(n int) error
}

func newCollector(t *testing.T) *collector {
	c := &collector{MockSink: evmock.NewMockSink(t)}
	c.EXPECT().Send(mock.Anything, mock.Anything).RunAndReturn(c.send).Maybe()
	c.EXPECT().Close().Return(nil).Maybe()
	return c
}

func (c *collector) send(_ context.Context, b event.Batch) error {
	c.mu.Lock()
	c.calls++
	n := c.calls
	hook := c.onSend
	c.mu.Unlock()
	if hook != nil {
		if err := hook(n); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, b)
	return nil
}

func (c *collector) events() []*event.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*event.Event
	for _, b := range c.batches {
		out = append(out, b.Events...)
	}
	return out
}

func baseConfig(url string, s event.Sink) Config {
	return Config{
		Name:     "snap",
		Postgres: config.PostgresConfig{URL: url},
		Retry:    config.RetryConfig{Policy: "stop"},
		Sink:     s,
		Chunk:    1000,
	}
}

// liveStream is a persistent logical slot created before the snapshot, read
// with the same decoder the engine uses.
type liveStream struct {
	url, slot string
}

func newLiveStream(t *testing.T, url string) *liveStream {
	t.Helper()
	c := connect(t, url)
	mustExec(t, c, `CREATE PUBLICATION live FOR ALL TABLES`)
	mustExec(t, c, `SELECT pg_create_logical_replication_slot('live', 'pgoutput')`)
	return &liveStream{url: url, slot: "live"}
}

// drain returns the committed transactions of the slot so far, with the
// commit LSN in each batch.
func (l *liveStream) drain(t *testing.T) []event.Batch {
	t.Helper()
	ctx := context.Background()
	c := connect(t, l.url)
	// The walsender pins these (postgres.Conn); a plain session must as well.
	mustExec(t, c, `SET datestyle = 'ISO'; SET bytea_output = 'hex'; SET extra_float_digits = 1; SET intervalstyle = 'iso_8601'`)
	rows, err := c.Query(ctx, `SELECT lsn::text, data FROM pg_logical_slot_get_binary_changes(
		'live', NULL, NULL, 'proto_version', '1', 'publication_names', 'live')`)
	require.NoError(t, err)
	defer rows.Close()
	buf := postgres.NewTxBuffer("snap", postgres.NewRelations(), nil)
	var out []event.Batch
	for rows.Next() {
		var lsn string
		var data []byte
		require.NoError(t, rows.Scan(&lsn, &data))
		msg, err := pglogrepl.Parse(data)
		require.NoError(t, err)
		start, err := pglogrepl.ParseLSN(lsn)
		require.NoError(t, err)
		b, err := buf.Push(msg, event.LSN(start))
		require.NoError(t, err)
		if b != nil {
			out = append(out, *b)
		}
	}
	require.NoError(t, rows.Err())
	return out
}
