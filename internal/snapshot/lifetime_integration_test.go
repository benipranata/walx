package snapshot

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

// A write committed after the slot's consistent point must not be visible to
// the reader, even when it lands between exporting and importing the
// snapshot, and the snapshot must survive the slot connection closing.
func TestSnapshotImportPinsConsistentPoint(t *testing.T) {
	tests := []struct {
		name string
	}{{"writes between export, import and close are invisible"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := startPostgres(t)
			ctx := context.Background()
			c := connect(t, url)
			mustExec(t, c, `CREATE TABLE t (id int PRIMARY KEY); INSERT INTO t VALUES (1)`)

			ex, err := exportSnapshot(ctx, config.PostgresConfig{URL: url}, config.RetryConfig{})
			require.NoError(t, err)
			mustExec(t, c, `INSERT INTO t VALUES (2)`)
			rd, err := openReader(ctx, url, ex.snapshot, 10)
			require.NoError(t, err)
			defer rd.close(ctx)
			ex.close(ctx)
			mustExec(t, c, `INSERT INTO t VALUES (3)`)

			var ids []int
			p, err := rd.plan(ctx, "public.t", "", nil)
			require.NoError(t, err)
			require.NoError(t, rd.read(ctx, p, func(_ []pgconn.FieldDescription, rows [][][]byte) error {
				for _, r := range rows {
					ids = append(ids, int(r[0][0]-'0'))
				}
				return nil
			}))
			require.Equal(t, []int{1}, ids)
		})
	}
}

func TestSnapshotLifecycle(t *testing.T) {
	tests := []struct {
		name    string
		setup   string
		run     func(t *testing.T, cfg *Config, s *collector, cancel context.CancelFunc)
		wantErr error
		check   func(t *testing.T, s *collector)
	}{
		{"cancel stops the run and drops the slot", `CREATE TABLE a (id int PRIMARY KEY); INSERT INTO a SELECT generate_series(1, 30)`,
			func(t *testing.T, cfg *Config, s *collector, cancel context.CancelFunc) {
				cfg.Chunk = 10
				s.onSend = func(int) error { cancel(); return nil }
			}, context.Canceled, func(t *testing.T, s *collector) { require.Len(t, s.batches, 1) }},
		{"a slow sink does not hit idle_in_transaction_session_timeout", `CREATE TABLE a (id int PRIMARY KEY); INSERT INTO a SELECT generate_series(1, 20);
ALTER DATABASE CURRENT_DB SET idle_in_transaction_session_timeout = '500ms'`,
			func(t *testing.T, cfg *Config, s *collector, _ context.CancelFunc) {
				cfg.Chunk = 10
				s.onSend = func(n int) error {
					if n == 1 {
						time.Sleep(1500 * time.Millisecond)
					}
					return nil
				}
			}, nil, func(t *testing.T, s *collector) { require.Len(t, s.events(), 20) }},
		{"a database transaction_timeout does not end the snapshot", `CREATE TABLE a (id int PRIMARY KEY); INSERT INTO a SELECT generate_series(1, 20);
ALTER DATABASE CURRENT_DB SET transaction_timeout = '200ms'`,
			func(t *testing.T, cfg *Config, s *collector, _ context.CancelFunc) {
				cfg.Chunk = 10
				s.onSend = func(n int) error {
					if n == 1 {
						time.Sleep(600 * time.Millisecond)
					}
					return nil
				}
			}, nil, func(t *testing.T, s *collector) { require.Len(t, s.events(), 20) }},
		{"a database statement_timeout does not cancel a chunk fetch", `CREATE TABLE a (id int PRIMARY KEY); INSERT INTO a SELECT generate_series(1, 2);
ALTER DATABASE CURRENT_DB SET statement_timeout = '300ms'`,
			func(t *testing.T, cfg *Config, s *collector, _ context.CancelFunc) {
				cfg.Chunk = 10
				// pg_sleep runs per row inside the FETCH only: 2 rows take 800ms, past the timeout.
				cfg.Targets = []Target{{Table: "public.a", Where: "pg_sleep(0.4) IS NOT NULL"}}
			}, nil, func(t *testing.T, s *collector) { require.Len(t, s.events(), 2) }},
		{"publication_where limits the rows", `CREATE TABLE a (id int PRIMARY KEY); INSERT INTO a SELECT generate_series(1, 20)`,
			func(t *testing.T, cfg *Config, s *collector, _ context.CancelFunc) {
				cfg.Tables = []config.TableConfig{{Name: "public.a", PublicationWhere: "id > 5"}}
				cfg.Targets = []Target{{Table: "public.a", Where: "id <= 10"}}
			}, nil, func(t *testing.T, s *collector) { require.Len(t, s.events(), 5) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := startPostgres(t)
			c := connect(t, url)
			var dbName string
			require.NoError(t, c.QueryRow(context.Background(), `SELECT current_database()`).Scan(&dbName))
			mustExec(t, c, strings.ReplaceAll(tt.setup, "CURRENT_DB", dbName))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := newCollector(t)
			cfg := baseConfig(url, s)
			tt.run(t, &cfg, s, cancel)
			_, err := Run(ctx, cfg)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			tt.check(t, s)
			var n int
			require.NoError(t, c.QueryRow(context.Background(), `SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'walx_snap_%'`).Scan(&n))
			require.Zero(t, n)
		})
	}
}
