package snapshot

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/filter"
)

func TestSnapshotRowsInKeyOrderAcrossChunks(t *testing.T) {
	tests := []struct {
		name  string
		setup string
		table string
		rows  int
		chunk int
		key   []string
		batch []int
	}{
		{
			name:  "single key",
			setup: `CREATE TABLE items (id int PRIMARY KEY, name text); INSERT INTO items SELECT g, 'n' || g FROM generate_series(2500, 1, -1) g`,
			table: "public.items", rows: 2500, chunk: 1000, key: []string{"id"}, batch: []int{1000, 1000, 500},
		},
		{
			name: "composite key with NULLs outside the key",
			setup: `CREATE TABLE lines (tenant text, n int, note text, PRIMARY KEY (tenant, n));
INSERT INTO lines SELECT 't' || (g % 3), g, CASE WHEN g % 2 = 0 THEN NULL ELSE 'x' END FROM generate_series(1, 25) g`,
			table: "public.lines", rows: 25, chunk: 10, key: []string{"tenant", "n"}, batch: []int{10, 10, 5},
		},
		{
			name:  "empty table",
			setup: `CREATE TABLE empty (id int PRIMARY KEY)`,
			table: "public.empty", rows: 0, chunk: 10, key: []string{"id"}, batch: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := startPostgres(t)
			mustExec(t, connect(t, url), tt.setup)
			s := &collector{}
			cfg := baseConfig(url, s)
			cfg.Chunk = tt.chunk
			cfg.Targets = []Target{{Table: tt.table}}
			res, err := Run(context.Background(), cfg)
			require.NoError(t, err)
			require.Equal(t, tt.rows, res.Rows)
			require.Equal(t, tt.rows, res.Delivered)

			var sizes []int
			for _, b := range s.batches {
				sizes = append(sizes, len(b.Events))
				require.Equal(t, res.ConsistentLSN, b.CommitLSN)
			}
			require.Equal(t, tt.batch, sizes)

			evs := s.events()
			require.Len(t, evs, tt.rows)
			seen := map[string]bool{}
			for i, e := range evs {
				require.Equal(t, event.OpSnapshot, e.Op)
				require.Equal(t, "snap", e.Source)
				require.Equal(t, res.ConsistentLSN, e.LSN)
				require.Equal(t, fmt.Sprintf("snap-%s-%d", res.ConsistentLSN, i+1), e.ID)
				require.False(t, seen[e.ID])
				seen[e.ID] = true
				for _, k := range tt.key {
					require.Contains(t, e.Key, k)
					require.Equal(t, e.After[k], e.Key[k])
				}
				if i > 0 {
					require.Negative(t, compareKeys(tt.key, evs[i-1].Key, e.Key), "key order at row %d", i)
				}
			}
		})
	}
}

// compareKeys orders two keys column by column; values are int64 or string.
func compareKeys(cols []string, a, b map[string]any) int {
	for _, c := range cols {
		var d int
		switch av := a[c].(type) {
		case int64:
			d = cmp.Compare(av, b[c].(int64))
		case string:
			d = cmp.Compare(av, b[c].(string))
		}
		if d != 0 {
			return d
		}
	}
	return 0
}

func TestSnapshotMatchesLiveStreamWithoutGaps(t *testing.T) {
	tests := []struct {
		name string
	}{{"changes after the consistent point reach the live slot only"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := startPostgres(t)
			c := connect(t, url)
			mustExec(t, c, `CREATE TABLE t (id int PRIMARY KEY, v text)`)
			live := newLiveStream(t, url)
			var slotBefore string
			require.NoError(t, c.QueryRow(context.Background(), `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'live'`).Scan(&slotBefore))
			mustExec(t, c, `INSERT INTO t SELECT g, 'old' FROM generate_series(1, 30) g`)

			s := &collector{}
			s.onSend = func(n int) error {
				if n != 1 {
					return nil
				}
				w := connect(t, url)
				mustExec(t, w, `INSERT INTO t VALUES (100, 'late')`)
				mustExec(t, w, `UPDATE t SET v = 'changed' WHERE id = 5`)
				mustExec(t, w, `DELETE FROM t WHERE id = 6`)
				return nil
			}
			cfg := baseConfig(url, s)
			cfg.Chunk = 10
			res, err := Run(context.Background(), cfg)
			require.NoError(t, err)

			snap := map[any]string{}
			for _, e := range s.events() {
				snap[e.Key["id"]] = e.After["v"].(string)
			}
			require.Len(t, snap, 30)
			require.Equal(t, "old", snap[int64(5)])
			require.Contains(t, snap, int64(6))
			require.NotContains(t, snap, int64(100))

			var slotAfter string
			require.NoError(t, c.QueryRow(context.Background(), `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'live'`).Scan(&slotAfter))
			require.Equal(t, slotBefore, slotAfter, "the persistent slot is untouched")
			var before, after []*event.Event
			for _, b := range live.drain(t) {
				for _, e := range b.Events {
					if b.CommitLSN <= res.ConsistentLSN {
						before = append(before, e)
					} else {
						after = append(after, e)
					}
				}
			}
			require.Len(t, before, 30, "everything committed before the consistent point is in the snapshot")
			require.Len(t, after, 3, "everything after it is in the live stream")
			require.Equal(t, event.OpInsert, after[0].Op)
			require.Equal(t, event.OpUpdate, after[1].Op)
			require.Equal(t, event.OpDelete, after[2].Op)

			var temp int
			require.NoError(t, c.QueryRow(context.Background(), `SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'walx_snap_%'`).Scan(&temp))
			require.Zero(t, temp, "the temporary slot is gone")
		})
	}
}

func TestSnapshotValuesMatchLiveDecode(t *testing.T) {
	tests := []struct {
		name   string
		ddl    string
		insert string
		nullIn string
	}{
		{
			name: "all mapped types",
			ddl: `CREATE TABLE ty (id int PRIMARY KEY, b bool, i2 int2, i8 bigint, f4 float4, f8 float8, n numeric(10,2), t text, c char(5),
				u uuid, by bytea, j json, jb jsonb, d date, ts timestamp, tz timestamptz, arr text[], iv interval, inet inet, mood text)`,
			insert: `INSERT INTO ty VALUES (1, true, 7, 9007199254740993, 1.5, 0.1, 12.50, 'héllo', 'ab',
				'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\xdeadbeef', '{"b": 1,  "a": [1.0, 2]}', '{"b": 1,  "a": [1.0, 2]}',
				'2024-05-01', '2024-05-01 10:00:00.123', '2024-05-01 10:00:00+02', '{a,b}', '1 day 2 hours', '10.0.0.1', NULL)`,
		},
		{
			name:   "specials",
			ddl:    `CREATE TABLE ty (id int PRIMARY KEY, f float8, d date, ts timestamptz, n numeric, t text)`,
			insert: `INSERT INTO ty VALUES (1, 'NaN', 'infinity', '-infinity', 'NaN', E'line\nbreak "q"'), (2, 'Infinity', '0044-03-15 BC', now(), -0.000, '')`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := startPostgres(t)
			c := connect(t, url)
			mustExec(t, c, tt.ddl)
			live := newLiveStream(t, url)
			mustExec(t, c, tt.insert)

			s := &collector{}
			cfg := baseConfig(url, s)
			_, err := Run(context.Background(), cfg)
			require.NoError(t, err)

			var liveEvents []*event.Event
			for _, b := range live.drain(t) {
				liveEvents = append(liveEvents, b.Events...)
			}
			snapEvents := s.events()
			require.NotEmpty(t, liveEvents)
			require.Len(t, snapEvents, len(liveEvents))
			for i := range liveEvents {
				require.Equal(t, liveEvents[i].After, snapEvents[i].After)
				require.Equal(t, liveEvents[i].Key, snapEvents[i].Key)
				require.Equal(t, event.OpSnapshot, snapEvents[i].Op)
			}
		})
	}
}

func TestSnapshotSelection(t *testing.T) {
	setup := `CREATE TABLE a (id int PRIMARY KEY, status text, secret text, gen int GENERATED ALWAYS AS (id * 2) STORED);
CREATE TABLE b (id int PRIMARY KEY);
CREATE TABLE nokey (x int, y text);
CREATE SCHEMA other; CREATE TABLE other.c (id int PRIMARY KEY);
INSERT INTO a VALUES (1, 'new', 's1'), (2, 'paid', 's2'), (3, 'new', 's3');
INSERT INTO b VALUES (1), (2);
INSERT INTO nokey VALUES (1, 'p'), (1, 'p'), (2, NULL);
INSERT INTO other.c VALUES (9);`
	tests := []struct {
		name       string
		targets    []Target
		tables     []config.TableConfig
		useFilter  bool
		wantTables []string
		wantCount  int
		check      func(t *testing.T, evs []*event.Event)
	}{
		{name: "every user table by default", wantTables: []string{"public.a", "public.b", "public.nokey", "other.c"}, wantCount: 3 + 2 + 3 + 1,
			check: func(t *testing.T, evs []*event.Event) {
				for _, e := range evs {
					switch e.Table {
					case "nokey":
						require.Nil(t, e.Key)
					case "a":
						require.NotContains(t, e.After, "gen", "generated columns are not replicated")
					}
				}
			}},
		{name: "where on one table", targets: []Target{{Table: "public.a", Where: "status = 'new' AND id > 1"}}, wantCount: 1,
			check: func(t *testing.T, evs []*event.Event) { require.Equal(t, int64(3), evs[0].Key["id"]) }},
		{name: "configured key and publication columns", targets: []Target{{Table: "public.a"}},
			tables:    []config.TableConfig{{Name: "public.a", Key: []string{"status", "id"}, PublicationColumns: []string{"id", "status"}}},
			wantCount: 3,
			check: func(t *testing.T, evs []*event.Event) {
				require.Equal(t, map[string]any{"id": int64(2), "status": "paid"}, evs[2].After)
				require.Equal(t, evs[2].After, evs[2].Key)
				require.Equal(t, "new", evs[0].Key["status"], "ordered by the configured key")
			}},
		{name: "filter chain applies", targets: []Target{{Table: "public.a"}},
			tables:    []config.TableConfig{{Name: "public.a", Where: map[string]any{"status": "new"}, DropColumns: []string{"secret"}}},
			useFilter: true, wantCount: 2,
			check: func(t *testing.T, evs []*event.Event) {
				for _, e := range evs {
					require.Equal(t, event.OpSnapshot, e.Op)
					require.NotContains(t, e.After, "secret")
				}
			}},
		{name: "reference_only keeps the key", targets: []Target{{Table: "public.b"}},
			tables:    []config.TableConfig{{Name: "public.b", ReferenceOnly: true}},
			useFilter: true, wantCount: 2,
			check: func(t *testing.T, evs []*event.Event) {
				require.Nil(t, evs[0].After)
				require.Equal(t, map[string]any{"id": int64(1)}, evs[0].Key)
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := startPostgres(t)
			mustExec(t, connect(t, url), setup)
			s := &collector{}
			cfg := baseConfig(url, s)
			cfg.Targets = tt.targets
			cfg.Tables = tt.tables
			if tt.useFilter {
				cfg.Filter = filter.New("snap", tt.tables, nil, slog.New(slog.DiscardHandler)).Apply
			}
			_, err := Run(context.Background(), cfg)
			require.NoError(t, err)
			evs := s.events()
			require.Len(t, evs, tt.wantCount)
			if tt.wantTables != nil {
				var got []string
				seen := map[string]bool{}
				for _, e := range evs {
					n := e.Schema + "." + e.Table
					if !seen[n] {
						seen[n] = true
						got = append(got, n)
					}
				}
				require.Equal(t, []string{"other.c", "public.a", "public.b", "public.nokey"}, got, "tables in name order")
			}
			tt.check(t, evs)
		})
	}
}

func TestSnapshotErrors(t *testing.T) {
	tests := []struct {
		name    string
		setup   string
		targets []Target
		tables  []config.TableConfig
		wantErr string
	}{
		{"missing table", `CREATE TABLE a (id int PRIMARY KEY)`, []Target{{Table: "public.nope"}}, nil, "table does not exist"},
		{"bad table name", `CREATE TABLE a (id int PRIMARY KEY)`, []Target{{Table: "nope"}}, nil, "schema.table"},
		{"bad where", `CREATE TABLE a (id int PRIMARY KEY)`, []Target{{Table: "public.a", Where: "nope = 1"}}, nil, "nope"},
		{"where cannot add statements", `CREATE TABLE a (id int PRIMARY KEY)`, []Target{{Table: "public.a", Where: "true); DROP TABLE a; SELECT (1"}}, nil, "cannot insert multiple commands"},
		{"NULL in a configured key", `CREATE TABLE a (id int, k text); INSERT INTO a VALUES (1, NULL)`, []Target{{Table: "public.a"}},
			[]config.TableConfig{{Name: "public.a", Key: []string{"k"}}}, "is NULL"},
		{"key outside the columns", `CREATE TABLE a (id int PRIMARY KEY, k text)`, []Target{{Table: "public.a"}},
			[]config.TableConfig{{Name: "public.a", Key: []string{"zz"}}}, `key column "zz"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := startPostgres(t)
			c := connect(t, url)
			mustExec(t, c, tt.setup)
			s := &collector{}
			cfg := baseConfig(url, s)
			cfg.Targets = tt.targets
			cfg.Tables = tt.tables
			_, err := Run(context.Background(), cfg)
			require.ErrorContains(t, err, tt.wantErr)
			require.NotContains(t, err.Error(), "secret")
			var n int
			require.NoError(t, c.QueryRow(context.Background(), `SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'walx_snap_%'`).Scan(&n))
			require.Zero(t, n)
			if strings.Contains(tt.name, "cannot add") {
				var exists bool
				require.NoError(t, c.QueryRow(context.Background(), `SELECT to_regclass('public.a') IS NOT NULL`).Scan(&exists))
				require.True(t, exists)
			}
		})
	}
}

func TestSnapshotSinkPolicy(t *testing.T) {
	boom := errors.New("sink down")
	tests := []struct {
		name      string
		retry     config.RetryConfig
		onPerm    string
		fail      func(n int) error
		wantErr   string
		wantBatch int
	}{
		{"transient failure is retried", config.RetryConfig{Policy: "forever", Initial: 1, Max: 2}, "stop",
			func(n int) error {
				if n == 1 {
					return boom
				}
				return nil
			}, "", 2},
		{"stop policy fails the snapshot", config.RetryConfig{Policy: "stop"}, "stop", func(int) error { return boom }, "sink down", 0},
		{"permanent error skipped", config.RetryConfig{Policy: "stop"}, "skip",
			func(n int) error {
				if n == 1 {
					return event.Permanent(boom)
				}
				return nil
			}, "", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := startPostgres(t)
			mustExec(t, connect(t, url), `CREATE TABLE a (id int PRIMARY KEY); INSERT INTO a SELECT generate_series(1, 20)`)
			s := &collector{onSend: tt.fail}
			cfg := baseConfig(url, s)
			cfg.Chunk = 10
			cfg.Retry = tt.retry
			cfg.OnPermanentError = tt.onPerm
			_, err := Run(context.Background(), cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, s.batches, tt.wantBatch)
		})
	}
}

func TestSnapshotRunErrors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"no sink", func(c *Config) { c.Sink = nil }, "required"},
		{"no name", func(c *Config) { c.Name = "" }, "required"},
		{"unreachable server", func(c *Config) { c.Postgres.URL = "postgres://u:secret@127.0.0.1:1/x?connect_timeout=1" }, "connect"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig("postgres://u:p@127.0.0.1:1/x", &collector{})
			tt.mutate(&cfg)
			_, err := Run(context.Background(), cfg)
			require.ErrorContains(t, err, tt.wantErr)
			require.NotContains(t, err.Error(), "secret")
		})
	}
}
