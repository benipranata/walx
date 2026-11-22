package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func TestReadKeysIntegration(t *testing.T) {
	url := isolatedDatabase(t, startPostgres(t))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `
		CREATE TABLE acct (id int PRIMARY KEY, email text UNIQUE, note text);
		CREATE TABLE pair (b int, a int, note text, PRIMARY KEY (b, a));
		CREATE TABLE loose (id int, note text);
		CREATE TABLE plain (id int PRIMARY KEY, note text);
		CREATE SCHEMA other;
		CREATE TABLE other.acct (k text PRIMARY KEY);
		ALTER TABLE acct REPLICA IDENTITY FULL; ALTER TABLE pair REPLICA IDENTITY FULL;
		ALTER TABLE loose REPLICA IDENTITY FULL; ALTER TABLE other.acct REPLICA IDENTITY FULL`)
	require.NoError(t, err)

	tests := []struct {
		name   string
		tables []config.TableConfig
		want   map[string][]string
	}{
		{"primary key", []config.TableConfig{tbl("public.acct")}, map[string][]string{"public.acct": {"id"}}},
		{"configured key wins", []config.TableConfig{{Name: "public.acct", Key: []string{"email"}}}, map[string][]string{"public.acct": {"email"}}},
		{"composite key in index order", []config.TableConfig{tbl("public.pair")}, map[string][]string{"public.pair": {"b", "a"}}},
		{"no primary key gives no entry", []config.TableConfig{tbl("public.loose")}, map[string][]string{}},
		{"default identity gets no primary key entry", []config.TableConfig{tbl("public.plain")}, map[string][]string{}},
		{"missing table gets no entry", []config.TableConfig{tbl("public.nope")}, map[string][]string{}},
		{"schema qualified", []config.TableConfig{tbl("other.acct")}, map[string][]string{"other.acct": {"k"}}},
		{"several tables", []config.TableConfig{tbl("public.acct"), tbl("public.loose"), tbl("public.pair")},
			map[string][]string{"public.acct": {"id"}, "public.pair": {"b", "a"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := connect(t, ctx, url)
			got, err := ReadKeys(ctx, c.PgConn(), config.DatabaseConfig{Tables: tt.tables})
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestReadKeysFullIdentityIntegration replays real messages of a table that
// is switched to REPLICA IDENTITY FULL: the key stays the primary key.
func TestReadKeysFullIdentityIntegration(t *testing.T) {
	url := isolatedDatabase(t, startPostgres(t))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const setup = `CREATE TABLE acct (id int PRIMARY KEY, email text, note text);
		CREATE TABLE pair (b int, a int, note text, PRIMARY KEY (b, a));
		ALTER TABLE acct REPLICA IDENTITY FULL; ALTER TABLE pair REPLICA IDENTITY FULL`
	const dml = `INSERT INTO acct VALUES (1, 'a', 'x'); INSERT INTO pair VALUES (2, 1, 'x'); COMMIT;
		BEGIN; UPDATE acct SET note = 'y' WHERE id = 1; UPDATE pair SET note = 'y'; DELETE FROM acct WHERE id = 1; COMMIT`
	raw := recordFixture(t, url, setup, dml)

	tests := []struct {
		name    string
		tables  []config.TableConfig
		wantKey map[string]any
		wantPK  map[string]any
	}{
		{"primary key from catalog", []config.TableConfig{tbl("public.acct"), tbl("public.pair")},
			map[string]any{"id": int64(1)}, map[string]any{"b": int64(2), "a": int64(1)}},
		{"configured key overrides", []config.TableConfig{{Name: "public.acct", Key: []string{"email"}}, tbl("public.pair")},
			map[string]any{"email": "a"}, map[string]any{"b": int64(2), "a": int64(1)}},
		{"no keys read keeps whole row", nil,
			map[string]any{"id": int64(1), "email": "a", "note": "x"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := connect(t, ctx, url)
			keys, err := ReadKeys(ctx, c.PgConn(), config.DatabaseConfig{Tables: tt.tables})
			require.NoError(t, err)
			b := NewTxBuffer("main", NewRelations(), keys)
			byTable := map[string][]map[string]any{}
			for _, r := range raw {
				m, err := pglogrepl.Parse(r)
				require.NoError(t, err)
				batch, err := b.Push(m, 0)
				require.NoError(t, err)
				if batch == nil {
					continue
				}
				for _, e := range batch.Events {
					byTable[e.Table] = append(byTable[e.Table], e.Key)
				}
			}
			// acct: insert, update of a non-key column, delete; the key never changes.
			require.Len(t, byTable["acct"], 3)
			for _, k := range byTable["acct"] {
				if tt.name == "no keys read keeps whole row" {
					require.Len(t, k, 3)
					continue
				}
				require.Equal(t, tt.wantKey, k)
			}
			if tt.wantPK != nil {
				for _, k := range byTable["pair"] {
					require.Equal(t, tt.wantPK, k)
				}
			}
		})
	}
}
