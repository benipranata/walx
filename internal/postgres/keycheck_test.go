package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

func TestKeyChecksIntegration(t *testing.T) {
	url := isolatedDatabase(t, startPostgres(t))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `
		CREATE TABLE two (id int primary key, email text not null unique, note text);
		CREATE TABLE loose (id int primary key, tag text, other text);
		CREATE UNIQUE INDEX loose_partial ON loose (tag) WHERE tag IS NOT NULL;
		CREATE TABLE cover (id int primary key, a text not null, b text not null);
		CREATE UNIQUE INDEX cover_a ON cover (a);
		CREATE TABLE pair (id int primary key, a text not null, b text not null, UNIQUE (a, b))`)
	require.NoError(t, err)

	keyed := func(name string, ops []event.Op, key ...string) config.TableConfig {
		return config.TableConfig{Name: name, Ops: ops, Key: key}
	}
	db := func(auto bool, tbls ...config.TableConfig) config.DatabaseConfig {
		return config.DatabaseConfig{
			Postgres: config.PostgresConfig{Publication: "p_key", AutoCreate: true, AutoReplicaIdentity: auto},
			Tables:   tbls,
		}
	}
	tests := []struct {
		name     string
		db       config.DatabaseConfig
		wantErr  string
		wantWarn []string
	}{
		{"no key configured", db(true, tbl("public.two")), "", nil},
		{"unique key covered", db(true, keyed("public.two", nil, "email")), "", nil},
		{"superset of a unique index is covered", db(true, keyed("public.cover", nil, "a", "b")), "", nil},
		{"missing column", db(true, keyed("public.two", nil, "email", "nope")), `key column "nope" is not a column of public.two`, nil},
		{"missing table", db(true, keyed("public.gone", nil, "id")), "public.gone", nil},
		{"no unique index warns", db(true, keyed("public.two", nil, "note")), "", []string{"public.two"}},
		{"partial index does not count", db(true, keyed("public.loose", nil, "tag")), "", []string{"public.loose"}},
		{"part of a composite unique index warns", db(true, keyed("public.pair", nil, "a")), "", []string{"public.pair"}},
		{"unlisted columns of a covering key", db(true, keyed("public.loose", nil, "id", "other")), "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := connect(t, ctx, url)
			warns, err := CheckKeys(ctx, c.PgConn(), tt.db)
			if tt.wantErr != "" {
				require.ErrorIs(t, err, ErrKey)
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, warns, len(tt.wantWarn))
			for i, w := range tt.wantWarn {
				require.Contains(t, warns[i], w)
			}
		})
	}

	t.Run("uncovered identity fails with auto off", func(t *testing.T) {
		c := connect(t, ctx, url)
		cfg := db(false, keyed("public.two", []event.Op{event.OpDelete}, "email"))
		_, err := EnsureReplicaIdentity(ctx, c.PgConn(), cfg)
		require.ErrorIs(t, err, ErrReplicaIdentity)
		require.ErrorContains(t, err, "outside the replica identity")
	})
}

// TestEventKeyIntegration records insert, update and delete on a table with
// two unique keys under REPLICA IDENTITY FULL and decodes them with the
// default and a chosen key.
func TestEventKeyIntegration(t *testing.T) {
	setup := `CREATE TABLE two (id int primary key, email text not null unique, note text);
		ALTER TABLE two REPLICA IDENTITY FULL`
	dml := `INSERT INTO two VALUES (1, 'a@x', 'n'); UPDATE two SET email = 'b@x', note = 'm' WHERE id = 1; DELETE FROM two WHERE id = 1`
	raw := recordFixture(t, isolatedDatabase(t, startPostgres(t)), setup, dml)
	tests := []struct {
		name string
		keys map[string][]string
		want []map[string]any
	}{
		{"chosen key", map[string][]string{"public.two": {"email"}}, []map[string]any{{"email": "a@x"}, {"email": "b@x"}, {"email": "b@x"}}},
		{"primary key passed by the caller", map[string][]string{"public.two": {"id"}}, []map[string]any{{"id": int64(1)}, {"id": int64(1)}, {"id": int64(1)}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rels := NewRelations()
			var got []map[string]any
			for _, b := range raw {
				m, err := pglogrepl.Parse(b)
				require.NoError(t, err)
				if r, ok := m.(*pglogrepl.RelationMessage); ok {
					rels.Set(r)
				}
				evs, err := DecodeDML(rels, tt.keys, m)
				require.NoError(t, err)
				for _, e := range evs {
					got = append(got, e.Key)
				}
			}
			require.Equal(t, tt.want, got)
		})
	}
}
