package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

type pubRow struct {
	All, Ins, Upd, Del, Trunc, Root bool
}

func TestPublicationIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `CREATE TABLE a (id int primary key); CREATE TABLE b (id int primary key)`)
	require.NoError(t, err)

	read := func(t *testing.T, name string) pubRow {
		t.Helper()
		var r pubRow
		require.NoError(t, admin.QueryRow(ctx, `SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot
			FROM pg_publication WHERE pubname = $1`, name).Scan(&r.All, &r.Ins, &r.Upd, &r.Del, &r.Trunc, &r.Root))
		return r
	}
	tables := func(t *testing.T, name string) []string {
		t.Helper()
		rows, err := admin.Query(ctx, `SELECT tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY 1`, name)
		require.NoError(t, err)
		got, err := pgx.CollectRows(rows, pgx.RowTo[string])
		require.NoError(t, err)
		return got
	}
	db := func(pub string, autoCreate, autoAlter bool, tbls ...config.TableConfig) config.DatabaseConfig {
		return config.DatabaseConfig{
			Postgres: config.PostgresConfig{Publication: pub, AutoCreate: autoCreate, AutoAlter: autoAlter},
			Tables:   tbls,
		}
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "all tables by default and idempotent", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			cfg := db("p_all", true, false)
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			require.Equal(t, pubRow{All: true, Ins: true, Upd: true, Root: true}, read(t, "p_all"))
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
		}},
		{name: "publish ops match config", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			cfg := db("p_ops", true, false,
				tbl("public.a", event.OpInsert), tbl("public.b", event.OpDelete, event.OpTruncate))
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			require.Equal(t, pubRow{Ins: true, Del: true, Trunc: true, Root: true}, read(t, "p_ops"))
			require.Equal(t, []string{"a", "b"}, tables(t, "p_ops"))
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
		}},
		{name: "lost creation race validates the winner", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			cfg := db("p_race", true, false, tbl("public.a"))
			race := func(create string) {
				afterPublicationCheck = func() {
					afterPublicationCheck = nil
					_, err := admin.Exec(ctx, create)
					require.NoError(t, err)
				}
				t.Cleanup(func() { afterPublicationCheck = nil })
			}
			race(`CREATE PUBLICATION p_race FOR TABLE a WITH (publish = 'insert, update', publish_via_partition_root = true)`)
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))

			cfg = db("p_race_drift", true, false, tbl("public.a"))
			race(`CREATE PUBLICATION p_race_drift FOR TABLE a WITH (publish = 'insert')`)
			err := EnsurePublication(ctx, c.PgConn(), cfg)
			require.ErrorIs(t, err, ErrPublicationDrift)
		}},
		{name: "validate-only fails when missing", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			err := EnsurePublication(ctx, c.PgConn(), db("p_missing", false, false))
			require.ErrorIs(t, err, ErrPublicationMissing)
			var n int
			require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM pg_publication WHERE pubname = 'p_missing'`).Scan(&n))
			require.Zero(t, n)
		}},
		{name: "validate-only accepts matching", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := admin.Exec(ctx, `CREATE PUBLICATION p_manual FOR TABLE a WITH (publish = 'insert, update', publish_via_partition_root = true)`)
			require.NoError(t, err)
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), db("p_manual", false, false, tbl("public.a"))))
		}},
		{name: "drift errors without auto_alter and is fixed with it", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := admin.Exec(ctx, `CREATE PUBLICATION p_drift FOR TABLE a WITH (publish = 'insert')`)
			require.NoError(t, err)
			cfg := db("p_drift", true, false, tbl("public.a", event.OpInsert, event.OpUpdate, event.OpDelete), tbl("public.b"))

			err = EnsurePublication(ctx, c.PgConn(), cfg)
			require.ErrorIs(t, err, ErrPublicationDrift)
			require.Equal(t, pubRow{Ins: true}, read(t, "p_drift"))

			cfg.Postgres.AutoAlter = true
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			require.Equal(t, pubRow{Ins: true, Upd: true, Del: true, Root: true}, read(t, "p_drift"))
			require.Equal(t, []string{"a", "b"}, tables(t, "p_drift"))

			cfg.Postgres.AutoAlter = false
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
		}},
		{name: "row filter and column list count as drift", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := admin.Exec(ctx, `CREATE TABLE f (id int primary key, v text)`)
			require.NoError(t, err)
			_, err = admin.Exec(ctx, `CREATE PUBLICATION p_filter FOR TABLE f (id) WHERE (id > 0)
				WITH (publish = 'insert, update', publish_via_partition_root = true)`)
			require.NoError(t, err)
			cfg := db("p_filter", true, false, tbl("public.f"))

			err = EnsurePublication(ctx, c.PgConn(), cfg)
			require.ErrorIs(t, err, ErrPublicationDrift)
			require.ErrorContains(t, err, "public.f")
			require.ErrorContains(t, err, "row filter or column list")

			cfg.Postgres.AutoAlter = true
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			var filtered bool
			require.NoError(t, admin.QueryRow(ctx, `SELECT r.prqual IS NOT NULL OR r.prattrs IS NOT NULL
				FROM pg_publication_rel r JOIN pg_publication p ON p.oid = r.prpubid WHERE p.pubname = 'p_filter'`).Scan(&filtered))
			require.False(t, filtered)
			cfg.Postgres.AutoAlter = false
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
		}},
		{name: "failed auto_alter leaves the publication unchanged", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := admin.Exec(ctx, `CREATE PUBLICATION p_half FOR TABLE a WITH (publish = 'insert')`)
			require.NoError(t, err)
			cfg := db("p_half", true, true, tbl("public.a", event.OpInsert, event.OpUpdate), tbl("public.missing"))

			err = EnsurePublication(ctx, c.PgConn(), cfg)
			require.Error(t, err)
			require.Equal(t, pubRow{Ins: true}, read(t, "p_half"))
			require.Equal(t, []string{"a"}, tables(t, "p_half"))
		}},
		{name: "all tables vs list cannot be altered", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := admin.Exec(ctx, `CREATE PUBLICATION p_kind FOR ALL TABLES WITH (publish_via_partition_root = true)`)
			require.NoError(t, err)
			err = EnsurePublication(ctx, c.PgConn(), db("p_kind", true, true, tbl("public.a")))
			require.ErrorIs(t, err, ErrPublicationDrift)
		}},
		{name: "role that cannot create all tables fails clearly", run: func(t *testing.T) {
			cfg, err := pgx.ParseConfig(url)
			require.NoError(t, err)
			_, err = admin.Exec(ctx, `DROP ROLE IF EXISTS lim`)
			require.NoError(t, err)
			_, err = admin.Exec(ctx, `CREATE ROLE lim LOGIN REPLICATION PASSWORD 'lim'`)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _ = admin.Exec(ctx, `REVOKE ALL ON DATABASE `+pgx.Identifier{cfg.Database}.Sanitize()+` FROM lim`)
				_, _ = admin.Exec(ctx, `DROP ROLE IF EXISTS lim`)
			})
			_, err = admin.Exec(ctx, `GRANT CREATE ON DATABASE `+pgx.Identifier{cfg.Database}.Sanitize()+` TO lim`)
			require.NoError(t, err)
			c := connect(t, ctx, withUser(t, url, "lim", "lim"))
			err = EnsurePublication(ctx, c.PgConn(), db("p_lim", true, false))
			require.ErrorContains(t, err, "FOR ALL TABLES")
			require.ErrorContains(t, err, "list tables explicitly")
		}},
		{name: "partitioned table arrives under the root name", run: func(t *testing.T) {
			_, err := admin.Exec(ctx, `CREATE TABLE events (id int, p int, PRIMARY KEY (id, p)) PARTITION BY LIST (p);
				CREATE TABLE events_1 PARTITION OF events FOR VALUES IN (1)`)
			require.NoError(t, err)
			c := connect(t, ctx, url)
			cfg := db("p_part", true, false, tbl("public.events"))
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			require.Equal(t, []string{"events"}, tables(t, "p_part"))

			_, err = EnsureSlot(ctx, c.PgConn(), "s_part", true)
			require.NoError(t, err)
			require.NoError(t, StartStream(ctx, c.PgConn(), "s_part", "p_part", 0))
			_, err = admin.Exec(ctx, `INSERT INTO events VALUES (1, 1)`)
			require.NoError(t, err)

			var rels []string
			for len(rels) == 0 {
				rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
				msg, err := c.PgConn().ReceiveMessage(rctx)
				rcancel()
				require.NoError(t, err)
				cd, ok := msg.(*pgproto3.CopyData)
				if !ok || len(cd.Data) == 0 || cd.Data[0] != pglogrepl.XLogDataByteID {
					continue
				}
				xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
				require.NoError(t, err)
				lm, err := pglogrepl.Parse(xld.WALData)
				require.NoError(t, err)
				if rel, ok := lm.(*pglogrepl.RelationMessage); ok {
					rels = append(rels, rel.RelationName)
				}
			}
			require.Equal(t, []string{"events"}, rels)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
