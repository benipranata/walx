package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

func TestReplicaIdentityIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `
		CREATE TABLE d_pk (id int primary key, email text not null, status text);
		CREATE TABLE d_nokey (id int, email text not null, status text);
		CREATE TABLE n_pk (id int primary key, email text, status text);
		ALTER TABLE n_pk REPLICA IDENTITY NOTHING;
		CREATE TABLE f_nokey (id int, email text, status text);
		ALTER TABLE f_nokey REPLICA IDENTITY FULL;
		CREATE TABLE i_idx (id int, email text not null, status text);
		CREATE UNIQUE INDEX i_idx_email ON i_idx (email);
		ALTER TABLE i_idx REPLICA IDENTITY USING INDEX i_idx_email`)
	require.NoError(t, err)

	ident := func(t *testing.T, table string) string {
		t.Helper()
		var s string
		require.NoError(t, admin.QueryRow(ctx, `SELECT relreplident::text FROM pg_class WHERE oid = $1::regclass`, table).Scan(&s))
		return s
	}
	db := func(auto bool, tbls ...config.TableConfig) config.DatabaseConfig {
		return config.DatabaseConfig{
			Postgres: config.PostgresConfig{Publication: "p_ident", AutoCreate: true, AutoReplicaIdentity: auto},
			Tables:   tbls,
		}
	}
	fsm := func(name string) config.TableConfig { return config.TableConfig{Name: name, FSMColumn: "status"} }
	delKey := func(name string, key ...string) config.TableConfig {
		return config.TableConfig{Name: name, Ops: []event.Op{event.OpUpdate, event.OpDelete}, Key: key}
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "d with key, f and i need nothing", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			rep, err := EnsureReplicaIdentity(ctx, c.PgConn(), db(true, tbl("public.d_pk"), tbl("public.f_nokey"), tbl("public.i_idx")))
			require.NoError(t, err)
			require.Empty(t, rep.Altered)
			require.Empty(t, rep.Warnings)
		}},
		{name: "d without key is reported and never altered", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := EnsureReplicaIdentity(ctx, c.PgConn(), db(true, tbl("public.d_nokey")))
			require.ErrorIs(t, err, ErrReplicaIdentity)
			require.ErrorContains(t, err, "public.d_nokey")
			require.Equal(t, "d", ident(t, "d_nokey"))
		}},
		{name: "n is reported and never altered", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := EnsureReplicaIdentity(ctx, c.PgConn(), db(true, tbl("public.n_pk")))
			require.ErrorIs(t, err, ErrReplicaIdentity)
			require.Equal(t, "n", ident(t, "n_pk"))
		}},
		{name: "insert-only keyless table is fine", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := EnsureReplicaIdentity(ctx, c.PgConn(), db(true, tbl("public.d_nokey", event.OpInsert)))
			require.NoError(t, err)
		}},
		{name: "unfixable table blocks the fixable one", run: func(t *testing.T) {
			_, err := admin.Exec(ctx, `CREATE TABLE blk_fsm (id int primary key, status text)`)
			require.NoError(t, err)
			c := connect(t, ctx, url)
			_, err = EnsureReplicaIdentity(ctx, c.PgConn(), db(true, fsm("public.blk_fsm"), tbl("public.d_nokey")))
			require.ErrorIs(t, err, ErrReplicaIdentity)
			require.Equal(t, "d", ident(t, "blk_fsm"))
		}},
		{name: "auto sets full once and is idempotent", run: func(t *testing.T) {
			_, err := admin.Exec(ctx, `CREATE TABLE auto_fsm (id int primary key, status text);
				CREATE TABLE auto_key (id int primary key, email text not null);
				CREATE TABLE auto_nokey_fsm (id int, status text)`)
			require.NoError(t, err)
			c := connect(t, ctx, url)
			cfg := db(true, fsm("public.auto_fsm"), delKey("public.auto_key", "email"), fsm("public.auto_nokey_fsm"), tbl("public.d_pk"))
			rep, err := EnsureReplicaIdentity(ctx, c.PgConn(), cfg)
			require.NoError(t, err)
			require.Equal(t, []string{"public.auto_fsm", "public.auto_key", "public.auto_nokey_fsm"}, rep.Altered)
			for _, tb := range []string{"auto_fsm", "auto_key", "auto_nokey_fsm"} {
				require.Equal(t, "f", ident(t, tb))
			}
			require.Equal(t, "d", ident(t, "d_pk"))
			rep, err = EnsureReplicaIdentity(ctx, c.PgConn(), cfg)
			require.NoError(t, err)
			require.Empty(t, rep.Altered)
		}},
		{name: "delete key inside identity is left alone", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			rep, err := EnsureReplicaIdentity(ctx, c.PgConn(), db(true, delKey("public.d_pk", "id")))
			require.NoError(t, err)
			require.Empty(t, rep.Altered)
		}},
		{name: "opt out fails and alters nothing", run: func(t *testing.T) {
			_, err := admin.Exec(ctx, `CREATE TABLE opt_fsm (id int primary key, status text)`)
			require.NoError(t, err)
			c := connect(t, ctx, url)
			_, err = EnsureReplicaIdentity(ctx, c.PgConn(), db(false, fsm("public.opt_fsm")))
			require.ErrorIs(t, err, ErrReplicaIdentity)
			require.ErrorContains(t, err, "auto_replica_identity")
			require.ErrorContains(t, err, `ALTER TABLE "public"."opt_fsm" REPLICA IDENTITY FULL`)
			require.Equal(t, "d", ident(t, "opt_fsm"))
		}},
		{name: "opt out passes when already full", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := EnsureReplicaIdentity(ctx, c.PgConn(), db(false, fsm("public.f_nokey")))
			require.NoError(t, err)
		}},
		{name: "missing table", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			_, err := EnsureReplicaIdentity(ctx, c.PgConn(), db(true, tbl("public.nope")))
			require.ErrorContains(t, err, "public.nope")
		}},
		{name: "all tables warns about keyless tables", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			cfg := db(true)
			cfg.Postgres.Publication = "p_ident_all"
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			rep, err := EnsureReplicaIdentity(ctx, c.PgConn(), cfg)
			require.NoError(t, err)
			require.Empty(t, rep.Altered)
			joined := ""
			for _, w := range rep.Warnings {
				joined += w + "\n"
			}
			require.Contains(t, joined, "public.d_nokey")
			require.Contains(t, joined, "public.n_pk")
			require.NotContains(t, joined, "public.d_pk")
		}},
		{name: "no rights to alter", run: func(t *testing.T) {
			cfg, err := pgx.ParseConfig(url)
			require.NoError(t, err)
			_, err = admin.Exec(ctx, `CREATE TABLE perm_fsm (id int primary key, status text);
				DROP ROLE IF EXISTS lim_ri; CREATE ROLE lim_ri LOGIN REPLICATION PASSWORD 'lim'`)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _ = admin.Exec(ctx, `REVOKE ALL ON ALL TABLES IN SCHEMA public FROM lim_ri;
					REVOKE ALL ON SCHEMA public FROM lim_ri;
					REVOKE ALL ON DATABASE `+pgx.Identifier{cfg.Database}.Sanitize()+` FROM lim_ri; DROP ROLE IF EXISTS lim_ri`)
			})
			_, err = admin.Exec(ctx, `GRANT USAGE ON SCHEMA public TO lim_ri; GRANT SELECT ON ALL TABLES IN SCHEMA public TO lim_ri`)
			require.NoError(t, err)
			c := connect(t, ctx, withUser(t, url, "lim_ri", "lim"))
			_, err = EnsureReplicaIdentity(ctx, c.PgConn(), db(true, fsm("public.perm_fsm")))
			require.ErrorContains(t, err, "grant ownership")
			require.ErrorContains(t, err, "auto_replica_identity: false")
			require.Equal(t, "d", ident(t, "perm_fsm"))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
