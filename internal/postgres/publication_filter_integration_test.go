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

func TestPublicationFilterIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `CREATE TABLE f (id int primary key, region text, v text);
		CREATE TABLE g (id int primary key, v text)`)
	require.NoError(t, err)

	// live returns the row filter and column list of table in publication pub.
	live := func(t *testing.T, pub, table string) (rowfilter string, cols []string) {
		t.Helper()
		var rf *string
		var attnames []string
		require.NoError(t, admin.QueryRow(ctx, `SELECT rowfilter, attnames FROM pg_publication_tables
			WHERE pubname = $1 AND tablename = $2`, pub, table).Scan(&rf, &attnames))
		if rf != nil {
			rowfilter = *rf
		}
		return rowfilter, attnames
	}
	db := func(pub string, autoAlter bool, tbls ...config.TableConfig) config.DatabaseConfig {
		return config.DatabaseConfig{
			Postgres: config.PostgresConfig{Publication: pub, AutoCreate: true, AutoAlter: autoAlter},
			Tables:   tbls,
		}
	}
	filt := func(where string, cols ...string) config.TableConfig {
		return config.TableConfig{Name: "public.f", Ops: []event.Op{event.OpInsert}, PublicationWhere: where, PublicationColumns: cols}
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "create with filter is idempotent despite deparsing", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			cfg := db("pf_create", false, filt("region = 'eu'", "region", "id"), tbl("public.g"))
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			rf, cols := live(t, "pf_create", "f")
			require.Contains(t, rf, "region")
			require.Equal(t, []string{"id", "region"}, cols)
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			_, cols = live(t, "pf_create", "g")
			require.Equal(t, []string{"id", "v"}, cols)
			var probes int
			require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM pg_publication WHERE pubname LIKE 'walx_probe_%'`).Scan(&probes))
			require.Zero(t, probes)
		}},
		{name: "validate-only accepts a matching manual publication", run: func(t *testing.T) {
			_, err := admin.Exec(ctx, `CREATE PUBLICATION pf_manual FOR TABLE f (id, region) WHERE (region = 'eu')
				WITH (publish = 'insert', publish_via_partition_root = true)`)
			require.NoError(t, err)
			c := connect(t, ctx, url)
			cfg := db("pf_manual", false, filt("region='eu'", "id", "region"))
			cfg.Postgres.AutoCreate = false
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
		}},
		{name: "drift is reported and auto_alter reconciles", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), db("pf_drift", false, filt("region = 'eu'", "id", "region"))))

			cfg := db("pf_drift", false, filt("region = 'us'", "id"))
			err := EnsurePublication(ctx, c.PgConn(), cfg)
			require.ErrorIs(t, err, ErrPublicationDrift)
			require.ErrorContains(t, err, "row filter on public.f")
			require.ErrorContains(t, err, "column list on public.f is (id, region), want (id)")
			rf, _ := live(t, "pf_drift", "f")
			require.Contains(t, rf, "'eu'")

			cfg.Postgres.AutoAlter = true
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			rf, cols := live(t, "pf_drift", "f")
			require.Contains(t, rf, "'us'")
			require.Equal(t, []string{"id"}, cols)

			cfg.Postgres.AutoAlter = false
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))

			// Dropping the config filter is drift too, and auto_alter clears it.
			cfg = db("pf_drift", false, tbl("public.f", event.OpInsert))
			require.ErrorIs(t, EnsurePublication(ctx, c.PgConn(), cfg), ErrPublicationDrift)
			cfg.Postgres.AutoAlter = true
			require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))
			rf, _ = live(t, "pf_drift", "f")
			require.Empty(t, rf)
		}},
		{name: "invalid expression fails on create", run: func(t *testing.T) {
			c := connect(t, ctx, url)
			err := EnsurePublication(ctx, c.PgConn(), db("pf_bad", false, filt("nosuchcol = 1")))
			require.ErrorContains(t, err, "create publication")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestPublicationRowFilterRewritesOpsIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `CREATE TABLE r (id int primary key, region text)`)
	require.NoError(t, err)

	cfg := config.DatabaseConfig{
		Postgres: config.PostgresConfig{Publication: "pf_e2e", AutoCreate: true, AutoReplicaIdentity: true},
		Tables: []config.TableConfig{{
			Name: "public.r", Ops: []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete}, PublicationWhere: "region = 'eu'",
		}},
	}
	c := connect(t, ctx, url)
	rep, err := EnsureReplicaIdentity(ctx, c.PgConn(), cfg)
	require.NoError(t, err)
	require.Equal(t, []string{"public.r"}, rep.Altered)
	require.NoError(t, EnsurePublication(ctx, c.PgConn(), cfg))

	_, err = admin.Exec(ctx, `SELECT pg_create_logical_replication_slot('pf_e2e', 'pgoutput')`)
	require.NoError(t, err)
	// Each statement commits on its own, so each produces its own transaction.
	for _, q := range []string{
		`INSERT INTO r VALUES (1, 'eu'), (2, 'us')`,
		`UPDATE r SET region = 'us' WHERE id = 1`, // leaves the filter
		`UPDATE r SET region = 'eu' WHERE id = 2`, // enters it
		`UPDATE r SET region = 'eu', id = 2 WHERE id = 2`,
		`DELETE FROM r WHERE id = 2`,
	} {
		_, err = admin.Exec(ctx, q)
		require.NoError(t, err, q)
	}
	rows, err := admin.Query(ctx, `SELECT data FROM pg_logical_slot_get_binary_changes(
		'pf_e2e', NULL, NULL, 'proto_version', '1', 'publication_names', 'pf_e2e')`)
	require.NoError(t, err)
	msgs, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	require.NoError(t, err)

	var got []string
	for _, raw := range msgs {
		m, err := pglogrepl.Parse(raw)
		require.NoError(t, err)
		switch dml := m.(type) {
		case *pglogrepl.InsertMessage:
			got = append(got, "insert "+string(dml.Tuple.Columns[0].Data))
		case *pglogrepl.UpdateMessage:
			got = append(got, "update "+string(dml.NewTuple.Columns[0].Data))
		case *pglogrepl.DeleteMessage:
			got = append(got, "delete "+string(dml.OldTuple.Columns[0].Data)+" "+string(dml.OldTuple.Columns[1].Data))
		}
	}
	require.Equal(t, []string{
		"insert 1",    // id 2 is outside the filter and never arrives
		"delete 1 eu", // the update that left the filter, with the full old row
		"insert 2",    // the update that entered it
		"update 2",
		"delete 2 eu",
	}, got)
}
