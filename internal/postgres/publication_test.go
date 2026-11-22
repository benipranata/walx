package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

func tbl(name string, ops ...event.Op) config.TableConfig {
	return config.TableConfig{Name: name, Ops: ops}
}

func TestPublishOps(t *testing.T) {
	ins, upd, del, trunc := event.OpInsert, event.OpUpdate, event.OpDelete, event.OpTruncate
	tests := []struct {
		name   string
		tables []config.TableConfig
		want   []event.Op
	}{
		{name: "no tables", want: []event.Op{ins, upd}},
		{name: "nil ops default", tables: []config.TableConfig{tbl("a.b")}, want: []event.Op{ins, upd}},
		{name: "union is canonical order", tables: []config.TableConfig{
			tbl("a.b", trunc, ins), tbl("a.c", del, upd)}, want: []event.Op{ins, upd, del, trunc}},
		{name: "only delete", tables: []config.TableConfig{tbl("a.b", del)}, want: []event.Op{del}},
		{name: "snapshot ignored", tables: []config.TableConfig{tbl("a.b", event.OpSnapshot, ins)}, want: []event.Op{ins}},
		{name: "duplicates", tables: []config.TableConfig{tbl("a.b", ins, ins), tbl("a.c", ins)}, want: []event.Op{ins}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, PublishOps(tt.tables))
		})
	}
}

func TestNewPubSpec(t *testing.T) {
	tests := []struct {
		name string
		db   config.DatabaseConfig
		want pubSpec
	}{
		{
			name: "all tables",
			db:   config.DatabaseConfig{Postgres: config.PostgresConfig{Publication: "p"}},
			want: pubSpec{Name: "p", AllTables: true, Ops: []event.Op{event.OpInsert, event.OpUpdate}},
		},
		{
			name: "listed and deduplicated",
			db: config.DatabaseConfig{
				Postgres: config.PostgresConfig{Publication: "p"},
				Tables:   []config.TableConfig{tbl("public.b"), tbl("public.a", event.OpDelete), tbl("public.b")},
			},
			want: pubSpec{Name: "p", Tables: []string{"public.a", "public.b"}, Ops: []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete}},
		},
		{
			name: "filters keyed by table with sorted columns",
			db: config.DatabaseConfig{
				Postgres: config.PostgresConfig{Publication: "p"},
				Tables: []config.TableConfig{
					{Name: "public.a", PublicationWhere: "  region = 'eu' ", PublicationColumns: []string{"region", "id"}},
					tbl("public.b"),
				},
			},
			want: pubSpec{
				Name: "p", Tables: []string{"public.a", "public.b"}, Ops: []event.Op{event.OpInsert, event.OpUpdate},
				Filters: map[string]tableFilter{"public.a": {Where: "region = 'eu'", Columns: []string{"id", "region"}}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, newPubSpec(tt.db))
		})
	}
}

func TestPubSpecCreateSQL(t *testing.T) {
	ops := []event.Op{event.OpInsert, event.OpUpdate}
	tests := []struct {
		name string
		spec pubSpec
		want string
	}{
		{
			name: "all tables",
			spec: pubSpec{Name: "walx_a", AllTables: true, Ops: ops},
			want: `CREATE PUBLICATION "walx_a" FOR ALL TABLES WITH (publish = 'insert, update', publish_via_partition_root = true)`,
		},
		{
			name: "tables quoted",
			spec: pubSpec{Name: "Pub", Tables: []string{"public.orders", `my"s.T`}, Ops: []event.Op{event.OpDelete, event.OpTruncate}},
			want: `CREATE PUBLICATION "Pub" FOR TABLE "public"."orders", "my""s"."T" WITH (publish = 'delete, truncate', publish_via_partition_root = true)`,
		},
		{
			name: "row filter and column list in order",
			spec: pubSpec{
				Name: "p", Tables: []string{"public.a", "public.b", "public.c"}, Ops: ops,
				Filters: map[string]tableFilter{
					"public.a": {Where: "region = 'eu' AND id > 0", Columns: []string{"Id", "region"}},
					"public.c": {Where: "x"},
				},
			},
			want: `CREATE PUBLICATION "p" FOR TABLE "public"."a" ("Id", "region") WHERE (region = 'eu' AND id > 0), "public"."b", "public"."c" WHERE (x) WITH (publish = 'insert, update', publish_via_partition_root = true)`,
		},
		{
			name: "column list only, quote in a column",
			spec: pubSpec{Name: "p", Tables: []string{"public.a"}, Ops: ops, Filters: map[string]tableFilter{"public.a": {Columns: []string{`a"b`}}}},
			want: `CREATE PUBLICATION "p" FOR TABLE "public"."a" ("a""b") WITH (publish = 'insert, update', publish_via_partition_root = true)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.spec.createSQL())
		})
	}
}

func TestQuoteTables(t *testing.T) {
	tests := []struct {
		name   string
		tables []string
		want   string
	}{
		{name: "schema and bare", tables: []string{"public.a", "bare"}, want: `"public"."a", "bare"`},
		{name: "quote in a name", tables: []string{`s."t`}, want: `"s"."""t"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, quoteTables(tt.tables))
		})
	}
}

func TestPubQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "publication quotes the name", query: pubQuery("p'x"), want: "FROM pg_publication WHERE pubname = 'p''x'"},
		{name: "tables", query: pubTablesQuery("p"), want: "FROM pg_publication_tables WHERE pubname = 'p'"},
		{name: "filtered quotes the name", query: pubFilteredQuery("p'x"), want: "p.pubname = 'p''x'"},
		{name: "filtered looks at filters and column lists", query: pubFilteredQuery("p"), want: "prqual IS NOT NULL OR r.prattrs IS NOT NULL"},
		{name: "filtered reads the deparsed filter", query: pubFilteredQuery("p"), want: "pg_get_expr(r.prqual, r.prrelid)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Contains(t, tt.query, tt.want)
		})
	}
}

func TestParsePub(t *testing.T) {
	tests := []struct {
		name    string
		row     [][]byte
		want    pubState
		wantErr bool
	}{
		{
			name: "default",
			row:  slotRow("f", "t", "t", "f", "f", "t"),
			want: pubState{Insert: true, Update: true, ViaRoot: true},
		},
		{
			name: "all tables everything",
			row:  slotRow("t", "t", "t", "t", "t", "f"),
			want: pubState{AllTables: true, Insert: true, Update: true, Delete: true, Truncate: true},
		},
		{name: "short", row: slotRow("t"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePub(tt.row)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestPubStateOps(t *testing.T) {
	tests := []struct {
		name  string
		state pubState
		want  []event.Op
	}{
		{name: "update and truncate", state: pubState{Update: true, Truncate: true}, want: []event.Op{event.OpUpdate, event.OpTruncate}},
		{name: "all in Postgres order", state: pubState{Insert: true, Update: true, Delete: true, Truncate: true},
			want: []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete, event.OpTruncate}},
		{name: "none", state: pubState{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.state.ops())
		})
	}
}

func TestPubSpecDrift(t *testing.T) {
	ins, upd, del := event.OpInsert, event.OpUpdate, event.OpDelete
	list := pubSpec{Name: "p", Tables: []string{"public.a", "public.b"}, Ops: []event.Op{ins, upd}}
	all := pubSpec{Name: "p", AllTables: true, Ops: []event.Op{ins, upd}}
	okState := pubState{Insert: true, Update: true, ViaRoot: true, Tables: []string{"public.a", "public.b"}}
	filtered := pubSpec{Name: "p", Tables: list.Tables, Ops: list.Ops,
		Filters: map[string]tableFilter{"public.a": {Where: "region = 'eu'", Columns: []string{"id", "region"}}}}
	filteredState := okState
	filteredState.Filters = map[string]tableFilter{"public.a": {Where: "(region = 'eu')", Columns: []string{"id", "region"}}}

	tests := []struct {
		name         string
		spec         pubSpec
		got          pubState
		wantProblems []string
		wantFixes    []string
		wantFixable  bool
	}{
		{name: "in sync", spec: list, got: okState, wantFixable: true},
		{name: "all tables in sync", spec: all, got: pubState{AllTables: true, Insert: true, Update: true, ViaRoot: true}, wantFixable: true},
		{
			name: "missing delete op", spec: pubSpec{Name: "p", Tables: list.Tables, Ops: []event.Op{ins, upd, del}}, got: okState,
			wantProblems: []string{"publishes insert, update, want insert, update, delete"},
			wantFixes:    []string{`ALTER PUBLICATION "p" SET (publish = 'insert, update, delete', publish_via_partition_root = true)`},
			wantFixable:  true,
		},
		{
			name: "extra op", spec: list, got: pubState{Insert: true, Update: true, Delete: true, ViaRoot: true, Tables: list.Tables},
			wantProblems: []string{"publishes insert, update, delete, want insert, update"},
			wantFixes:    []string{`ALTER PUBLICATION "p" SET (publish = 'insert, update', publish_via_partition_root = true)`},
			wantFixable:  true,
		},
		{
			name: "not via root", spec: list, got: pubState{Insert: true, Update: true, Tables: list.Tables},
			wantProblems: []string{"publish_via_partition_root is off"},
			wantFixes:    []string{`ALTER PUBLICATION "p" SET (publish = 'insert, update', publish_via_partition_root = true)`},
			wantFixable:  true,
		},
		{
			name: "table set differs", spec: list, got: pubState{Insert: true, Update: true, ViaRoot: true, Tables: []string{"public.a", "public.c"}},
			wantProblems: []string{"tables are public.a, public.c, want public.a, public.b"},
			wantFixes:    []string{`ALTER PUBLICATION "p" SET TABLE "public"."a", "public"."b"`},
			wantFixable:  true,
		},
		{
			name: "hand-added row filter and column list", spec: list,
			got: pubState{Insert: true, Update: true, ViaRoot: true, Tables: list.Tables,
				Filters: map[string]tableFilter{"public.a": {Where: "(id > 0)", Columns: []string{"id"}}}},
			wantProblems: []string{`row filter on public.a is "(id > 0)", want none`, "column list on public.a is (id), want all columns"},
			wantFixes:    []string{`ALTER PUBLICATION "p" SET TABLE "public"."a", "public"."b"`},
			wantFixable:  true,
		},
		{
			name: "table set and row filter need one SET TABLE", spec: list,
			got: pubState{Insert: true, Update: true, ViaRoot: true, Tables: []string{"public.a", "public.c"},
				Filters: map[string]tableFilter{"public.c": {Where: "(x)"}}},
			wantProblems: []string{"tables are public.a, public.c, want public.a, public.b", `row filter on public.c is "(x)", want none`},
			wantFixes:    []string{`ALTER PUBLICATION "p" SET TABLE "public"."a", "public"."b"`},
			wantFixable:  true,
		},
		{name: "configured filter in sync", spec: filtered, got: filteredState, wantFixable: true},
		{
			name: "configured filter matches modulo deparse", spec: filtered,
			got: func() pubState {
				g := filteredState
				g.Filters = map[string]tableFilter{"public.a": {Where: "((region = 'eu'))", Columns: []string{"id", "region"}}}
				return g
			}(),
			wantFixable: true,
		},
		{
			name: "configured filter missing", spec: filtered,
			got:          okState,
			wantProblems: []string{`row filter on public.a is none, want "region = 'eu'"`, "column list on public.a is all columns, want (id, region)"},
			wantFixes:    []string{`ALTER PUBLICATION "p" SET TABLE "public"."a" ("id", "region") WHERE (region = 'eu'), "public"."b"`},
			wantFixable:  true,
		},
		{
			name: "configured filter differs", spec: filtered,
			got: func() pubState {
				g := filteredState
				g.Filters = map[string]tableFilter{"public.a": {Where: "(region = 'us')", Columns: []string{"id"}}}
				return g
			}(),
			wantProblems: []string{`row filter on public.a is "(region = 'us')", want "region = 'eu'"`, "column list on public.a is (id), want (id, region)"},
			wantFixes:    []string{`ALTER PUBLICATION "p" SET TABLE "public"."a" ("id", "region") WHERE (region = 'eu'), "public"."b"`},
			wantFixable:  true,
		},
		{
			name: "list wanted but all tables published", spec: list, got: pubState{AllTables: true, Insert: true, Update: true, ViaRoot: true},
			wantProblems: []string{"is FOR ALL TABLES, want a table list"},
		},
		{
			name: "all tables wanted but list published", spec: all, got: pubState{Insert: true, Update: true, ViaRoot: true, Tables: list.Tables},
			wantProblems: []string{"is not FOR ALL TABLES, want FOR ALL TABLES"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems, fixes, fixable := tt.spec.drift(tt.got)
			require.Equal(t, tt.wantProblems, problems)
			require.Equal(t, tt.wantFixes, fixes)
			require.Equal(t, tt.wantFixable, fixable)
		})
	}
}

func TestCreateError(t *testing.T) {
	denied := func() error { return &pgconn.PgError{Code: codeInsufficientPrivilege} }
	tests := []struct {
		name    string
		spec    pubSpec
		err     error
		wantMsg string
	}{
		{name: "all tables denied", spec: pubSpec{Name: "p", AllTables: true}, err: denied(), wantMsg: "list tables explicitly"},
		{name: "table list denied", spec: pubSpec{Name: "p", Tables: []string{"a.b"}}, err: denied(), wantMsg: "own"},
		{name: "other", spec: pubSpec{Name: "p"}, err: &pgconn.PgError{Code: "XX000"}, wantMsg: "create publication"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.spec.createError(tt.err)
			require.ErrorIs(t, err, tt.err)
			require.ErrorContains(t, err, tt.wantMsg)
		})
	}
}
