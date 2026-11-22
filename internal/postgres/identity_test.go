package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

func identityRel(name string, ident byte, keyCols ...string) *pglogrepl.RelationMessage {
	rel := &pglogrepl.RelationMessage{RelationID: 1, Namespace: "public", RelationName: name, ReplicaIdentity: ident}
	for _, c := range []string{"id", "email", "status"} {
		var flags uint8
		for _, k := range keyCols {
			if k == c {
				flags = 1
			}
		}
		rel.Columns = append(rel.Columns, &pglogrepl.RelationMessageColumn{Name: c, Flags: flags})
	}
	return rel
}

func TestIdentityPolicyCheckRelation(t *testing.T) {
	ins, upd, del := event.OpInsert, event.OpUpdate, event.OpDelete
	tests := []struct {
		name     string
		tables   []config.TableConfig
		rel      *pglogrepl.RelationMessage
		wantErr  string
		wantWarn string
	}{
		{name: "default with key", tables: []config.TableConfig{tbl("public.t")}, rel: identityRel("t", 'd', "id")},
		{name: "default without key fails", tables: []config.TableConfig{tbl("public.t")}, rel: identityRel("t", 'd'),
			wantErr: "UPDATE and DELETE fail"},
		{name: "nothing fails", tables: []config.TableConfig{tbl("public.t")}, rel: identityRel("t", 'n'),
			wantErr: "UPDATE and DELETE fail"},
		{name: "insert only needs no key", tables: []config.TableConfig{tbl("public.t", ins)}, rel: identityRel("t", 'n')},
		{name: "index with columns", tables: []config.TableConfig{tbl("public.t")}, rel: identityRel("t", 'i', "email")},
		{name: "full", tables: []config.TableConfig{tbl("public.t")}, rel: identityRel("t", 'f')},
		{name: "fsm needs full", tables: []config.TableConfig{{Name: "public.t", FSMColumn: "status"}},
			rel: identityRel("t", 'd', "id"), wantErr: "fsm_column"},
		{name: "fsm with full", tables: []config.TableConfig{{Name: "public.t", FSMColumn: "status"}},
			rel: identityRel("t", 'f')},
		{name: "on_change needs full", tables: []config.TableConfig{{Name: "public.t", OnChange: []string{"email"}}},
			rel: identityRel("t", 'd', "id"), wantErr: "on_change"},
		{name: "on_change with full", tables: []config.TableConfig{{Name: "public.t", OnChange: []string{"email"}}},
			rel: identityRel("t", 'f')},
		{name: "where on deletes needs full", tables: []config.TableConfig{
			{Name: "public.t", Ops: []event.Op{del}, Where: map[string]any{"status": "x"}}},
			rel: identityRel("t", 'd', "id"), wantErr: "where"},
		{name: "where without deletes is fine", tables: []config.TableConfig{
			{Name: "public.t", Ops: []event.Op{ins, upd}, Where: map[string]any{"status": "x"}}},
			rel: identityRel("t", 'd', "id")},
		{name: "key outside identity on delete table", tables: []config.TableConfig{
			{Name: "public.t", Ops: []event.Op{del}, Key: []string{"email"}}},
			rel: identityRel("t", 'd', "id"), wantErr: "outside the replica identity"},
		{name: "key inside identity", tables: []config.TableConfig{
			{Name: "public.t", Ops: []event.Op{del}, Key: []string{"id"}}},
			rel: identityRel("t", 'd', "id")},
		{name: "key outside identity without deletes", tables: []config.TableConfig{
			{Name: "public.t", Ops: []event.Op{ins, upd}, Key: []string{"email"}}},
			rel: identityRel("t", 'd', "id")},
		{name: "unlisted table under all tables only warns", rel: identityRel("t", 'n'),
			wantWarn: "UPDATE and DELETE fail"},
		{name: "unlisted table with key", rel: identityRel("t", 'd', "id")},
		{name: "table outside the list is ignored", tables: []config.TableConfig{tbl("public.other")},
			rel: identityRel("t", 'n')},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewIdentityPolicy(config.DatabaseConfig{Tables: tt.tables})
			warn, err := p.CheckRelation(tt.rel)
			if tt.wantErr != "" {
				require.ErrorIs(t, err, ErrReplicaIdentity)
				require.ErrorContains(t, err, tt.wantErr)
				require.ErrorContains(t, err, "public.t")
				return
			}
			require.NoError(t, err)
			if tt.wantWarn == "" {
				require.Empty(t, warn)
			} else {
				require.Contains(t, warn, tt.wantWarn)
			}
		})
	}
}

func TestTxBufferIdentityCheck(t *testing.T) {
	tests := []struct {
		name    string
		rel     *pglogrepl.RelationMessage
		wantErr bool
		warns   int
	}{
		{name: "ok", rel: identityRel("users", 'd', "id")},
		{name: "bad listed table", rel: identityRel("users", 'n'), wantErr: true},
		{name: "warn for unlisted", rel: identityRel("other", 'n'), warns: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var warns []string
			p := NewIdentityPolicy(config.DatabaseConfig{Tables: []config.TableConfig{tbl("public.users"), tbl("public.audit")}})
			if tt.warns > 0 {
				p = NewIdentityPolicy(config.DatabaseConfig{})
			}
			b := NewTxBuffer("db", NewRelations(), nil, WithIdentityCheck(p, func(s string) { warns = append(warns, s) }))
			_, err := b.Push(tt.rel, 1)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrReplicaIdentity)
				return
			}
			require.NoError(t, err)
			require.Len(t, warns, tt.warns)
		})
	}
}

func TestAlterFullSQL(t *testing.T) {
	tests := []struct {
		name   string
		tables []string
		want   string
	}{
		{name: "one", tables: []string{"public.a"},
			want: `SET LOCAL lock_timeout = '10s'; ALTER TABLE "public"."a" REPLICA IDENTITY FULL`},
		{name: "two", tables: []string{"public.a", "public.b"},
			want: `SET LOCAL lock_timeout = '10s'; ALTER TABLE "public"."a" REPLICA IDENTITY FULL; ALTER TABLE "public"."b" REPLICA IDENTITY FULL`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, alterFullSQL(tt.tables...))
		})
	}
}
