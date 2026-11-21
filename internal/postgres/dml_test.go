package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func dmlRelations() *Relations {
	rels := NewRelations()
	rels.Set(&pglogrepl.RelationMessage{
		RelationID: 1, Namespace: "public", RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Flags: 1, Name: "id", DataType: oidInt4},
			{Name: "email", DataType: oidText},
		},
	})
	rels.Set(&pglogrepl.RelationMessage{
		RelationID: 2, Namespace: "public", RelationName: "logs",
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "msg", DataType: oidText}},
	})
	rels.Set(&pglogrepl.RelationMessage{
		RelationID: 3, Namespace: "public", RelationName: "full",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Flags: 1, Name: "id", DataType: oidInt4},
			{Flags: 1, Name: "email", DataType: oidText},
		},
	})
	return rels
}

func tup(cols ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{Columns: cols}
}

func txt(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(s)}
}

func null() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
}

func toast() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
}

func TestDecodeDML(t *testing.T) {
	row := func(id int64, email any) map[string]any { return map[string]any{"id": id, "email": email} }
	tests := []struct {
		name string
		keys map[string][]string
		msg  pglogrepl.Message
		want []event.Event
	}{
		{name: "insert", msg: &pglogrepl.InsertMessage{RelationID: 1, Tuple: tup(txt("1"), txt("a"))},
			want: []event.Event{{Op: event.OpInsert, Schema: "public", Table: "users",
				After: row(1, "a"), Key: map[string]any{"id": int64(1)}}}},
		{name: "update without old tuple, key unchanged",
			msg: &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tup(txt("1"), txt("b"))},
			want: []event.Event{{Op: event.OpUpdate, Schema: "public", Table: "users",
				After: row(1, "b"), Key: map[string]any{"id": int64(1)}}}},
		{name: "update with key change takes the new key and drops the K tuple",
			msg: &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'K', OldTuple: tup(txt("1"), null()),
				NewTuple: tup(txt("2"), txt("b"))},
			want: []event.Event{{Op: event.OpUpdate, Schema: "public", Table: "users",
				After: row(2, "b"), Key: map[string]any{"id": int64(2)}}}},
		{name: "update with replica identity full fills Before",
			msg: &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'O', OldTuple: tup(txt("1"), txt("a")),
				NewTuple: tup(txt("1"), txt("b"))},
			want: []event.Event{{Op: event.OpUpdate, Schema: "public", Table: "users",
				Before: row(1, "a"), After: row(1, "b"), Key: map[string]any{"id": int64(1)}}}},
		{name: "delete with key tuple",
			msg: &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: 'K', OldTuple: tup(txt("1"), null())},
			want: []event.Event{{Op: event.OpDelete, Schema: "public", Table: "users",
				Key: map[string]any{"id": int64(1)}}}},
		{name: "delete with replica identity full fills Before",
			msg: &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: 'O', OldTuple: tup(txt("1"), txt("a"))},
			want: []event.Event{{Op: event.OpDelete, Schema: "public", Table: "users",
				Before: row(1, "a"), Key: map[string]any{"id": int64(1)}}}},
		{name: "configured key columns replace the primary key",
			keys: map[string][]string{"public.users": {"email"}},
			msg:  &pglogrepl.InsertMessage{RelationID: 1, Tuple: tup(txt("1"), txt("a"))},
			want: []event.Event{{Op: event.OpInsert, Schema: "public", Table: "users",
				After: row(1, "a"), Key: map[string]any{"email": "a"}}}},
		{name: "configured key on delete reads the old tuple",
			keys: map[string][]string{"public.users": {"email"}},
			msg:  &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: 'O', OldTuple: tup(txt("1"), txt("a"))},
			want: []event.Event{{Op: event.OpDelete, Schema: "public", Table: "users",
				Before: row(1, "a"), Key: map[string]any{"email": "a"}}}},
		{name: "update with unchanged configured key column takes the key from Before",
			keys: map[string][]string{"public.users": {"email"}},
			msg: &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'O', OldTuple: tup(txt("1"), txt("a")),
				NewTuple: tup(txt("1"), toast())},
			want: []event.Event{{Op: event.OpUpdate, Schema: "public", Table: "users",
				Before: row(1, "a"), After: row(1, event.Unchanged), Key: map[string]any{"email": "a"}}}},
		{name: "replica identity full flags every column, so the default Key is the whole row",
			msg: &pglogrepl.InsertMessage{RelationID: 3, Tuple: tup(txt("1"), txt("a"))},
			want: []event.Event{{Op: event.OpInsert, Schema: "public", Table: "full",
				After: row(1, "a"), Key: row(1, "a")}}},
		{name: "table without a key has a nil Key",
			msg: &pglogrepl.InsertMessage{RelationID: 2, Tuple: tup(txt("x"))},
			want: []event.Event{{Op: event.OpInsert, Schema: "public", Table: "logs",
				After: map[string]any{"msg": "x"}}}},
		{name: "truncate gives one event per relation",
			msg: &pglogrepl.TruncateMessage{RelationNum: 2, Option: 3, RelationIDs: []uint32{1, 2}},
			want: []event.Event{
				{Op: event.OpTruncate, Schema: "public", Table: "users"},
				{Op: event.OpTruncate, Schema: "public", Table: "logs"}}},
		{name: "non-DML message yields nothing", msg: &pglogrepl.BeginMessage{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeDML(dmlRelations(), tt.keys, tt.msg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDecodeDMLErrors(t *testing.T) {
	tests := []struct {
		name string
		keys map[string][]string
		msg  pglogrepl.Message
		want string
	}{
		{name: "insert for an unknown relation", msg: &pglogrepl.InsertMessage{RelationID: 9, Tuple: tup(txt("1"))}, want: "no Relation message"},
		{name: "update for an unknown relation", msg: &pglogrepl.UpdateMessage{RelationID: 9, NewTuple: tup(txt("1"))}, want: "no Relation message"},
		{name: "delete for an unknown relation", msg: &pglogrepl.DeleteMessage{RelationID: 9, OldTuple: tup(txt("1"))}, want: "no Relation message"},
		{name: "truncate for an unknown relation", msg: &pglogrepl.TruncateMessage{RelationIDs: []uint32{9}}, want: "no Relation message"},
		{name: "bad tuple", msg: &pglogrepl.InsertMessage{RelationID: 1, Tuple: tup(txt("x"), txt("a"))}, want: `column "id"`},
		{name: "bad old tuple", msg: &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'O',
			OldTuple: tup(txt("x"), txt("a")), NewTuple: tup(txt("1"), txt("a"))}, want: `column "id"`},
		{name: "bad new tuple", msg: &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tup(txt("x"), txt("a"))}, want: `column "id"`},
		{name: "delete without old tuple", msg: &pglogrepl.DeleteMessage{RelationID: 1}, want: "no old tuple"},
		{name: "bad delete tuple", msg: &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: 'K', OldTuple: tup(txt("x"), null())}, want: `column "id"`},
		{name: "configured key column missing", keys: map[string][]string{"public.users": {"nope"}},
			msg: &pglogrepl.InsertMessage{RelationID: 1, Tuple: tup(txt("1"), txt("a"))}, want: `key column "nope"`},
		{name: "key column unchanged TOAST with no Before", keys: map[string][]string{"public.users": {"email"}},
			msg:  &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tup(txt("1"), toast())},
			want: "unchanged"},
		{name: "key column unchanged TOAST in Before too", keys: map[string][]string{"public.users": {"email"}},
			msg: &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'O', OldTuple: tup(txt("1"), toast()),
				NewTuple: tup(txt("1"), toast())},
			want: "unchanged"},
		{name: "delete key column outside the replica identity", keys: map[string][]string{"public.users": {"email"}},
			msg:  &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: 'K', OldTuple: tup(txt("1"), null())},
			want: "not in the replica identity"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeDML(dmlRelations(), tt.keys, tt.msg)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestDecodeDMLIntegration(t *testing.T) {
	url := startPostgres(t)
	const setup = `CREATE TABLE acct (id int PRIMARY KEY, email text)`
	const dml = `INSERT INTO acct VALUES (1, 'a');
		UPDATE acct SET email = 'b' WHERE id = 1;
		UPDATE acct SET id = 2 WHERE id = 1;
		DELETE FROM acct WHERE id = 2;
		TRUNCATE acct`
	row := func(id int64, email string) map[string]any { return map[string]any{"id": id, "email": email} }
	key := func(id int64) map[string]any { return map[string]any{"id": id} }
	tests := []struct {
		name  string
		setup string
		keys  map[string][]string
		want  []event.Event
	}{
		{name: "default identity leaves Before nil and Key is the primary key", setup: setup, want: []event.Event{
			{Op: event.OpInsert, Schema: "public", Table: "acct", After: row(1, "a"), Key: key(1)},
			{Op: event.OpUpdate, Schema: "public", Table: "acct", After: row(1, "b"), Key: key(1)},
			{Op: event.OpUpdate, Schema: "public", Table: "acct", After: row(2, "b"), Key: key(2)},
			{Op: event.OpDelete, Schema: "public", Table: "acct", Key: key(2)},
			{Op: event.OpTruncate, Schema: "public", Table: "acct"},
		}},
		{name: "replica identity full fills Before", setup: setup + `; ALTER TABLE acct REPLICA IDENTITY FULL`,
			keys: map[string][]string{"public.acct": {"id"}},
			want: []event.Event{
				{Op: event.OpInsert, Schema: "public", Table: "acct", After: row(1, "a"), Key: key(1)},
				{Op: event.OpUpdate, Schema: "public", Table: "acct", Before: row(1, "a"), After: row(1, "b"), Key: key(1)},
				{Op: event.OpUpdate, Schema: "public", Table: "acct", Before: row(1, "b"), After: row(2, "b"), Key: key(2)},
				{Op: event.OpDelete, Schema: "public", Table: "acct", Before: row(2, "b"), Key: key(2)},
				{Op: event.OpTruncate, Schema: "public", Table: "acct"},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := recordFixture(t, isolatedDatabase(t, url), tt.setup, dml)
			rels := NewRelations()
			var got []event.Event
			for _, b := range raw {
				m, err := pglogrepl.Parse(b)
				require.NoError(t, err)
				if r, ok := m.(*pglogrepl.RelationMessage); ok {
					rels.Set(r)
				}
				evs, err := DecodeDML(rels, tt.keys, m)
				require.NoError(t, err)
				got = append(got, evs...)
			}
			require.Equal(t, tt.want, got)
		})
	}
}
