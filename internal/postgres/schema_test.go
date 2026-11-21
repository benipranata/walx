package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func txRelationCols(cols ...string) *pglogrepl.RelationMessage {
	rel := &pglogrepl.RelationMessage{RelationID: 1, Namespace: "public", RelationName: "users"}
	for _, c := range cols {
		rel.Columns = append(rel.Columns, &pglogrepl.RelationMessageColumn{Name: c, DataType: oidText})
	}
	return rel
}

func txRow(vals ...string) *pglogrepl.InsertMessage {
	t := &pglogrepl.TupleData{}
	for _, v := range vals {
		t.Columns = append(t.Columns, &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(v)})
	}
	return &pglogrepl.InsertMessage{RelationID: 1, Tuple: t}
}

func TestTxBufferSchemaChange(t *testing.T) {
	tests := []struct {
		name string
		msgs []pglogrepl.Message
		want []map[string]any
	}{
		{"column added between transactions", []pglogrepl.Message{
			txRelationCols("id"), txBegin(1), txRow("1"), txCommit(),
			txRelationCols("id", "email"), txBegin(2), txRow("2", "a"), txCommit(),
		}, []map[string]any{{"id": "1"}, {"id": "2", "email": "a"}}},
		{"relation re-sent inside a transaction", []pglogrepl.Message{
			txBegin(1), txRelationCols("id"), txRow("1"),
			txRelationCols("id", "email"), txRow("2", "a"), txCommit(),
		}, []map[string]any{{"id": "1"}, {"id": "2", "email": "a"}}},
		{"column dropped", []pglogrepl.Message{
			txRelationCols("id", "email"), txBegin(1), txRow("1", "a"), txCommit(),
			txRelationCols("id"), txBegin(2), txRow("2"), txCommit(),
		}, []map[string]any{{"id": "1", "email": "a"}, {"id": "2"}}},
		{"column renamed", []pglogrepl.Message{
			txRelationCols("id", "email"), txBegin(1), txRow("1", "a"), txCommit(),
			txRelationCols("id", "mail"), txBegin(2), txRow("2", "b"), txCommit(),
		}, []map[string]any{{"id": "1", "email": "a"}, {"id": "2", "mail": "b"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewTxBuffer("main", NewRelations(), nil)
			var got []map[string]any
			for _, batch := range push(t, b, tt.msgs...) {
				for _, e := range batch.Events {
					got = append(got, e.After)
				}
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTxBufferSchemaChangeIntegration(t *testing.T) {
	url := startPostgres(t)
	const setup = `CREATE TABLE person (id int PRIMARY KEY, email text)`
	const dml = `INSERT INTO person VALUES (1, 'a'); COMMIT;
		BEGIN; ALTER TABLE person ADD COLUMN age int; INSERT INTO person VALUES (2, 'b', 30); COMMIT;
		BEGIN; UPDATE person SET age = 5 WHERE id = 1; COMMIT;
		BEGIN; ALTER TABLE person DROP COLUMN age; ALTER TABLE person RENAME COLUMN email TO mail;
		INSERT INTO person VALUES (3, 'c'); COMMIT`
	var msgs []pglogrepl.Message
	for _, r := range recordFixture(t, isolatedDatabase(t, url), setup, dml) {
		m, err := pglogrepl.Parse(r)
		require.NoError(t, err)
		msgs = append(msgs, m)
	}
	batches := push(t, NewTxBuffer("main", NewRelations(), nil), msgs...)
	require.Len(t, batches, 4)

	tests := []struct {
		name  string
		batch int
		op    event.Op
		want  map[string]any
	}{
		{"before the change", 0, event.OpInsert, map[string]any{"id": int64(1), "email": "a"}},
		{"insert after add column", 1, event.OpInsert, map[string]any{"id": int64(2), "email": "b", "age": int64(30)}},
		{"update of a row from before the change", 2, event.OpUpdate, map[string]any{"id": int64(1), "email": "a", "age": int64(5)}},
		{"insert after drop and rename", 3, event.OpInsert, map[string]any{"id": int64(3), "mail": "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Len(t, batches[tt.batch].Events, 1)
			e := batches[tt.batch].Events[0]
			require.Equal(t, tt.op, e.Op)
			require.Equal(t, tt.want, e.After)
		})
	}
}
