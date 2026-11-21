package postgres

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

var txTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// txLocalTime is txTime in a non-UTC zone, as pglogrepl returns it.
var txLocalTime = txTime.In(time.FixedZone("x", 7*3600))

func txRelation() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{RelationID: 1, Namespace: "public", RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{{Flags: 1, Name: "id", DataType: oidInt4}}}
}

func txInsert(id string) *pglogrepl.InsertMessage {
	return &pglogrepl.InsertMessage{RelationID: 1, Tuple: &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{{DataType: pglogrepl.TupleDataTypeText, Data: []byte(id)}}}}
}

func txBegin(xid uint32) *pglogrepl.BeginMessage {
	return &pglogrepl.BeginMessage{FinalLSN: 0x100, CommitTime: txLocalTime, Xid: xid}
}

func txCommit() *pglogrepl.CommitMessage {
	return &pglogrepl.CommitMessage{CommitLSN: 0x100, TransactionEndLSN: 0x120, CommitTime: txTime}
}

// push feeds msgs to b and returns the batches it emitted.
func push(t *testing.T, b *TxBuffer, msgs ...pglogrepl.Message) []*event.Batch {
	t.Helper()
	var out []*event.Batch
	for i, m := range msgs {
		batch, err := b.Push(m, event.LSN(0x10+i))
		require.NoError(t, err)
		if batch != nil {
			out = append(out, batch)
		}
	}
	return out
}

func TestTxBufferPush(t *testing.T) {
	tests := []struct {
		name string
		msgs []pglogrepl.Message
		ids  [][]string
	}{
		{
			name: "multi statement transaction keeps wal order and numbers from 1",
			msgs: []pglogrepl.Message{txRelation(), txBegin(7), txInsert("1"), txInsert("2"),
				&pglogrepl.TruncateMessage{RelationNum: 2, RelationIDs: []uint32{1, 1}}, txCommit()},
			ids: [][]string{{"db-0/100-1", "db-0/100-2", "db-0/100-3", "db-0/100-4"}},
		},
		{
			name: "empty transaction still emits a batch",
			msgs: []pglogrepl.Message{txBegin(8), txCommit()},
			ids:  [][]string{{}},
		},
		{
			name: "origin type and message are ignored",
			msgs: []pglogrepl.Message{txBegin(8), &pglogrepl.OriginMessage{}, &pglogrepl.TypeMessage{},
				&pglogrepl.LogicalDecodingMessage{}, txCommit()},
			ids: [][]string{{}},
		},
		{
			name: "relation inside a transaction feeds the cache",
			msgs: []pglogrepl.Message{txBegin(9), txRelation(), txInsert("5"), txCommit()},
			ids:  [][]string{{"db-0/100-1"}},
		},
		{
			name: "two transactions give two batches",
			msgs: []pglogrepl.Message{txRelation(), txBegin(1), txInsert("1"), txCommit(), txBegin(2), txInsert("2"), txCommit()},
			ids:  [][]string{{"db-0/100-1"}, {"db-0/100-1"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewTxBuffer("db", NewRelations(), nil)
			got := push(t, b, tt.msgs...)
			require.Len(t, got, len(tt.ids))
			for i, batch := range got {
				var ids []string
				for _, e := range batch.Events {
					ids = append(ids, e.ID)
					require.Equal(t, "db", e.Source)
					require.Equal(t, txTime, e.CommitTS)
				}
				if len(tt.ids[i]) == 0 {
					require.Empty(t, ids)
				} else {
					require.Equal(t, tt.ids[i], ids)
				}
				require.Equal(t, event.LSN(0x100), batch.CommitLSN)
				require.Equal(t, event.LSN(0x120), batch.EndLSN)
			}
		})
	}
}

func TestTxBufferEventFields(t *testing.T) {
	tests := []struct {
		name string
		msgs []pglogrepl.Message
		want []*event.Event
	}{
		{
			name: "inserts carry transaction metadata",
			msgs: []pglogrepl.Message{txRelation(), txBegin(7), txInsert("1"), txInsert("2"), txCommit()},
			want: []*event.Event{
				{ID: "db-0/100-1", Source: "db", Op: event.OpInsert, Schema: "public", Table: "users",
					After: map[string]any{"id": int64(1)}, Key: map[string]any{"id": int64(1)}, LSN: 0x12, TxID: 7, CommitTS: txTime},
				{ID: "db-0/100-2", Source: "db", Op: event.OpInsert, Schema: "public", Table: "users",
					After: map[string]any{"id": int64(2)}, Key: map[string]any{"id": int64(2)}, LSN: 0x13, TxID: 7, CommitTS: txTime},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := push(t, NewTxBuffer("db", NewRelations(), nil), tt.msgs...)
			require.Len(t, got, 1)
			require.Equal(t, tt.want, got[0].Events)
		})
	}
}

func TestTxBufferKeysDoNotChangeIDs(t *testing.T) {
	msgs := func() []pglogrepl.Message {
		return []pglogrepl.Message{txRelation(), txBegin(1), txInsert("1"), txInsert("2"), txCommit()}
	}
	tests := []struct {
		name string
		keys map[string][]string
	}{
		{"no config", nil},
		{"other key config", map[string][]string{"public.users": {"id"}}},
	}
	var first []string
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := push(t, NewTxBuffer("db", NewRelations(), tt.keys), msgs()...)
			var ids []string
			for _, e := range got[0].Events {
				ids = append(ids, e.ID)
			}
			if first == nil {
				first = ids
			}
			require.Equal(t, first, ids)
		})
	}
}

func TestTxBufferResetsAfterError(t *testing.T) {
	tests := []struct {
		name string
		bad  []pglogrepl.Message
	}{
		{"decode failure", []pglogrepl.Message{txBegin(1), txInsert("1")}},
		{"commit lsn mismatch", []pglogrepl.Message{txBegin(1), &pglogrepl.CommitMessage{CommitLSN: 0x101}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewTxBuffer("db", NewRelations(), nil)
			for _, m := range tt.bad {
				_, _ = b.Push(m, 0)
			}
			got := push(t, b, txRelation(), txBegin(2), txInsert("9"), txCommit())
			require.Len(t, got, 1)
			require.Len(t, got[0].Events, 1)
			require.Equal(t, "db-0/100-1", got[0].Events[0].ID)
		})
	}
}

func TestTxBufferErrors(t *testing.T) {
	tests := []struct {
		name string
		msgs []pglogrepl.Message
		want string
	}{
		{"dml outside a transaction", []pglogrepl.Message{txRelation(), txInsert("1")}, "outside a transaction"},
		{"commit without begin", []pglogrepl.Message{txCommit()}, "commit without begin"},
		{"begin inside a transaction", []pglogrepl.Message{txBegin(1), txBegin(2)}, "begin inside"},
		{"commit lsn differs from begin", []pglogrepl.Message{txBegin(1),
			&pglogrepl.CommitMessage{CommitLSN: 0x101, TransactionEndLSN: 0x120}}, "does not match"},
		{"decode failure", []pglogrepl.Message{txBegin(1), txInsert("1")}, "no Relation message"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewTxBuffer("db", NewRelations(), nil)
			var err error
			for _, m := range tt.msgs {
				if _, err = b.Push(m, 0); err != nil {
					break
				}
			}
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestTxBufferReplayIntegration(t *testing.T) {
	tests := []struct {
		name string
		keys map[string][]string
	}{
		{"same ids when key config changes", map[string][]string{"public.acct": {"id"}}},
	}
	url := startPostgres(t)
	const setup = `CREATE TABLE acct (id int PRIMARY KEY, email text)`
	const dml = `INSERT INTO acct VALUES (1, 'a'), (2, 'b'); COMMIT;
		BEGIN; UPDATE acct SET email = 'c' WHERE id = 1; DELETE FROM acct WHERE id = 2; TRUNCATE acct; COMMIT;
		BEGIN; INSERT INTO acct VALUES (3, 'd'); COMMIT`
	raw := recordFixture(t, isolatedDatabase(t, url), setup, dml)
	replay := func(keys map[string][]string) []*event.Batch {
		b := NewTxBuffer("main", NewRelations(), keys)
		var out []*event.Batch
		for _, r := range raw {
			m, err := pglogrepl.Parse(r)
			require.NoError(t, err)
			batch, err := b.Push(m, 0)
			require.NoError(t, err)
			if batch != nil {
				out = append(out, batch)
			}
		}
		return out
	}
	first := replay(nil)
	require.Len(t, first, 3)
	ids := func(bs []*event.Batch) [][]string {
		var out [][]string
		for _, b := range bs {
			var s []string
			for _, e := range b.Events {
				s = append(s, e.ID)
			}
			out = append(out, s)
		}
		return out
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, ids(first), ids(replay(tt.keys)))
		})
	}
	require.Len(t, first[0].Events, 2)
	require.Len(t, first[1].Events, 3)
	require.Equal(t, event.OpTruncate, first[1].Events[2].Op)
	require.Equal(t, fmt.Sprintf("main-%s-3", first[1].CommitLSN), first[1].Events[2].ID)
	require.Less(t, first[0].CommitLSN, first[1].CommitLSN)
}

func TestResetClearsState(t *testing.T) {
	tests := []struct {
		name string
	}{{"open transaction and relations"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rels := NewRelations()
			b := NewTxBuffer("db", rels, nil, WithTxLimits(TxLimits{MaxEvents: 1}))
			push(t, b, txRelation(), txBegin(1), txInsert("1"))
			_, ok := rels.Get(1)
			require.True(t, ok)

			b.Reset()
			rels.Reset()
			_, ok = rels.Get(1)
			require.False(t, ok)

			_, err := b.Push(txBegin(2), 0)
			require.NoError(t, err)

			// The caps survive a reset.
			b.Reset()
			rels.Set(txRelation())
			_, err = b.Push(txBegin(3), 0)
			require.NoError(t, err)
			_, err = b.Push(txInsert("1"), 0)
			require.NoError(t, err)
			_, err = b.Push(txInsert("2"), 0)
			var tooLarge *TxTooLargeError
			require.ErrorAs(t, err, &tooLarge)
		})
	}
}
