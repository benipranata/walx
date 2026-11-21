package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestTxBufferLimits(t *testing.T) {
	update := &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple: txInsert("111").Tuple, NewTuple: txInsert("22").Tuple}
	tests := []struct {
		name   string
		limits TxLimits
		msgs   []pglogrepl.Message
		want   *TxTooLargeError
	}{
		{"unlimited", TxLimits{}, []pglogrepl.Message{txRelation(), txBegin(1), txInsert("1"), txInsert("2"), txCommit()}, nil},
		{"bytes at the cap", TxLimits{MaxBytes: 2}, []pglogrepl.Message{txRelation(), txBegin(1), txInsert("1"), txInsert("2"), txCommit()}, nil},
		{"events at the cap", TxLimits{MaxEvents: 2}, []pglogrepl.Message{txRelation(), txBegin(1), txInsert("1"), txInsert("2"), txCommit()}, nil},
		{"bytes over the cap", TxLimits{MaxBytes: 3}, []pglogrepl.Message{txRelation(), txBegin(1), txInsert("1"), txInsert("22"), txInsert("3")},
			&TxTooLargeError{Limit: LimitBytes, Max: 3, Observed: 4, Xid: 1, CommitLSN: 0x100}},
		{"bytes count old and new tuples", TxLimits{MaxBytes: 4}, []pglogrepl.Message{txRelation(), txBegin(1), update},
			&TxTooLargeError{Limit: LimitBytes, Max: 4, Observed: 5, Xid: 1, CommitLSN: 0x100}},
		{"events over the cap", TxLimits{MaxEvents: 2}, []pglogrepl.Message{txRelation(), txBegin(1), txInsert("1"), txInsert("2"), txInsert("3")},
			&TxTooLargeError{Limit: LimitEvents, Max: 2, Observed: 3, Xid: 1, CommitLSN: 0x100}},
		{"truncate counts one event per relation", TxLimits{MaxEvents: 1}, []pglogrepl.Message{txRelation(), txBegin(1),
			&pglogrepl.TruncateMessage{RelationNum: 2, RelationIDs: []uint32{1, 1}}},
			&TxTooLargeError{Limit: LimitEvents, Max: 1, Observed: 2, Xid: 1, CommitLSN: 0x100}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewTxBuffer("db", NewRelations(), nil, WithTxLimits(tt.limits))
			var err error
			for _, m := range tt.msgs {
				if _, err = b.Push(m, 0); err != nil {
					break
				}
			}
			if tt.want == nil {
				require.NoError(t, err)
				return
			}
			var got *TxTooLargeError
			require.True(t, errors.As(err, &got))
			require.Equal(t, tt.want, got)
			require.False(t, event.IsPermanent(err))
			require.ErrorContains(t, err, "max_tx_")

			// The open transaction was dropped, so the next one starts clean:
			// its counters restart and it fits the limits again.
			for _, m := range []pglogrepl.Message{txBegin(2), txInsert("1")} {
				_, err = b.Push(m, 0)
				require.NoError(t, err)
			}
			batch, err := b.Push(txCommit(), 0)
			require.NoError(t, err)
			require.Len(t, batch.Events, 1)
			require.Equal(t, "db-0/100-1", batch.Events[0].ID)
		})
	}
}
