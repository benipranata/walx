package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func slotRow(vals ...string) [][]byte {
	row := make([][]byte, len(vals))
	for i, v := range vals {
		if v != "\x00" {
			row[i] = []byte(v)
		}
	}
	return row
}

func TestSlotQuery(t *testing.T) {
	q := slotQuery("walx_a")
	require.Contains(t, q, "FROM pg_replication_slots WHERE slot_name = 'walx_a'")
	require.Contains(t, q, "invalidation_reason")
}

func TestParseSlot(t *testing.T) {
	tests := []struct {
		name    string
		row     [][]byte
		want    slotState
		wantErr bool
	}{
		{
			name: "healthy",
			row:  slotRow("logical", "pgoutput", "walx", "walx", "f", "\x00", "reserved", "\x00", "0/16B3748"),
			want: slotState{Type: "logical", Plugin: "pgoutput", Database: "walx", CurrentDB: "walx",
				WALStatus: "reserved", ConfirmedFlush: 0x16B3748},
		},
		{
			name: "active lost with reason",
			row:  slotRow("logical", "pgoutput", "walx", "walx", "t", "42", "lost", "wal_removed", "\x00"),
			want: slotState{Type: "logical", Plugin: "pgoutput", Database: "walx", CurrentDB: "walx",
				Active: true, ActivePID: "42", WALStatus: "lost", InvalidationReason: "wal_removed"},
		},
		{name: "short row", row: slotRow("logical"), wantErr: true},
		{name: "bad lsn", row: slotRow("logical", "pgoutput", "d", "d", "f", "\x00", "reserved", "\x00", "zzz"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSlot(tt.row)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSlotStateCheck(t *testing.T) {
	ok := slotState{Type: "logical", Plugin: "pgoutput", Database: "walx", CurrentDB: "walx", WALStatus: "reserved"}
	with := func(f func(*slotState)) slotState { s := ok; f(&s); return s }
	tests := []struct {
		name    string
		state   slotState
		wantErr error
		wantMsg string
	}{
		{name: "ok", state: ok},
		{name: "extended is fine", state: with(func(s *slotState) { s.WALStatus = "extended" })},
		{name: "null wal_status is fine", state: with(func(s *slotState) { s.WALStatus = "" })},
		{name: "lost", state: with(func(s *slotState) { s.WALStatus = "lost" }), wantErr: ErrSlotLost},
		{name: "invalidated", state: with(func(s *slotState) { s.InvalidationReason = "idle_timeout" }),
			wantErr: ErrSlotLost, wantMsg: "idle_timeout"},
		{name: "active elsewhere", state: with(func(s *slotState) { s.Active, s.ActivePID = true, "77" }),
			wantErr: ErrSlotActive, wantMsg: "77"},
		{name: "physical", state: with(func(s *slotState) { s.Type = "physical" }), wantMsg: "not a logical"},
		{name: "wrong plugin", state: with(func(s *slotState) { s.Plugin = "wal2json" }), wantMsg: "wal2json"},
		{name: "wrong database", state: with(func(s *slotState) { s.Database = "other" }), wantMsg: "other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.state.check("walx_a")
			if tt.wantErr == nil && tt.wantMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.ErrorContains(t, err, "walx_a")
			require.ErrorContains(t, err, tt.wantMsg)
		})
	}
}
