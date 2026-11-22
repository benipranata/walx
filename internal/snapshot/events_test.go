package snapshot

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestBuildEvents(t *testing.T) {
	fields := []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: 23},
		{Name: "tenant", DataTypeOID: 25},
		{Name: "amount", DataTypeOID: 1700},
		{Name: "blob", DataTypeOID: 17},
		{Name: "doc", DataTypeOID: 3802},
		{Name: "note", DataTypeOID: 25},
	}
	row := func(id, tenant, amount, blob, doc string, note []byte) [][]byte {
		return [][]byte{[]byte(id), []byte(tenant), []byte(amount), []byte(blob), []byte(doc), note}
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	lsn := event.LSN(0x16B3748)

	tests := []struct {
		name  string
		plan  tablePlan
		rows  [][][]byte
		start int
		want  []event.Event
		next  int
	}{
		{
			name: "composite key, types and null",
			plan: tablePlan{Schema: "public", Table: "orders", Key: []string{"id", "tenant"}},
			rows: [][][]byte{
				row("1", "a", "12.50", `\x0102`, `{"b":1,"a":[2]}`, nil),
				row("2", "b", "0", `\x`, `null`, []byte("x")),
			},
			start: 0,
			want: []event.Event{
				{ID: "orders-0/16B3748-1", Source: "orders", Op: event.OpSnapshot, Schema: "public", Table: "orders", LSN: lsn, CommitTS: ts,
					After: map[string]any{"id": int64(1), "tenant": "a", "amount": "12.50", "blob": []byte{1, 2}, "doc": map[string]any{"a": []any{jsonNum("2")}, "b": jsonNum("1")}, "note": nil},
					Key:   map[string]any{"id": int64(1), "tenant": "a"}},
				{ID: "orders-0/16B3748-2", Source: "orders", Op: event.OpSnapshot, Schema: "public", Table: "orders", LSN: lsn, CommitTS: ts,
					After: map[string]any{"id": int64(2), "tenant": "b", "amount": "0", "blob": []byte{}, "doc": nil, "note": "x"},
					Key:   map[string]any{"id": int64(2), "tenant": "b"}},
			},
			next: 2,
		},
		{
			name:  "sequence continues across calls, no key",
			plan:  tablePlan{Schema: "public", Table: "orders"},
			rows:  [][][]byte{row("3", "c", "1", `\x00`, `1`, nil)},
			start: 41,
			want: []event.Event{
				{ID: "orders-0/16B3748-42", Source: "orders", Op: event.OpSnapshot, Schema: "public", Table: "orders", LSN: lsn, CommitTS: ts,
					After: map[string]any{"id": int64(3), "tenant": "c", "amount": "1", "blob": []byte{0}, "doc": jsonNum("1"), "note": nil}},
			},
			next: 42,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &builder{name: "orders", lsn: lsn, ts: ts, seq: tt.start}
			got, err := b.build(tt.plan, fields, tt.rows)
			require.NoError(t, err)
			require.Len(t, got, len(tt.want))
			for i := range tt.want {
				require.Equal(t, tt.want[i], *got[i])
			}
			require.Equal(t, tt.next, b.seq)
		})
	}
}

func TestBuildEventsDeterministic(t *testing.T) {
	fields := []pgconn.FieldDescription{{Name: "id", DataTypeOID: 23}}
	rows := [][][]byte{{[]byte("1")}, {[]byte("2")}}
	plan := tablePlan{Schema: "public", Table: "t", Key: []string{"id"}}
	ids := func() []string {
		b := &builder{name: "d", lsn: 5, ts: time.Time{}}
		evs, err := b.build(plan, fields, rows)
		require.NoError(t, err)
		return []string{evs[0].ID, evs[1].ID}
	}
	require.Equal(t, ids(), ids())
	require.Equal(t, []string{"d-0/5-1", "d-0/5-2"}, ids())
}

func TestBuildEventsErrors(t *testing.T) {
	fields := []pgconn.FieldDescription{{Name: "id", DataTypeOID: 23}, {Name: "v", DataTypeOID: 16}}
	tests := []struct {
		name    string
		plan    tablePlan
		rows    [][][]byte
		wantErr string
	}{
		{"null key", tablePlan{Schema: "s", Table: "t", Key: []string{"id"}}, [][][]byte{{nil, []byte("t")}}, "NULL"},
		{"missing key column", tablePlan{Schema: "s", Table: "t", Key: []string{"nope"}}, [][][]byte{{[]byte("1"), []byte("t")}}, `key column "nope"`},
		{"bad value", tablePlan{Schema: "s", Table: "t"}, [][][]byte{{[]byte("x"), []byte("t")}}, `column "id"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &builder{name: "d"}
			_, err := b.build(tt.plan, fields, tt.rows)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func jsonNum(s string) json.Number { return json.Number(s) }
