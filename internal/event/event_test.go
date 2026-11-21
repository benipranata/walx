package event_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestUnchangedMarshalJSON(t *testing.T) {
	b, err := json.Marshal(event.Unchanged)
	require.NoError(t, err)
	require.JSONEq(t, `{"$unchanged":true}`, string(b))
}

func TestUnchangedInsideRow(t *testing.T) {
	row := map[string]any{"id": 1, "body": event.Unchanged}
	b, err := json.Marshal(row)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":1,"body":{"$unchanged":true}}`, string(b))
}

func TestOpJSON(t *testing.T) {
	for _, op := range []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete, event.OpTruncate} {
		b, err := json.Marshal(op)
		require.NoError(t, err)
		require.Equal(t, `"`+string(op)+`"`, string(b))
	}
}

func TestLSNString(t *testing.T) {
	require.Equal(t, "0/0", event.LSN(0).String())
	require.Equal(t, "16/B374D848", event.LSN(0x16B374D848).String())
}

func TestEventJSON(t *testing.T) {
	e := event.Event{
		ID:     "main-0/10-1",
		Op:     event.OpUpdate,
		Schema: "public",
		Table:  "users",
		Before: map[string]any{"id": 1},
		After:  map[string]any{"id": 1, "bio": event.Unchanged},
	}
	b, err := json.Marshal(e)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"id":"main-0/10-1","op":"update","schema":"public","table":"users",
		"before":{"id":1},"after":{"id":1,"bio":{"$unchanged":true}}
	}`, string(b))
}

func TestChangedAndTransition(t *testing.T) {
	u := event.Unchanged
	tests := []struct {
		name             string
		before, after    map[string]any
		col              string
		wantChanged      bool
		wantFrom, wantTo any
		wantOK           bool
	}{
		{"insert has no before", nil, map[string]any{"a": 1}, "a", false, nil, nil, false},
		{"delete has no after", map[string]any{"a": 1}, nil, "a", false, nil, nil, false},
		{"update changed", map[string]any{"a": 1}, map[string]any{"a": 2}, "a", true, 1, 2, true},
		{"same value", map[string]any{"a": 1}, map[string]any{"a": 1}, "a", false, 1, 1, true},
		{"nil to value", map[string]any{"a": nil}, map[string]any{"a": 1}, "a", true, nil, 1, true},
		{"uncomparable equal", map[string]any{"a": []any{1}}, map[string]any{"a": []any{1}}, "a", false, []any{1}, []any{1}, true},
		{"uncomparable differ", map[string]any{"a": map[string]any{"k": 1}}, map[string]any{"a": map[string]any{"k": 2}}, "a", true, map[string]any{"k": 1}, map[string]any{"k": 2}, true},
		{"missing in before", map[string]any{"b": 1}, map[string]any{"a": 1}, "a", false, nil, nil, false},
		{"missing in after", map[string]any{"a": 1}, map[string]any{"b": 1}, "a", false, nil, nil, false},
		{"sentinel in before", map[string]any{"a": u}, map[string]any{"a": 1}, "a", false, u, 1, true},
		{"sentinel in after", map[string]any{"a": 1}, map[string]any{"a": u}, "a", false, 1, u, true},
		{"sentinel in both", map[string]any{"a": u}, map[string]any{"a": u}, "a", false, u, u, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := event.Event{Before: tt.before, After: tt.after}

			changed, ok := e.Changed(tt.col)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantChanged, changed)

			from, to, ok := e.Transition(tt.col)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantFrom, from)
			require.Equal(t, tt.wantTo, to)
		})
	}
}
