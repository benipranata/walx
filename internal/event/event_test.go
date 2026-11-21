package event_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestEventJSON(t *testing.T) {
	tests := []struct {
		name string
		in   event.Event
		want string
	}{
		{
			"every field",
			event.Event{
				ID:       "main-0/10-1",
				Source:   "main",
				Op:       event.OpUpdate,
				Schema:   "public",
				Table:    "users",
				Before:   map[string]any{"id": 1},
				After:    map[string]any{"id": 1, "bio": event.Unchanged},
				Key:      map[string]any{"id": 1},
				LSN:      16,
				TxID:     7,
				CommitTS: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			},
			`{
				"id":"main-0/10-1","source":"main","op":"update","schema":"public","table":"users",
				"before":{"id":1},"after":{"id":1,"bio":{"$unchanged":true}},
				"key":{"id":1},"lsn":"0/10","tx_id":7,"commit_ts":"2026-01-02T03:04:05Z"
			}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.in)
			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(b))
		})
	}
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
			e := &event.Event{Before: tt.before, After: tt.after}

			require.Equal(t, tt.wantChanged, e.Changed(tt.col))

			from, to, ok := e.Transition(tt.col)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantFrom, from)
			require.Equal(t, tt.wantTo, to)
		})
	}
}
