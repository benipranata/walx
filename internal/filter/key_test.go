package filter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/filter"
)

func TestReferenceOnly(t *testing.T) {
	key := map[string]any{"id": 1}
	mk := func(op event.Op, before, after map[string]any) *event.Event {
		return &event.Event{ID: "x", Op: op, Schema: "public", Table: "t", Before: before, After: after, Key: key}
	}
	tests := []struct {
		name  string
		table config.TableConfig
		in    *event.Event
		want  *event.Event
	}{
		{"insert drops images", config.TableConfig{Name: "public.t", ReferenceOnly: true},
			mk(event.OpInsert, nil, map[string]any{"id": 1, "a": "x"}), mk(event.OpInsert, nil, nil)},
		{"update keeps key only", config.TableConfig{Name: "public.t", ReferenceOnly: true},
			mk(event.OpUpdate, map[string]any{"id": 1, "a": "x"}, map[string]any{"id": 1, "a": "y"}), mk(event.OpUpdate, nil, nil)},
		{"fsm value stays in After", config.TableConfig{Name: "public.t", ReferenceOnly: true, FSMColumn: "status", Ops: []event.Op{event.OpUpdate}},
			mk(event.OpUpdate, map[string]any{"id": 1, "status": "a", "n": 1}, map[string]any{"id": 1, "status": "b", "n": 2}),
			mk(event.OpUpdate, nil, map[string]any{"status": "b"})},
		{"unchanged fsm falls back to Before", config.TableConfig{Name: "public.t", ReferenceOnly: true, FSMColumn: "status", Ops: []event.Op{event.OpUpdate}},
			mk(event.OpUpdate, map[string]any{"id": 1, "status": "a"}, map[string]any{"id": 1, "status": event.Unchanged}),
			mk(event.OpUpdate, nil, map[string]any{"status": "a"})},
		{"delete has no new state", config.TableConfig{Name: "public.t", ReferenceOnly: true, FSMColumn: "status", Ops: []event.Op{event.OpDelete}},
			mk(event.OpDelete, map[string]any{"id": 1, "status": "a"}, nil), mk(event.OpDelete, nil, nil)},
		{"off keeps images", config.TableConfig{Name: "public.t"},
			mk(event.OpInsert, nil, map[string]any{"id": 1}), mk(event.OpInsert, nil, map[string]any{"id": 1})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := *tt.in
			c := filter.New("db", []config.TableConfig{tt.table}, nil, nil)
			out := c.Apply(event.Batch{Events: []*event.Event{&in}}).Events
			require.Len(t, out, 1)
			require.Equal(t, tt.want, out[0])
			require.Equal(t, tt.in, &in, "input must not be mutated")
		})
	}
}

func TestReferenceOnlyRunsLast(t *testing.T) {
	c := filter.New("db", []config.TableConfig{
		{Name: "public.t", ReferenceOnly: true, Where: map[string]any{"a": 1}, DropColumns: []string{"a"}},
	}, nil, nil)
	hit := &event.Event{Op: event.OpInsert, Schema: "public", Table: "t", After: map[string]any{"id": 1, "a": 1}, Key: map[string]any{"id": 1}}
	miss := &event.Event{Op: event.OpInsert, Schema: "public", Table: "t", After: map[string]any{"id": 2, "a": 2}, Key: map[string]any{"id": 2}}
	out := c.Apply(event.Batch{Events: []*event.Event{hit, miss}}).Events
	require.Len(t, out, 1)
	require.Nil(t, out[0].After)
	require.Equal(t, map[string]any{"id": 1}, out[0].Key)
}
