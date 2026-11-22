package filter_test

import (
	"bytes"
	"log/slog"
	"maps"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/filter"
)

type count struct {
	name   string
	delta  float64
	labels []string
}

type recorder struct {
	event.NopMetrics
	counts []count
}

func (r *recorder) Count(name string, delta float64, labels ...string) {
	r.counts = append(r.counts, count{name, delta, labels})
}

func ev(op event.Op, schema, table string) *event.Event {
	return &event.Event{ID: string(op) + table, Op: op, Schema: schema, Table: table}
}

func TestChainApply(t *testing.T) {
	tests := []struct {
		name   string
		tables []config.TableConfig
		in     []*event.Event
		want   []string
	}{
		{
			"no tables, default ops",
			nil,
			[]*event.Event{ev(event.OpInsert, "public", "a"), ev(event.OpUpdate, "public", "a"), ev(event.OpDelete, "public", "a"), ev(event.OpTruncate, "public", "a")},
			[]string{"inserta", "updatea"},
		},
		{
			"unlisted table dropped",
			[]config.TableConfig{{Name: "public.a"}},
			[]*event.Event{ev(event.OpInsert, "public", "a"), ev(event.OpInsert, "public", "b"), ev(event.OpInsert, "other", "a")},
			[]string{"inserta"},
		},
		{
			"ops per table with opt-in delete",
			[]config.TableConfig{
				{Name: "public.a", Ops: []event.Op{event.OpInsert, event.OpDelete}},
				{Name: "public.b", Ops: []event.Op{event.OpTruncate}},
			},
			[]*event.Event{
				ev(event.OpInsert, "public", "a"), ev(event.OpUpdate, "public", "a"), ev(event.OpDelete, "public", "a"),
				ev(event.OpInsert, "public", "b"), ev(event.OpTruncate, "public", "b"),
			},
			[]string{"inserta", "deletea", "truncateb"},
		},
		{
			"snapshot passes for a listed table",
			[]config.TableConfig{{Name: "public.a"}},
			[]*event.Event{ev(event.OpSnapshot, "public", "a")},
			[]string{"snapshota"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := filter.New("db1", tt.tables, event.NopMetrics{}, nil)
			in := event.Batch{Events: tt.in, CommitLSN: 5, EndLSN: 9}
			orig := append([]*event.Event(nil), tt.in...)

			out := c.Apply(in)

			var got []string
			for _, e := range out.Events {
				got = append(got, e.ID)
			}
			require.Equal(t, tt.want, got)
			require.Equal(t, event.LSN(5), out.CommitLSN)
			require.Equal(t, event.LSN(9), out.EndLSN)
			require.Equal(t, orig, in.Events, "input slice must not change")
		})
	}
}

func TestChainEmptyBatch(t *testing.T) {
	tests := []struct {
		name string
		in   []*event.Event
	}{
		{"all dropped", []*event.Event{ev(event.OpDelete, "public", "a")}},
		{"already empty", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := filter.New("db1", nil, event.NopMetrics{}, nil)
			out := c.Apply(event.Batch{Events: tt.in, EndLSN: 42})
			require.Empty(t, out.Events)
			require.Equal(t, event.LSN(42), out.EndLSN)
		})
	}
}

func TestChainMetricsAndLog(t *testing.T) {
	tests := []struct {
		name       string
		logger     bool
		wantCounts []count
		wantLogs   int
	}{
		{
			"counts every drop, logs first per table and op",
			true,
			[]count{
				{"events_dropped_total", 1, []string{"database", "db1", "op", "delete", "table", "public.a"}},
				{"events_dropped_total", 1, []string{"database", "db1", "op", "delete", "table", "public.a"}},
				{"events_dropped_total", 1, []string{"database", "db1", "op", "insert", "table", "public.b"}},
			},
			2,
		},
		{"nil logger and nil metrics are safe", false, nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			var buf bytes.Buffer
			var log *slog.Logger
			var m event.Metrics = rec
			if tt.logger {
				log = slog.New(slog.NewTextHandler(&buf, nil))
			} else {
				m = nil
			}
			c := filter.New("db1", []config.TableConfig{{Name: "public.a"}}, m, log)

			c.Apply(event.Batch{Events: []*event.Event{
				ev(event.OpDelete, "public", "a"), ev(event.OpDelete, "public", "a"), ev(event.OpInsert, "public", "b"),
			}})

			require.Equal(t, tt.wantCounts, rec.counts)
			if tt.wantLogs == 0 {
				require.Empty(t, buf.String())
				return
			}
			require.Equal(t, tt.wantLogs, strings.Count(buf.String(), "\n"))
			require.Contains(t, buf.String(), "db=db1")
		})
	}
}

func TestChainRowFilters(t *testing.T) {
	un := event.Unchanged
	row := func(op event.Op, before, after map[string]any) *event.Event {
		e := ev(op, "public", "a")
		e.Before, e.After = before, after
		return e
	}
	all := []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete, event.OpTruncate}
	tests := []struct {
		name  string
		table config.TableConfig
		in    *event.Event
		want  bool
	}{
		{"on_change column changed", config.TableConfig{OnChange: []string{"status"}},
			row(event.OpUpdate, map[string]any{"status": "a"}, map[string]any{"status": "b"}), true},
		{"on_change column same", config.TableConfig{OnChange: []string{"status"}},
			row(event.OpUpdate, map[string]any{"status": "a", "n": int64(1)}, map[string]any{"status": "a", "n": int64(2)}), false},
		{"on_change any of several", config.TableConfig{OnChange: []string{"status", "n"}},
			row(event.OpUpdate, map[string]any{"status": "a", "n": int64(1)}, map[string]any{"status": "a", "n": int64(2)}), true},
		{"on_change unchanged value is not a change", config.TableConfig{OnChange: []string{"big"}},
			row(event.OpUpdate, map[string]any{"big": "x"}, map[string]any{"big": un}), false},
		{"on_change without before drops", config.TableConfig{OnChange: []string{"status"}},
			row(event.OpUpdate, nil, map[string]any{"status": "b"}), false},
		{"on_change ignores insert", config.TableConfig{OnChange: []string{"status"}},
			row(event.OpInsert, nil, map[string]any{"status": "b"}), true},
		{"on_change ignores delete", config.TableConfig{OnChange: []string{"status"}},
			row(event.OpDelete, map[string]any{"status": "b"}, nil), true},
		{"where matches after", config.TableConfig{Where: map[string]any{"tenant": "t1"}},
			row(event.OpInsert, nil, map[string]any{"tenant": "t1"}), true},
		{"where differs", config.TableConfig{Where: map[string]any{"tenant": "t1"}},
			row(event.OpInsert, nil, map[string]any{"tenant": "t2"}), false},
		{"where all pairs needed", config.TableConfig{Where: map[string]any{"tenant": "t1", "live": true}},
			row(event.OpInsert, nil, map[string]any{"tenant": "t1", "live": false}), false},
		{"where missing column", config.TableConfig{Where: map[string]any{"tenant": "t1"}},
			row(event.OpInsert, nil, map[string]any{"other": "t1"}), false},
		{"where int against int64", config.TableConfig{Where: map[string]any{"n": 3}},
			row(event.OpInsert, nil, map[string]any{"n": int64(3)}), true},
		{"where float against int64", config.TableConfig{Where: map[string]any{"n": 3.0}},
			row(event.OpInsert, nil, map[string]any{"n": int64(3)}), true},
		{"where float32 column", config.TableConfig{Where: map[string]any{"f": 1.5}},
			row(event.OpInsert, nil, map[string]any{"f": float32(1.5)}), true},
		{"where int32 column", config.TableConfig{Where: map[string]any{"n": 3}},
			row(event.OpInsert, nil, map[string]any{"n": int32(3)}), true},
		{"where int16 column", config.TableConfig{Where: map[string]any{"n": 3}},
			row(event.OpInsert, nil, map[string]any{"n": int16(3)}), true},
		{"where uint64 column", config.TableConfig{Where: map[string]any{"n": 3}},
			row(event.OpInsert, nil, map[string]any{"n": uint64(3)}), true},
		{"where uint64 above int64 never matches", config.TableConfig{Where: map[string]any{"n": 3}},
			row(event.OpInsert, nil, map[string]any{"n": uint64(math.MaxUint64)}), false},
		{"where string never equals number", config.TableConfig{Where: map[string]any{"n": "3"}},
			row(event.OpInsert, nil, map[string]any{"n": int64(3)}), false},
		{"where null", config.TableConfig{Where: map[string]any{"deleted_at": nil}},
			row(event.OpInsert, nil, map[string]any{"deleted_at": nil}), true},
		{"where null differs", config.TableConfig{Where: map[string]any{"deleted_at": nil}},
			row(event.OpInsert, nil, map[string]any{"deleted_at": "x"}), false},
		{"where on update reads after", config.TableConfig{Where: map[string]any{"s": "b"}},
			row(event.OpUpdate, map[string]any{"s": "a"}, map[string]any{"s": "b"}), true},
		{"where on update ignores before", config.TableConfig{Where: map[string]any{"s": "a"}},
			row(event.OpUpdate, map[string]any{"s": "a"}, map[string]any{"s": "b"}), false},
		{"where on delete reads before", config.TableConfig{Where: map[string]any{"s": "a"}},
			row(event.OpDelete, map[string]any{"s": "a"}, nil), true},
		{"where on delete differs", config.TableConfig{Where: map[string]any{"s": "a"}},
			row(event.OpDelete, map[string]any{"s": "b"}, nil), false},
		{"where unchanged after falls back to before", config.TableConfig{Where: map[string]any{"big": "x"}},
			row(event.OpUpdate, map[string]any{"big": "x"}, map[string]any{"big": un}), true},
		{"where unchanged on both sides never matches", config.TableConfig{Where: map[string]any{"big": "x"}},
			row(event.OpUpdate, map[string]any{"big": un}, map[string]any{"big": un}), false},
		{"where unchanged after without before never matches", config.TableConfig{Where: map[string]any{"big": "x"}},
			row(event.OpUpdate, nil, map[string]any{"big": un}), false},
		{"where does not apply to truncate", config.TableConfig{Where: map[string]any{"s": "a"}},
			row(event.OpTruncate, nil, nil), true},
		{"where applies to snapshot after", config.TableConfig{Where: map[string]any{"s": "a"}},
			row(event.OpSnapshot, nil, map[string]any{"s": "b"}), false},
		{"on_change and where combine", config.TableConfig{OnChange: []string{"s"}, Where: map[string]any{"t": "t1"}},
			row(event.OpUpdate, map[string]any{"s": "a"}, map[string]any{"s": "b", "t": "t2"}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.table.Name = "public.a"
			tt.table.Ops = all
			c := filter.New("db1", []config.TableConfig{tt.table}, event.NopMetrics{}, nil)
			before := *tt.in

			out := c.Apply(event.Batch{Events: []*event.Event{tt.in}, EndLSN: 7})

			require.Equal(t, tt.want, len(out.Events) == 1)
			require.Equal(t, event.LSN(7), out.EndLSN)
			require.Equal(t, before, *tt.in, "event must not change")
		})
	}
}

func TestChainRowFilterDropIsCounted(t *testing.T) {
	tests := []struct {
		name string
	}{{"where drop counted and logged once"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			var buf bytes.Buffer
			c := filter.New("db1", []config.TableConfig{{Name: "public.a", Where: map[string]any{"s": "a"}}}, rec, slog.New(slog.NewTextHandler(&buf, nil)))
			e := ev(event.OpInsert, "public", "a")
			e.After = map[string]any{"s": "b"}

			c.Apply(event.Batch{Events: []*event.Event{e, e}})

			require.Equal(t, []count{
				{"events_dropped_total", 1, []string{"database", "db1", "op", "insert", "table", "public.a"}},
				{"events_dropped_total", 1, []string{"database", "db1", "op", "insert", "table", "public.a"}},
			}, rec.counts)
			require.Equal(t, 1, strings.Count(buf.String(), "\n"))
			require.Contains(t, buf.String(), "where does not match")
		})
	}
}

func TestChainDropColumns(t *testing.T) {
	tests := []struct {
		name       string
		table      config.TableConfig
		in         *event.Event
		wantBefore map[string]any
		wantAfter  map[string]any
		wantKey    map[string]any
	}{
		{
			"removed from before and after, key kept",
			config.TableConfig{Name: "public.users", Ops: []event.Op{event.OpUpdate}, DropColumns: []string{"id", "secret"}},
			&event.Event{Op: event.OpUpdate, Schema: "public", Table: "users",
				Before: map[string]any{"id": 1, "secret": "a", "name": "x"},
				After:  map[string]any{"id": 1, "secret": "b", "name": "y"},
				Key:    map[string]any{"id": 1}},
			map[string]any{"name": "x"},
			map[string]any{"name": "y"},
			map[string]any{"id": 1},
		},
		{
			"insert has no before",
			config.TableConfig{Name: "public.users", DropColumns: []string{"secret"}},
			&event.Event{Op: event.OpInsert, Schema: "public", Table: "users",
				After: map[string]any{"secret": "b", "name": "y"}},
			nil,
			map[string]any{"name": "y"},
			nil,
		},
		{
			"missing column is a no-op",
			config.TableConfig{Name: "public.users", DropColumns: []string{"nope"}},
			&event.Event{Op: event.OpInsert, Schema: "public", Table: "users",
				After: map[string]any{"name": "y"}},
			nil,
			map[string]any{"name": "y"},
			nil,
		},
		{
			"where and on_change still see dropped columns",
			config.TableConfig{Name: "public.users", Ops: []event.Op{event.OpUpdate}, DropColumns: []string{"status"},
				Where: map[string]any{"status": "on"}, OnChange: []string{"status"}},
			&event.Event{Op: event.OpUpdate, Schema: "public", Table: "users",
				Before: map[string]any{"status": "off", "name": "x"},
				After:  map[string]any{"status": "on", "name": "x"}},
			map[string]any{"name": "x"},
			map[string]any{"name": "x"},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := filter.New("db", []config.TableConfig{tt.table}, nil, nil)
			orig := *tt.in
			origBefore := maps.Clone(tt.in.Before)
			origAfter := maps.Clone(tt.in.After)

			out := c.Apply(event.Batch{Events: []*event.Event{tt.in}})

			require.Len(t, out.Events, 1)
			got := out.Events[0]
			require.Equal(t, tt.wantBefore, got.Before)
			require.Equal(t, tt.wantAfter, got.After)
			require.Equal(t, tt.wantKey, got.Key)
			require.Equal(t, origBefore, tt.in.Before, "input Before mutated")
			require.Equal(t, origAfter, tt.in.After, "input After mutated")
			require.Equal(t, orig.Key, tt.in.Key)
		})
	}
}

func TestChainDropColumnsNoCopyWhenUntouched(t *testing.T) {
	c := filter.New("db", []config.TableConfig{{Name: "public.users", DropColumns: []string{"secret"}}}, nil, nil)
	in := &event.Event{Op: event.OpInsert, Schema: "public", Table: "users", After: map[string]any{"name": "y"}}
	out := c.Apply(event.Batch{Events: []*event.Event{in}})
	require.Same(t, in, out.Events[0])
}
