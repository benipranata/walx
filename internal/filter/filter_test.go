package filter_test

import (
	"bytes"
	"log/slog"
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
			c := filter.New(tt.tables, event.NopMetrics{}, nil)
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
			c := filter.New(nil, event.NopMetrics{}, nil)
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
				{"events_dropped_total", 1, []string{"op", "delete", "table", "public.a"}},
				{"events_dropped_total", 1, []string{"op", "delete", "table", "public.a"}},
				{"events_dropped_total", 1, []string{"op", "insert", "table", "public.b"}},
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
			c := filter.New([]config.TableConfig{{Name: "public.a"}}, m, log)

			c.Apply(event.Batch{Events: []*event.Event{
				ev(event.OpDelete, "public", "a"), ev(event.OpDelete, "public", "a"), ev(event.OpInsert, "public", "b"),
			}})

			require.Equal(t, tt.wantCounts, rec.counts)
			if tt.wantLogs == 0 {
				require.Empty(t, buf.String())
				return
			}
			require.Equal(t, tt.wantLogs, strings.Count(buf.String(), "\n"))
		})
	}
}
