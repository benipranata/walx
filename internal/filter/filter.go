// Package filter applies the per-table config rules to decoded batches.
package filter

import (
	"log/slog"
	"maps"
	"math"
	"reflect"
	"slices"
	"sync"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

// Chain drops events by table, op, on_change and where, removes
// drop_columns from the row images and, last, strips reference_only tables.
// It never mutates its input.
type Chain struct {
	name    string
	tables  map[string]tableRule
	listed  bool
	metrics event.Metrics
	log     *slog.Logger

	mu     sync.Mutex
	logged map[[2]string]bool
}

type tableRule struct {
	ops      []event.Op
	where    map[string]any
	onChange []string
	drop     []string
	fsm      string
	refOnly  bool
}

var defaultOps = []event.Op{event.OpInsert, event.OpUpdate}

// New builds a Chain from the config tables. With no tables every table
// passes with the default ops; otherwise unlisted tables are dropped. A nil
// metrics or log discards counts or log lines. name is the database name,
// added to every metric and log line.
func New(name string, tables []config.TableConfig, m event.Metrics, log *slog.Logger) *Chain {
	if m == nil {
		m = event.NopMetrics{}
	}
	if log != nil {
		log = log.With("db", name)
	}
	c := &Chain{
		name:    name,
		tables:  make(map[string]tableRule, len(tables)),
		listed:  len(tables) > 0,
		metrics: m,
		log:     log,
		logged:  map[[2]string]bool{},
	}
	for _, t := range tables {
		ops := t.Ops
		if ops == nil {
			ops = defaultOps
		}
		c.tables[t.Name] = tableRule{ops: ops, where: t.Where, onChange: t.OnChange, drop: t.DropColumns, fsm: t.FSMColumn, refOnly: t.ReferenceOnly}
	}
	return c
}

// Apply returns b with the dropped events removed. When every event is
// dropped the result is a Batch with zero events and the same LSNs, so the
// engine can still confirm EndLSN.
func (c *Chain) Apply(b event.Batch) event.Batch {
	kept := make([]*event.Event, 0, len(b.Events))
	for _, e := range b.Events {
		ok, reason := c.keep(e)
		if ok {
			kept = append(kept, c.reference(c.dropColumns(e)))
			continue
		}
		c.drop(e, reason)
	}
	b.Events = kept
	return b
}

// dropColumns removes the drop_columns from Before and After, never from Key.
// It returns e itself when nothing is removed, else a copy with fresh maps.
func (c *Chain) dropColumns(e *event.Event) *event.Event {
	cols := c.tables[e.Schema+"."+e.Table].drop
	if !hasAny(e.Before, cols) && !hasAny(e.After, cols) {
		return e
	}
	cp := *e
	cp.Before = without(e.Before, cols)
	cp.After = without(e.After, cols)
	return &cp
}

func hasAny(row map[string]any, cols []string) bool {
	return slices.ContainsFunc(cols, func(c string) bool { _, ok := row[c]; return ok })
}

func without(row map[string]any, cols []string) map[string]any {
	if row == nil {
		return nil
	}
	out := maps.Clone(row)
	for _, c := range cols {
		delete(out, c)
	}
	return out
}

func (c *Chain) keep(e *event.Event) (ok bool, reason string) {
	rule, listed := c.tables[e.Schema+"."+e.Table]
	if !listed {
		if c.listed {
			return false, "table or op not enabled in config"
		}
		rule = tableRule{ops: defaultOps}
	}
	if e.Op != event.OpSnapshot && !slices.Contains(rule.ops, e.Op) {
		return false, "table or op not enabled in config"
	}
	if e.Op == event.OpUpdate && len(rule.onChange) > 0 && !slices.ContainsFunc(rule.onChange, e.Changed) {
		return false, "no on_change column changed"
	}
	if !matchWhere(e, rule.where) {
		return false, "where does not match"
	}
	return true, ""
}

// matchWhere checks every pair against After, or Before for a delete. A
// truncate has no row, so where does not apply. An Unchanged value in After
// stands for the same value as in Before, so it is read from there; any other
// Unchanged value never matches.
func matchWhere(e *event.Event, where map[string]any) bool {
	if len(where) == 0 || e.Op == event.OpTruncate {
		return true
	}
	row := e.After
	if e.Op == event.OpDelete {
		row = e.Before
	}
	for col, want := range where {
		got, ok := row[col]
		if got == event.Unchanged && e.Op == event.OpUpdate {
			got, ok = e.Before[col]
		}
		if !ok || got == event.Unchanged || !equalValue(got, want) {
			return false
		}
	}
	return true
}

// equalValue compares a decoded column value with a YAML scalar. Numbers
// compare by value across Go types, since the decoder gives int64 and float
// while YAML gives int and float64.
func equalValue(got, want any) bool {
	if g, ok := asInt(got); ok {
		if w, ok := asInt(want); ok {
			return g == w
		}
	}
	if g, ok := asFloat(got); ok {
		if w, ok := asFloat(want); ok {
			return g == w
		}
	}
	return reflect.DeepEqual(got, want)
}

func asInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case uint64:
		if n <= math.MaxInt64 {
			return int64(n), true
		}
	}
	return 0, false
}

func asFloat(v any) (float64, bool) {
	if i, ok := asInt(v); ok {
		return float64(i), true
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	}
	return 0, false
}

func (c *Chain) drop(e *event.Event, reason string) {
	table := e.Schema + "." + e.Table
	c.metrics.Count("events_dropped_total", 1, "database", c.name, "op", string(e.Op), "table", table)
	if c.log == nil {
		return
	}
	key := [2]string{table, string(e.Op)}
	c.mu.Lock()
	first := !c.logged[key]
	c.logged[key] = true
	c.mu.Unlock()
	if first {
		c.log.Warn("dropping events by config filter", "reason", reason, "table", table, "op", string(e.Op))
	}
}
