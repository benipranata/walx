// Package filter applies the per-table config rules to decoded batches.
package filter

import (
	"log/slog"
	"slices"
	"sync"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

// Chain drops events by table and op. It never mutates its input.
type Chain struct {
	name    string
	tables  map[string][]event.Op
	listed  bool
	metrics event.Metrics
	log     *slog.Logger

	mu     sync.Mutex
	logged map[[2]string]bool
}

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
		tables:  make(map[string][]event.Op, len(tables)),
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
		c.tables[t.Name] = ops
	}
	return c
}

var defaultOps = []event.Op{event.OpInsert, event.OpUpdate}

// Apply returns b with the dropped events removed. When every event is
// dropped the result is a Batch with zero events and the same LSNs, so the
// engine can still confirm EndLSN.
func (c *Chain) Apply(b event.Batch) event.Batch {
	kept := make([]*event.Event, 0, len(b.Events))
	for _, e := range b.Events {
		if c.keep(e) {
			kept = append(kept, e)
			continue
		}
		c.drop(e)
	}
	b.Events = kept
	return b
}

func (c *Chain) keep(e *event.Event) bool {
	ops, ok := c.tables[e.Schema+"."+e.Table]
	if !ok {
		if c.listed {
			return false
		}
		ops = defaultOps
	}
	return e.Op == event.OpSnapshot || slices.Contains(ops, e.Op)
}

func (c *Chain) drop(e *event.Event) {
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
		c.log.Warn("dropping events not enabled in config", "table", table, "op", string(e.Op))
	}
}
