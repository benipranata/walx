package filter

import "github.com/benipranata/walx/internal/event"

// reference strips a reference_only event to its Key, plus the fsm_column
// value in After. It runs after every other filter and returns e itself for
// other tables, else a copy.
func (c *Chain) reference(e *event.Event) *event.Event {
	rule := c.tables[e.Schema+"."+e.Table]
	if !rule.refOnly {
		return e
	}
	cp := *e
	cp.Before, cp.After = nil, nil
	if rule.fsm != "" {
		if v, ok := fsmValue(e, rule.fsm); ok {
			cp.After = map[string]any{rule.fsm: v}
		}
	}
	return &cp
}

// fsmValue is the new state: After, or Before when After holds Unchanged. A
// delete has no new state.
func fsmValue(e *event.Event, col string) (any, bool) {
	if e.Op == event.OpDelete {
		return nil, false
	}
	if v, ok := e.After[col]; ok && v != event.Unchanged {
		return v, true
	}
	if v, ok := e.Before[col]; ok && v != event.Unchanged {
		return v, true
	}
	return nil, false
}
