// Package event holds the types shared by the engine and the sinks.
package event

import (
	"reflect"
	"time"
)

// Event is one row change. ID is "<name>-<commitLSN>-<seq>" and stays stable
// across redelivery. Source is the name of the databases entry it came from.
type Event struct {
	ID       string         `json:"id"`
	Source   string         `json:"source"`
	Op       Op             `json:"op"`
	Schema   string         `json:"schema"`
	Table    string         `json:"table"`
	Before   map[string]any `json:"before,omitempty"`
	After    map[string]any `json:"after,omitempty"`
	Key      map[string]any `json:"key,omitempty"`
	LSN      LSN            `json:"lsn"` // WALStart: informational, never an ordering key
	TxID     uint32         `json:"tx_id"`
	CommitTS time.Time      `json:"commit_ts"`
}

// Transition returns the value of col before and after the change. ok is
// false when either row image or the column is missing, so a delete or an
// insert has no transition. A value may be Unchanged.
func (e *Event) Transition(col string) (from, to any, ok bool) {
	if e.Before == nil || e.After == nil {
		return nil, nil, false
	}
	from, okFrom := e.Before[col]
	to, okTo := e.After[col]
	if !okFrom || !okTo {
		return nil, nil, false
	}
	return from, to, true
}

// Changed reports whether col differs between Before and After. It is false
// when Transition has no answer, and when col is Unchanged on either side,
// since that is not known to have changed.
func (e *Event) Changed(col string) bool {
	from, to, ok := e.Transition(col)
	if !ok || from == Unchanged || to == Unchanged {
		return false
	}
	return !reflect.DeepEqual(from, to)
}
