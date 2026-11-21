package event

import "reflect"

// Event is one row change. ID is "<name>-<commitLSN>-<seq>" and stays stable
// across redelivery.
type Event struct {
	ID     string         `json:"id"`
	Op     Op             `json:"op"`
	Schema string         `json:"schema"`
	Table  string         `json:"table"`
	Before map[string]any `json:"before,omitempty"`
	After  map[string]any `json:"after,omitempty"`
}

// Transition returns the value of col before and after the change. ok is
// false when either row image or the column is missing. A value may be
// Unchanged.
func (e Event) Transition(col string) (from, to any, ok bool) {
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

// Changed reports whether col differs between Before and After. ok is false
// when Transition has no answer. A col that is Unchanged on either side is
// not known to have changed, so it reports false.
func (e Event) Changed(col string) (changed, ok bool) {
	from, to, ok := e.Transition(col)
	if !ok || from == Unchanged || to == Unchanged {
		return false, ok
	}
	return !reflect.DeepEqual(from, to), true
}
