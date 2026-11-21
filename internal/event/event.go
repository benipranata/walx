// Package event holds the types shared by the engine and the sinks.
package event

import "fmt"

// Op is the kind of change an Event describes.
type Op string

// The operations walx emits.
const (
	OpInsert   Op = "insert"
	OpUpdate   Op = "update"
	OpDelete   Op = "delete"
	OpTruncate Op = "truncate"
)

// LSN is a Postgres write-ahead log position.
type LSN uint64

// String formats the LSN the way Postgres does, as two hex halves.
func (l LSN) String() string {
	return fmt.Sprintf("%X/%X", uint32(l>>32), uint32(l))
}

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

// Batch is the unit a Sink makes durable. The engine confirms EndLSN only
// after Send returns nil.
type Batch struct {
	Events    []Event
	CommitLSN LSN
	EndLSN    LSN
}

type unchanged struct{}

// Unchanged marks a TOAST column the update did not rewrite, so its value is
// unknown rather than null.
var Unchanged = unchanged{}

// MarshalJSON renders the sentinel as {"$unchanged":true}.
func (unchanged) MarshalJSON() ([]byte, error) {
	return []byte(`{"$unchanged":true}`), nil
}
