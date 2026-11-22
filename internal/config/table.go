package config

import "github.com/benipranata/walx/internal/event"

// TableConfig is one entry of the tables list.
//
// Where reads After (Before for deletes). On an update, a TOAST column that did
// not change is Unchanged in After and falls back to Before; under the default
// replica identity Before is nil, so such an update is dropped as "where does
// not match". REPLICA IDENTITY FULL avoids this, and auto_replica_identity sets
// it for where only when the table enables deletes, not for updates, so set it
// yourself if a where column can be TOAST-stored and updates are enabled.
type TableConfig struct {
	Name          string         `yaml:"name"`           // schema.table
	Ops           []event.Op     `yaml:"ops"`            // default: insert, update
	FSMColumn     string         `yaml:"fsm_column"`     // optional, e.g. "status"
	DropColumns   []string       `yaml:"drop_columns"`   // removed from Before and After, never from Key
	Where         map[string]any `yaml:"where"`          // After must equal every pair (Before for deletes)
	OnChange      []string       `yaml:"on_change"`      // updates pass only when one of these columns changed
	Key           []string       `yaml:"key"`            // key columns; default: the primary key
	ReferenceOnly bool           `yaml:"reference_only"` // omit Before and After, keep Key (and fsm_column in After)
}
