package config

import "github.com/benipranata/walx/internal/event"

// TableConfig is one entry of the tables list.
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
