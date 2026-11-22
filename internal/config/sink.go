package config

import "time"

// SinkConfig selects and configures the one sink of a database.
type SinkConfig struct {
	Type string `yaml:"type"` // stdout | file | nats
	Path string `yaml:"path"` // file only

	// URL is the NATS server URL, or a comma-separated list of them; nats only.
	URL string `yaml:"url"`
	// Subject is the NATS subject template; nats only. Empty means the
	// sink's default (subject.DefaultTemplate).
	Subject string `yaml:"subject"`

	// FSMColumns maps "schema.table" to its fsm_column. It is derived from
	// tables, not read from the file, so a sink can build a state token.
	FSMColumns map[string]string `yaml:"-"`

	// Timeout bounds one Send attempt; default 60s, 0 = no timeout. A
	// timed-out call is retried, so a late success can duplicate events
	// (dedupe by Event.ID).
	Timeout time.Duration `yaml:"timeout"`
}
