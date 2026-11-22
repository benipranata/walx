package config

import "time"

// SinkConfig selects and configures the one sink of a database.
type SinkConfig struct {
	Type string `yaml:"type"` // stdout | file
	Path string `yaml:"path"` // file only

	// Timeout bounds one Send call; 0 = no timeout. A timed-out call is
	// retried, so a late success can duplicate events (dedupe by Event.ID).
	Timeout time.Duration `yaml:"timeout"`
}
