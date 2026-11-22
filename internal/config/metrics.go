package config

import "time"

// MetricsConfig configures the Prometheus endpoint and the slot lag poller,
// both shared by all databases.
type MetricsConfig struct {
	Listen      string        `yaml:"listen"`       // host:port of the /metrics listener; empty disables it
	LagInterval time.Duration `yaml:"lag_interval"` // how often slot lag and wal_status are read
}
