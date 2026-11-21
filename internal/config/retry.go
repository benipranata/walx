package config

import "time"

// RetryConfig controls how a failed batch is retried.
type RetryConfig struct {
	Policy      string        `yaml:"policy"` // forever | max_attempts | stop
	Initial     time.Duration `yaml:"initial"`
	Max         time.Duration `yaml:"max"`
	MaxAttempts int           `yaml:"max_attempts"` // 0 = unlimited; used by policy max_attempts
}
