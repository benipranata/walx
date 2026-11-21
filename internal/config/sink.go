package config

// SinkConfig selects and configures the one sink of a database.
type SinkConfig struct {
	Type string `yaml:"type"` // stdout | file
	Path string `yaml:"path"` // file only
}
