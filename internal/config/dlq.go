package config

// DLQConfig configures the dead-letter file of a database.
type DLQConfig struct {
	// Path is the file that receives batches skipped by on_permanent_error:
	// skip; empty means no dead-letter file.
	Path string `yaml:"path"`
}
