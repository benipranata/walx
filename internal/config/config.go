package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"go.yaml.in/yaml/v3"
)

// DefaultPath is where the binary looks for the config when -config is unset.
const DefaultPath = "./config.yaml"

const defaultShutdownTimeout = 30 * time.Second

// Config is the whole of config.yaml.
type Config struct {
	ShutdownTimeout time.Duration    `yaml:"shutdown_timeout"` // shared by all databases
	Databases       []DatabaseConfig `yaml:"databases"`
}

// Load reads, expands, decodes and validates the config at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	cfg, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, nil
}

// Parse expands ${ENV_VAR} references in data, decodes it strictly, applies
// defaults and validates the result. Errors name the offending key.
func Parse(data []byte) (*Config, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("expected a YAML mapping at the top level")
	}
	root := doc.Content[0]

	keys := lineKeys{}
	errs := checkKeys(root, reflect.TypeFor[Config](), "", keys)
	errs = append(errs, expandEnv(root, "")...)
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	cfg := Config{ShutdownTimeout: defaultShutdownTimeout}
	if err := root.Decode(&cfg); err != nil {
		return nil, keys.name(err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
