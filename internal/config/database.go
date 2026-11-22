package config

import "go.yaml.in/yaml/v3"

// DatabaseConfig is one entry of the databases list. Each entry runs its own
// pipeline.
type DatabaseConfig struct {
	Name             string         `yaml:"name"` // required and unique; becomes Event.Source
	Postgres         PostgresConfig `yaml:"postgres"`
	Tables           []TableConfig  `yaml:"tables"`
	Sink             SinkConfig     `yaml:"sink"`
	Retry            RetryConfig    `yaml:"retry"`
	OnPermanentError string         `yaml:"on_permanent_error"` // stop | skip
	DLQ              DLQConfig      `yaml:"dlq"`
}

// UnmarshalYAML decodes the entry on top of its defaults.
func (d *DatabaseConfig) UnmarshalYAML(n *yaml.Node) error {
	*d = newDatabase()
	type plain DatabaseConfig // drops the methods, so decoding does not recurse
	if err := n.Decode((*plain)(d)); err != nil {
		return err
	}
	if d.Name != "" {
		if d.Postgres.Slot == "" {
			d.Postgres.Slot = "walx_" + d.Name
		}
		if d.Postgres.Publication == "" {
			d.Postgres.Publication = "walx_" + d.Name
		}
	}
	for i := range d.Tables {
		if d.Tables[i].Ops == nil {
			d.Tables[i].Ops = defaultOps()
		}
		if col := d.Tables[i].FSMColumn; col != "" {
			if d.Sink.FSMColumns == nil {
				d.Sink.FSMColumns = map[string]string{}
			}
			d.Sink.FSMColumns[d.Tables[i].Name] = col
		}
	}
	return nil
}
