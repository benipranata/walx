package config

// PostgresConfig is the connection, slot and publication of one database.
type PostgresConfig struct {
	URL                 string   `yaml:"url"`                   // walx adds replication=database itself
	Slot                string   `yaml:"slot"`                  // default walx_<name>
	Publication         string   `yaml:"publication"`           // default walx_<name>
	AutoCreate          bool     `yaml:"auto_create"`           // create the slot and publication
	AutoReplicaIdentity bool     `yaml:"auto_replica_identity"` // set REPLICA IDENTITY FULL
	AutoAlter           bool     `yaml:"auto_alter"`            // keep the publication in step with tables
	MaxTxBytes          ByteSize `yaml:"max_tx_bytes"`          // raw tuple bytes of one transaction, not a memory limit
	MaxTxEvents         int      `yaml:"max_tx_events"`         // 0 means unlimited
}
