package config

import (
	"time"

	"github.com/benipranata/walx/internal/event"
)

const defaultShutdownTimeout = 30 * time.Second

// newDatabase returns an entry holding every default that does not depend on
// the entry's name or tables.
func newDatabase() DatabaseConfig {
	return DatabaseConfig{
		Postgres: PostgresConfig{
			AutoCreate:          true,
			AutoReplicaIdentity: true,
			MaxTxBytes:          256 << 20,
		},
		Retry: RetryConfig{
			Policy:  "forever",
			Initial: 100 * time.Millisecond,
			Max:     30 * time.Second,
		},
		OnPermanentError: "stop",
	}
}

func defaultOps() []event.Op {
	return []event.Op{event.OpInsert, event.OpUpdate}
}
