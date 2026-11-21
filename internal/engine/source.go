package engine

import (
	"context"

	"github.com/jackc/pglogrepl"
)

// Source is a started replication stream. *postgres.Standby implements it:
// Run is the only user of the connection, and SetConfirmed is safe to call
// from the sink goroutine.
type Source interface {
	Run(ctx context.Context) error
	Messages() <-chan pglogrepl.XLogData
	// Keepalives delivers the server WAL end of primary keepalives. When it
	// yields a position, every message sent before it was already returned
	// by Messages.
	Keepalives() <-chan pglogrepl.LSN
	SetConfirmed(lsn pglogrepl.LSN)
}
