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
	SetConfirmed(lsn pglogrepl.LSN)
}
