package snapshot

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/postgres"
)

var snapshotName = regexp.MustCompile(`^[0-9A-F]+-[0-9A-F]+-[0-9]+$`)

// exported is a temporary logical slot with its exported snapshot. The
// snapshot is importable only while conn stays open and idle, so nothing may
// run on it until close.
type exported struct {
	conn     *postgres.Conn
	lsn      event.LSN // the slot's consistent point
	snapshot string
}

// exportSnapshot creates a temporary pgoutput slot on a fresh replication
// connection and returns its consistent point and exported snapshot. Postgres
// drops the slot when the connection closes.
func exportSnapshot(ctx context.Context, pg config.PostgresConfig, retry config.RetryConfig) (*exported, error) {
	conn := postgres.New(pg, retry, nil)
	if _, err := conn.Connect(ctx); err != nil {
		return nil, err
	}
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	slot := "walx_snap_" + hex.EncodeToString(suffix[:])
	res, err := pglogrepl.ParseCreateReplicationSlot(conn.PgConn().Exec(ctx, createSlotSQL(slot)))
	if err != nil {
		_ = conn.Close(ctx)
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "42501" {
			return nil, fmt.Errorf("create temporary slot: the role needs the REPLICATION attribute: %w", err)
		}
		return nil, fmt.Errorf("create temporary slot: %w", err)
	}
	lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil || !snapshotName.MatchString(res.SnapshotName) {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("create temporary slot: unexpected result %q, %q", res.ConsistentPoint, res.SnapshotName)
	}
	return &exported{conn: conn, lsn: event.LSN(lsn), snapshot: res.SnapshotName}, nil
}

// close drops the slot by closing its connection.
func (e *exported) close(ctx context.Context) {
	_ = e.conn.Close(ctx)
}
