package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// StartStream starts pgoutput streaming from slot on the replication
// connection with proto_version '1' (no streaming) and the publication. lsn is
// the confirmed LSN, or 0 to resume from the slot's own position. A slot
// another session is using fails with ErrSlotActive, a lost one with
// ErrSlotLost and a missing one with ErrSlotMissing, all wrapped. A missing
// publication is not reported here: the server only warns once streaming
// starts, and Standby.Run turns that into ErrPublicationMissing.
func StartStream(ctx context.Context, pg *pgconn.PgConn, slot, publication string, lsn pglogrepl.LSN) error {
	opts := pglogrepl.StartReplicationOptions{PluginArgs: streamArgs(publication)}
	if err := pglogrepl.StartReplication(ctx, pg, slot, lsn, opts); err != nil {
		return classifyStart(slot, err)
	}
	return nil
}

// streamArgs quotes the publication as an identifier inside the literal,
// which pgoutput parses as an identifier list.
func streamArgs(publication string) []string {
	return []string{
		"proto_version '1'",
		"publication_names " + quoteLiteral(pgx.Identifier{publication}.Sanitize()),
	}
}

func classifyStart(slot string, err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return fmt.Errorf("start replication on slot %q: %w", slot, err)
	}
	switch {
	case pgErr.Code == codeObjectInUse:
		return fmt.Errorf("slot %q: %w: %w", slot, ErrSlotActive, err)
	case pgErr.Code == codeUndefinedObject && strings.Contains(pgErr.Message, "replication slot"):
		return fmt.Errorf("slot %q: %w: %w", slot, ErrSlotMissing, err)
	case pgErr.Code == codeNotInPrerequisite && strings.Contains(pgErr.Message, "replication slot"):
		return fmt.Errorf("slot %q: %w: %w", slot, ErrSlotLost, err)
	}
	return fmt.Errorf("start replication on slot %q: %w", slot, err)
}
