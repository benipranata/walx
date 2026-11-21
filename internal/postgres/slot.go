package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	// ErrSlotMissing means the slot does not exist and may not be created.
	ErrSlotMissing = errors.New("replication slot does not exist")
	// ErrSlotActive means another session is streaming from the slot.
	ErrSlotActive = errors.New("replication slot is active in another session")
	// ErrSlotLost means Postgres invalidated the slot; the consumer needs a resync.
	ErrSlotLost = errors.New("replication slot is lost")
)

// SlotInfo describes a validated logical slot.
type SlotInfo struct {
	Name              string
	ConfirmedFlushLSN pglogrepl.LSN // 0 when the server reports none
	Created           bool          // this call created the slot
}

type slotState struct {
	Type, Plugin, Database, CurrentDB string
	Active                            bool
	ActivePID                         string
	WALStatus, InvalidationReason     string
	ConfirmedFlush                    pglogrepl.LSN
}

// EnsureSlot returns the named logical pgoutput slot, creating it when it is
// missing and autoCreate is set. It fails with ErrSlotMissing, ErrSlotActive
// or ErrSlotLost, wrapped, for a slot walx cannot use. pg must be the
// replication=database connection: it accepts both CREATE_REPLICATION_SLOT
// and plain SELECTs over the simple protocol, so no second connection is
// needed. The slot is created with NOEXPORT_SNAPSHOT since stage 1 takes no
// snapshot.
func EnsureSlot(ctx context.Context, pg *pgconn.PgConn, name string, autoCreate bool) (SlotInfo, error) {
	st, found, err := loadSlot(ctx, pg, name)
	if err != nil {
		return SlotInfo{}, err
	}
	created := false
	if !found {
		if !autoCreate {
			return SlotInfo{}, fmt.Errorf("slot %q: %w (postgres.auto_create is false; create it with pg_create_logical_replication_slot or CREATE_REPLICATION_SLOT)", name, ErrSlotMissing)
		}
		_, err := pglogrepl.CreateReplicationSlot(ctx, pg, name, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication, SnapshotAction: "NOEXPORT_SNAPSHOT"})
		switch {
		case err == nil:
			created = true
		case pgCode(err) == codeDuplicateObject: // lost a creation race; validate the winner's slot
		case pgCode(err) == codeInsufficientPrivilege:
			return SlotInfo{}, fmt.Errorf("create slot %q: role lacks the REPLICATION attribute: %w", name, err)
		default:
			return SlotInfo{}, fmt.Errorf("create slot %q: %w", name, err)
		}
		st, found, err = loadSlot(ctx, pg, name)
		if err != nil {
			return SlotInfo{}, err
		}
		if !found {
			return SlotInfo{}, fmt.Errorf("slot %q: %w right after creation", name, ErrSlotMissing)
		}
	}
	if err := st.check(name); err != nil {
		return SlotInfo{}, err
	}
	return SlotInfo{Name: name, ConfirmedFlushLSN: st.ConfirmedFlush, Created: created}, nil
}

func loadSlot(ctx context.Context, pg *pgconn.PgConn, name string) (slotState, bool, error) {
	rows, err := query(ctx, pg, slotQuery(name))
	if err != nil {
		return slotState{}, false, fmt.Errorf("read slot %q: %w", name, err)
	}
	if len(rows) == 0 {
		return slotState{}, false, nil
	}
	st, err := parseSlot(rows[0])
	if err != nil {
		return slotState{}, false, fmt.Errorf("read slot %q: %w", name, err)
	}
	return st, true, nil
}

func slotQuery(name string) string {
	return "SELECT slot_type, plugin, database, current_database(), active, active_pid, " +
		"wal_status, invalidation_reason, confirmed_flush_lsn " +
		"FROM pg_replication_slots WHERE slot_name = " + quoteLiteral(name)
}

func parseSlot(row [][]byte) (slotState, error) {
	if len(row) != 9 {
		return slotState{}, fmt.Errorf("expected 9 columns, got %d", len(row))
	}
	st := slotState{
		Type: str(row[0]), Plugin: str(row[1]), Database: str(row[2]), CurrentDB: str(row[3]),
		Active: isTrue(row[4]), ActivePID: str(row[5]),
		WALStatus: str(row[6]), InvalidationReason: str(row[7]),
	}
	if len(row[8]) > 0 {
		lsn, err := pglogrepl.ParseLSN(str(row[8]))
		if err != nil {
			return slotState{}, fmt.Errorf("confirmed_flush_lsn: %w", err)
		}
		st.ConfirmedFlush = lsn
	}
	return st, nil
}

func (s slotState) check(name string) error {
	switch {
	case s.Type != "logical":
		return fmt.Errorf("slot %q is a %s slot, not a logical one", name, s.Type)
	case s.Plugin != "pgoutput":
		return fmt.Errorf("slot %q uses plugin %q, want pgoutput", name, s.Plugin)
	case s.Database != s.CurrentDB:
		return fmt.Errorf("slot %q belongs to database %q, not %q", name, s.Database, s.CurrentDB)
	case s.WALStatus == "lost" || s.InvalidationReason != "":
		return fmt.Errorf("slot %q: %w (wal_status=%q, invalidation_reason=%q); the consumer needs a resync and walx never recreates it silently",
			name, ErrSlotLost, s.WALStatus, s.InvalidationReason)
	case s.Active:
		return fmt.Errorf("slot %q: %w (pid %s); only one walx may consume it", name, ErrSlotActive, s.ActivePID)
	}
	return nil
}
