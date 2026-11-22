package postgres

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgconn"
)

// CheckWALRetention warns when the server settings that bound the WAL a stuck
// slot can retain are unset: max_slot_wal_keep_size is -1 or
// idle_replication_slot_timeout is 0. walx never sets them. A setting the
// server does not have (idle_replication_slot_timeout before 18) is skipped.
// The caller logs the warnings.
func CheckWALRetention(ctx context.Context, pg *pgconn.PgConn) (warnings []string, err error) {
	rows, err := query(ctx, pg, "SELECT name, setting FROM pg_settings WHERE name IN ('max_slot_wal_keep_size', 'idle_replication_slot_timeout')")
	if err != nil {
		return nil, fmt.Errorf("read WAL retention settings: %w", err)
	}
	vals := map[string]int64{}
	for _, r := range rows {
		v, err := strconv.ParseInt(str(r[1]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("read WAL retention settings: %s = %q: %w", str(r[0]), str(r[1]), err)
		}
		vals[str(r[0])] = v
	}
	get := func(name string) *int64 {
		if v, ok := vals[name]; ok {
			return &v
		}
		return nil
	}
	return retentionWarnings(get("max_slot_wal_keep_size"), get("idle_replication_slot_timeout")), nil
}

// retentionWarnings turns the settings (nil when the server lacks them) into
// warnings.
func retentionWarnings(maxKeepMB, idleTimeoutSec *int64) (warnings []string) {
	if maxKeepMB != nil && *maxKeepMB == -1 {
		warnings = append(warnings, "max_slot_wal_keep_size is -1, so a stuck pipeline can fill the source disk with WAL; a value such as 100GB is recommended")
	}
	if idleTimeoutSec != nil && *idleTimeoutSec == 0 {
		warnings = append(warnings, "idle_replication_slot_timeout is 0, so an abandoned slot is never invalidated and keeps WAL forever; a value such as 30d is recommended")
	}
	return warnings
}
