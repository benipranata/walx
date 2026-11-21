package postgres

import (
	"fmt"

	"github.com/benipranata/walx/internal/event"
)

// The values of TxTooLargeError.Limit.
const (
	LimitBytes  = "max_tx_bytes"
	LimitEvents = "max_tx_events"
)

// TxTooLargeError reports that one transaction went over postgres.max_tx_bytes
// or postgres.max_tx_events. TxBuffer returns it as is, not wrapped by
// event.Permanent: Permanent routes to on_permanent_error, and with skip that
// would silently drop the whole transaction. Find it with errors.As.
//
// The engine must stop the pipeline without retrying or confirming the
// transaction, whatever on_permanent_error says. A
// restart resumes at the same slot position and reads the same transaction
// again, so it fails again until the limit is raised (streaming of in-progress
// transactions is not supported).
type TxTooLargeError struct {
	Limit     string // LimitBytes or LimitEvents
	Max       int64  // the configured cap
	Observed  int64  // the size or count when the cap was crossed, not the total
	Xid       uint32
	CommitLSN event.LSN // the begin message's final LSN, equal to the commit LSN
}

// Error implements error.
func (e *TxTooLargeError) Error() string {
	return fmt.Sprintf("transaction %d (commit LSN %s) exceeds postgres.%s: reached %d, limit %d; a restart reads it again, so raise the limit",
		e.Xid, e.CommitLSN, e.Limit, e.Observed, e.Max)
}
