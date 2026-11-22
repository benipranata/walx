package postgres

import "errors"

// IsFatal reports whether reconnecting cannot fix err: a fatal connect error
// (see isFatalConnect), a missing or lost slot, a missing publication, an
// unusable replica identity or a transaction over the configured limit. ErrSlotActive and network errors are
// not fatal, as a stream that just died can hold its slot for a short while.
func IsFatal(err error) bool {
	if err == nil {
		return false
	}
	var tooLarge *TxTooLargeError
	return isFatalConnect(err) || errors.Is(err, ErrSlotMissing) || errors.Is(err, ErrSlotLost) ||
		errors.Is(err, ErrPublicationMissing) || errors.Is(err, ErrReplicaIdentity) || errors.As(err, &tooLarge)
}
