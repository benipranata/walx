package event

// Op is the kind of change an Event describes.
type Op string

// The operations walx emits.
const (
	OpInsert   Op = "insert"
	OpUpdate   Op = "update"
	OpDelete   Op = "delete"
	OpTruncate Op = "truncate"
	OpSnapshot Op = "snapshot" // backfill rows; never a table op in config
)
