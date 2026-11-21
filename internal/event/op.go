package event

// Op is the kind of change an Event describes.
type Op string

const (
	// OpInsert is a new row.
	OpInsert Op = "insert"
	// OpUpdate is a changed row.
	OpUpdate Op = "update"
	// OpDelete is a removed row.
	OpDelete Op = "delete"
	// OpTruncate is a table truncation.
	OpTruncate Op = "truncate"
	// OpSnapshot is a backfilled row; it is never a table op in config.
	OpSnapshot Op = "snapshot"
)
