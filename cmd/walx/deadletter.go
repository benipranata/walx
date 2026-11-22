package main

import (
	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/dlq"
)

// openDeadLetter opens the dead-letter file of db, or returns nil when the
// entry has no dlq.path. An unusable path is a startup error.
func openDeadLetter(db config.DatabaseConfig) (event.DeadLetter, error) {
	if db.DLQ.Path == "" {
		return nil, nil
	}
	s, err := dlq.Open(db.DLQ.Path, db.Name, db.Sink.Type)
	if err != nil {
		return nil, err
	}
	return s, nil
}
