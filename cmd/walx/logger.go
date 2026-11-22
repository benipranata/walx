package main

import (
	"io"
	"log/slog"
)

// newLogger writes structured text logs to w. The engine, filter and startup
// code add the "db" attribute themselves.
func newLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
