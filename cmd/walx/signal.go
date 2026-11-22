package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// signalContext returns a context cancelled by SIGINT or SIGTERM, which starts
// the graceful shutdown. The handler is released at the first signal, so a
// second one kills a stuck shutdown. Call stop to release it early.
func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	context.AfterFunc(ctx, stop)
	return ctx, stop
}
