package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/benipranata/walx/internal/config"
)

// runServe parses the serve flags, loads the config and runs until SIGINT or
// SIGTERM or a pipeline fails.
func runServe(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("walx", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("config", config.DefaultPath, "path of the YAML config")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "walx: unexpected argument %q\n%s\n", fs.Arg(0), usage)
		return 2
	}
	ctx, stop := signalContext(context.Background())
	defer stop()
	return serve(ctx, *path, stderr)
}

// serve loads the config at path and runs the app until ctx ends.
func serve(ctx context.Context, path string, stderr io.Writer) int {
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintf(stderr, "walx: %v\n", err)
		return 1
	}
	log := newLogger(stderr)
	err = newApp(cfg, newRegistry(), log).run(ctx)
	if code := exitCode(err); code != 0 {
		fmt.Fprintf(stderr, "walx: %v\n", err)
		return code
	}
	log.Info("walx stopped")
	return 0
}

// exitCode maps the result of a run to the process exit code: a cancelled
// context is the requested shutdown and exits 0.
func exitCode(err error) int {
	if err == nil || errors.Is(err, context.Canceled) {
		return 0
	}
	return 1
}
