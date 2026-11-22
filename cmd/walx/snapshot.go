package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/filter"
	"github.com/benipranata/walx/internal/sink"
	"github.com/benipranata/walx/internal/snapshot"
)

const snapshotUsage = "usage: walx snapshot [-config path] [-database name] [-table schema.table]... [-where expr] [-chunk n]"

// snapshotOptions are the parsed flags of walx snapshot.
type snapshotOptions struct {
	config, database, where string
	tables                  []string
	chunk                   int
}

// runSnapshot parses the flags, loads the config and delivers a snapshot of
// one database entry to its sink. Exit codes: 0 done, 1 runtime or config
// error (an interrupted snapshot included), 2 usage error.
func runSnapshot(args []string, _, stderr io.Writer) int {
	opts, err := parseSnapshotFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	cfg, err := config.Load(opts.config)
	if err != nil {
		fmt.Fprintf(stderr, "walx snapshot: %v\n", err)
		return 1
	}
	db, targets, err := selectSnapshot(cfg, opts)
	if err != nil {
		fmt.Fprintf(stderr, "walx snapshot: %v\n%s\n", err, snapshotUsage)
		return 2
	}
	ctx, stop := signalContext(context.Background())
	defer stop()
	if err := deliverSnapshot(ctx, db, targets, opts.chunk, newRegistry(), stderr); err != nil {
		fmt.Fprintf(stderr, "walx snapshot: %v\n", err)
		return 1
	}
	return 0
}

func parseSnapshotFlags(args []string, stderr io.Writer) (snapshotOptions, error) {
	o := snapshotOptions{}
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.config, "config", config.DefaultPath, "path of the YAML config")
	fs.StringVar(&o.database, "database", "", "name of the database entry to snapshot (optional when the config has one)")
	fs.Func("table", "schema.table to snapshot; repeat for several (default: the configured tables, else every table)", func(s string) error {
		o.tables = append(o.tables, s)
		return nil
	})
	fs.StringVar(&o.where, "where", "", "SQL boolean expression limiting the rows; needs exactly one selected table")
	fs.IntVar(&o.chunk, "chunk", snapshot.DefaultChunk, "rows per batch")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "walx snapshot: unexpected argument %q\n%s\n", fs.Arg(0), snapshotUsage)
		return o, errors.New("unexpected argument")
	}
	if o.chunk <= 0 {
		fmt.Fprintf(stderr, "walx snapshot: -chunk must be positive\n%s\n", snapshotUsage)
		return o, errors.New("invalid chunk")
	}
	return o, nil
}

// selectSnapshot picks the database entry and the tables to read. -table
// restricts the selection (to the configured tables when the entry lists
// any); without it the configured tables are read, or every table when the
// entry lists none. -where needs exactly one selected table.
func selectSnapshot(cfg *config.Config, o snapshotOptions) (config.DatabaseConfig, []snapshot.Target, error) {
	var names []string
	for _, d := range cfg.Databases {
		names = append(names, d.Name)
	}
	i := slices.IndexFunc(cfg.Databases, func(d config.DatabaseConfig) bool { return d.Name == o.database })
	switch {
	case o.database == "" && len(cfg.Databases) == 1:
		i = 0
	case o.database == "":
		return config.DatabaseConfig{}, nil, fmt.Errorf("-database is required, one of: %s", strings.Join(names, ", "))
	case i < 0:
		return config.DatabaseConfig{}, nil, fmt.Errorf("unknown database %q, one of: %s", o.database, strings.Join(names, ", "))
	}
	db := cfg.Databases[i]

	configured := make([]string, len(db.Tables))
	for j, t := range db.Tables {
		configured[j] = t.Name
	}
	selected := configured
	if len(o.tables) > 0 {
		selected = nil
		for _, t := range o.tables {
			if len(configured) > 0 && !slices.Contains(configured, t) {
				return db, nil, fmt.Errorf("table %q is not in the tables of database %q", t, db.Name)
			}
			if !slices.Contains(selected, t) {
				selected = append(selected, t)
			}
		}
	}
	if o.where != "" && len(selected) != 1 {
		return db, nil, errors.New("-where needs exactly one table: pass one -table")
	}
	targets := make([]snapshot.Target, len(selected))
	for j, t := range selected {
		targets[j] = snapshot.Target{Table: t, Where: o.where}
	}
	return db, targets, nil
}

// deliverSnapshot builds the sink of db from reg and runs the snapshot. The
// sink is closed afterwards; the filters are those of the entry.
func deliverSnapshot(ctx context.Context, db config.DatabaseConfig, targets []snapshot.Target, chunk int, reg *sink.Registry, stderr io.Writer) error {
	log := newLogger(stderr)
	s, err := reg.Build(db.Name, db.Sink)
	if err != nil {
		return err
	}
	defer s.Close()
	_, err = snapshot.Run(ctx, snapshot.Config{
		Name:             db.Name,
		Postgres:         db.Postgres,
		Retry:            db.Retry,
		OnPermanentError: db.OnPermanentError,
		SinkTimeout:      db.Sink.Timeout,
		Tables:           db.Tables,
		Targets:          targets,
		Chunk:            chunk,
		Sink:             s,
		Filter:           filter.New(db.Name, db.Tables, event.NopMetrics{}, log).Apply,
		Metrics:          event.NopMetrics{},
		Logger:           log,
	})
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("interrupted: %w", err)
	}
	return err
}
