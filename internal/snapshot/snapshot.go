// Package snapshot reads the current rows of a database at the consistent
// point of a temporary replication slot and delivers them as Op=snapshot
// events through the filters and sink of its databases entry.
package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

// DefaultChunk is the number of rows read and delivered per batch.
const DefaultChunk = 1000

// Target is one table to read: "schema.table" and an optional SQL boolean
// expression limiting its rows. Where is operator input run in a read-only
// transaction, one statement only.
type Target struct {
	Table string
	Where string
}

// Config describes one snapshot run.
type Config struct {
	Name             string // databases[].name: Event.Source and ID prefix
	Postgres         config.PostgresConfig
	Retry            config.RetryConfig
	OnPermanentError string // stop | skip
	SinkTimeout      time.Duration
	Tables           []config.TableConfig // key and publication_columns of the tables
	Targets          []Target             // empty: every user table
	Chunk            int                  // rows per batch; 0 = DefaultChunk
	Sink             event.Sink           // not closed by Run
	Filter           func(event.Batch) event.Batch
	Metrics          event.Metrics
	Logger           *slog.Logger
}

// Result summarizes a finished snapshot.
type Result struct {
	ConsistentLSN event.LSN // Event.LSN and the LSN in every Event.ID
	Rows          int       // rows read
	Delivered     int       // events that passed the filters and reached the sink
}

// Run takes a snapshot and delivers it. It creates a temporary slot, imports
// its exported snapshot in a read-only transaction, drops the slot, then reads
// each target table in key order in chunks and sends every chunk as one
// batch. It never touches the persistent slot or its confirmed position. It
// returns at the first error; a rerun starts over with a new consistent point
// and so new IDs.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Sink == nil || cfg.Name == "" || cfg.Postgres.URL == "" {
		return Result{}, errors.New("snapshot: name, postgres.url and sink are required")
	}
	if cfg.Chunk <= 0 {
		cfg.Chunk = DefaultChunk
	}
	if cfg.Metrics == nil {
		cfg.Metrics = event.NopMetrics{}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("db", cfg.Name)

	ex, err := exportSnapshot(ctx, cfg.Postgres, cfg.Retry)
	if err != nil {
		return Result{}, err
	}
	rd, err := openReader(ctx, cfg.Postgres.URL, ex.snapshot, cfg.Chunk)
	// The reading transaction holds its own snapshot now, so the slot (which
	// would pin WAL) is dropped at once.
	ex.close(ctx)
	if err != nil {
		return Result{}, err
	}
	defer rd.close(ctx)
	log.Info("snapshot started", "consistent_lsn", ex.lsn.String())

	targets := cfg.Targets
	if len(targets) == 0 {
		names, err := rd.tables(ctx)
		if err != nil {
			return Result{}, err
		}
		for _, n := range names {
			targets = append(targets, Target{Table: n})
		}
	}
	d := &deliverer{name: cfg.Name, sink: cfg.Sink, retry: cfg.Retry, onPermanent: cfg.OnPermanentError,
		timeout: cfg.SinkTimeout, metrics: cfg.Metrics, log: log, sleep: sleepCtx}
	b := &builder{name: cfg.Name, lsn: ex.lsn, ts: time.Now().UTC()}
	res := Result{ConsistentLSN: ex.lsn}
	for _, t := range targets {
		plan, err := rd.plan(ctx, t.Table, t.Where, tableConfig(cfg.Tables, t.Table))
		if err != nil {
			return res, err
		}
		if len(plan.Key) == 0 {
			log.Warn("table has no key: rows are read unordered and events carry no key", "table", t.Table)
		}
		before := res.Rows
		err = rd.read(ctx, plan, func(fields []pgconn.FieldDescription, rows [][][]byte) error {
			events, err := b.build(plan, fields, rows)
			if err != nil {
				return err
			}
			res.Rows += len(events)
			batch := event.Batch{Events: events, CommitLSN: ex.lsn, EndLSN: ex.lsn}
			if cfg.Filter != nil {
				batch = cfg.Filter(batch)
			}
			if len(batch.Events) == 0 {
				return nil
			}
			if err := d.send(ctx, batch); err != nil {
				return fmt.Errorf("deliver %s: %w", t.Table, err)
			}
			res.Delivered += len(batch.Events)
			return nil
		})
		if err != nil {
			return res, err
		}
		log.Info("table read", "table", t.Table, "rows", res.Rows-before)
	}
	log.Info("snapshot finished", "consistent_lsn", ex.lsn.String(), "rows", res.Rows, "delivered", res.Delivered)
	return res, nil
}

func tableConfig(tables []config.TableConfig, name string) *config.TableConfig {
	if i := slices.IndexFunc(tables, func(t config.TableConfig) bool { return t.Name == name }); i >= 0 {
		return &tables[i]
	}
	return nil
}
