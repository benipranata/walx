package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/engine"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/filter"
	"github.com/benipranata/walx/internal/postgres"
	"github.com/benipranata/walx/internal/sink"
)

// pipeline is one databases entry: its sink, replication connection and, once
// started, its engine.
type pipeline struct {
	db   config.DatabaseConfig
	log  *slog.Logger // carries "db"
	sink event.Sink
	dlq  event.DeadLetter // nil without dlq.path
	conn *postgres.Conn
	rels *postgres.Relations
	buf  *postgres.TxBuffer
	slot postgres.SlotInfo

	eng     *engine.Engine
	running bool // engine.Run owns closing the sink and the dead letter
}

// preparePipeline builds the sink and dead-letter file and runs every startup check of db, without
// starting a stream, so a bad entry fails before any pipeline streams. The
// caller releases the result with close.
func preparePipeline(ctx context.Context, db config.DatabaseConfig, reg *sink.Registry, log *slog.Logger) (*pipeline, error) {
	s, err := reg.Build(db.Name, db.Sink)
	if err != nil {
		return nil, err
	}
	p := &pipeline{db: db, log: log.With("db", db.Name), sink: s, rels: postgres.NewRelations()}
	if p.dlq, err = openDeadLetter(db); err != nil {
		p.close(ctx)
		return nil, fmt.Errorf("database %q: %w", db.Name, err)
	}
	if err := p.check(ctx); err != nil {
		p.close(ctx)
		return nil, fmt.Errorf("database %q: %w", db.Name, err)
	}
	return p, nil
}

func (p *pipeline) check(ctx context.Context) (err error) {
	db := p.db
	p.conn = postgres.New(db.Postgres, db.Retry, func() {
		p.rels.Reset()
		p.buf.Reset()
	})
	p.conn.SetLogger(p.log)
	if _, err = p.conn.Connect(ctx); err != nil {
		return err
	}
	pg := p.conn.PgConn()
	if err = postgres.EnsurePublication(ctx, pg, db); err != nil {
		return err
	}
	rep, err := postgres.EnsureReplicaIdentity(ctx, pg, db)
	if err != nil {
		return err
	}
	for _, t := range rep.Altered {
		p.log.Info("set replica identity full", "table", t)
	}
	p.warn(rep.Warnings...)
	keys, err := postgres.ReadKeys(ctx, pg, db)
	if err != nil {
		return err
	}
	warnings, err := postgres.CheckKeys(ctx, pg, db)
	if err != nil {
		return err
	}
	p.warn(warnings...)
	if warnings, err = postgres.CheckWALRetention(ctx, pg); err != nil {
		return err
	}
	p.warn(warnings...)
	// The slot comes last, after the identity change (no WAL it covers was
	// written under the old identity) and the checks, so a failed check does
	// not leave a new slot pinning WAL.
	if p.slot, err = postgres.EnsureSlot(ctx, pg, db.Postgres.Slot, db.Postgres.AutoCreate); err != nil {
		return err
	}

	p.buf = postgres.NewTxBuffer(db.Name, p.rels, keys,
		postgres.WithTxLimits(postgres.TxLimits{MaxBytes: int64(db.Postgres.MaxTxBytes), MaxEvents: int64(db.Postgres.MaxTxEvents)}),
		postgres.WithIdentityCheck(postgres.NewIdentityPolicy(db), func(w string) { p.warn(w) }),
	)
	p.log.Info("startup checks passed", "slot", db.Postgres.Slot, "publication", db.Postgres.Publication, "slot_created", p.slot.Created)
	return nil
}

// start begins streaming and builds the engine. A slot active in another
// session is fatal here, unlike on a reconnect.
func (p *pipeline) start(ctx context.Context, log *slog.Logger, m event.Metrics, shutdown time.Duration) error {
	pg := p.conn.PgConn()
	if err := postgres.StartStream(ctx, pg, p.db.Postgres.Slot, p.db.Postgres.Publication, 0); err != nil {
		return fmt.Errorf("database %q: %w", p.db.Name, err)
	}
	standby, err := postgres.NewStandby(pg, p.slot.ConfirmedFlushLSN, 0)
	if err != nil {
		return fmt.Errorf("database %q: %w", p.db.Name, err)
	}
	p.eng = engine.New(engine.Config{
		Name:             p.db.Name,
		Source:           standby,
		Buffer:           p.buf,
		Filter:           filter.New(p.db.Name, p.db.Tables, m, log).Apply,
		Sink:             p.sink,
		Retry:            p.db.Retry,
		Logger:           log,
		Metrics:          m,
		NewSource:        p.newSource,
		OnPermanentError: p.db.OnPermanentError,
		DeadLetter:       p.dlq,
		ShutdownTimeout:  shutdown,
		SinkTimeout:      p.db.Sink.Timeout,
	})
	return nil
}

// newSource is engine.Config.NewSource: it reconnects, checks the slot
// without recreating it, and resumes the stream.
func (p *pipeline) newSource(ctx context.Context, resume pglogrepl.LSN) (engine.Source, error) {
	if _, err := p.conn.Reconnect(ctx); err != nil {
		return nil, err
	}
	pg := p.conn.PgConn()
	info, err := postgres.EnsureSlot(ctx, pg, p.db.Postgres.Slot, false)
	if err != nil {
		return nil, err
	}
	if err := postgres.StartStream(ctx, pg, p.db.Postgres.Slot, p.db.Postgres.Publication, resume); err != nil {
		return nil, err
	}
	return postgres.NewStandby(pg, max(info.ConfirmedFlushLSN, resume), 0)
}

// close releases what the pipeline still owns: the connection, and the sink
// and dead letter unless the engine took them over.
func (p *pipeline) close(ctx context.Context) {
	if p.conn != nil {
		_ = p.conn.Close(ctx)
	}
	if !p.running {
		_ = p.sink.Close()
		if p.dlq != nil {
			_ = p.dlq.Close()
		}
	}
}

func (p *pipeline) warn(msgs ...string) {
	for _, m := range msgs {
		p.log.Warn(m)
	}
}
