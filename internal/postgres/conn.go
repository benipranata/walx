package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benipranata/walx/internal/config"
)

const (
	minVersionNum  = 180000
	defaultInitial = 100 * time.Millisecond
	defaultMax     = 30 * time.Second
)

// Conn is a logical replication connection (replication=database) to one
// database. It is not safe for concurrent use, like the pgconn it wraps.
type Conn struct {
	url     string
	initial time.Duration
	max     time.Duration
	onReset func()
	pg      *pgconn.PgConn
}

// New returns an unconnected Conn. The reconnect backoff uses retry.Initial
// and retry.Max, falling back to 100ms and 30s when unset. onReset, which may
// be nil, runs at the start of every Reconnect so the caller can clear its
// relation cache and discard a partial transaction.
func New(pg config.PostgresConfig, retry config.RetryConfig, onReset func()) *Conn {
	c := &Conn{url: pg.URL, initial: retry.Initial, max: retry.Max, onReset: onReset}
	if c.initial <= 0 {
		c.initial = defaultInitial
	}
	if c.max <= 0 {
		c.max = defaultMax
	}
	return c
}

// Connect dials once and returns the IdentifySystem result. It fails on
// Postgres older than 18.
func (c *Conn) Connect(ctx context.Context) (pglogrepl.IdentifySystemResult, error) {
	var zero pglogrepl.IdentifySystemResult
	cfg, err := replicationConfig(c.url)
	if err != nil {
		return zero, err
	}
	pg, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return zero, fmt.Errorf("connect: %w", err)
	}
	if err := checkVersion(pg.ParameterStatus("server_version_num")); err != nil {
		_ = pg.Close(context.WithoutCancel(ctx))
		return zero, err
	}
	sys, err := pglogrepl.IdentifySystem(ctx, pg)
	if err != nil {
		_ = pg.Close(context.WithoutCancel(ctx))
		return zero, fmt.Errorf("identify system: %w", err)
	}
	c.pg = pg
	return sys, nil
}

// Reconnect drops the current connection, calls the reset hook, then dials
// with exponential backoff (factor 2) until it succeeds or ctx is done.
func (c *Conn) Reconnect(ctx context.Context) (pglogrepl.IdentifySystemResult, error) {
	_ = c.Close(ctx)
	if c.onReset != nil {
		c.onReset()
	}
	for attempt := 0; ; attempt++ {
		sys, err := c.Connect(ctx)
		if err == nil {
			return sys, nil
		}
		if ctx.Err() != nil {
			return sys, fmt.Errorf("reconnect: %w", errors.Join(ctx.Err(), err))
		}
		t := time.NewTimer(backoff(c.initial, c.max, attempt))
		select {
		case <-ctx.Done():
			t.Stop()
			return sys, fmt.Errorf("reconnect: %w", errors.Join(ctx.Err(), err))
		case <-t.C:
		}
	}
}

// PgConn returns the underlying connection, or nil when not connected.
func (c *Conn) PgConn() *pgconn.PgConn {
	return c.pg
}

// Close closes the connection. It is safe to call when not connected.
func (c *Conn) Close(ctx context.Context) error {
	if c.pg == nil {
		return nil
	}
	pg := c.pg
	c.pg = nil
	return pg.Close(context.WithoutCancel(ctx))
}

func replicationConfig(url string) (*pgconn.Config, error) {
	cfg, err := pgconn.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	cfg.RuntimeParams["replication"] = "database"
	return cfg, nil
}

func checkVersion(num string) error {
	n, err := strconv.Atoi(num)
	if err != nil || n < minVersionNum {
		return fmt.Errorf("postgres 18 or later required, server_version_num=%q", num)
	}
	return nil
}

func backoff(initial, max time.Duration, attempt int) time.Duration {
	d := initial
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	return min(d, max)
}
