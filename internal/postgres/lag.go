package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/benipranata/walx/internal/event"
)

// SlotLag is a slot's state as seen from the server.
type SlotLag struct {
	Bytes     int64  // pg_current_wal_lsn() minus confirmed_flush_lsn
	WALStatus string // pg_replication_slots.wal_status, empty when NULL
}

// WALStatusValue maps pg_replication_slots.wal_status to the wal_status gauge:
// reserved 0, extended 1, unreserved 2, lost 3, anything else (NULL) -1.
// Alert at 1 or 2, before the slot is lost.
func WALStatusValue(status string) float64 {
	switch status {
	case "reserved":
		return 0
	case "extended":
		return 1
	case "unreserved":
		return 2
	case "lost":
		return 3
	}
	return -1
}

// LagPoller periodically reports slot lag and wal_status to event.Metrics. It
// uses its own plain connection, since the replication connection belongs to
// Standby and pgconn is not safe for concurrent use.
type LagPoller struct {
	name     string
	slot     string
	url      string
	interval time.Duration
	metrics  event.Metrics
	log      *slog.Logger
	conn     *pgx.Conn
	read     func(ctx context.Context) (SlotLag, error)
}

// NewLagPoller returns a poller for slot on the database at url. name is the
// metrics database label; a nil log discards.
func NewLagPoller(url, name, slot string, interval time.Duration, m event.Metrics, log *slog.Logger) *LagPoller {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	p := &LagPoller{name: name, slot: slot, url: url, interval: interval, metrics: m, log: log}
	p.read = p.readSlot
	return p
}

// Run polls every interval until ctx ends, then returns ctx.Err(). Read
// errors are logged and retried on the next tick.
func (p *LagPoller) Run(ctx context.Context) error {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		if err := p.poll(ctx); err != nil && ctx.Err() == nil && p.log != nil {
			p.log.Warn("slot lag poll failed", "database", p.name, "slot", p.slot, "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Close closes the poller's connection.
func (p *LagPoller) Close(ctx context.Context) {
	if p.conn != nil {
		_ = p.conn.Close(ctx)
		p.conn = nil
	}
}

func (p *LagPoller) poll(ctx context.Context) error {
	lag, err := p.read(ctx)
	if err != nil {
		return err
	}
	p.metrics.Gauge("slot_lag_bytes", float64(lag.Bytes), "database", p.name)
	p.metrics.Gauge("wal_status", WALStatusValue(lag.WALStatus), "database", p.name)
	return nil
}

func (p *LagPoller) readSlot(ctx context.Context) (SlotLag, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if p.conn == nil {
		c, err := pgx.Connect(ctx, p.url)
		if err != nil {
			return SlotLag{}, fmt.Errorf("connect for slot lag: %w", err)
		}
		p.conn = c
	}
	var lag SlotLag
	err := p.conn.QueryRow(ctx, `SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint,
		COALESCE(wal_status, '') FROM pg_replication_slots WHERE slot_name = $1`, p.slot).Scan(&lag.Bytes, &lag.WALStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return SlotLag{}, fmt.Errorf("slot %q: %w", p.slot, ErrSlotMissing)
	}
	if err != nil {
		p.Close(ctx)
		return SlotLag{}, fmt.Errorf("read slot lag: %w", err)
	}
	return lag, nil
}
